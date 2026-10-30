//! Processing Handler — registers stream processing opcodes with Dispatcher.
//!
//! Stream processing is a Layer 2 "Intelligent Layer" that composes Layer 1
//! primitives for continuous data pipelines. The handler
//! manages job lifecycle: submit, stop, cancel, status and list.
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
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const run_id_mod = @import("../node/run_id.zig");
const stream_handler_mod = @import("../stream/handler.zig");
const kv_handler_mod = @import("../kv/handler.zig");
const ns_keys = @import("../namespace/handler.zig");
const ts_handler_mod = @import("../ts/handler.zig");
const queue_handler_mod = @import("../queue/handler.zig");
const StreamID = @import("../projection/stream.zig").StreamID;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
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

/// The first byte of a processing_submit entry. It is outside JobStatus's
/// range, where an entry from before it was added starts with the status.
const SUBMIT_LAYOUT: u8 = 0x80;

pub const ProcessingHandler = struct {
    allocator: Allocator,

    /// In-memory job store: job_id → JobRecord.
    jobs: std.StringHashMap(JobRecord),
    sink_drops: u64 = 0,
    sink_warn_ms: i64 = 0,
    chain_errors: u64 = 0,
    chain_warn_ms: i64 = 0,
    /// The job whose pipeline is ticking, so a drop or chain error names it.
    ticking_job: []const u8 = "",

    /// Per-job pipeline execution state (keyed by same job_id as jobs map).
    pipelines: std.StringHashMap(PipelineState),

    /// Cross-shard stream handlers — one per shard, wired by Runtime.
    /// Enables pipelines to read/write streams on the shard that owns them.
    peer_stream_handlers: ?[]const *stream_handler_mod.StreamHandler = null,
    peer_shard_count: u16 = 1,
    peer_partition_count: u32 = 0,

    /// KV handlers — one per shard, wired by Runtime.
    peer_kv_handlers: ?[]const *kv_handler_mod.KVHandler = null,
    /// The shard this handler runs on; kv_lookup reads only its KV.
    own_shard: u16 = 0,

    /// Cross-shard TS / queue handlers — one per shard, wired by Runtime.
    /// Enables TS and queue sinks to write to the shard that owns the target
    /// measurement / queue (matching how `ts read` / `queue dequeue` route).
    peer_ts_handlers: ?[]const *ts_handler_mod.TSHandler = null,
    peer_queue_handlers: ?[]const *queue_handler_mod.QueueHandler = null,

    /// Most jobs a server holds. Jobs are never removed (stopped ones still
    /// count) and each keeps its definition, up to 256 KiB, so a submit past
    /// it is refused. A field only so tests can reach it.
    max_jobs: usize = MAX_JOBS,
    /// Jobs whose status is failed, for /status.
    failed_jobs: u64 = 0,
    /// Running jobs this node holds without pipelines, for /status.
    held_jobs: u64 = 0,

    const MAX_JOBS: usize = 10_000;

    /// Sink configuration for a single sink in the pipeline.
    pub const SinkConfig = struct {
        kind: SinkKind,
        // Owned when not empty; freed by `freePipelineState`.
        target: []const u8 = "", // stream/queue sink: target name
        namespace: []const u8 = "", // sink namespace
        measurement: []const u8 = "", // TS sink: measurement name
        value_field: []const u8 = "", // TS sink: value field name
        ts_tag_keys: []const u8 = "", // serialized flat pairs: tag_key\0json_key\0…
        ts_field_keys: []const u8 = "", // serialized flat pairs: field_name\0json_key\0…
        required_tags: u32 = 0, // tag bitmask — 0 = firehose (receives all records)

        // KV sink options
        kv_key_prefix: []const u8 = "", // prepended to record key
        kv_separator: []const u8 = "", // between prefix and key
        kv_ttl_ms: ?u64 = null, // optional TTL in ms

        // Queue sink options
        queue_priority: u8 = 0, // message priority
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
        batch_size: u32 = 100, // max records read from the source per tick

        // Multi-sink configuration (tag-based routing)
        sinks: []SinkConfig = &.{},

        // Read cursors — track what's been processed
        ts_cursor_ns: u64, // TS source: next timestamp_ns to read from
        stream_cursor_ts: u64, // Stream source: last StreamID timestamp_ms read
        stream_cursor_seq: u64, // Stream source: last StreamID sequence read
        last_poll_ms: i64, // last time this pipeline was ticked

        // Durable-cursor checkpointing: the last cursor persisted via a
        // processing_checkpoint UAL entry, plus when. Debounces persistence so a
        // continuously-fed pipeline doesn't emit a Raft write per poll.
        persisted_cursor_ts: u64 = 0,
        persisted_cursor_seq: u64 = 0,
        last_persist_ms: i64 = 0,

        // Operator chain — instantiated from job definition
        operators: []Operator = &.{},
        operator_backings: []native_registry.CreateResult = &.{},

        // ── Live runtime metrics (ephemeral; reset on submit/restart) ──
        // Surfaced per-job on the dashboard Metrics tab (issue #27). All cheap
        // counters updated on the hot tick path; no persistence.
        /// Records emitted from the operator chain into the sink stage.
        records_out: u64 = 0,
        /// Records read on the last productive tick (vs batch_size → saturation).
        last_read_count: u32 = 0,
        /// Highest event-time watermark observed (0 = none / processing-time source).
        watermark_ms: i64 = 0,
        /// Per-tick processing time (ns) over productive ticks: last/max/sum/count.
        tick_ns_last: u64 = 0,
        tick_ns_max: u64 = 0,
        tick_ns_sum: u64 = 0,
        tick_count: u64 = 0,
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
        batch_size: u32,
        yaml_owned: []const u8,
        created_at_ms: i64,
        records_processed: u64,
        /// Why a stored job couldn't be rebuilt; empty unless it failed.
        failure_owned: []const u8 = "",
        /// This node runs no pipelines for this running job, for a cause of
        /// its own (its shard count, or memory) rather than the log's, and
        /// why ("" when that couldn't be copied). Never replicated or
        /// persisted: replay recomputes it.
        held: bool = false,
        held_owned: []const u8 = "",
        /// Each source's last saved cursor, kept with the job so one without
        /// a pipeline (stopped, or failed to rebuild) doesn't lose it.
        cursors: std.ArrayListUnmanaged(Cursor) = .empty,

        pub const Cursor = struct { source: u32, a: u64, b: u64 };
    };

    /// The most of a failure reason a job keeps.
    pub const MAX_FAILURE_REASON = 1024;
    /// A refused submit's message: the reason, the job id twice and words.
    const JOB_FAILED_MESSAGE_MAX = MAX_FAILURE_REASON + 256;

    pub fn init(allocator: Allocator) ProcessingHandler {
        return .{
            .allocator = allocator,
            .jobs = std.StringHashMap(JobRecord).init(allocator),
            .pipelines = std.StringHashMap(PipelineState).init(allocator),
        };
    }

    pub fn deinit(self: *ProcessingHandler) void {
        // Free pipeline state + multi-source pipeline keys
        var pit = self.pipelines.iterator();
        while (pit.next()) |entry| {
            self.freePipelineState(entry.value_ptr);
            self.allocator.free(entry.key_ptr.*);
        }
        self.pipelines.deinit();

        // Free all job records
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            self.freeJobRecord(entry.value_ptr);
        }
        self.jobs.deinit();
    }

    /// Wire cross-shard stream handler references.
    /// Called by Runtime after all shards are initialized.
    pub fn setPeerStreamHandlers(self: *ProcessingHandler, handlers: []const *stream_handler_mod.StreamHandler) void {
        self.peer_stream_handlers = handlers;
    }

    /// Wire cross-shard KV handler references.
    /// Called by Runtime after all shards are initialized.
    pub fn setPeerKvHandlers(self: *ProcessingHandler, handlers: []const *kv_handler_mod.KVHandler) void {
        self.peer_kv_handlers = handlers;
    }

    /// Wire cross-shard TS handler references (for TS sinks).
    pub fn setPeerTsHandlers(self: *ProcessingHandler, handlers: []const *ts_handler_mod.TSHandler) void {
        self.peer_ts_handlers = handlers;
    }

    /// Wire cross-shard queue handler references (for queue sinks).
    pub fn setPeerQueueHandlers(self: *ProcessingHandler, handlers: []const *queue_handler_mod.QueueHandler) void {
        self.peer_queue_handlers = handlers;
    }

    /// Resolve the correct StreamHandler for a given stream name + namespace.
    /// Uses hashKeyWithNamespace (namespace + key) to match the Dispatcher's
    /// stream_append routing — the Dispatcher uses preRouteByStream which
    /// hashes with hashKeyWithNamespace(namespace, key), so stream data lives
    /// on the shard selected by that composite hash.
    fn resolveStreamHandler(self: *ProcessingHandler, local: *stream_handler_mod.StreamHandler, stream_name: []const u8, namespace: []const u8) *stream_handler_mod.StreamHandler {
        const peers = self.peer_stream_handlers orelse return local;
        if (peers.len <= 1) return local;
        return peers[self.peerShardId(namespace, stream_name)];
    }

    /// Shard that owns `key` in `namespace`, using the same composite hash the
    /// dispatchers use to route stream/kv/queue/ts commands. Sinks resolve through
    /// this so a write lands on the shard a subsequent read will query.
    fn peerShardId(self: *const ProcessingHandler, namespace: []const u8, key: []const u8) u16 {
        const hash = router.hashKeyWithNamespace(namespace, key);
        const partition_id: u32 = @intCast(hash % self.peer_partition_count);
        return @intCast(partition_id % self.peer_shard_count);
    }

    /// A stream or queue sink proposes its write on the shard that owns its
    /// target, from this shard's thread, without that shard's request gate.
    /// Outside "default" that shard may never have bound the namespace, and
    /// its log would then hold a write it cannot apply. Such a sink is
    /// refused until sinks write through the shard that owns their target.
    fn crossShardSink(self: *const ProcessingHandler, kind: definition.SinkKind, namespace: []const u8, target: []const u8) bool {
        if (kind != .stream and kind != .queue) return false;
        if (std.mem.eql(u8, namespace, "default")) return false;
        if (self.peer_shard_count <= 1 or self.peer_partition_count == 0) return false;
        return self.peerShardId(namespace, target) != self.own_shard;
    }

    /// Why a job's sinks can't run here: one that `crossShardSink` refuses.
    fn sinkShardRefusal(self: *const ProcessingHandler, def: *const definition.JobDefinition, buf: []u8) ?[]const u8 {
        for (def.sinks.items) |snk| {
            const namespace = if (snk.namespace.len > 0) snk.namespace else "default";
            if (!self.crossShardSink(snk.kind, namespace, snk.target)) continue;
            return std.fmt.bufPrint(buf, "sink '{s}': a {s} sink outside the default namespace must be on the job's shard, and '{s}' is on another; use the default namespace, or one shard (--shards 1)", .{ snk.name, @tagName(snk.kind), snk.target }) catch
                "a stream or queue sink outside the default namespace must be on the job's shard; use the default namespace, or one shard (--shards 1)";
        }
        return null;
    }

    /// Resolve the TS handler that owns `measurement` in `namespace` (for TS sinks).
    fn resolveTsHandler(self: *ProcessingHandler, local: *ts_handler_mod.TSHandler, namespace: []const u8, measurement: []const u8) *ts_handler_mod.TSHandler {
        const peers = self.peer_ts_handlers orelse return local;
        if (peers.len <= 1 or self.peer_partition_count == 0 or self.peer_shard_count == 0) return local;
        const shard_id = self.peerShardId(namespace, measurement);
        return if (shard_id < peers.len) peers[shard_id] else local;
    }

    /// Resolve the queue handler that owns `queue_name` in `namespace` (for queue sinks).
    fn resolveQueueHandler(self: *ProcessingHandler, local: *queue_handler_mod.QueueHandler, namespace: []const u8, queue_name: []const u8) *queue_handler_mod.QueueHandler {
        const peers = self.peer_queue_handlers orelse return local;
        if (peers.len <= 1 or self.peer_partition_count == 0 or self.peer_shard_count == 0) return local;
        const shard_id = self.peerShardId(namespace, queue_name);
        return if (shard_id < peers.len) peers[shard_id] else local;
    }

    fn freeJobRecord(self: *ProcessingHandler, job: *JobRecord) void {
        self.allocator.free(job.job_id_owned);
        self.allocator.free(job.name_owned);
        self.allocator.free(job.namespace_owned);
        self.allocator.free(job.yaml_owned);
        if (job.failure_owned.len > 0) self.allocator.free(job.failure_owned);
        self.freeHeldReason(job);
        job.cursors.deinit(self.allocator);
    }

    fn setStatus(self: *ProcessingHandler, job: *JobRecord, status: JobStatus) void {
        if (status != .running) self.clearHeld(job);
        if (job.status == .failed) self.failed_jobs -= 1;
        if (status == .failed) self.failed_jobs += 1;
        job.status = status;
    }

    /// Note that this node holds running job `job` without pipelines, for
    /// `reason`; said once, when the hold starts. Nothing in this version
    /// lets the same node run it later: its shard count is fixed at boot,
    /// and memory short at apply is tried again only at the next replay. A
    /// leader change runs it on a node that can.
    fn holdJob(self: *ProcessingHandler, job: *JobRecord, reason: []const u8) void {
        if (!job.held) {
            job.held = true;
            self.held_jobs += 1;
            log.warn("processing job {s} held on this node: {s}", .{ job.job_id_owned, reason });
        }
        self.freeHeldReason(job);
        job.held_owned = boundedReason(self.allocator, reason) catch HELD_NO_MEMORY;
    }

    /// Why a job is held when copying its reason ran out of memory.
    const HELD_NO_MEMORY = "out of memory";

    fn freeHeldReason(self: *ProcessingHandler, job: *JobRecord) void {
        if (job.held_owned.len > 0 and job.held_owned.ptr != HELD_NO_MEMORY.ptr) self.allocator.free(job.held_owned);
        job.held_owned = "";
    }

    fn clearHeld(self: *ProcessingHandler, job: *JobRecord) void {
        if (!job.held) return;
        job.held = false;
        self.held_jobs -= 1;
        self.freeHeldReason(job);
    }

    /// Mark a job failed with `reason`, cut to MAX_FAILURE_REASON and marked.
    fn failJob(self: *ProcessingHandler, job: *JobRecord, reason: []const u8) void {
        log.err("processing job {s} not started: {s}", .{ job.job_id_owned, reason });
        self.setStatus(job, .failed);
        if (job.failure_owned.len > 0) self.allocator.free(job.failure_owned);
        job.failure_owned = boundedReason(self.allocator, reason) catch "";
    }

    fn boundedReason(allocator: Allocator, reason: []const u8) ![]const u8 {
        if (reason.len <= MAX_FAILURE_REASON) return allocator.dupe(u8, reason);
        const mark = "... (cut)";
        var end: usize = MAX_FAILURE_REASON - mark.len;
        // Never split a UTF-8 sequence.
        while (end > 0 and reason[end] & 0xC0 == 0x80) end -= 1;
        return std.mem.concat(allocator, u8, &.{ reason[0..end], mark });
    }

    fn freePipelineState(self: *ProcessingHandler, pipe: *PipelineState) void {
        // Free operator chain
        for (pipe.operator_backings) |*backing| {
            backing.deinit(self.allocator);
        }
        if (pipe.operator_backings.len > 0) self.allocator.free(pipe.operator_backings);
        if (pipe.operators.len > 0) self.allocator.free(pipe.operators);
        freePipeStateFields(self.allocator, pipe);
    }

    /// The source and sink strings `makePipeState` copies.
    fn freePipeStateFields(allocator: Allocator, pipe: *const PipelineState) void {
        if (pipe.src_measurement.len > 0) allocator.free(pipe.src_measurement);
        if (pipe.src_field.len > 0) allocator.free(pipe.src_field);
        if (pipe.src_stream.len > 0) allocator.free(pipe.src_stream);
        if (pipe.src_namespace.len > 0) allocator.free(pipe.src_namespace);
        for (pipe.sinks) |snk| {
            if (snk.target.len > 0) allocator.free(snk.target);
            if (snk.namespace.len > 0) allocator.free(snk.namespace);
            if (snk.measurement.len > 0) allocator.free(snk.measurement);
            if (snk.value_field.len > 0) allocator.free(snk.value_field);
            if (snk.ts_tag_keys.len > 0) allocator.free(snk.ts_tag_keys);
            if (snk.ts_field_keys.len > 0) allocator.free(snk.ts_field_keys);
            if (snk.kv_key_prefix.len > 0) allocator.free(snk.kv_key_prefix);
            if (snk.kv_separator.len > 0) allocator.free(snk.kv_separator);
        }
        if (pipe.sinks.len > 0) allocator.free(pipe.sinks);
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.processing_submit, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_stop, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_cancel, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_status, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.processing_list, dispatchProcessing, localScanJobs);
    }

    /// Every submit goes to shard 0, whatever its key: one shard holds the
    /// job registry, so `max_jobs` bounds the server, and each job id
    /// carries that shard back for the ops that name it.
    fn preRouteByProcessing(_: Request) ?u64 {
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
        const name_buf = dispatcher_mod.scanScratch(handler.jobs.count());

        const req_ns = if (namespace.len > 0) namespace else "default";
        var count: usize = 0;
        var it = handler.jobs.iterator();
        while (it.next()) |entry| {
            if (count >= name_buf.len) break;
            const job = entry.value_ptr;
            if (!std.mem.eql(u8, job.namespace_owned, req_ns)) continue;
            name_buf[count] = job.name_owned;
            count += 1;
        }

        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    fn dispatchProcessing(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        shard.processing_handler.handleCommand(shard, conn, req);
    }

    /// The submit applied: answered with its job's id. Counted where the
    /// client is answered.
    fn answerSubmit(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        if (shard.metrics_registry) |m| m.processing.recordSubmitted();
        var r = result.reader();
        shard.sendOkResponse(conn, request_id, r.slice() orelse "");
    }

    fn answerStop(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        if (shard.metrics_registry) |m| m.processing.recordCompleted();
        shard.sendOkResponse(conn, request_id, "");
    }

    fn answerCancel(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        if (shard.metrics_registry) |m| m.processing.recordCancelled();
        shard.sendOkResponse(conn, request_id, "");
    }

    // ── Command Routing ─────────────────────────────────────────────────

    fn handleCommand(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        switch (op) {
            .processing_submit => self.handleSubmit(shard, conn, req),
            .processing_stop => self.handleStop(shard, conn, req),
            .processing_cancel => self.handleCancel(shard, conn, req),
            .processing_status => self.handleStatus(shard, conn, req),
            .processing_list => self.handleList(shard, conn, req),
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
        // The applier refuses too, so submits in flight together can't
        // pass the limit; this spares the log an entry that changes nothing.
        if (self.jobs.count() >= self.max_jobs) return self.refuseAtJobLimit(shard, conn, req.header.request_id);

        // Parse the YAML/JSON definition, using the request namespace as fallback
        const req_ns: ?[]const u8 = if (req.namespace.len > 0) req.namespace else null;
        var diag: parser.Diagnostic = .{};
        var def = parser.parseJobDefinitionWithNamespace(self.allocator, yaml, req_ns, &diag) catch |err| {
            if (err == error.OutOfMemory) {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "out of memory");
            } else {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, diag.message());
            }
            return;
        };
        defer def.deinit(self.allocator);
        if (def.namespaceRefusal()) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return;
        }
        const home = if (req.namespace.len > 0) req.namespace else "default";
        var why_buf: [256]u8 = undefined;
        if (def.homeRefusal(home, &why_buf)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return;
        }
        if (def.shardRefusal(self.peer_shard_count)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return;
        }
        if (self.sinkShardRefusal(&def, &why_buf)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return;
        }
        if (self.operatorRefusal(&def)) |refusal| switch (refusal) {
            .invalid => |name| {
                var buf: [192]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "operator '{s}' has a missing or invalid setting", .{name}) catch "an operator has a missing or invalid setting";
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, msg);
                return;
            },
            .out_of_memory => {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "out of memory");
                return;
            },
        };

        // Generate a unique job ID with embedded partition bits.
        //
        // The partition MUST route back to the shard that stores this job's
        // registry entry — i.e. THIS shard. Submit is routed here by
        // preRouteByProcessing (every submit → partition 0 → shard 0), so the
        // entry lives in this shard's `self.jobs`. The job_id's partition bits
        // are how every ID-routed op (status/stop/cancel) finds it again:
        // partitionToShard = partition % shard_count.
        //
        // Deriving the partition from the job *name* (keyToPartitionNs) was
        // wrong: it resolves to an arbitrary partition whose shard differs from
        // the storing shard whenever keyToPartitionNs(ns,name) % shard_count
        // != shard.id, yielding spurious "Job not found" on any multi-shard
        // deployment. It only worked by luck when the name happened to hash to
        // a partition congruent to the storing shard (e.g. 1152 % 3 == 0).
        //
        // Embedding shard.id is correct and future-proof: shard.id < shard_count
        // <= partition_count, and shard.id % shard_count == shard.id, so the ID
        // always routes back to the shard that created and stores the job.
        const partition_id: u32 = shard.id;
        var id_buf: [32]u8 = undefined;
        const job_id = shard.run_id_gen.next(.job, partition_id, &id_buf);

        // Persist through Raft; the applier builds the job record and its
        // pipelines from the entry — the same applier a restart uses.
        // Admitted at dispatch; the definition has now passed, so reserve.
        shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
        const proposed = self.proposeSubmit(shard, req.namespace, job_id, .running, def.batch_size, home, yaml) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "job not persisted"));
            return;
        };
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        shard.park(conn, req, proposed, answerSubmit);
    }

    fn refuseAtJobLimit(self: *const ProcessingHandler, shard: *Shard, conn: *Connection, request_id: u64) void {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "job not submitted: this server holds at most {d} processing jobs", .{self.max_jobs}) catch "job not submitted: this server holds as many processing jobs as it may";
        shard.sendRefusal(conn, request_id, .overloaded, .{ .reason = .unclassified, .ran = .no, .message = msg });
    }

    // ── Stop ─────────────────────────────────────────────────────────────

    /// The job `job_id`, if it lives in `namespace` (empty is "default"):
    /// a job is seen, stopped and changed only from its own namespace, so
    /// from any other it is not found.
    fn ownedJob(self: *ProcessingHandler, job_id: []const u8, namespace: []const u8) ?*JobRecord {
        const job = self.jobs.getPtr(job_id) orelse return null;
        const home = if (namespace.len > 0) namespace else "default";
        return if (std.mem.eql(u8, job.namespace_owned, home)) job else null;
    }

    fn handleStop(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.ownedJob(job_id, req.namespace) != null) {
            const proposed = self.proposeStatusChange(shard, req.namespace, job_id, .stopped) catch |err| {
                shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "stop not persisted"));
                return;
            };
            shard.park(conn, req, proposed, answerStop);
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

        if (self.ownedJob(job_id, req.namespace) != null) {
            const proposed = self.proposeStatusChange(shard, req.namespace, job_id, .cancelled) catch |err| {
                shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "cancel not persisted"));
                return;
            };
            shard.park(conn, req, proposed, answerCancel);
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

        if (self.ownedJob(job_id, req.namespace)) |job| {
            // Binary wire format:
            // [job_id_len:u16][job_id][name_len:u16][name][status:u8]
            // [batch_size:u32][records_processed:u64][created_at:i64]
            // [reason_len:u16][reason] — a failed job's reason, "held here:
            // <why>" for a running one this node holds, or empty.
            // Sized to the job. The id is 32 bytes and the name came from a
            // submit entry under 64 KiB, so both lengths fit u16.
            const note = noteOf(job);
            const size = 2 + job.job_id_owned.len + 2 + job.name_owned.len + 1 + 4 + 8 + 8 + 2 + note.reasonLen();
            const buf = self.allocator.alloc(u8, size) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "out of memory");
                return;
            };
            defer self.allocator.free(buf);
            var fbs: std.Io.Writer = .fixed(buf);
            const w = &fbs;
            w.writeInt(u16, @intCast(job.job_id_owned.len), .little) catch unreachable;
            w.writeAll(job.job_id_owned) catch unreachable;
            w.writeInt(u16, @intCast(job.name_owned.len), .little) catch unreachable;
            w.writeAll(job.name_owned) catch unreachable;
            w.writeByte(@backingInt(job.status)) catch unreachable;
            w.writeInt(u32, job.batch_size, .little) catch unreachable;
            w.writeInt(u64, job.records_processed, .little) catch unreachable;
            w.writeInt(i64, job.created_at_ms, .little) catch unreachable;
            w.writeInt(u16, @intCast(note.reasonLen()), .little) catch unreachable;
            note.writeReason(w);
            shard.sendOkResponse(conn, req.header.request_id, fbs.buffered());
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── List ─────────────────────────────────────────────────────────────

    fn handleList(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        // [limit:u32][cursor...]?
        var limit: u32 = 100;
        if (req.value.len >= 4) limit = std.mem.readInt(u32, req.value[0..4], .little);
        if (limit == 0) limit = 100;
        const req_ns = if (req.namespace.len > 0) req.namespace else "default";

        var jobs: std.ArrayListUnmanaged(*const JobRecord) = .empty;
        defer jobs.deinit(self.allocator);
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            if (jobs.items.len >= limit) break;
            if (!std.mem.eql(u8, entry.value_ptr.namespace_owned, req_ns)) continue;
            jobs.append(self.allocator, entry.value_ptr) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }
        const buf = encodeJobList(self.allocator, jobs.items, null) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);
        shard.sendOkResponse(conn, req.header.request_id, buf);
    }

    /// A walk's list answer: each named job in `namespace`, from every
    /// shard's handler in `contexts`.
    pub fn serializeWalkJobs(allocator: Allocator, names: []const []const u8, next_cursor: ?[]const u8, contexts: []const *anyopaque, namespace: []const u8) ![]u8 {
        const req_ns = if (namespace.len > 0) namespace else "default";
        var jobs: std.ArrayListUnmanaged(*const JobRecord) = .empty;
        defer jobs.deinit(allocator);
        for (names) |name| {
            for (contexts) |ctx| {
                const handler: *ProcessingHandler = @ptrCast(@alignCast(ctx));
                var it = handler.jobs.iterator();
                while (it.next()) |entry| {
                    const job = entry.value_ptr;
                    if (std.mem.eql(u8, job.name_owned, name) and std.mem.eql(u8, job.namespace_owned, req_ns)) {
                        try jobs.append(allocator, job);
                        break;
                    }
                }
            }
        }
        return encodeJobList(allocator, jobs.items, next_cursor);
    }

    /// The list answer, for a single shard and a walk alike:
    /// [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
    ///  [status_len:u16][status][created_at:i64])*[has_more:u8][cursor_len:u16][cursor]
    /// A failed job's status reads `FAILED: <reason>`. Ids are 32 bytes,
    /// names came from a submit entry under 64 KiB and reasons are bounded,
    /// so every length fits u16.
    fn encodeJobList(allocator: Allocator, jobs: []const *const JobRecord, next_cursor: ?[]const u8) ![]u8 {
        const cursor = next_cursor orelse "";
        var total: usize = 4 + 1 + 2 + cursor.len;
        for (jobs) |j| total += 2 + j.name_owned.len + 2 + j.job_id_owned.len + 2 + statusLen(j) + 8;
        const buf = try allocator.alloc(u8, total);
        var w: std.Io.Writer = .fixed(buf);
        w.writeInt(u32, @intCast(jobs.len), .little) catch unreachable;
        for (jobs) |j| {
            w.writeInt(u16, @intCast(j.name_owned.len), .little) catch unreachable;
            w.writeAll(j.name_owned) catch unreachable;
            w.writeInt(u16, @intCast(j.job_id_owned.len), .little) catch unreachable;
            w.writeAll(j.job_id_owned) catch unreachable;
            w.writeInt(u16, @intCast(statusLen(j)), .little) catch unreachable;
            writeStatus(j, &w);
            w.writeInt(i64, j.created_at_ms, .little) catch unreachable;
        }
        w.writeByte(@intFromBool(next_cursor != null)) catch unreachable;
        w.writeInt(u16, @intCast(cursor.len), .little) catch unreachable;
        w.writeAll(cursor) catch unreachable;
        return buf;
    }

    /// What status and list say beside a job's status: a failed job's
    /// reason, or why this node holds a running one.
    fn noteOf(job: *const JobRecord) Note {
        if (job.status == .failed) return .{ .failed = job.failure_owned };
        if (job.status == .running and job.held) return .{ .held = job.held_owned };
        return .none;
    }

    const Note = union(enum) {
        none,
        failed: []const u8,
        held: []const u8,

        /// The status answer's reason: as it is, or "held here: <why>".
        fn reasonLen(n: Note) usize {
            return switch (n) {
                .none => 0,
                .failed => |r| r.len,
                .held => |r| HELD.len + r.len,
            };
        }
        fn writeReason(n: Note, w: *std.Io.Writer) void {
            switch (n) {
                .none => {},
                .failed => |r| w.writeAll(r) catch unreachable,
                .held => |r| {
                    w.writeAll(HELD) catch unreachable;
                    w.writeAll(r) catch unreachable;
                },
            }
        }
        const HELD = "held here: ";
    };

    /// The list's status text: "FAILED: <why>", "RUNNING (held here: <why>)",
    /// or the status alone.
    fn statusLen(job: *const JobRecord) usize {
        const note = noteOf(job);
        return job.status.toString().len + switch (note) {
            .none => 0,
            .failed => |r| 2 + r.len,
            .held => 2 + note.reasonLen() + 1,
        };
    }

    fn writeStatus(job: *const JobRecord, w: *std.Io.Writer) void {
        w.writeAll(job.status.toString()) catch unreachable;
        const note = noteOf(job);
        switch (note) {
            .none => {},
            .failed => |r| {
                w.writeAll(": ") catch unreachable;
                w.writeAll(r) catch unreachable;
            },
            .held => {
                w.writeAll(" (") catch unreachable;
                note.writeReason(w);
                w.writeAll(")") catch unreachable;
            },
        }
    }

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// A processing_submit entry. Key = job_id. The job is created at the
    /// entry's stamp.
    /// Value format: [layout:u8][status:u8][batch_size:u32][ns_len:u16][namespace][yaml...]
    fn proposeSubmit(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, job_id: []const u8, status: JobStatus, batch_size: u32, job_namespace: []const u8, yaml: []const u8) !persistence_mod.ProposeResult {
        _ = self;
        var value_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
        var off: usize = 0;

        value_buf[off] = SUBMIT_LAYOUT;
        off += 1;
        value_buf[off] = @backingInt(status);
        off += 1;
        std.mem.writeInt(u32, value_buf[off..][0..4], batch_size, .little);
        off += 4;

        // Embed the effective namespace so the applier does not depend on
        // re-parsing quirks.
        const ns = job_namespace;
        // A request's namespace, whose length is a u16 on the wire.
        const ns_len: u16 = @intCast(ns.len);
        std.mem.writeInt(u16, value_buf[off..][0..2], ns_len, .little);
        off += 2;
        if (off + ns.len + yaml.len > value_buf.len) return error.PayloadTooLarge;
        @memcpy(value_buf[off .. off + ns.len], ns);
        off += ns.len;
        @memcpy(value_buf[off .. off + yaml.len], yaml);
        off += yaml.len;

        return persistence_mod.proposeEntry(shard, .processing_submit, Flags.NONE, namespace, job_id, value_buf[0..off]);
    }

    /// A processing_stop or processing_cancel entry. Key = job_id, value = [new_status:u8].
    fn proposeStatusChange(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, job_id: []const u8, status: JobStatus) !persistence_mod.ProposeResult {
        _ = self;
        const entry_type: EntryType = switch (status) {
            .stopped => .processing_stop,
            .cancelled => .processing_cancel,
            else => return error.NotATerminalStatus,
        };
        const value = &[_]u8{@backingInt(status)};
        return persistence_mod.proposeEntry(shard, entry_type, Flags.NONE, namespace, job_id, value);
    }

    // ── Replay ──────────────────────────────────────────────────────────

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *ProcessingHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.processing_submit, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_stop, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_cancel, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_checkpoint, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const self: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        return self.replayEntry(entry, out);
    }

    /// Apply a processing entry (live commit and boot replay alike). A
    /// submit is answered with its job's id.
    pub fn replayEntry(self: *ProcessingHandler, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const etype: EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return error.Halt;
        const stamp_ms: i64 = @intCast(entry.header.timestamp_ns / std.time.ns_per_ms);

        return switch (etype) {
            .processing_submit => self.replaySubmit(cmd.key, cmd.value, stamp_ms, out),
            .processing_stop => self.replayStatusChange(cmd.key, .stopped),
            .processing_cancel => self.replayStatusChange(cmd.key, .cancelled),
            .processing_checkpoint => self.replayCheckpoint(cmd.key, cmd.value),
            else => error.Halt,
        };
    }

    /// Replay a processing_submit entry. Rebuilds JobRecord from persisted bytes.
    /// Value format: [layout:u8][status:u8][batch_size:u32][ns_len:u16][namespace][yaml...]
    fn replaySubmit(self: *ProcessingHandler, key: []const u8, value: []const u8, created_at_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        const job_id = key;
        if (value.len == 0 or value[0] != SUBMIT_LAYOUT) {
            log.err("processing job {s} not started: its submit entry is from an older server, with a parallelism the server no longer takes; submit the job again", .{job_id});
            return out.refuse(.bad_request, .unclassified, "job not started: its submit is from an older server; submit the job again");
        }
        if (value.len < 8) return error.Halt; // 1+1+4+2 minimum

        var off: usize = 1;
        const status = std.enums.fromInt(JobStatus, value[off]) orelse return error.Halt;
        off += 1;
        const batch_size = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;

        // Read embedded namespace
        const ns_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + ns_len > value.len) return error.Halt;
        const ns_raw = value[off .. off + ns_len];
        off += ns_len;

        const yaml = value[off..];

        // Refused in log order, so every replica and restart refuses the
        // same submits. A job already held is a replay of itself.
        if (!self.jobs.contains(job_id) and self.jobs.count() >= self.max_jobs) {
            log.warn("processing job {s} not created: this server holds at most {d} jobs", .{ job_id, self.max_jobs });
            return out.refuseFmt(.overloaded, .unclassified, "job not submitted: this server holds at most {d} processing jobs", .{self.max_jobs});
        }

        // Re-parse with the job's namespace as fallback, as the submit did, so
        // endpoints and lookups that name none resolve alike on every replica
        // and restart. `def` stays alive for createPipeline, which deep-copies
        // what it keeps. A stored job that no longer parses, or that the
        // server now refuses, is kept as failed with its definition and
        // cursors; only running out of memory here drops it.
        var diag: parser.Diagnostic = .{};
        var why_buf: [256]u8 = undefined;
        var failure: ?[]const u8 = null;
        var parsed: ?definition.JobDefinition = parser.parseJobDefinitionWithNamespace(self.allocator, yaml, ns_raw, &diag) catch |err| blk: {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            failure = diag.message();
            break :blk null;
        };
        defer if (parsed) |*def| def.deinit(self.allocator);
        if (parsed) |*def| {
            // The submit checked these; the applier holds every replica to them.
            failure = def.namespaceRefusal() orelse def.homeRefusal(ns_raw, &why_buf);
        }

        const name = self.allocator.dupe(u8, if (parsed) |def| def.name else job_id) catch return error.OutOfMemory;
        // Use the embedded namespace (authoritative from original submit)
        const namespace = self.allocator.dupe(u8, ns_raw) catch {
            self.allocator.free(name);
            return error.OutOfMemory;
        };

        const owned_id = self.allocator.dupe(u8, job_id) catch {
            self.allocator.free(name);
            self.allocator.free(namespace);
            return error.OutOfMemory;
        };
        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(namespace);
            return error.OutOfMemory;
        };

        // Replace an existing record (idempotent replay), keeping its cursors.
        var cursors: std.ArrayListUnmanaged(JobRecord.Cursor) = .empty;
        if (self.jobs.fetchRemove(owned_id)) |old| {
            var rec = old.value;
            if (rec.status == .failed) self.failed_jobs -= 1;
            if (rec.held) self.held_jobs -= 1;
            cursors = rec.cursors;
            rec.cursors = .empty;
            self.freeJobRecord(&rec);
        }

        self.jobs.put(owned_id, .{
            .job_id_owned = owned_id,
            .name_owned = name,
            .namespace_owned = namespace,
            .status = status,
            .batch_size = batch_size,
            .yaml_owned = owned_yaml,
            .created_at_ms = created_at_ms,
            .records_processed = 0,
            .cursors = cursors,
        }) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(namespace);
            self.allocator.free(owned_yaml);
            cursors.deinit(self.allocator);
            return error.OutOfMemory;
        };
        if (status == .failed) self.failed_jobs += 1;
        const job = self.jobs.getPtr(owned_id).?;

        if (failure) |why| {
            self.failJob(job, why);
            // The job is stored, and keeps its slot: jobs are never removed.
            return refuseStoredFailure(out, owned_id, job.failure_owned);
        }

        // Rebuild the execution pipeline so a RUNNING job resumes consuming
        // after restart instead of just reappearing in the registry.
        // Guard against a double build on idempotent replay. Its cursors
        // come from the record, and from replayCheckpoint for checkpoints
        // after this submit in log order.
        if (status == .running and self.pipelines.getPtr(owned_id) == null) {
            if (self.startPipelines(owned_id, &parsed.?, &why_buf)) |refusal| switch (refusal) {
                .failed => |why| self.failJob(job, why),
                .held => |why| self.holdJob(job, why),
            };
        }
        if (job.status == .failed) return refuseStoredFailure(out, owned_id, job.failure_owned);
        out.putBytes(owned_id);
    }

    /// A submit whose job applied as failed: refused, but the job is stored
    /// and keeps its slot, so it ran.
    fn refuseStoredFailure(out: *persistence_mod.Result, job_id: []const u8, why: []const u8) void {
        var buf: [persistence_mod.Result.MESSAGE_MAX]u8 = undefined;
        const message = std.fmt.bufPrint(&buf, "processing job {s} failed: {s}; cancel it with `flo processing cancel {s}`", .{ job_id, why, job_id }) catch buf[0..];
        out.refuseRan(.bad_request, .unclassified, .yes, message);
    }

    /// Replay a status change (stop/cancel).
    fn replayStatusChange(self: *ProcessingHandler, key: []const u8, status: JobStatus) persistence_mod.ApplyError!void {
        // Jobs are never removed: a change names one that was submitted.
        const job = self.jobs.getPtr(key) orelse return error.Halt;
        self.setStatus(job, status);
    }

    /// Replay a processing_checkpoint entry: advance the pipeline's source
    /// cursor, if it has one, and the cursor its job keeps, so it resumes
    /// where it left off instead of from the start.
    /// Key = pipeline_key (job_id, or job_id\0idx); value = [a:u64 LE][b:u64 LE].
    fn replayCheckpoint(self: *ProcessingHandler, key: []const u8, value: []const u8) persistence_mod.ApplyError!void {
        if (value.len < 16) return error.Halt;
        const a = std.mem.readInt(u64, value[0..8], .little);
        const b = std.mem.readInt(u64, value[8..16], .little);
        // A pipeline stopped since its checkpoint was proposed has no
        // cursor left to move.
        if (self.pipelines.getPtr(key)) |pipe| {
            applyCursor(pipe, a, b);
            pipe.persisted_cursor_ts = a;
            pipe.persisted_cursor_seq = b;
        }
        // The job keeps it too, for a pipeline built later.
        const sep = std.mem.indexOfScalar(u8, key, 0);
        const job = self.jobs.getPtr(key[0 .. sep orelse key.len]) orelse return;
        const source: u32 = if (sep) |i| std.fmt.parseInt(u32, key[i + 1 ..], 10) catch return else 0;
        for (job.cursors.items) |*c| {
            if (c.source != source) continue;
            if (a > c.a or (a == c.a and b > c.b)) {
                c.a = a;
                c.b = b;
            }
            return;
        }
        try job.cursors.append(self.allocator, .{ .source = source, .a = a, .b = b });
    }

    /// Move a pipeline's read cursor to `(a, b)`, forward only: this is also
    /// the live applier, and a leader may have read further by the time its
    /// checkpoint commits. `(a, b)` is a stream source's (timestamp,
    /// sequence), or a TS source's next timestamp and 0.
    fn applyCursor(pipe: *PipelineState, a: u64, b: u64) void {
        switch (pipe.source_kind) {
            .stream => if (a > pipe.stream_cursor_ts or (a == pipe.stream_cursor_ts and b > pipe.stream_cursor_seq)) {
                pipe.stream_cursor_ts = a;
                pipe.stream_cursor_seq = b;
            },
            .ts => pipe.ts_cursor_ns = @max(pipe.ts_cursor_ns, a),
        }
    }

    // ── Pipeline Execution ──────────────────────────────────────────────

    /// Build the tag registry + execution pipelines for a parsed job definition
    /// and register them under `job_id`. Shared by live submit and replay so a
    /// RUNNING job actually resumes consuming after restart, not just a registry
    /// entry. Does not take ownership of `def` — createPipeline
    /// deep-copies everything it retains, so the caller may free `def` after.
    /// Returns why the job can't run, written into `why_buf` if need be:
    /// `failed` when every replica finds the same from the entry, `held`
    /// when the cause is this node's own.
    fn startPipelines(self: *ProcessingHandler, job_id: []const u8, def: *definition.JobDefinition, why_buf: []u8) ?StartRefusal {
        if (def.sinks.items.len == 0) return null;
        // The shard count is this node's: another replica may run it.
        if (def.shardRefusal(self.peer_shard_count)) |why| return .{ .held = why };
        if (self.sinkShardRefusal(def, why_buf)) |why| return .{ .held = why };

        // The job's tags, from its sinks and classify operators: read while
        // operators are built (classify keeps only tag bits), so no pipeline
        // keeps it.
        var tags = definition.TagRegistry{};
        const tag_registry = &tags;
        fillTagRegistry(tag_registry, def);

        // Resolve required_tags bitmask for each sink.
        for (def.sinks.items) |*snk| {
            if (snk.match) |tag_list| {
                snk.required_tags = tag_registry.buildMask(tag_list);
            }
        }

        // A pipeline without one of its operators would run a different job
        // (a dropped filter passes everything), so a job any of whose
        // operators can't be built doesn't start at all.
        if (self.operatorRefusal(def)) |refusal| return switch (refusal) {
            .invalid => |name| .{ .failed = std.fmt.bufPrint(why_buf, "operator '{s}' can't be built", .{name}) catch "an operator can't be built" },
            .out_of_memory => .{ .held = "an operator couldn't be built: out of memory" },
        };

        // Create pipeline execution state. For multi-source jobs, one pipeline
        // per source (all sinks per pipeline).
        // Each pipeline owns its key: the job's id for the first source, a
        // suffixed one for each more. Not the job record's slice, which a
        // repeated submit replaces.
        // A source whose pipeline already runs (a submit applied again
        // after part of it started) keeps it.
        // A job runs whole or not at all here: one source's pipeline that
        // can't be built takes the rest down with it.
        for (def.sources.items, 0..) |*src, idx| {
            const owned_key = (if (idx == 0) self.allocator.dupe(u8, job_id) else std.fmt.allocPrint(self.allocator, "{s}\x00{d}", .{ job_id, idx })) catch {
                self.dropPipelines(job_id);
                return .{ .held = "a pipeline couldn't be built: out of memory" };
            };
            if (self.pipelines.contains(owned_key)) {
                self.allocator.free(owned_key);
                continue;
            }
            if (!self.createPipeline(owned_key, src, def.sinks.items, def, tag_registry)) {
                self.dropPipelines(job_id);
                return .{ .held = "a pipeline couldn't be built: out of memory" };
            }
        }

        // Pick up where the job's kept cursors say.
        const job = self.jobs.getPtr(job_id) orelse return null;
        var it = self.pipelines.iterator();
        while (it.next()) |e| {
            const source = pipelineSource(e.key_ptr.*, job_id) orelse continue;
            for (job.cursors.items) |c| if (c.source == source) {
                applyCursor(e.value_ptr, c.a, c.b);
                e.value_ptr.persisted_cursor_ts = c.a;
                e.value_ptr.persisted_cursor_seq = c.b;
            };
        }
        return null;
    }

    const StartRefusal = union(enum) { failed: []const u8, held: []const u8 };

    /// Remove whatever pipelines job `job_id`'s `sources` have here.
    fn dropPipelines(self: *ProcessingHandler, job_id: []const u8) void {
        // Removing invalidates the iterator: look again after each one.
        outer: while (true) {
            var it = self.pipelines.iterator();
            while (it.next()) |e| {
                if (pipelineSource(e.key_ptr.*, job_id) == null) continue;
                const key = e.key_ptr.*;
                self.freePipelineState(e.value_ptr);
                self.pipelines.removeByPtr(e.key_ptr);
                self.allocator.free(key);
                continue :outer;
            }
            return;
        }
    }

    /// The source a pipeline key names, if the key is one of `job_id`'s:
    /// the job id for source 0, else the id, a NUL and the source's index.
    fn pipelineSource(key: []const u8, job_id: []const u8) ?u32 {
        if (!std.mem.startsWith(u8, key, job_id)) return null;
        if (key.len == job_id.len) return 0;
        if (key[job_id.len] != 0) return null;
        return std.fmt.parseInt(u32, key[job_id.len + 1 ..], 10) catch null;
    }

    /// The first operator that can't be built, by name: checked at submit,
    /// and again before a job's pipelines start, so a job never runs with an
    /// operator missing.
    fn operatorRefusal(self: *ProcessingHandler, def: *const definition.JobDefinition) ?OperatorRefusal {
        var tags = definition.TagRegistry{};
        fillTagRegistry(&tags, def);
        for (def.operators.items) |*spec| {
            if (!native_registry.isNativeType(spec.type_name)) return .{ .invalid = spec.name };
            const built = native_registry.create(self.allocator, spec, &tags) catch |err| return if (err == error.OutOfMemory) .out_of_memory else .{ .invalid = spec.name };
            built.deinit(self.allocator);
        }
        return null;
    }

    /// Why an operator wasn't built: its setting, by operator name, or memory.
    const OperatorRefusal = union(enum) { invalid: []const u8, out_of_memory };

    /// Every tag the job's sinks match on and its classify operators assign.
    fn fillTagRegistry(tags: *definition.TagRegistry, def: *const definition.JobDefinition) void {
        for (def.sinks.items) |snk| {
            if (snk.match) |tag_list| {
                for (tag_list) |tag| _ = tags.getOrCreate(tag);
            }
        }
        for (def.operators.items) |op_spec| {
            if (!std.mem.eql(u8, op_spec.type_name, "classify")) continue;
            const entries = op_spec.config orelse continue;
            for (entries) |entry| {
                if (std.mem.startsWith(u8, entry.key, "tag_") or std.mem.eql(u8, entry.key, "default_tag")) {
                    _ = tags.getOrCreate(entry.value);
                }
            }
        }
    }

    /// False when it couldn't be built; its key is freed either way it fails.
    fn createPipeline(self: *ProcessingHandler, job_id: []const u8, src: *const definition.SourceSpec, sinks: []const definition.SinkSpec, def: *const definition.JobDefinition, tags: *const definition.TagRegistry) bool {
        // A pipeline not built leaves its key, which it owns
        // (`startPipelines`), behind for no one: freed.
        var built = false;
        defer if (!built) self.allocator.free(job_id);

        // Canonical tag hash for TS source selection — must match the write
        // path exactly (ts_mod.canonicalTagHash), or the series won't be found.
        const tag_hash: u64 = if (src.ts_tags.len >= 2) blk: {
            var pairs_buf: [32]ts_mod.TagPair = undefined;
            var n: usize = 0;
            var i: usize = 0;
            while (i + 1 < src.ts_tags.len and n < pairs_buf.len) : (i += 2) {
                pairs_buf[n] = .{ .key = src.ts_tags[i], .value = src.ts_tags[i + 1] };
                n += 1;
            }
            var tag_buf: [1024]u8 = undefined;
            break :blk if (n > 0) ts_mod.canonicalTagHashPairs(pairs_buf[0..n], &tag_buf) else 0;
        } else 0;

        // Instantiate operator chain from definition
        const op_specs = def.operators.items;
        // Never fall back to a pipeline without its operators: that runs a
        // different job.
        var ops = self.allocator.alloc(Operator, op_specs.len) catch {
            log.err("processing job {s}: pipeline not started (out of memory)", .{job_id});
            return false;
        };
        var backings = self.allocator.alloc(native_registry.CreateResult, op_specs.len) catch {
            self.allocator.free(ops);
            log.err("processing job {s}: pipeline not started (out of memory)", .{job_id});
            return false;
        };

        var count: usize = 0;
        log.debug("createPipeline: {d} operator specs to process", .{op_specs.len});
        for (op_specs) |*spec| {
            log.debug("createPipeline: spec type='{s}' name='{s}' has_config={}", .{ spec.type_name, spec.name, spec.config != null });
            if (spec.config) |cfg| {
                for (cfg) |entry| {
                    log.debug("createPipeline:   config: {s} = {s}", .{ entry.key, entry.value });
                }
            }
            const result = native_registry.create(self.allocator, spec, tags) catch |err| {
                log.err("processing job {s}: pipeline not started: operator '{s}' (type {s}) can't be built: {}", .{ job_id, spec.name, spec.type_name, err });
                for (backings[0..count]) |*b| b.deinit(self.allocator);
                self.allocator.free(backings);
                self.allocator.free(ops);
                return false;
            };
            if (result.backing == .kv_lookup) {
                result.backing.kv_lookup.setLookupFn(shardKvLookup, @ptrCast(self));
            }
            backings[count] = result;
            ops[count] = result.op;
            count += 1;
        }

        var pipe_state = makePipeState(self.allocator, src, sinks, tag_hash) catch {
            for (backings[0..count]) |*b| b.deinit(self.allocator);
            self.allocator.free(backings);
            self.allocator.free(ops);
            log.err("processing job {s}: pipeline not started (out of memory)", .{job_id});
            return false;
        };
        built = true;
        log.debug("createPipeline: {d} operators loaded (out of {d} specs)", .{ count, op_specs.len });
        if (count > 0) {
            pipe_state.operators = ops[0..count];
            pipe_state.operator_backings = backings[0..count];
        } else {
            self.allocator.free(ops);
            self.allocator.free(backings);
        }
        self.pipelines.put(job_id, pipe_state) catch {
            self.freePipelineState(&pipe_state);
            self.allocator.free(job_id);
            return false;
        };
        return true;
    }

    /// KV lookup callback passed to KvLookupOperator. It reads this shard's
    /// KV on this shard's thread. A job with a lookup is refused on a server
    /// of more than one shard, so this KV is all there is.
    fn shardKvLookup(ctx: *anyopaque, namespace: []const u8, key: []const u8) ?[]const u8 {
        const self: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        const handlers = self.peer_kv_handlers orelse return null;
        if (self.own_shard >= handlers.len) return null;
        const ns_handler = @import("../namespace/handler.zig");
        var qbuf: [ns_handler.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = ns_handler.qualifyKey(&qbuf, namespace, key) catch return null;
        const entry = handlers[self.own_shard].*.kv.get(qkey) orelse return null;
        return entry.value;
    }

    /// `{"measurement":<name>,"value":<v>,"timestamp_ms":<ms>}`, the name
    /// escaped as a JSON string and a non-finite value as null, in an
    /// allocation sized to it.
    fn tsPointJson(allocator: Allocator, measurement: []const u8, value: f64, ts_ms: i64) ![]u8 {
        const v: ?f64 = if (std.math.isFinite(value)) value else null;
        return std.json.Stringify.valueAlloc(allocator, .{ .measurement = measurement, .value = v, .timestamp_ms = ts_ms }, .{});
    }

    /// A PipelineState with source/sink config (no operators). Out of
    /// memory fails the build rather than leave a field empty: a sink with
    /// an empty target or separator is a different job.
    fn makePipeState(allocator: Allocator, src: *const definition.SourceSpec, sinks: []const definition.SinkSpec, tag_hash: u64) !PipelineState {
        var pipe: PipelineState = .{
            .source_kind = src.kind,
            .src_measurement = "",
            .src_field = "",
            .src_tag_hash = tag_hash,
            .src_stream = "",
            .src_namespace = "",
            .src_poll_ms = if (src.ts_poll_interval_ms > 0) src.ts_poll_interval_ms else 1000,
            .batch_size = if (src.batch_size > 0) src.batch_size else 100,
            .sinks = &.{},
            .ts_cursor_ns = 0,
            .stream_cursor_ts = 0,
            .stream_cursor_seq = 0,
            .last_poll_ms = 0,
        };
        errdefer freePipeStateFields(allocator, &pipe);

        if (sinks.len > 0) {
            const sink_configs = try allocator.alloc(SinkConfig, sinks.len);
            for (sink_configs, sinks) |*sc, snk| sc.* = .{ .kind = snk.kind, .required_tags = snk.required_tags, .kv_ttl_ms = snk.ttl_ms, .queue_priority = snk.priority };
            pipe.sinks = sink_configs;
            for (sink_configs, sinks) |*sc, snk| {
                sc.target = try allocator.dupe(u8, snk.target);
                sc.namespace = try allocator.dupe(u8, if (snk.namespace.len > 0) snk.namespace else "default");
                sc.measurement = try allocator.dupe(u8, snk.ts_measurement);
                sc.value_field = try allocator.dupe(u8, if (snk.ts_value_field.len > 0) snk.ts_value_field else "value");
                sc.ts_tag_keys = try serializeFlatPairs(allocator, snk.ts_tag_keys);
                sc.ts_field_keys = try serializeFlatPairs(allocator, snk.ts_field_keys);
                sc.kv_key_prefix = try allocator.dupe(u8, snk.key_prefix);
                sc.kv_separator = try allocator.dupe(u8, snk.separator);
            }
        }
        pipe.src_measurement = try allocator.dupe(u8, src.ts_measurement);
        pipe.src_field = try allocator.dupe(u8, if (src.ts_field.len > 0) src.ts_field else "value");
        pipe.src_stream = try allocator.dupe(u8, src.stream);
        pipe.src_namespace = try allocator.dupe(u8, if (src.namespace.len > 0) src.namespace else "default");
        return pipe;
    }

    /// Drive all running pipelines: poll sources, push to sinks.
    /// Called from Shard.tick() on each reactor iteration.
    pub fn tickPipelines(self: *ProcessingHandler, shard: *Shard) void {
        const now_ms = @import("stdx").time.milliTimestamp();

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

            // Time the tick, but only record latency/throughput for *productive*
            // ticks (those that actually read records) so idle polls don't skew
            // the averages toward zero. `records_processed` counts source reads.
            const before = job.records_processed;
            const t0 = @import("stdx").time.nanoTimestamp();
            self.ticking_job = base_job_id;
            defer self.ticking_job = "";
            switch (pipe.source_kind) {
                .ts => self.tickTsSource(pipeline_key, pipe, shard, job),
                .stream => self.tickStreamSource(pipeline_key, pipe, shard, job),
            }
            const processed = job.records_processed - before;
            if (processed > 0) {
                if (shard.metrics_registry) |m| m.processing.recordProcessed(processed);
                const dur: u64 = @intCast(@max(@as(i128, 0), @import("stdx").time.nanoTimestamp() - t0));
                pipe.last_read_count = @intCast(@min(processed, std.math.maxInt(u32)));
                pipe.tick_ns_last = dur;
                pipe.tick_ns_max = @max(pipe.tick_ns_max, dur);
                pipe.tick_ns_sum += dur;
                pipe.tick_count += 1;
            }
        }
    }

    /// Tick a TS source pipeline: read points from TSProjection, apply operators, write to sink.
    fn tickTsSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        var point_buf: [256]StoredPoint = undefined;
        // Tags are part of the series key now, so a configured tag set selects
        // the series directly instead of being scanned for after the fact (which
        // also wasted the point budget on non-matching points).
        const result = shard.ts_handler.ts.queryRange(
            router.namespaceHash(pipe.src_namespace),
            pipe.src_measurement,
            pipe.src_field,
            if (pipe.src_tag_hash != 0) pipe.src_tag_hash else null,
            pipe.ts_cursor_ns,
            std.math.maxInt(u64),
            &point_buf,
        ) catch return;

        const points = point_buf[0..result.points_in_buffer];
        if (points.len == 0) return;

        for (points) |pt| {
            // Format TS point as JSON record. Short of memory, the tick
            // stops here, the point still ahead of the cursor.
            const ts_ms: i64 = @intCast(pt.timestamp_ns / 1_000_000);
            const json = tsPointJson(shard.allocator, pipe.src_measurement, pt.field_value, ts_ms) catch break;
            defer shard.allocator.free(json);

            // Apply operator chain (or pass through directly). A record
            // the chain failed on is counted and skipped: passing it through
            // raw would write what the operators were there to change.
            const output_records = applyOperatorChain(pipe.operators, json, ts_ms, shard.allocator) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };

            if (output_records) |records| {
                defer if (records.len > 0) shard.allocator.free(records);
                for (records) |rec| {
                    self.writeSinkRecord(pipe, shard, rec.value, pt.field_value, pt.timestamp_ns, shard.ts_handler.ts.tagsForHash(pt.tag_hash), rec.tags);
                }
            } else {
                // No operators — direct passthrough
                self.writeSinkRecord(pipe, shard, json, pt.field_value, pt.timestamp_ns, shard.ts_handler.ts.tagsForHash(pt.tag_hash), 0);
            }

            // Advance cursor past this point
            if (pt.timestamp_ns >= pipe.ts_cursor_ns) {
                pipe.ts_cursor_ns = pt.timestamp_ns + 1;
            }

            // Event-time watermark = highest source point timestamp seen.
            if (ts_ms > pipe.watermark_ms) pipe.watermark_ms = ts_ms;

            job.records_processed += 1;
        }
        // As a stream source does: a restart resumes here, not from the
        // measurement's first point.
        maybePersistCheckpoint(pipeline_key, pipe, shard, job.namespace_owned);
    }

    /// Tick a stream source pipeline: read payloads from stream, apply operators, write to sink.
    fn tickStreamSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        log.debug("TICK_ENTRY: src={s} ns={s} cursor_ts={d} cursor_seq={d}", .{ pipe.src_stream, pipe.src_namespace, pipe.stream_cursor_ts, pipe.stream_cursor_seq });
        // Build cursor from last-read StreamID
        const cursor = StreamID{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };

        // Resolve the stream handler that owns the source stream (may be on a different shard)
        const src_handler = self.resolveStreamHandler(shard.stream_handler, pipe.src_stream, pipe.src_namespace);

        // Read from the named source stream using namespace-qualified name-hash filtering
        const result = src_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, cursor, pipe.batch_size);
        log.debug("TICK: read {d} payloads, last_id ts={d} seq={d}", .{ result.payloads.len, result.last_id.timestamp_ms, result.last_id.sequence });
        defer src_handler.allocator.free(result.payloads);
        defer src_handler.allocator.free(result.ids);
        if (result.payloads.len == 0) {
            // Appends that couldn't be read still move the cursor past them
            // (the stream handler logged them), so the source doesn't stall.
            pipe.stream_cursor_ts = result.last_id.timestamp_ms;
            pipe.stream_cursor_seq = result.last_id.sequence;
            return;
        }

        for (result.payloads) |payload| {
            log.debug("tickStreamSource: payload len={d} first100='{s}'", .{ payload.len, if (payload.len > 100) payload[0..100] else payload });
            const output_records = applyOperatorChain(pipe.operators, payload, @import("stdx").time.milliTimestamp(), shard.allocator) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };

            if (output_records) |records| {
                defer if (records.len > 0) shard.allocator.free(records);
                log.debug("TICK: chain returned {d} records for job", .{records.len});
                for (records) |rec| {
                    log.debug("TICK: value_len={d} first50='{s}'", .{ rec.value.len, if (rec.value.len > 50) rec.value[0..50] else rec.value });
                    self.writeSinkRecordFromStream(pipe, shard, rec.key, rec.value, rec.tags);
                }
            } else {
                log.debug("tickStreamSource: no operators, passthrough", .{});
                self.writeSinkRecordFromStream(pipe, shard, "", payload, 0);
            }

            job.records_processed += 1;
        }

        // Advance cursor to last StreamID read
        pipe.stream_cursor_ts = result.last_id.timestamp_ms;
        pipe.stream_cursor_seq = result.last_id.sequence;

        // Persist the advanced cursor durably (throttled) so the pipeline
        // resumes from here after restart instead of re-reading from the start.
        // At-least-once: a crash loses at most CHECKPOINT_INTERVAL_MS
        // of progress, which redelivers — it never skips records.
        maybePersistCheckpoint(pipeline_key, pipe, shard, job.namespace_owned);
    }

    /// Minimum wall-clock gap between durable cursor checkpoints per pipeline.
    /// Bounds Raft write amplification; a crash loses at most this much progress
    /// (redelivered on resume — at-least-once, never skipped).
    const CHECKPOINT_INTERVAL_MS: i64 = 1000;

    /// Persist a pipeline's source cursor as a processing_checkpoint UAL entry,
    /// debounced. Key = pipeline_key (job_id, or job_id\0idx for multi-source);
    /// value as `replayCheckpoint` reads it.
    fn maybePersistCheckpoint(pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, namespace: []const u8) void {
        const a: u64, const b: u64 = switch (pipe.source_kind) {
            .stream => .{ pipe.stream_cursor_ts, pipe.stream_cursor_seq },
            .ts => .{ pipe.ts_cursor_ns, 0 },
        };
        // Only when the cursor actually advanced past what we last persisted.
        if (a == pipe.persisted_cursor_ts and b == pipe.persisted_cursor_seq) return;

        const now_ms = @import("stdx").time.milliTimestamp();
        if (pipe.last_persist_ms != 0 and now_ms - pipe.last_persist_ms < CHECKPOINT_INTERVAL_MS) return;
        // Set before the attempt so a failing disk is retried once per
        // interval, not once per tick.
        pipe.last_persist_ms = now_ms;

        var val_buf: [16]u8 = undefined;
        std.mem.writeInt(u64, val_buf[0..8], a, .little);
        std.mem.writeInt(u64, val_buf[8..16], b, .little);
        // The applier records the persisted cursor.
        _ = persistence_mod.proposeEntry(shard, .processing_checkpoint, Flags.NONE, namespace, pipeline_key, &val_buf) catch |err| {
            if (!shard.quietHalted(err)) log.err("pipeline {s}: checkpoint not persisted: {s}", .{ pipeline_key, @errorName(err) });
            if (shard.metrics_registry) |m| m.processing.recordError();
            return;
        };
        if (shard.metrics_registry) |m| m.processing.recordCheckpoint();
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
            .current_processing_time_ms = @import("stdx").time.milliTimestamp(),
            .operator_name = "",
        };

        // Feed the initial record into the first operator
        const input_rec = ProcessingRecord.fromValue(value, event_time_ms);
        log.debug("applyOperatorChain: calling operator[0] name='{s}'", .{operators[0].getName()});
        try operators[0].processElement(input_rec, &ctx);
        log.debug("applyOperatorChain: operator[0] done, collector count={d}", .{collector.count()});

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
            log.debug("applyOperatorChain: calling operator[{d}] name='{s}' with {d} records", .{ i, operators[i].getName(), staged.len });
            for (staged) |rec| {
                try operators[i].processElement(rec, &ctx);
            }
            log.debug("applyOperatorChain: operator[{d}] done, collector count={d}", .{ i, collector.count() });
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

    /// A record a sink could not write is lost to that sink (the cursor
    /// has moved past it); said once per interval with the count.
    pub fn noteSinkDrop(self: *ProcessingHandler, metrics: ?*MetricsRegistry, kind: []const u8, target: []const u8, why: []const u8) void {
        self.sink_drops += 1;
        if (metrics) |m| m.processing.recordDropped();
        const now = @import("stdx").time.milliTimestamp();
        if (now - self.sink_warn_ms < 30_000) return;
        self.sink_warn_ms = now;
        log.err("job '{s}': {s} sink '{s}': record dropped: {s} ({d} sink records dropped so far)", .{ self.ticking_job, kind, target, why, self.sink_drops });
    }

    /// A record the operator chain failed on is skipped; said once per
    /// interval with the count.
    fn noteChainError(self: *ProcessingHandler, shard: *Shard, err: anyerror) void {
        self.chain_errors += 1;
        if (shard.metrics_registry) |m| m.processing.recordError();
        const now = @import("stdx").time.milliTimestamp();
        if (now - self.chain_warn_ms < 30_000) return;
        self.chain_warn_ms = now;
        log.err("job '{s}': operator chain failed on a record ({s}); record skipped ({d} so far)", .{ self.ticking_job, @errorName(err), self.chain_errors });
    }

    /// Write a processed record to all matching sinks (TS source variant).
    /// Tag filtering: sinks with required_tags=0 (firehose) get all records;
    /// sinks with required_tags set only get records where record_tags matches.
    fn writeSinkRecord(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, payload: []const u8, field_value: f64, timestamp_ns: u64, src_tags: []const u8, record_tags: u32) void {
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            // A sink writes without passing a client's request gate: a
            // namespace this shard may not create takes no write.
            if (shard.namespace_handler.admission(snk.namespace)) |r| {
                self.noteSinkDrop(shard.metrics_registry, @tagName(snk.kind), snk.target, r.message);
                continue;
            }
            switch (snk.kind) {
                .stream => {
                    const sink_handler = self.resolveStreamHandler(shard.stream_handler, snk.target, snk.namespace);
                    // The applier names the stream by resolving its
                    // namespace. Only this shard's handler may be touched
                    // from this thread; a sink on another shard writes
                    // only to "default" (`crossShardSink`).
                    if (sink_handler == shard.stream_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
                    sink_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch |err| {
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, @errorName(err));
                        continue;
                    };
                },
                .ts => {
                    const ts_h = self.resolveTsHandler(shard.ts_handler, snk.namespace, snk.measurement);
                    const ual_idx = ts_h.nextUalIndex();
                    ts_h.ts.insert(
                        router.namespaceHash(snk.namespace),
                        snk.measurement,
                        snk.value_field,
                        field_value,
                        timestamp_ns,
                        ual_idx,
                        src_tags,
                    ) catch |err| {
                        self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, @errorName(err));
                        continue;
                    };
                },
                .kv => self.writeKvSink(snk, shard, "", payload),
                .queue => self.writeQueueSink(snk, shard, payload),
            }
        }
    }

    /// Write a processed record to all matching sinks (stream source variant).
    /// `record_key` is the record's partitioning key (set by `keyby`, empty otherwise);
    /// it is used to construct the KV sink key.
    fn writeSinkRecordFromStream(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, record_key: []const u8, payload: []const u8, record_tags: u32) void {
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            // A sink writes without passing a client's request gate: a
            // namespace this shard may not create takes no write.
            if (shard.namespace_handler.admission(snk.namespace)) |r| {
                self.noteSinkDrop(shard.metrics_registry, @tagName(snk.kind), snk.target, r.message);
                continue;
            }
            switch (snk.kind) {
                .stream => {
                    const sink_handler = self.resolveStreamHandler(shard.stream_handler, snk.target, snk.namespace);
                    // The applier names the stream by resolving its
                    // namespace. Only this shard's handler may be touched
                    // from this thread; a sink on another shard writes
                    // only to "default" (`crossShardSink`).
                    if (sink_handler == shard.stream_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
                    sink_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch |err| {
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, @errorName(err));
                        continue;
                    };
                },
                .ts => self.writeTsSinkFromJson(snk, shard, payload),
                .kv => self.writeKvSink(snk, shard, record_key, payload),
                .queue => self.writeQueueSink(snk, shard, payload),
            }
        }
    }

    /// TS sink (JSON source variant): honors the `fields:` map (one point per
    /// `field_name ← json_key` pair) and the `tags:` map (real tag hash). Falls back
    /// to the `value_field` shorthand (single "value" field) when no fields map is set.
    /// Routes to the shard that owns the measurement so `ts read` finds the points.
    fn writeTsSinkFromJson(self: *ProcessingHandler, snk: SinkConfig, shard: *Shard, payload: []const u8) void {
        const ts_h = self.resolveTsHandler(shard.ts_handler, snk.namespace, snk.measurement);
        const now_ns: u64 = @intCast(@as(u64, @bitCast(@import("stdx").time.milliTimestamp())) * 1_000_000);
        var sink_tag_buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
        const sink_tags = computeTsSinkTags(snk.ts_tag_keys, payload, &sink_tag_buf);

        if (snk.ts_field_keys.len > 0) {
            var it = FlatPairIterator.init(snk.ts_field_keys);
            while (it.next()) |pair| {
                // pair.key = TS field name, pair.value = JSON field to extract from.
                const value = extractJsonFloat(payload, pair.value) orelse {
                    self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the record has no numeric field for it");
                    continue;
                };
                if (!std.math.isFinite(value)) {
                    self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the value is not a finite number");
                    continue;
                }
                const ual_idx = ts_h.nextUalIndex();
                ts_h.ts.insert(router.namespaceHash(snk.namespace), snk.measurement, pair.key, value, now_ns, ual_idx, sink_tags) catch |err| {
                    self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, @errorName(err));
                    continue;
                };
            }
            return;
        }

        // value_field shorthand — extract the named JSON field and store it as "value".
        const value = extractJsonFloat(payload, snk.value_field) orelse
            (std.fmt.parseFloat(f64, payload) catch return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the record is not a number"));
        if (!std.math.isFinite(value)) return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the value is not a finite number");
        const ual_idx = ts_h.nextUalIndex();
        ts_h.ts.insert(router.namespaceHash(snk.namespace), snk.measurement, "value", value, now_ns, ual_idx, sink_tags) catch |err| self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, @errorName(err));
    }

    const MAX_SINK_KEY = @import("../namespace/handler.zig").MAX_KEY_LENGTH;

    /// A KV sink's key for a record: `prefix + separator + record_key` (a
    /// record with no key writes `prefix + separator`), or the record's key
    /// alone with no prefix, when it has one.
    fn kvSinkKey(buf: *[MAX_SINK_KEY]u8, prefix: []const u8, separator: []const u8, record_key: []const u8) error{ NoKey, KeyTooLong }![]const u8 {
        if (prefix.len == 0) {
            if (record_key.len == 0) return error.NoKey;
            return if (record_key.len > MAX_SINK_KEY) error.KeyTooLong else record_key;
        }
        return std.fmt.bufPrint(buf, "{s}{s}{s}", .{ prefix, separator, record_key }) catch error.KeyTooLong;
    }

    /// The sink's key for a record, or null with the record counted as dropped.
    fn kvSinkKeyOrDrop(self: *ProcessingHandler, buf: *[MAX_SINK_KEY]u8, snk: SinkConfig, metrics: ?*MetricsRegistry, record_key: []const u8) ?[]const u8 {
        return kvSinkKey(buf, snk.kv_key_prefix, snk.kv_separator, record_key) catch |err| {
            self.noteSinkDrop(metrics, "kv", if (snk.kv_key_prefix.len > 0) snk.kv_key_prefix else "(no prefix)", switch (err) {
                error.NoKey => "the record has no key",
                error.KeyTooLong => std.fmt.comptimePrint("the key is longer than {d} bytes", .{MAX_SINK_KEY}),
            });
            return null;
        };
    }

    /// KV sink: write the record under `key_prefix + separator + record_key`
    /// on the shard that owns that key, so a later `kv get` finds it: here,
    /// or, when another shard owns it, by a message that shard proposes
    /// from. A message to a full inbox is not retried: the record is lost,
    /// and counted, until channels give sinks durable delivery.
    fn writeKvSink(self: *ProcessingHandler, snk: SinkConfig, shard: *Shard, record_key: []const u8, payload: []const u8) void {
        var key_buf: [MAX_SINK_KEY]u8 = undefined;
        const full_key = self.kvSinkKeyOrDrop(&key_buf, snk, shard.metrics_registry, record_key) orelse return;
        var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = ns_keys.qualifyKey(&qbuf, snk.namespace, full_key) catch |err| return self.noteSinkDrop(shard.metrics_registry, "kv", full_key, switch (err) {
            error.KeyHasNul => "key must not contain NUL",
            error.KeyTooLarge => "namespace + key too large",
        });
        const owner = self.kvOwner(shard, snk.namespace, full_key);
        if (owner == shard.id) {
            if (kv_handler_mod.KVHandler.sinkPropose(shard, snk.namespace, qkey, payload, snk.kv_ttl_ms)) |why| self.noteSinkDrop(shard.metrics_registry, "kv", full_key, why);
            return;
        }
        const message = (kv_handler_mod.KVHandler.SinkPut{ .namespace = snk.namespace, .qkey = qkey, .value = payload, .ttl_ms = snk.kv_ttl_ms }).encode(shard.allocator) orelse
            return self.noteSinkDrop(shard.metrics_registry, "kv", full_key, "out of memory");
        if (!shard.peer_mailboxes.?[owner].inbox.send(.{ .tag = .kv_sink_put, .src_shard = @intCast(shard.id), .payload_len = @intCast(message.len), .payload_ptr = message.ptr })) {
            shard.allocator.free(message);
            if (shard.shard_metrics) |sm| sm.recordCrossShardOverloaded(.engine);
            self.noteSinkDrop(shard.metrics_registry, "kv", full_key, "the owning shard's inbox is full; the record is lost");
        }
    }

    /// The shard that owns KV `key` in `namespace`: this one when there are
    /// no others to send to.
    fn kvOwner(self: *const ProcessingHandler, shard: *const Shard, namespace: []const u8, key: []const u8) u16 {
        const mailboxes = shard.peer_mailboxes orelse return @intCast(shard.id);
        if (mailboxes.len <= 1 or self.peer_partition_count == 0 or self.peer_shard_count == 0) return @intCast(shard.id);
        const owner = self.peerShardId(namespace, key);
        return if (owner < mailboxes.len) owner else @intCast(shard.id);
    }

    /// Queue sink: enqueue the record payload into the configured queue (durable,
    /// via the queue handler's Raft-backed enqueue path), routed to the shard that
    /// owns the queue so a later `queue dequeue` finds it.
    fn writeQueueSink(self: *ProcessingHandler, snk: SinkConfig, shard: *Shard, payload: []const u8) void {
        var opt_buf: [64]u8 = undefined;
        var ob = proto.OptionsBuilder.init(&opt_buf);
        if (snk.queue_priority > 0) ob.addU8(.priority, snk.queue_priority) catch {};

        const req = proto.Request{
            .header = makeSinkHeader(.queue_enqueue),
            .namespace = snk.namespace,
            .key = snk.target, // queue name
            .value = payload,
            .options = opt_buf[0..ob.offset],
        };
        const q = self.resolveQueueHandler(shard.queue_handler, snk.namespace, snk.target);
        // The applier labels the queue by resolving its namespace; only
        // this shard's handler may be touched from this thread, and a sink
        // on another shard writes only to "default" (`crossShardSink`).
        if (q == shard.queue_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
        const result = q.sinkEnqueue(req);
        defer q.freeResult(result);
        if (result == .err) self.noteSinkDrop(shard.metrics_registry, "queue", snk.target, result.err.message);
    }

    /// Build a minimal in-process request header carrying just the opcode.
    fn makeSinkHeader(op: proto.OpCode) proto.RequestHeader {
        return proto.RequestHeader.init(op, 0);
    }

    /// Iterates serialized flat pairs (`a\0b\0c\0d\0`) as (key, value) tuples.
    const FlatPair = struct { key: []const u8, value: []const u8 };
    const FlatPairIterator = struct {
        data: []const u8,
        pos: usize = 0,

        fn init(data: []const u8) FlatPairIterator {
            return .{ .data = data };
        }

        fn nextToken(self: *FlatPairIterator) ?[]const u8 {
            if (self.pos >= self.data.len) return null;
            const start = self.pos;
            while (self.pos < self.data.len and self.data[self.pos] != 0) self.pos += 1;
            const tok = self.data[start..self.pos];
            if (self.pos < self.data.len) self.pos += 1; // skip the NUL separator
            return tok;
        }

        fn next(self: *FlatPairIterator) ?FlatPair {
            const k = self.nextToken() orelse return null;
            const v = self.nextToken() orelse return null;
            return .{ .key = k, .value = v };
        }
    };

    /// Compute the TS tag hash for a sink write from its `tags:` map and the record
    /// payload. Builds `tagname=value,…` (config order) and Wyhashes it — the SAME
    /// algorithm the TS source filter and `ts read --tags` use, so writes are queryable.
    /// Build the sink's tag set as a `k=v,k=v` string from the configured tag
    /// keys and the record payload.
    ///
    /// This used to return a raw `Wyhash` of the string — NOT the canonical
    /// hash the write and read paths use — so every point a TS sink wrote
    /// landed in a series that no query could ever address. Returning the
    /// string and letting the projection canonicalize is what keeps the one
    /// hashing rule in one place.
    fn computeTsSinkTags(tag_keys: []const u8, payload: []const u8, buf: []u8) []const u8 {
        if (tag_keys.len == 0) return "";
        var pos: usize = 0;
        var it = FlatPairIterator.init(tag_keys);
        while (it.next()) |pair| {
            // pair.key = TS tag name, pair.value = JSON field to read the tag value from.
            const tag_val = extractJsonString(payload, pair.value) orelse continue;
            if (pos > 0 and pos < buf.len) {
                buf[pos] = ',';
                pos += 1;
            }
            const kv = std.fmt.bufPrint(buf[pos..], "{s}={s}", .{ pair.key, tag_val }) catch break;
            pos += kv.len;
        }
        return buf[0..pos];
    }

    /// Extract a string field value from a JSON payload (unescaped, first match).
    fn extractJsonString(payload: []const u8, field_name: []const u8) ?[]const u8 {
        if (field_name.len == 0) return null;
        var search_buf: [256]u8 = undefined;
        const pattern = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{field_name}) catch return null;
        const idx = std.mem.indexOf(u8, payload, pattern) orelse return null;
        const after = payload[idx + pattern.len ..];

        var start: usize = 0;
        while (start < after.len and (after[start] == ' ' or after[start] == '\t')) start += 1;
        if (start >= after.len or after[start] != '"') return null;
        start += 1; // skip opening quote
        var end = start;
        while (end < after.len and after[end] != '"') end += 1;
        if (end >= after.len) return null;
        return after[start..end];
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

fn applySubmit(h: *ProcessingHandler, key: []const u8, value: []const u8, created_at_ms: i64) persistence_mod.ApplyError!Result {
    var out: Result = .{};
    try h.replaySubmit(key, value, created_at_ms, &out);
    return out;
}

fn applyLogged(h: *ProcessingHandler, e: *const entry_mod.Entry) persistence_mod.ApplyError!Result {
    var out = Result.start(e);
    try h.replayEntry(e, &out);
    return out;
}

test "ProcessingHandler: all opcodes are registered" {
    const Dispatch = @import("../node/dispatcher.zig").Dispatcher;
    var d = Dispatch.init();
    ProcessingHandler.register(&d);

    // Verify all 8 processing opcodes are registered
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_submit));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_stop));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_cancel));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_status));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_list));
}

test "ProcessingHandler: init and deinit" {
    const handler = ProcessingHandler.init(std.testing.allocator);
    var h = handler;
    h.deinit();
}

test "ProcessingHandler: replayCheckpoint restores a pipeline's stream cursor" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();

    // A minimal stream-source pipeline; deinit frees its key.
    const src = definition.SourceSpec{
        .name = "s",
        .stream = "raw",
        .namespace = "default",
        .batch_size = 0,
    };
    try handler.pipelines.put(try allocator.dupe(u8, "job-cp"), try ProcessingHandler.makePipeState(allocator, &src, &.{}, 0));

    var val: [16]u8 = undefined;
    std.mem.writeInt(u64, val[0..8], 1700, .little);
    std.mem.writeInt(u64, val[8..16], 5, .little);
    try handler.replayCheckpoint("job-cp", &val);

    const pipe = handler.pipelines.getPtr("job-cp").?;
    try std.testing.expectEqual(@as(u64, 1700), pipe.stream_cursor_ts);
    try std.testing.expectEqual(@as(u64, 5), pipe.stream_cursor_seq);
    // persisted_* mirror the restored cursor so the first post-restart tick
    // doesn't immediately re-persist an unchanged cursor.
    try std.testing.expectEqual(@as(u64, 1700), pipe.persisted_cursor_ts);
    try std.testing.expectEqual(@as(u64, 5), pipe.persisted_cursor_seq);

    // Unknown key is a no-op; short value is ignored (no crash, cursor unchanged).
    try handler.replayCheckpoint("missing", &val);
    try std.testing.expectError(error.Halt, handler.replayCheckpoint("job-cp", &[_]u8{0}));
    try std.testing.expectEqual(@as(u64, 1700), pipe.stream_cursor_ts);
}

test "processing: a TS source's checkpoint restores its cursor, and never moves it back" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    const src = definition.SourceSpec{
        .kind = .ts,
        .name = "t",
        .stream = "",
        .namespace = "default",
        .batch_size = 0,
        .ts_measurement = "cpu",
        .ts_field = "value",
    };
    try handler.pipelines.put(try allocator.dupe(u8, "job-ts"), try ProcessingHandler.makePipeState(allocator, &src, &.{}, 0));
    var val: [16]u8 = undefined;
    std.mem.writeInt(u64, val[0..8], 1_700_000_000_123, .little);
    std.mem.writeInt(u64, val[8..16], 0, .little);
    try handler.replayCheckpoint("job-ts", &val);
    const pipe = handler.pipelines.getPtr("job-ts").?;
    try std.testing.expectEqual(@as(u64, 1_700_000_000_123), pipe.ts_cursor_ns);
    try std.testing.expectEqual(@as(u64, 0), pipe.stream_cursor_ts);
    try std.testing.expectEqual(@as(u64, 1_700_000_000_123), pipe.persisted_cursor_ts);

    // A checkpoint that commits after the pipeline read further does not
    // move it back.
    pipe.ts_cursor_ns = 1_700_000_000_500;
    try handler.replayCheckpoint("job-ts", &val);
    try std.testing.expectEqual(@as(u64, 1_700_000_000_500), pipe.ts_cursor_ns);
}

test "processing: a stream source's checkpoint never moves its cursor back" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    const src = definition.SourceSpec{ .name = "s", .stream = "raw", .namespace = "default", .batch_size = 0 };
    try handler.pipelines.put(try allocator.dupe(u8, "job-fw"), try ProcessingHandler.makePipeState(allocator, &src, &.{}, 0));
    const pipe = handler.pipelines.getPtr("job-fw").?;
    pipe.stream_cursor_ts = 2000;
    pipe.stream_cursor_seq = 3;
    var val: [16]u8 = undefined;
    for ([_][2]u64{ .{ 1999, 9 }, .{ 2000, 2 } }) |older| {
        std.mem.writeInt(u64, val[0..8], older[0], .little);
        std.mem.writeInt(u64, val[8..16], older[1], .little);
        try handler.replayCheckpoint("job-fw", &val);
        try std.testing.expectEqual(@as(u64, 2000), pipe.stream_cursor_ts);
        try std.testing.expectEqual(@as(u64, 3), pipe.stream_cursor_seq);
    }
    std.mem.writeInt(u64, val[0..8], 2000, .little);
    std.mem.writeInt(u64, val[8..16], 4, .little);
    try handler.replayCheckpoint("job-fw", &val);
    try std.testing.expectEqual(@as(u64, 4), pipe.stream_cursor_seq);
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

test "ProcessingHandler: a job is created at its submit entry's stamp, not the host's clock" {
    const yaml =
        \\kind: Processing
        \\name: stamped
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    const host_ms = @import("stdx").time.milliTimestamp();
    // One stamp far behind the host's clock and one a day ahead.
    for ([_]i64{ 1_000, host_ms + std.time.ms_per_day }) |stamp_ms| {
        var handler = ProcessingHandler.init(std.testing.allocator);
        defer handler.deinit();
        // [layout][status][batch_size=0][ns_len=7]["default"][yaml]
        var value: [512]u8 = undefined;
        value[0] = SUBMIT_LAYOUT;
        value[1] = @backingInt(ProcessingHandler.JobStatus.stopped);
        std.mem.writeInt(u32, value[2..6], 0, .little);
        std.mem.writeInt(u16, value[6..8], 7, .little);
        @memcpy(value[8..15], "default");
        @memcpy(value[15 .. 15 + yaml.len], yaml);
        var pbuf: [1024]u8 = undefined;
        _ = try applyLogged(&handler, &entry_mod.buildCommandEntry(.processing_submit, 0, 1, 1, @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms, 0, "job-1", value[0 .. 15 + yaml.len], &pbuf).?);
        try std.testing.expectEqual(stamp_ms, (handler.jobs.get("job-1") orelse return error.JobNotCreated).created_at_ms);
    }
}

test "ProcessingHandler: a replayed submit with an unknown status is skipped" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();

    // [layout][status=0xee][batch_size][ns_len=0]
    var val = @as([8]u8, @splat(0));
    val[0] = SUBMIT_LAYOUT;
    val[1] = 0xee;
    try std.testing.expectError(error.Halt, applySubmit(&handler, "job-bad", &val, 1));
    try std.testing.expectEqual(@as(usize, 0), handler.jobs.count());
}

test "ProcessingHandler: a TS source record is JSON for any measurement name" {
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, "cp\"u\\", 1.5, 7);
    defer std.testing.allocator.free(json);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("cp\"u\\", parsed.value.object.get("measurement").?.string);
    try std.testing.expectEqual(@as(f64, 1.5), parsed.value.object.get("value").?.float);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("timestamp_ms").?.integer);
}

test "ProcessingHandler: a TS source record holds a measurement name of any length" {
    // Past the 512-byte buffer a point was built in, whose points were skipped.
    const name = comptime @import("stdx").repeat("\"", 600);
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, name, 1.5, 7);
    defer std.testing.allocator.free(json);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(name, parsed.value.object.get("measurement").?.string);
}

test "ProcessingHandler: a KV sink key is refused when there is none at all or it's too long, not dropped unseen" {
    var buf: [ProcessingHandler.MAX_SINK_KEY]u8 = undefined;
    try std.testing.expectEqualStrings("user:42", try ProcessingHandler.kvSinkKey(&buf, "user", ":", "42"));
    try std.testing.expectEqualStrings("42", try ProcessingHandler.kvSinkKey(&buf, "", ":", "42"));
    try std.testing.expectEqualStrings("user:", try ProcessingHandler.kvSinkKey(&buf, "user", ":", ""));
    try std.testing.expectError(error.NoKey, ProcessingHandler.kvSinkKey(&buf, "", ":", ""));
    // Past the old 1024-byte buffer, and still within a key.
    const long = comptime @import("stdx").repeat("k", 2000);
    try std.testing.expectEqual(@as(usize, 2005), (try ProcessingHandler.kvSinkKey(&buf, "user", ":", long)).len);
    const too_long = comptime @import("stdx").repeat("k", ProcessingHandler.MAX_SINK_KEY);
    try std.testing.expectError(error.KeyTooLong, ProcessingHandler.kvSinkKey(&buf, "user", ":", too_long));
    try std.testing.expectError(error.KeyTooLong, ProcessingHandler.kvSinkKey(&buf, "", ":", too_long ++ "k"));
}

test "ProcessingHandler: a KV sink record with no key, or one too long, is counted as dropped" {
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [ProcessingHandler.MAX_SINK_KEY]u8 = undefined;
    const bare: ProcessingHandler.SinkConfig = .{ .kind = .kv, .kv_separator = ":" };
    const prefixed: ProcessingHandler.SinkConfig = .{ .kind = .kv, .kv_key_prefix = "user", .kv_separator = ":" };
    try std.testing.expectEqualStrings("42", handler.kvSinkKeyOrDrop(&buf, bare, null, "42").?);
    try std.testing.expectEqual(@as(u64, 0), handler.sink_drops);
    try std.testing.expect(handler.kvSinkKeyOrDrop(&buf, bare, null, "") == null);
    try std.testing.expectEqual(@as(u64, 1), handler.sink_drops);
    const too_long = comptime @import("stdx").repeat("k", ProcessingHandler.MAX_SINK_KEY);
    try std.testing.expect(handler.kvSinkKeyOrDrop(&buf, prefixed, null, too_long) == null);
    try std.testing.expectEqual(@as(u64, 2), handler.sink_drops);
}

test "ProcessingHandler: a TS source record writes a non-finite value as null" {
    for ([_]f64{ std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64) }) |v| {
        const json = try ProcessingHandler.tsPointJson(std.testing.allocator, "m", v, 7);
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings("{\"measurement\":\"m\",\"value\":null,\"timestamp_ms\":7}", json);
    }
}

var test_why_buf: [256]u8 = undefined;

/// A processing_submit value: [layout][status][batch_size=0][ns_len]["default"][yaml].
fn testSubmitValue(buf: []u8, status: ProcessingHandler.JobStatus, yaml: []const u8) []const u8 {
    buf[0] = SUBMIT_LAYOUT;
    buf[1] = @backingInt(status);
    std.mem.writeInt(u32, buf[2..6], 0, .little);
    std.mem.writeInt(u16, buf[6..8], 7, .little);
    @memcpy(buf[8..15], "default");
    @memcpy(buf[15 .. 15 + yaml.len], yaml);
    return buf[0 .. 15 + yaml.len];
}

test "ProcessingHandler: a stored definition that no longer parses is kept as failed, with its definition and reason" {
    const yaml =
        \\kind: Processing
        \\name: ranged
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\      partitions: "0-4294967295"
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    _ = try applySubmit(&handler, "job-r", testSubmitValue(&buf, .running, yaml), 1);
    const job = handler.jobs.getPtr("job-r").?;
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, job.status);
    try std.testing.expect(std.mem.indexOf(u8, job.failure_owned, "partitions") != null);
    try std.testing.expectEqualStrings(yaml, job.yaml_owned);
    try std.testing.expectEqual(@as(usize, 0), handler.pipelines.count());
    try std.testing.expectEqual(@as(u64, 1), handler.failed_jobs);

    // The same submit again is still one failed job.
    _ = try applySubmit(&handler, "job-r", testSubmitValue(&buf, .running, yaml), 1);
    try std.testing.expectEqual(@as(u64, 1), handler.failed_jobs);
    // Cancelling it leaves none failed.
    try handler.replayStatusChange("job-r", .cancelled);
    try std.testing.expectEqual(@as(u64, 0), handler.failed_jobs);
}

test "ProcessingHandler: a job without a pipeline keeps its cursor, forward only, and a pipeline built later starts from it" {
    const yaml =
        \\kind: Processing
        \\name: kept
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    _ = try applySubmit(&handler, "job-k", testSubmitValue(&buf, .stopped, yaml), 1);
    try std.testing.expectEqual(@as(usize, 0), handler.pipelines.count());

    var val: [16]u8 = undefined;
    std.mem.writeInt(u64, val[0..8], 10, .little);
    std.mem.writeInt(u64, val[8..16], 5, .little);
    try handler.replayCheckpoint("job-k", &val);
    // A stale checkpoint doesn't move it back.
    std.mem.writeInt(u64, val[0..8], 9, .little);
    std.mem.writeInt(u64, val[8..16], 9, .little);
    try handler.replayCheckpoint("job-k", &val);
    const kept = handler.jobs.getPtr("job-k").?.cursors.items;
    try std.testing.expectEqual(@as(usize, 1), kept.len);
    try std.testing.expectEqual(ProcessingHandler.JobRecord.Cursor{ .source = 0, .a = 10, .b = 5 }, kept[0]);

    // The submit applied again as running builds the pipeline at the kept cursor.
    _ = try applySubmit(&handler, "job-k", testSubmitValue(&buf, .running, yaml), 1);
    const pipe = handler.pipelines.getPtr("job-k").?;
    try std.testing.expectEqual(@as(u64, 10), pipe.stream_cursor_ts);
    try std.testing.expectEqual(@as(u64, 5), pipe.stream_cursor_seq);
    try std.testing.expectEqual(@as(u64, 10), pipe.persisted_cursor_ts);
}

test "ProcessingHandler: a failure reason is kept up to 1 KiB, cut on a character and marked" {
    const allocator = std.testing.allocator;
    const short = try ProcessingHandler.boundedReason(allocator, "bad key");
    defer allocator.free(short);
    try std.testing.expectEqualStrings("bad key", short);

    // A 2-byte character straddles the cut.
    var long: [2000]u8 = @splat('x');
    const cut_at = ProcessingHandler.MAX_FAILURE_REASON - "... (cut)".len;
    long[cut_at - 1] = 0xC3;
    long[cut_at] = 0xA9;
    const cut = try ProcessingHandler.boundedReason(allocator, &long);
    defer allocator.free(cut);
    try std.testing.expect(cut.len <= ProcessingHandler.MAX_FAILURE_REASON);
    try std.testing.expect(std.unicode.utf8ValidateSlice(cut));
    try std.testing.expect(std.mem.endsWith(u8, cut, "x... (cut)"));
}

test "ProcessingHandler: a submit past the job limit is refused in log order" {
    const yaml =
        \\kind: Processing
        \\name: one
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    handler.max_jobs = 1;
    var buf: [512]u8 = undefined;
    var pbuf: [1024]u8 = undefined;
    const value = testSubmitValue(&buf, .stopped, yaml);
    for ([_]struct { []const u8, bool }{ .{ "job-1", false }, .{ "job-2", true }, .{ "job-1", false } }) |c| {
        const entry = entry_mod.buildCommandEntry(.processing_submit, 0, 1, 1, 0, 0, c[0], value, &pbuf).?;
        var out = Result.start(&entry);
        try handler.replayEntry(&entry, &out);
        try std.testing.expectEqual(c[1], out.refused() != null);
        try std.testing.expectEqual(@as(usize, 1), handler.jobs.count());
    }
    try std.testing.expect(!handler.jobs.contains("job-2"));
}

test "ProcessingHandler: every submit routes to shard 0, whatever its key" {
    var req: proto.Request = undefined;
    req.namespace = "acme";
    for ([_][]const u8{ "", "k", "another-key" }) |key| {
        req.key = key;
        try std.testing.expectEqual(@as(?u64, 0), ProcessingHandler.preRouteByProcessing(req));
    }
}

test "ProcessingHandler: kv_lookup reads its own shard's KV, whatever the key hashes to" {
    const allocator = std.testing.allocator;
    const TestKv = @import("../kv/machine.zig").TestKv;
    var t0 = TestKv.init(allocator);
    defer t0.deinit();
    var t1 = TestKv.init(allocator);
    defer t1.deinit();
    var kv0 = t0.state();
    var kv1 = t1.state();
    var h0 = kv_handler_mod.KVHandler.init(allocator, &kv0);
    var h1 = kv_handler_mod.KVHandler.init(allocator, &kv1);
    const peers = [_]*kv_handler_mod.KVHandler{ &h0, &h1 };

    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    handler.peer_partition_count = 8;
    handler.peer_shard_count = 2;
    handler.setPeerKvHandlers(&peers);

    // A key whose hash names shard 1, written here (as a routing key does).
    var elsewhere: []const u8 = "";
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" }) |c| {
        if (handler.peerShardId("default", c) == 1) elsewhere = c;
    }
    try std.testing.expect(elsewhere.len > 0);
    const ns_handler = @import("../namespace/handler.zig");
    var qbuf: [ns_handler.MAX_QUALIFIED_KEY]u8 = undefined;
    try t0.put(try ns_handler.qualifyKey(&qbuf, "default", elsewhere), "here", 0);
    try t1.put(try ns_handler.qualifyKey(&qbuf, "default", "only-there"), "there", 0);

    try std.testing.expectEqualStrings("here", ProcessingHandler.shardKvLookup(&handler, "default", elsewhere).?);
    try std.testing.expect(ProcessingHandler.shardKvLookup(&handler, "default", "only-there") == null);
}

const lookup_yaml =
    \\kind: Processing
    \\name: lookup
    \\sources:
    \\  - name: src
    \\    stream:
    \\      name: in
    \\operators:
    \\  - type: kv_lookup
    \\    name: l
    \\    lookup_key: k
    \\sinks:
    \\  - name: out
    \\    stream:
    \\      name: out
;

test "ProcessingHandler: replicas of different shard counts store the same status; only the one that can't run it holds it" {
    var one = ProcessingHandler.init(std.testing.allocator);
    defer one.deinit();
    var two = ProcessingHandler.init(std.testing.allocator);
    defer two.deinit();
    two.peer_shard_count = 2;
    var value: [512]u8 = undefined;
    for ([_]*ProcessingHandler{ &one, &two }) |h| {
        const out = try applySubmit(h, "job-1", testSubmitValue(&value, .running, lookup_yaml), 1);
        // Stored and running on both: the entry is all either read.
        try std.testing.expect(out.refused() == null);
        try std.testing.expectEqual(ProcessingHandler.JobStatus.running, h.jobs.get("job-1").?.status);
        try std.testing.expectEqual(@as(u64, 0), h.failed_jobs);
    }
    try std.testing.expect(one.pipelines.contains("job-1"));
    try std.testing.expect(!one.jobs.get("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 0), one.held_jobs);

    try std.testing.expect(!two.pipelines.contains("job-1"));
    const held = two.jobs.getPtr("job-1").?;
    try std.testing.expect(held.held);
    try std.testing.expectEqualStrings("kv_lookup needs a one-shard server (--shards 1) until lookups can read other shards", held.held_owned);
    try std.testing.expectEqual(@as(u64, 1), two.held_jobs);

    // Applied again, it is still one held job.
    _ = try applySubmit(&two, "job-1", testSubmitValue(&value, .running, lookup_yaml), 2);
    try std.testing.expect(two.jobs.get("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 1), two.held_jobs);

    // The list says so, beside the status.
    const ctxs = [_]*anyopaque{@ptrCast(&two)};
    const answer = try ProcessingHandler.serializeWalkJobs(std.testing.allocator, &.{"lookup"}, null, &ctxs, "");
    defer std.testing.allocator.free(answer);
    var r = @import("../util/wire.zig").WireReader.init(answer);
    _ = r.readU32().?;
    _ = r.readLengthPrefixed(u16).?;
    _ = r.readLengthPrefixed(u16).?;
    try std.testing.expectEqualStrings("RUNNING (held here: kv_lookup needs a one-shard server (--shards 1) until lookups can read other shards)", r.readLengthPrefixed(u16).?);

    // Stopped, it is no longer held.
    try two.replayStatusChange("job-1", .stopped);
    try std.testing.expect(!two.jobs.get("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 0), two.held_jobs);
}

test "ProcessingHandler: a submit stored as failed is refused, but says it ran" {
    var h = ProcessingHandler.init(std.testing.allocator);
    defer h.deinit();
    var value: [512]u8 = undefined;
    const out = try applySubmit(&h, "job-f", testSubmitValue(&value, .running, "kind: Processing\nname: bad\nsources: [{stream: {name: in, partitions: \"0-1\"}}]\nsinks: [{stream: {name: out}}]\n"), 1);
    const refused = out.refused().?;
    try std.testing.expectEqual(proto.Ran.yes, refused.refusal.ran);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, h.jobs.get("job-f").?.status);
}

test "ProcessingHandler: memory running out while a submit applies never stores it as failed" {
    // One job per operator type, each reading two streams under an id long
    // enough that its pipeline keys outgrow a small buffer.
    const operators = [_][]const u8{
        "{type: kv_lookup, name: o, lookup_key: k}",
        "{type: keyby, name: o, key_expression: \"$.user.id\"}",
        "{type: aggregate, name: o, function: sum, field: \"$.amount\"}",
        "{type: map, name: o, user: \"$.user.name\", source: signup}",
        "{type: flatmap, name: o, array_field: \"$.order.items\", element_key: \"$.sku\"}",
    };
    const job_id = comptime @import("stdx").repeat("j", 300);
    for (operators) |op| {
        var yaml_buf: [512]u8 = undefined;
        const yaml = try std.fmt.bufPrint(&yaml_buf, "kind: Processing\nname: oom\nsources: [{{stream: {{name: a}}}}, {{stream: {{name: b}}}}]\noperators: [{s}]\nsinks: [{{stream: {{name: out}}}}]\n", .{op});
        var value: [1024]u8 = undefined;
        const submit = testSubmitValue(&value, .running, yaml);
        // Each allocation in turn fails; past the last, the apply runs clean.
        var fail_index: usize = 0;
        while (true) : (fail_index += 1) {
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
            var h = ProcessingHandler.init(failing.allocator());
            defer h.deinit();
            if (applySubmit(&h, job_id, submit, 1)) |_| {
                // Applied: running, held here or not, never failed; held
                // with no pipeline left, or running both.
                const job = h.jobs.get(job_id).?;
                try std.testing.expectEqual(ProcessingHandler.JobStatus.running, job.status);
                try std.testing.expectEqual(@as(u64, 0), h.failed_jobs);
                try std.testing.expectEqual(@as(u32, if (job.held) 0 else 2), h.pipelines.count());
            } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
            if (!failing.has_induced_failure) break;
        }
        try std.testing.expect(fail_index > 3);
    }
}

test "ProcessingHandler: a pipeline's copied config is freed once, and a build out of memory frees what it copied" {
    const src = definition.SourceSpec{ .name = "s", .stream = "raw", .namespace = "app", .batch_size = 0, .ts_measurement = "cpu", .ts_field = "v" };
    const sinks = [_]definition.SinkSpec{
        .{ .name = "k", .kind = .kv, .target = "", .namespace = "app", .key_prefix = "user", .separator = "-", .ttl_ms = null, .priority = 0 },
        .{ .name = "t", .kind = .ts, .target = "", .namespace = "", .key_prefix = "", .separator = "", .ttl_ms = null, .priority = 0, .ts_measurement = "m", .ts_tag_keys = &.{ "host", "h" }, .ts_field_keys = &.{ "load", "l" } },
    };
    const Build = struct {
        fn run(allocator: Allocator, s: *const definition.SourceSpec, k: []const definition.SinkSpec) !void {
            var pipe = try ProcessingHandler.makePipeState(allocator, s, k, 0);
            defer ProcessingHandler.freePipeStateFields(allocator, &pipe);
            try std.testing.expectEqualStrings("user", pipe.sinks[0].kv_key_prefix);
            try std.testing.expectEqualStrings("-", pipe.sinks[0].kv_separator);
            try std.testing.expectEqualStrings("", pipe.sinks[1].kv_separator);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Build.run, .{ &src, &sinks });
}

test "ProcessingHandler: a job reading two streams frees its tag registry once" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();

    var def = try parser.parseJobDefinition(allocator,
        \\kind: Processing
        \\name: two-sources
        \\sources:
        \\  - name: a
        \\    stream: { name: in-a }
        \\  - name: b
        \\    stream: { name: in-b }
        \\sinks:
        \\  - name: out
        \\    stream: { name: out }
    , null);
    defer def.deinit(allocator);

    _ = handler.startPipelines("job-two", &def, &test_why_buf);
    try std.testing.expectEqual(@as(u32, 2), handler.pipelines.count());
}

test "ProcessingHandler: a stream or queue sink outside default on another shard is refused, on its own shard or in default it runs" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    handler.peer_shard_count = 4;
    handler.peer_partition_count = 256;
    handler.own_shard = 0;
    // A target on this shard and one on another, in "acme".
    var here_buf: [16]u8 = undefined;
    var there_buf: [16]u8 = undefined;
    var here: ?[]const u8 = null;
    var there: ?[]const u8 = null;
    var i: usize = 0;
    while (here == null or there == null) : (i += 1) {
        const on_this = handler.peerShardId("acme", try std.fmt.bufPrint(&here_buf, "out{d}", .{i})) == 0;
        if (on_this and here == null) here = try std.fmt.bufPrint(&here_buf, "out{d}", .{i});
        if (!on_this and there == null) there = try std.fmt.bufPrint(&there_buf, "out{d}", .{i});
    }
    var yaml_buf: [512]u8 = undefined;
    var why: [256]u8 = undefined;
    const Case = struct { kind: []const u8, namespace: []const u8, target: []const u8, refused: bool };
    for ([_]Case{
        .{ .kind = "stream", .namespace = "acme", .target = there.?, .refused = true },
        .{ .kind = "queue", .namespace = "acme", .target = there.?, .refused = true },
        .{ .kind = "stream", .namespace = "acme", .target = here.?, .refused = false },
        .{ .kind = "stream", .namespace = "default", .target = there.?, .refused = false },
        .{ .kind = "kv", .namespace = "acme", .target = "", .refused = false },
    }) |c| {
        const yaml = try std.fmt.bufPrint(&yaml_buf,
            \\kind: Processing
            \\name: j
            \\namespace: {s}
            \\sources:
            \\  - name: a
            \\    stream: {{ name: in }}
            \\sinks:
            \\  - name: out
            \\    {s}: {{ {s}{s} }}
        , .{ c.namespace, c.kind, if (c.target.len > 0) "name: " else "key_prefix: p", c.target });
        var def = try parser.parseJobDefinition(allocator, yaml, null);
        defer def.deinit(allocator);
        try std.testing.expectEqual(c.refused, handler.sinkShardRefusal(&def, &why) != null);
        // The same on replay: the job's pipelines are held, not run.
        if (c.refused) try std.testing.expect(handler.startPipelines("job-x", &def, &why).? == .held);
    }
}

test "ProcessingHandler: a two-source job short of memory at any point leaves nothing behind" {
    var def = try parser.parseJobDefinition(std.testing.allocator,
        \\kind: Processing
        \\name: two-sources
        \\sources:
        \\  - name: a
        \\    stream: { name: in-a }
        \\  - name: b
        \\    stream: { name: in-b }
        \\sinks:
        \\  - name: out
        \\    stream: { name: out }
    , null);
    defer def.deinit(std.testing.allocator);

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var handler = ProcessingHandler.init(failing.allocator());
        _ = handler.startPipelines("job-two", &def, &test_why_buf);
        const built = handler.pipelines.count();
        handler.deinit();
        if (!failing.has_induced_failure) {
            try std.testing.expectEqual(@as(u32, 2), built);
            break;
        }
    }
}

test "ProcessingHandler: a submit applied twice keeps one pipeline, under a key it owns" {
    const yaml =
        \\kind: Processing
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    for (0..2) |_| _ = try applySubmit(&handler, "job-twice", testSubmitValue(&buf, .running, yaml), 1);
    try std.testing.expectEqual(@as(usize, 1), handler.pipelines.count());
    try std.testing.expect(handler.pipelines.contains("job-twice"));
}

test "ProcessingHandler: starting a part-started job again keeps the pipelines it has, and a long job id starts every source" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    var def = try parser.parseJobDefinition(allocator,
        \\kind: Processing
        \\sources:
        \\  - name: a
        \\    stream: { name: in-a }
        \\  - name: b
        \\    stream: { name: in-b }
        \\sinks:
        \\  - name: out
        \\    stream: { name: out }
    , null);
    defer def.deinit(allocator);

    // Source 0's pipeline is missing, as if it failed to start.
    _ = handler.startPipelines("job-part", &def, &test_why_buf);
    const first = handler.pipelines.fetchRemove("job-part").?;
    var first_state = first.value;
    handler.freePipelineState(&first_state);
    allocator.free(first.key);
    const kept = handler.pipelines.getPtr("job-part\x001").?;

    _ = handler.startPipelines("job-part", &def, &test_why_buf);
    try std.testing.expectEqual(@as(u32, 2), handler.pipelines.count());
    try std.testing.expectEqual(kept, handler.pipelines.getPtr("job-part\x001").?);

    const long_id = comptime @import("stdx").repeat("j", 300);
    _ = handler.startPipelines(long_id, &def, &test_why_buf);
    try std.testing.expect(handler.pipelines.contains(long_id ++ "\x001"));
}

test "ProcessingHandler: a submit entry from before the layout byte is refused, not misread" {
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    // The old layout: [status=running][parallelism=1][batch_size=100][ns_len=7]["default"][yaml]
    var val: [128]u8 = undefined;
    val[0] = @backingInt(ProcessingHandler.JobStatus.running);
    std.mem.writeInt(u32, val[1..5], 1, .little);
    std.mem.writeInt(u32, val[5..9], 100, .little);
    std.mem.writeInt(u16, val[9..11], 7, .little);
    @memcpy(val[11..18], "default");
    const yaml = "kind: Processing\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n";
    @memcpy(val[18 .. 18 + yaml.len], yaml);
    const old = try applySubmit(&handler, "job-old", val[0 .. 18 + yaml.len], 1);
    try std.testing.expectEqual(proto.StatusCode.bad_request, old.refused().?.status);
    try std.testing.expectEqual(@as(usize, 0), handler.jobs.count());
}

test "ProcessingHandler: list shows a failed job's reason, through the walk every server answers list with" {
    const yaml =
        \\kind: Processing
        \\name: ranged
        \\sources:
        \\  - name: src
        \\    stream:
        \\      name: in
        \\      partitions: "0-1"
        \\sinks:
        \\  - name: out
        \\    stream:
        \\      name: out
    ;
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    _ = try applySubmit(&handler, "job-r", testSubmitValue(&buf, .running, yaml), 1);
    const reason = handler.jobs.get("job-r").?.failure_owned;
    try std.testing.expect(reason.len > 0);

    // A definition that doesn't parse has no name of its own: the job's id stands in.
    const ctxs = [_]*anyopaque{@ptrCast(&handler)};
    const answer = try ProcessingHandler.serializeWalkJobs(std.testing.allocator, &.{"job-r"}, null, &ctxs, "");
    defer std.testing.allocator.free(answer);
    var r = @import("../util/wire.zig").WireReader.init(answer);
    try std.testing.expectEqual(@as(u32, 1), r.readU32().?);
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    const status = r.readLengthPrefixed(u16).?;
    try std.testing.expect(std.mem.startsWith(u8, status, "FAILED: "));
    try std.testing.expectEqualStrings(reason, status["FAILED: ".len..]);
    try std.testing.expectEqual(@as(i64, 1), r.readI64().?);
}
