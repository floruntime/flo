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
const NamespaceId = @import("../namespace/table.zig").Id;
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
const table_run = @import("../node/table_run.zig");
const stream_handler_mod = @import("../stream/handler.zig");
const kv_handler_mod = @import("../kv/handler.zig");
const ns_keys = @import("../namespace/handler.zig");
const ts_handler_mod = @import("../ts/handler.zig");
const queue_handler_mod = @import("../queue/handler.zig");
const StreamID = @import("../projection/stream.zig").StreamID;
const Shard = shard_mod.Shard;
const raft_node_mod = @import("../raft/node.zig");
const Connection = connection_mod.Connection;

const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const queue_machine = @import("../queue/machine.zig");
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
    /// Sink writes proposed on this shard and not yet applied, every pipeline's.
    pending_sinks: usize = 0,
    /// Where the pending gauge is kept: a pipeline dropped with writes
    /// pending, off any tick, takes them off it there.
    pending_metrics: ?*MetricsRegistry = null,
    /// The term every pending write was proposed in.
    pending_term: u64 = 0,
    /// Sink writes refused for room at apply, whose records were read again.
    sink_rewinds: u64 = 0,
    sink_warn_ms: i64 = 0,
    chain_errors: u64 = 0,
    chain_warn_ms: i64 = 0,
    /// The job whose pipeline is ticking, so a drop or chain error names it.
    ticking_job: []const u8 = "",
    /// A sink write in this tick was refused because the shard can take no
    /// more writes for now: the pipeline stops and keeps its cursor, and the
    /// next tick reads the batch again (at-least-once), so nothing is dropped.
    sink_overloaded: bool = false,
    /// When a held pipeline was last said to be waiting (`noteHeld`).
    held_warn_ms: i64 = 0,

    /// Per-job pipeline execution state (keyed by same job_id as jobs map).
    pipelines: std.StringHashMap(PipelineState),

    /// Cross-shard stream handlers — one per shard, wired by Runtime.
    /// Enables pipelines to read/write streams on the shard that owns them.
    peer_stream_handlers: ?[]const *stream_handler_mod.StreamHandler = null,
    peer_shard_count: u16 = 1,
    /// The node's namespace table, wired by the shard: a sink routes by its
    /// namespace's id, as the dispatcher does.
    ns_table: ?*const @import("../namespace/table.zig").Table = null,

    /// KV handlers — one per shard, wired by Runtime.
    peer_kv_handlers: ?[]const *kv_handler_mod.KVHandler = null,
    /// The shard this handler runs on; kv_lookup reads only its KV.
    own_shard: u16 = 0,

    /// Cross-shard TS / queue handlers — one per shard, wired by Runtime.
    /// Enables TS and queue sinks to write to the shard that owns the target
    /// measurement / queue (matching how `ts read` / `queue dequeue` route).
    peer_ts_handlers: ?[]const *ts_handler_mod.TSHandler = null,
    peer_queue_handlers: ?[]const *queue_handler_mod.QueueHandler = null,

    /// Most jobs a server holds. Every held job counts, stopped or not: each
    /// keeps its definition, up to 256 KiB, until `processing.remove` drops
    /// it, so this bounds their memory. A submit past it is refused. A
    /// field only so tests can reach it.
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
    /// Where a pipeline reads again from: past this stream id, or from this
    /// time on.
    pub const Resume = union(enum) { stream: StreamID, ts: u64 };

    /// A sink write proposed and not yet applied.
    pub const PendingSink = struct {
        term: u64,
        index: u64,
        /// Where its record is read again from, should it be refused for room.
        again: Resume,
        kind: SinkKind,
        /// The sink's target, as its `SinkConfig` holds it.
        target: []const u8,
        /// What it holds against a queue's caps (0 for other sinks).
        held: u64,
    };

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
        /// Records read from the source since this pipeline started here.
        records_in: u64 = 0,
        /// Records emitted from the operator chain into the sink stage.
        records_out: u64 = 0,
        /// When this node started the pipeline, in Unix milliseconds.
        started_ms: i64 = 0,
        /// Records read on the last productive tick (vs batch_size → saturation).
        last_read_count: u32 = 0,
        /// Highest event-time watermark observed (0 = none / processing-time source).
        watermark_ms: i64 = 0,
        /// When its ticks began stopping for room in a sink's shard, or a
        /// sink write was refused for room at apply; 0 while they go through.
        held_since_ms: i64 = 0,
        /// Where the record whose sinks are being written is read again from.
        resume_here: Resume = .{ .ts = 0 },
        /// The sink writes this leader proposed on this shard that haven't
        /// applied: one refused at apply is read again or counted, never lost
        /// in silence. Each is a log entry not yet applied, so the log's cap
        /// on those (`sinkRoom`) bounds the list.
        pending: std.ArrayListUnmanaged(PendingSink) = .empty,
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
        /// The namespace its submit was stamped with: the one every request
        /// about the job must name.
        ns_id: NamespaceId,
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
    /// Routes as the dispatcher routes stream_append (`peerShardId`), so the
    /// stream's data is on the shard chosen.
    fn resolveStreamHandler(self: *ProcessingHandler, local: *stream_handler_mod.StreamHandler, stream_name: []const u8, namespace: []const u8) *stream_handler_mod.StreamHandler {
        const peers = self.peer_stream_handlers orelse return local;
        if (peers.len <= 1) return local;
        return peers[self.peerShardId(namespace, stream_name)];
    }

    /// Shard that owns `key` in `namespace`, by the router the dispatchers use
    /// for stream/kv/queue/ts commands. Sinks resolve through this so a write
    /// lands on the shard a subsequent read will query.
    fn peerShardId(self: *const ProcessingHandler, namespace: []const u8, key: []const u8) u16 {
        const r = router.Router.init(self.peer_shard_count, self.own_shard);
        return r.hashToShard(router.routeNsKey(self.namespaceIdOf(namespace), key));
    }

    /// The id of namespace `namespace` (empty is "default") as the node's
    /// table has it; `.invalid` for one it doesn't.
    fn namespaceIdOf(self: *const ProcessingHandler, namespace: []const u8) router.NamespaceId {
        const name = if (namespace.len == 0) "default" else namespace;
        if (std.mem.eql(u8, name, "default")) return .default;
        const t = self.ns_table orelse return .invalid;
        return t.lookup(name) orelse .invalid;
    }

    /// A stream or queue sink proposes its write on the shard that owns its
    /// target, from this shard's thread, without that shard's request gate.
    /// Outside "default" that shard may never have bound the namespace, and
    /// its log would then hold a write it cannot apply. Such a sink is
    /// refused until sinks write through the shard that owns their target.
    /// A namespace not yet published has no id to route by, so whether
    /// its target is here isn't known yet: `sinksReady` asks again once it
    /// is, before the pipeline writes anything.
    fn crossShardSink(self: *const ProcessingHandler, kind: definition.SinkKind, namespace: []const u8, target: []const u8) bool {
        if (kind != .stream and kind != .queue) return false;
        if (std.mem.eql(u8, namespace, "default")) return false;
        if (self.peer_shard_count <= 1) return false;
        if (self.namespaceIdOf(namespace) == .invalid) return false;
        return self.peerShardId(namespace, target) != self.own_shard;
    }

    const SinksReady = union(enum) { ready, wait, refused: []const u8 };

    /// Whether `pipe` may write now. Every sink routes by its namespace's
    /// published id: before shard 0 publishes it, a sink can't tell which
    /// shard owns its target, so the pipeline reads nothing and waits,
    /// its cursor where it was. Once every id is known, a stream or queue
    /// sink whose target is on another shard is refused, as at submit.
    fn sinksReady(self: *const ProcessingHandler, shard: *Shard, pipe: *const PipelineState, buf: []u8) SinksReady {
        var wait = false;
        for (pipe.sinks) |snk| {
            if (self.namespaceIdOf(snk.namespace) != .invalid) continue;
            // A namespace this shard refuses for good takes no write; its
            // records are dropped and counted as they come. One only shard
            // 0 can publish is waited for.
            if (shard.namespace_handler.admission(snk.namespace)) |r| if (r.status != .unavailable) continue;
            shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
            wait = true;
        }
        if (wait) return .wait;
        for (pipe.sinks) |snk| {
            if (!self.crossShardSink(snk.kind, snk.namespace, snk.target)) continue;
            return .{ .refused = std.fmt.bufPrint(buf, "a {s} sink outside the default namespace must be on the job's shard, and '{s}' is on another; use the default namespace, or one shard (--shards 1)", .{ @tagName(snk.kind), snk.target }) catch
                "a stream or queue sink outside the default namespace must be on the job's shard; use the default namespace, or one shard (--shards 1)" };
        }
        return .ready;
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
        if (peers.len <= 1 or self.peer_shard_count == 0) return local;
        const shard_id = self.peerShardId(namespace, measurement);
        return if (shard_id < peers.len) peers[shard_id] else local;
    }

    /// Resolve the queue handler that owns `queue_name` in `namespace` (for queue sinks).
    fn resolveQueueHandler(self: *ProcessingHandler, local: *queue_handler_mod.QueueHandler, namespace: []const u8, queue_name: []const u8) *queue_handler_mod.QueueHandler {
        const peers = self.peer_queue_handlers orelse return local;
        if (peers.len <= 1 or self.peer_shard_count == 0) return local;
        const shard_id = self.peerShardId(namespace, queue_name);
        return if (shard_id < peers.len) peers[shard_id] else local;
    }

    fn freeJobRecord(self: *ProcessingHandler, job: *JobRecord) void {
        self.allocator.free(job.job_id_owned);
        self.allocator.free(job.name_owned);
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
        const was = self.pending_sinks;
        self.pending_sinks -|= pipe.pending.items.len;
        if (self.pending_metrics) |m| m.processing.moveSinkPending(was, self.pending_sinks);
        pipe.pending.deinit(self.allocator);
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
        dispatcher.registerWithRoute(.processing_remove, table_run.handler("processing.remove", ProcessingHandler, runRemove), run_id_mod.preRouteByRunId);
    }

    /// Every submit goes to shard 0, whatever its key: one shard holds the
    /// job registry, so `max_jobs` bounds the server, and each job id
    /// carries that shard back for the ops that name it.
    fn preRouteByProcessing(_: Request) ?u64 {
        return 0;
    }

    /// ShardWalker LocalScanFn for processing_list: one shard's job ids.
    /// Ids, not names: jobs may share a name, and each is its own row.
    /// The list takes no filter: no client sends a key with it. It returns
    /// no cursor either, so the walker pages it by offset.
    fn localScanJobs(
        ctx: *anyopaque,
        ns: dispatcher_mod.WalkNamespace,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        const name_buf = dispatcher_mod.scanScratch(handler.jobs.count());

        var count: usize = 0;
        var it = handler.jobs.iterator();
        while (it.next()) |entry| {
            if (count >= name_buf.len) break;
            const job = entry.value_ptr;
            if (job.ns_id != ns.id) continue;
            name_buf[count] = job.job_id_owned;
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

    // ── Remove ───────────────────────────────────────────────────────────

    pub const Error = error{ NotFound, Running };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.NotFound => .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such processing job" } },
            error.Running => .{ .status = .conflict, .refusal = .{ .reason = .job_running, .ran = .no, .message = "job is running: stop or cancel it, then remove it" } },
        };
    }

    const RemoveResponse = table_run.op("processing.remove").response;

    /// Remove a job that no longer runs: every replica drops it, its slot
    /// and its memory, when the entry applies.
    fn runRemove(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(RemoveResponse) {
        const self = shard.processing_handler;
        const job = self.ownedJob(req.key, req.ns_id) orelse return error.NotFound;
        if (job.status == .running) return error.Running;
        const proposed = persistence_mod.proposeEntryIn(shard, .processing_remove, Flags.NONE, job.ns_id, req.key, "") catch |err|
            return .{ .refused = .{ .status = persistence_mod.failureStatus(err), .refusal = persistence_mod.failureRefusal(err, "remove not persisted") } };
        return .{ .park = .{ .proposed = proposed, .answer = answerRemove } };
    }

    fn answerRemove(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        table_run.answer(RemoveResponse, shard, conn, request_id, .{});
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
        if (self.operatorRefusal(&def)) |refused| switch (refused) {
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

        // The job id carries a slot this shard owns (its own number, as
        // slot n is on shard n), so every op routed by the id (status, stop,
        // cancel) finds the job where it was stored. Not the job name's slot:
        // that would be another shard's whenever submits all run here.
        var id_buf: [32]u8 = undefined;
        const job_id = shard.run_id_gen.next(.job, shard.id, &id_buf);

        // Persist through Raft; the applier builds the job record and its
        // pipelines from the entry — the same applier a restart uses.
        // Admitted at dispatch; the definition has now passed, so reserve.
        shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
        // A submit's key is empty, so dispatch neither creates its namespace
        // nor gives it an id when this submit is the one creating it; shard
        // 0, where every submit runs, knows the id it just proposed.
        const ns_id = if (req.ns_id != .invalid) req.ns_id else shard.namespaceId(req.namespace);
        const proposed = self.proposeSubmit(shard, ns_id, job_id, .running, def.batch_size, home, yaml) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "job not persisted"));
            return;
        };
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        shard.park(conn, req, proposed, answerSubmit);
    }

    fn refuseAtJobLimit(self: *const ProcessingHandler, shard: *Shard, conn: *Connection, request_id: u64) void {
        var buf: [192]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "job not submitted: this server holds at most {d} processing jobs; remove a stopped, cancelled or failed one with `flo processing remove <job_id>`", .{self.max_jobs}) catch "job not submitted: this server holds as many processing jobs as it may";
        shard.sendRefusal(conn, request_id, .overloaded, .{ .reason = .unclassified, .ran = .no, .message = msg });
    }

    // ── Stop ─────────────────────────────────────────────────────────────

    /// The job `job_id`, if it lives in namespace `ns`: a job is seen,
    /// stopped and changed only from its own namespace, so from any other it
    /// is not found.
    fn ownedJob(self: *ProcessingHandler, job_id: []const u8, ns: NamespaceId) ?*JobRecord {
        const job = self.jobs.getPtr(job_id) orelse return null;
        return if (job.ns_id == ns) job else null;
    }

    fn handleStop(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.ownedJob(job_id, req.ns_id)) |job| {
            const proposed = self.proposeStatusChange(shard, job.ns_id, job_id, .stopped) catch |err| {
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

        if (self.ownedJob(job_id, req.ns_id)) |job| {
            const proposed = self.proposeStatusChange(shard, job.ns_id, job_id, .cancelled) catch |err| {
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

        if (self.ownedJob(job_id, req.ns_id)) |job| {
            // Binary wire format:
            // [job_id_len:u16][job_id][name_len:u16][name][status:u8]
            // [batch_size:u32][records_processed:u64][created_at:i64]
            // [reason_len:u16][reason] — a failed job's reason, "held here:
            // <why>" for a running one this node holds, or empty —
            // then this node's runtime of the job, `Runtime`, and
            // [yaml_len:u32][yaml], the submitted definition.
            // Sized to the job. The id is 32 bytes and the name came from a
            // submit entry under 64 KiB, so both lengths fit u16.
            const note = noteOf(job);
            const runtime = self.runtimeOf(job.job_id_owned, @import("stdx").time.milliTimestamp());
            const size = 2 + job.job_id_owned.len + 2 + job.name_owned.len + 1 + 4 + 8 + 8 + 2 + note.reasonLen() + Runtime.SIZE + 4 + job.yaml_owned.len;
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
            runtime.write(w) catch unreachable;
            w.writeInt(u32, @intCast(job.yaml_owned.len), .little) catch unreachable;
            w.writeAll(job.yaml_owned) catch unreachable;
            shard.sendOkResponse(conn, req.header.request_id, fbs.buffered());
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    /// A job's pipelines on this node, summed: what they read and wrote
    /// since this node started them, and how long their ticks took. None
    /// of it is replicated, so it starts again with the node or a new
    /// leader.
    pub const Runtime = struct {
        running: bool = false,
        records_in: u64 = 0,
        records_out: u64 = 0,
        watermark_ms: i64 = 0,
        tick_avg_us: u64 = 0,
        tick_max_us: u64 = 0,
        /// Since the earliest of them started.
        uptime_ms: u64 = 0,

        /// `[running:u8][records_in:u64][records_out:u64][watermark_ms:i64]`
        /// `[tick_avg_us:u64][tick_max_us:u64][uptime_ms:u64]`
        pub const SIZE = 1 + 8 * 6;

        fn write(self: Runtime, w: *std.Io.Writer) std.Io.Writer.Error!void {
            try w.writeByte(@intFromBool(self.running));
            try w.writeInt(u64, self.records_in, .little);
            try w.writeInt(u64, self.records_out, .little);
            try w.writeInt(i64, self.watermark_ms, .little);
            try w.writeInt(u64, self.tick_avg_us, .little);
            try w.writeInt(u64, self.tick_max_us, .little);
            try w.writeInt(u64, self.uptime_ms, .little);
        }
    };

    /// Job `job_id`'s pipelines on this node: the job's id, or `id\0n`
    /// for each of a multi-source job's sources.
    fn runtimeOf(self: *const ProcessingHandler, job_id: []const u8, now_ms: i64) Runtime {
        var r: Runtime = .{};
        var started: i64 = std.math.maxInt(i64);
        var tick_sum: u64 = 0;
        var ticks: u64 = 0;
        var it = self.pipelines.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            const base = if (std.mem.indexOfScalar(u8, key, 0)) |nul| key[0..nul] else key;
            if (!std.mem.eql(u8, base, job_id)) continue;
            const p = e.value_ptr;
            r.running = true;
            r.records_in += p.records_in;
            r.records_out += p.records_out;
            r.watermark_ms = @max(r.watermark_ms, p.watermark_ms);
            r.tick_max_us = @max(r.tick_max_us, p.tick_ns_max / std.time.ns_per_us);
            tick_sum += p.tick_ns_sum;
            ticks += p.tick_count;
            started = @min(started, p.started_ms);
        }
        if (ticks > 0) r.tick_avg_us = tick_sum / ticks / std.time.ns_per_us;
        if (r.running) r.uptime_ms = @intCast(@max(0, now_ms - started));
        return r;
    }

    // ── List ─────────────────────────────────────────────────────────────

    fn handleList(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        // [limit:u32][cursor...]?
        var limit: u32 = 100;
        if (req.value.len >= 4) limit = std.mem.readInt(u32, req.value[0..4], .little);
        if (limit == 0) limit = 100;

        var jobs: std.ArrayListUnmanaged(*const JobRecord) = .empty;
        defer jobs.deinit(self.allocator);
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            if (jobs.items.len >= limit) break;
            if (entry.value_ptr.ns_id != req.ns_id) continue;
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

    /// The list answer, for a single shard and a walk alike:
    /// [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
    ///  [status_len:u16][status][created_at:i64])*[has_more:u8][cursor_len:u16][cursor]
    /// A failed job's status reads `FAILED: <reason>`. Ids are 32 bytes,
    /// names came from a submit entry under 64 KiB and reasons are bounded,
    /// so every length fits u16.
    fn encodeJobList(allocator: Allocator, jobs: []const *const JobRecord, next_cursor: ?[]const u8) ![]u8 {
        const cursor = next_cursor orelse "";
        var total: usize = 4 + 1 + 2 + cursor.len;
        for (jobs) |j| total += entrySize(j);
        const buf = try allocator.alloc(u8, total);
        var w: std.Io.Writer = .fixed(buf);
        w.writeInt(u32, @intCast(jobs.len), .little) catch unreachable;
        for (jobs) |j| writeEntry(j, &w) catch unreachable;
        w.writeByte(@intFromBool(next_cursor != null)) catch unreachable;
        w.writeInt(u16, @intCast(cursor.len), .little) catch unreachable;
        w.writeAll(cursor) catch unreachable;
        return buf;
    }

    pub fn entrySize(j: *const JobRecord) usize {
        return 2 + j.name_owned.len + 2 + j.job_id_owned.len + 2 + statusLen(j) + 8 + 4 + 8;
    }

    /// One job as the list answer lists it: `[name_len:u16][name][job_id_len:u16][job_id]`
    /// `[status_len:u16][status][created_at_ms:i64][batch_size:u32][records_processed:u64]`.
    pub fn writeEntry(j: *const JobRecord, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u16, @intCast(j.name_owned.len), .little);
        try w.writeAll(j.name_owned);
        try w.writeInt(u16, @intCast(j.job_id_owned.len), .little);
        try w.writeAll(j.job_id_owned);
        try w.writeInt(u16, @intCast(statusLen(j)), .little);
        writeStatus(j, w);
        try w.writeInt(i64, j.created_at_ms, .little);
        try w.writeInt(u32, j.batch_size, .little);
        try w.writeInt(u64, j.records_processed, .little);
    }

    /// The job `job_id` in namespace `ns`, if this shard holds it.
    pub fn listedJob(self: *const ProcessingHandler, ns: NamespaceId, job_id: []const u8) ?*const JobRecord {
        const job = self.jobs.getPtr(job_id) orelse return null;
        return if (job.ns_id == ns) job else null;
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
    fn proposeSubmit(self: *ProcessingHandler, shard: *Shard, ns_id: NamespaceId, job_id: []const u8, status: JobStatus, batch_size: u32, job_namespace: []const u8, yaml: []const u8) !persistence_mod.ProposeResult {
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

        return persistence_mod.proposeEntryIn(shard, .processing_submit, Flags.NONE, ns_id, job_id, value_buf[0..off]);
    }

    /// A processing_stop or processing_cancel entry. Key = job_id, value = [new_status:u8].
    fn proposeStatusChange(self: *ProcessingHandler, shard: *Shard, ns_id: NamespaceId, job_id: []const u8, status: JobStatus) !persistence_mod.ProposeResult {
        _ = self;
        const entry_type: EntryType = switch (status) {
            .stopped => .processing_stop,
            .cancelled => .processing_cancel,
            else => return error.NotATerminalStatus,
        };
        const value = &[_]u8{@backingInt(status)};
        return persistence_mod.proposeEntryIn(shard, entry_type, Flags.NONE, ns_id, job_id, value);
    }

    // ── Replay ──────────────────────────────────────────────────────────

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *ProcessingHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.processing_submit, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_stop, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_cancel, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_checkpoint, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_remove, @ptrCast(self), replayEntryThunk);
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
            .processing_submit => self.replaySubmit(cmd.key, cmd.value, cmd.namespace_id, stamp_ms, out),
            .processing_stop => self.replayStatusChange(cmd.key, .stopped, out),
            .processing_cancel => self.replayStatusChange(cmd.key, .cancelled, out),
            .processing_remove => self.replayRemove(cmd.key, cmd.namespace_id, out),
            .processing_checkpoint => self.replayCheckpoint(cmd.key, cmd.value),
            else => error.Halt,
        };
    }

    /// Replay a processing_submit entry. Rebuilds JobRecord from persisted bytes.
    /// Value format: [layout:u8][status:u8][batch_size:u32][ns_len:u16][namespace][yaml...]
    fn replaySubmit(self: *ProcessingHandler, key: []const u8, value: []const u8, ns_id: NamespaceId, created_at_ms: i64, out: *Result) persistence_mod.ApplyError!void {
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
            return out.refuseFmt(.overloaded, .unclassified, "job not submitted: this server holds at most {d} processing jobs; remove a stopped, cancelled or failed one with `flo processing remove <job_id>`", .{self.max_jobs});
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
        const owned_id = self.allocator.dupe(u8, job_id) catch {
            self.allocator.free(name);
            return error.OutOfMemory;
        };
        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
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
            .ns_id = ns_id,
            .status = status,
            .batch_size = batch_size,
            .yaml_owned = owned_yaml,
            .created_at_ms = created_at_ms,
            .records_processed = 0,
            .cursors = cursors,
        }) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(owned_yaml);
            cursors.deinit(self.allocator);
            return error.OutOfMemory;
        };
        if (status == .failed) self.failed_jobs += 1;
        const job = self.jobs.getPtr(owned_id).?;

        if (failure) |why| {
            self.failJob(job, why);
            // The job is stored, and keeps its slot until it is removed.
            return refuseStoredFailure(out, owned_id, job.failure_owned);
        }

        // Rebuild the execution pipeline so a RUNNING job resumes consuming
        // after restart instead of just reappearing in the registry.
        // Guard against a double build on idempotent replay. Its cursors
        // come from the record, and from replayCheckpoint for checkpoints
        // after this submit in log order.
        if (status == .running and self.pipelines.getPtr(owned_id) == null) {
            if (self.startPipelines(owned_id, &parsed.?, &why_buf)) |refused| switch (refused) {
                .failed => |why| self.failJob(job, why),
                .held => |why| self.holdJob(job, why),
            };
        }
        if (job.status == .failed) return refuseStoredFailure(out, owned_id, job.failure_owned);
        out.putBytes(owned_id);
    }

    /// A submit whose job applied as failed: refused, but the job is stored
    /// and keeps its slot until removed, so it ran.
    fn refuseStoredFailure(out: *persistence_mod.Result, job_id: []const u8, why: []const u8) void {
        var buf: [persistence_mod.Result.MESSAGE_MAX]u8 = undefined;
        const message = std.fmt.bufPrint(&buf, "processing job {s} failed: {s}; free its slot with `flo processing remove {s}`", .{ job_id, why, job_id }) catch buf[0..];
        out.refuseRan(.bad_request, .unclassified, .yes, message);
    }

    /// Replay a status change (stop/cancel). Only a running job changes:
    /// asking again for the status it has is a no-op that succeeds, and any
    /// other is refused, so a failed job keeps its status, its reason and
    /// its count. Checked here, not when proposed, since a stop and a
    /// cancel may both be in flight.
    fn replayStatusChange(self: *ProcessingHandler, key: []const u8, status: JobStatus, out: *Result) persistence_mod.ApplyError!void {
        // A stop or cancel taken before a remove applied lands after it.
        const job = self.jobs.getPtr(key) orelse return out.refuse(.not_found, .not_found, "not found: no such processing job");
        if (job.status == status) return;
        if (job.status != .running) return out.refuseFmt(.conflict, .unclassified, "job isn't running (it is {s})", .{@tagName(job.status)});
        self.setStatus(job, status);
    }

    /// Replay a processing_remove entry: drop the job and any pipelines it
    /// left. A running job stays, refused in log order on every replica; a
    /// job already gone was removed by an earlier entry.
    fn replayRemove(self: *ProcessingHandler, key: []const u8, ns_id: NamespaceId, out: *Result) persistence_mod.ApplyError!void {
        const job = self.jobs.getPtr(key) orelse return out.refuse(.not_found, .not_found, "not found: no such processing job");
        // The leader checked this too (ownedJob); every replica holds the
        // entry to it, so a remove never reaches another namespace's job.
        if (job.ns_id != ns_id) return out.refuse(.not_found, .not_found, "not found: no such processing job");
        if (job.status == .running) return out.refuse(.conflict, .job_running, "job is running: stop or cancel it, then remove it");
        self.dropPipelines(key);
        var removed = self.jobs.fetchRemove(key).?;
        if (removed.value.status == .failed) self.failed_jobs -= 1;
        self.freeJobRecord(&removed.value);
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
        if (self.operatorRefusal(def)) |refused| return switch (refused) {
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
        const entry = handlers[self.own_shard].*.kv.getIn(self.namespaceIdOf(namespace), key) orelse return null;
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
            .started_ms = @import("stdx").time.milliTimestamp(),
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

    /// The shard a sink writes to, or null when it can be any: a KV sink's
    /// keys spread across shards.
    fn sinkShard(self: *const ProcessingHandler, snk: SinkConfig) ?u16 {
        return switch (snk.kind) {
            .kv => if (self.peer_shard_count <= 1) self.own_shard else null,
            .stream, .queue => if (self.peer_shard_count <= 1) self.own_shard else self.peerShardId(snk.namespace, snk.target),
            // TS sinks write the owner's measurement, as stream sinks do.
            .ts => if (self.peer_shard_count <= 1) self.own_shard else self.peerShardId(snk.namespace, snk.measurement),
        };
    }

    /// Whether one of `pipe`'s sinks writes to a halted shard. Then the
    /// pipeline reads nothing, so its cursor stays at the first record
    /// not written. Writes already on their way are dropped and counted.
    fn sinksHeld(self: *const ProcessingHandler, shard: *const Shard, pipe: *const PipelineState) bool {
        for (pipe.sinks) |snk| {
            if (self.sinkShard(snk)) |to| {
                if (to != shard.id and shard.shardHalted(to)) return true;
            } else if (shard.peerHalted()) return true;
        }
        return false;
    }

    /// Drive all running pipelines: poll sources, push to sinks. Returns
    /// whether one was held for a halted sink shard.
    /// Called from Shard.tick() on each reactor iteration.
    pub fn tickPipelines(self: *ProcessingHandler, shard: *Shard) bool {
        var held = false;
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
            if (job.status != .running or job.held) continue;

            // Enforce poll interval
            const poll_ms: i64 = @intCast(pipe.src_poll_ms);
            if (now_ms - pipe.last_poll_ms < poll_ms) continue;
            var why_buf: [256]u8 = undefined;
            switch (self.sinksReady(shard, pipe, &why_buf)) {
                .ready => {},
                .wait => continue,
                .refused => |why| {
                    self.holdJob(job, why);
                    continue;
                },
            }
            if (self.sinksHeld(shard, pipe)) {
                held = true;
                continue;
            }
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
        return held;
    }

    /// Tick a TS source pipeline: read points from TSProjection, apply operators, write to sink.
    fn tickTsSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        var point_buf: [256]StoredPoint = undefined;
        // Tags are part of the series key now, so a configured tag set selects
        // the series directly instead of being scanned for after the fact (which
        // also wasted the point budget on non-matching points).
        const src_ns = shard.namespaceId(pipe.src_namespace);
        const result = shard.ts_handler.ts.queryRange(
            src_ns,
            pipe.src_measurement,
            pipe.src_field,
            if (pipe.src_tag_hash != 0) pipe.src_tag_hash else null,
            pipe.ts_cursor_ns,
            std.math.maxInt(u64),
            &point_buf,
        ) catch return;

        const room = sinkRoom(shard, pipe);
        // Room for none: the source waits, its cursor where it is. So it
        // does while a batch of its writes is still to apply.
        if (room == 0 or pipe.pending.items.len >= pipe.batch_size) return;
        const points = point_buf[0..@min(result.points_in_buffer, room)];
        if (points.len == 0) return;
        const cursor_before = pipe.ts_cursor_ns;
        self.sink_overloaded = false;

        // One point at a time: its JSON and what its chain allocates are
        // freed once its outputs are written.
        var arena = std.heap.ArenaAllocator.init(shard.allocator);
        defer arena.deinit();
        for (points) |pt| {
            defer _ = arena.reset(.retain_capacity);
            // Format TS point as JSON record. Short of memory, the tick
            // stops here, the point still ahead of the cursor.
            const ts_ms: i64 = @intCast(pt.timestamp_ns / 1_000_000);
            const json = tsPointJson(arena.allocator(), pipe.src_measurement, pt.field_value, ts_ms) catch break;
            pipe.resume_here = .{ .ts = pt.timestamp_ns };

            // Apply operator chain (or pass through directly). A record
            // the chain failed on is counted and skipped: passing it through
            // raw would write what the operators were there to change.
            const output_records = applyOperatorChain(pipe.operators, json, ts_ms, arena.allocator()) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };

            if (output_records) |records| {
                for (records) |rec| {
                    self.writeSinkRecord(pipe, shard, rec.value, pt.field_value, pt.timestamp_ns, shard.ts_handler.ts.tagsForHash(src_ns, pt.tag_hash), rec.tags);
                }
            } else {
                // No operators — direct passthrough
                self.writeSinkRecord(pipe, shard, json, pt.field_value, pt.timestamp_ns, shard.ts_handler.ts.tagsForHash(src_ns, pt.tag_hash), 0);
            }

            // A sink found its shard full: the next tick reads again from
            // this point's time. Points before it at that time are written
            // again; those before it are not.
            if (self.sink_overloaded) {
                self.sink_overloaded = false;
                pipe.ts_cursor_ns = @max(cursor_before, pt.timestamp_ns);
                self.noteHeld(shard, pipe);
                return;
            }

            // Advance cursor past this point
            if (pt.timestamp_ns >= pipe.ts_cursor_ns) {
                pipe.ts_cursor_ns = pt.timestamp_ns + 1;
            }

            // Event-time watermark = highest source point timestamp seen.
            if (ts_ms > pipe.watermark_ms) pipe.watermark_ms = ts_ms;

            job.records_processed += 1;
            pipe.records_in += 1;
        }
        // Still held while a write it proposed may be refused for room.
        if (pipe.pending.items.len == 0) pipe.held_since_ms = 0;
        // As a stream source does: a restart resumes here, not from the
        // measurement's first point.
        maybePersistCheckpoint(pipeline_key, pipe, shard, job.ns_id);
    }

    /// Tick a stream source pipeline: read payloads from stream, apply operators, write to sink.
    fn tickStreamSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        log.debug("TICK_ENTRY: src={s} ns={s} cursor_ts={d} cursor_seq={d}", .{ pipe.src_stream, pipe.src_namespace, pipe.stream_cursor_ts, pipe.stream_cursor_seq });
        // Build cursor from last-read StreamID
        const cursor = StreamID{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };

        // Resolve the stream handler that owns the source stream (may be on a different shard)
        const src_handler = self.resolveStreamHandler(shard.stream_handler, pipe.src_stream, pipe.src_namespace);

        // As many as this shard can write out now; with room for none the
        // source waits, its cursor where it is.
        const room = sinkRoom(shard, pipe);
        if (room == 0 or pipe.pending.items.len >= pipe.batch_size) return;
        // Read from the named source stream using namespace-qualified name-hash filtering
        const result = src_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, cursor, @min(pipe.batch_size, room));
        self.sink_overloaded = false;
        log.debug("TICK: read {d} payloads, last_id ts={d} seq={d}", .{ result.payloads.len, result.last_id.timestamp_ms, result.last_id.sequence });
        defer result.deinit(src_handler.allocator);
        if (result.payloads.len == 0) {
            // Appends that couldn't be read still move the cursor past them
            // (the stream handler logged them), so the source doesn't stall.
            pipe.stream_cursor_ts = result.last_id.timestamp_ms;
            pipe.stream_cursor_seq = result.last_id.sequence;
            return;
        }

        // One payload's chain at a time: what it allocates is freed once its
        // outputs are written.
        var arena = std.heap.ArenaAllocator.init(shard.allocator);
        defer arena.deinit();
        for (result.payloads, 0..) |payload, i| {
            defer _ = arena.reset(.retain_capacity);
            pipe.resume_here = .{ .stream = if (i > 0) result.ids[i - 1] else cursor };
            log.debug("tickStreamSource: payload len={d} first100='{s}'", .{ payload.len, if (payload.len > 100) payload[0..100] else payload });
            const output_records = applyOperatorChain(pipe.operators, payload, @import("stdx").time.milliTimestamp(), arena.allocator()) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };

            if (output_records) |records| {
                log.debug("TICK: chain returned {d} records for job", .{records.len});
                for (records) |rec| {
                    log.debug("TICK: value_len={d} first50='{s}'", .{ rec.value.len, if (rec.value.len > 50) rec.value[0..50] else rec.value });
                    self.writeSinkRecordFromStream(pipe, shard, rec.key, rec.value, rec.tags);
                }
            } else {
                log.debug("tickStreamSource: no operators, passthrough", .{});
                self.writeSinkRecordFromStream(pipe, shard, "", payload, 0);
            }

            // A sink found its shard full: the next tick reads again from
            // this record, every sink of those before it written.
            if (self.sink_overloaded) {
                self.sink_overloaded = false;
                if (i > 0) {
                    pipe.stream_cursor_ts = result.ids[i - 1].timestamp_ms;
                    pipe.stream_cursor_seq = result.ids[i - 1].sequence;
                }
                self.noteHeld(shard, pipe);
                return;
            }

            job.records_processed += 1;
            pipe.records_in += 1;
        }

        // Advance cursor to last StreamID read
        pipe.stream_cursor_ts = result.last_id.timestamp_ms;
        pipe.stream_cursor_seq = result.last_id.sequence;
        // Still held while a write it proposed may be refused for room.
        if (pipe.pending.items.len == 0) pipe.held_since_ms = 0;

        // Persist the advanced cursor durably (throttled) so the pipeline
        // resumes from here after restart instead of re-reading from the start.
        // At-least-once: a crash loses at most CHECKPOINT_INTERVAL_MS
        // of progress, which redelivers — it never skips records.
        maybePersistCheckpoint(pipeline_key, pipe, shard, job.ns_id);
    }

    /// Minimum wall-clock gap between durable cursor checkpoints per pipeline.
    /// Bounds Raft write amplification; a crash loses at most this much progress
    /// (redelivered on resume — at-least-once, never skipped).
    const CHECKPOINT_INTERVAL_MS: i64 = 1000;

    /// Persist a pipeline's source cursor as a processing_checkpoint UAL entry,
    /// debounced. Key = pipeline_key (job_id, or job_id\0idx for multi-source);
    /// value as `replayCheckpoint` reads it.
    fn maybePersistCheckpoint(pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, ns_id: NamespaceId) void {
        var a: u64, var b: u64 = switch (pipe.source_kind) {
            .stream => .{ pipe.stream_cursor_ts, pipe.stream_cursor_seq },
            .ts => .{ pipe.ts_cursor_ns, 0 },
        };
        // Never past a record whose sink write may yet be refused.
        if (earliestPending(pipe)) |at| switch (at) {
            .stream => |id| if (id.lessThan(.{ .timestamp_ms = a, .sequence = b })) {
                a = id.timestamp_ms;
                b = id.sequence;
            },
            .ts => |t| a = @min(a, t),
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
        _ = persistence_mod.proposeEntryIn(shard, .processing_checkpoint, Flags.NONE, ns_id, pipeline_key, &val_buf) catch |err| {
            if (!shard.quietHalted(err)) log.err("pipeline {s}: checkpoint not persisted: {s}", .{ pipeline_key, @errorName(err) });
            if (shard.metrics_registry) |m| m.processing.recordError();
            return;
        };
        if (shard.metrics_registry) |m| m.processing.recordCheckpoint();
    }

    /// The operator chain over one input value: its output records, or
    /// null with no operators. Everything the chain allocates, the records
    /// and what they point at, is in `arena`, which the caller frees (or
    /// resets) once it has written them; a record may also point into
    /// `value`, which must live as long.
    fn applyOperatorChain(operators: []Operator, value: []const u8, event_time_ms: i64, arena: Allocator) !?[]const ProcessingRecord {
        if (operators.len == 0) return null;
        var collector = OutputCollector.init(arena);
        var metrics = OperatorMetrics{};
        var ctx = OperatorContext{
            .collector = &collector,
            .metrics = &metrics,
            .allocator = arena,
            .current_processing_time_ms = @import("stdx").time.milliTimestamp(),
            .operator_name = "",
        };
        try operators[0].processElement(ProcessingRecord.fromValue(value, event_time_ms), &ctx);
        // Each operator takes the one before's output; a staged record and
        // what it points at stay in the arena, so one passed through, still
        // pointing at them, stays good.
        for (operators[1..]) |op| {
            const staged = try collector.records.toOwnedSlice(arena);
            for (staged) |rec| try op.processElement(rec, &ctx);
        }
        return collector.records.items;
    }

    /// A record a sink could not write is lost to that sink (the cursor
    /// has moved past it); said once per interval with the count.
    /// A sink write found the shard full: stop the tick here.
    fn holdForRoom(self: *ProcessingHandler) void {
        self.sink_overloaded = true;
    }

    /// A tick stopped for room. A pipeline held this long by the same full
    /// shard is said so, once per interval, as a stuck one would be.
    fn noteHeld(self: *ProcessingHandler, shard: *Shard, pipe: *PipelineState) void {
        if (shard.metrics_registry) |m| m.processing.recordHeldTick();
        const now = @import("stdx").time.milliTimestamp();
        if (pipe.held_since_ms == 0) pipe.held_since_ms = now;
        const held_for = now - pipe.held_since_ms;
        if (held_for < HELD_WARN_MS or now - self.held_warn_ms < 30_000) return;
        self.held_warn_ms = now;
        log.warn("job '{s}': a pipeline has waited {d} s for room in a sink's shard", .{ self.ticking_job, @divTrunc(held_for, 1000) });
    }

    const HELD_WARN_MS: i64 = 30_000;

    /// How many source records a tick may take now: the writes this shard
    /// can still propose before its cap, shared among the pipeline's sinks
    /// that propose here. An operator that emits several records per input
    /// can still exceed it; that write is held (`holdForRoom`), not dropped.
    fn sinkRoom(shard: *const Shard, pipe: *const PipelineState) usize {
        const raft = shard.raft_node;
        const used = raft.log.lastIndex() -| raft.last_applied;
        // One spare, for a sink namespace's implicit create.
        const room = raft_node_mod.MAX_OUTSTANDING -| used -| 1;
        var proposing: usize = 0;
        var kv_sinks: usize = 0;
        for (pipe.sinks) |snk| {
            if (snk.kind != .ts) proposing += 1;
            if (snk.kind == .kv) kv_sinks += 1;
        }
        const log_room = room / @max(proposing, 1);
        // A KV sink's write for a key on another shard goes through that
        // shard's inbox, which no larger batch fits: a send it refuses holds
        // the tick, and a batch that never fits would be refused at the same
        // record every tick.
        const mailboxes = shard.peer_mailboxes orelse return log_room;
        if (kv_sinks == 0 or mailboxes.len <= 1) return log_room;
        var inbox_room: usize = std.math.maxInt(usize);
        for (mailboxes, 0..) |mb, i| {
            if (i == shard.id) continue;
            inbox_room = @min(inbox_room, mb.inbox.capacity -| mb.inbox.pending());
        }
        return @min(log_room, inbox_room / kv_sinks);
    }

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
        if (self.sink_overloaded) return;
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            // A sink held the record: it is read again, so the sinks after
            // this one take it then, not twice.
            if (self.sink_overloaded) return;
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
                    // only to "default" (`crossShardSink`, `sinksReady`).
                    if (sink_handler != shard.stream_handler and !std.mem.eql(u8, snk.namespace, "default")) {
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, "the stream is on another shard");
                        continue;
                    }
                    if (sink_handler == shard.stream_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
                    const proposed = sink_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch |err| {
                        if (err == error.Overloaded) return self.holdForRoom();
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, @errorName(err));
                        continue;
                    };
                    // Another shard's log: refused there, its leader counts it.
                    if (sink_handler == shard.stream_handler) self.notePending(shard, pipe, proposed, .stream, snk.target, 0);
                },
                .ts => {
                    const ts_h = self.resolveTsHandler(shard.ts_handler, snk.namespace, snk.measurement);
                    const ual_idx = ts_h.nextUalIndex();
                    ts_h.ts.insert(
                        shard.namespaceId(snk.namespace),
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
                .kv => self.writeKvSink(pipe, snk, shard, "", payload),
                .queue => self.writeQueueSink(pipe, snk, shard, payload),
            }
        }
    }

    /// Write a processed record to all matching sinks (stream source variant).
    /// `record_key` is the record's partitioning key (set by `keyby`, empty otherwise);
    /// it is used to construct the KV sink key.
    fn writeSinkRecordFromStream(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, record_key: []const u8, payload: []const u8, record_tags: u32) void {
        if (self.sink_overloaded) return;
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            // A sink held the record: it is read again, so the sinks after
            // this one take it then, not twice.
            if (self.sink_overloaded) return;
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
                    // only to "default" (`crossShardSink`, `sinksReady`).
                    if (sink_handler != shard.stream_handler and !std.mem.eql(u8, snk.namespace, "default")) {
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, "the stream is on another shard");
                        continue;
                    }
                    if (sink_handler == shard.stream_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
                    const proposed = sink_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch |err| {
                        if (err == error.Overloaded) return self.holdForRoom();
                        self.noteSinkDrop(shard.metrics_registry, "stream", snk.target, @errorName(err));
                        continue;
                    };
                    // Another shard's log: refused there, its leader counts it.
                    if (sink_handler == shard.stream_handler) self.notePending(shard, pipe, proposed, .stream, snk.target, 0);
                },
                .ts => self.writeTsSinkFromJson(snk, shard, payload),
                .kv => self.writeKvSink(pipe, snk, shard, record_key, payload),
                .queue => self.writeQueueSink(pipe, snk, shard, payload),
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
                ts_h.ts.insert(shard.namespaceId(snk.namespace), snk.measurement, pair.key, value, now_ns, ual_idx, sink_tags) catch |err| {
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
        ts_h.ts.insert(shard.namespaceId(snk.namespace), snk.measurement, "value", value, now_ns, ual_idx, sink_tags) catch |err| self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, @errorName(err));
    }

    const MAX_SINK_KEY = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

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
    /// from. A full inbox holds the source as a full log does, so the batch
    /// is read and sent again.
    fn writeKvSink(self: *ProcessingHandler, pipe: *PipelineState, snk: SinkConfig, shard: *Shard, record_key: []const u8, payload: []const u8) void {
        var key_buf: [MAX_SINK_KEY]u8 = undefined;
        const full_key = self.kvSinkKeyOrDrop(&key_buf, snk, shard.metrics_registry, record_key) orelse return;
        // As a client's key: the CLI and SDKs have never written one with NUL.
        if (std.mem.indexOfScalar(u8, full_key, 0) != null) return self.noteSinkDrop(shard.metrics_registry, "kv", full_key, "key must not contain NUL");
        const owner = self.kvOwner(shard, snk.namespace, full_key);
        if (owner == shard.id) {
            switch (kv_handler_mod.KVHandler.sinkPropose(shard, snk.namespace, full_key, payload, snk.kv_ttl_ms)) {
                .proposed => |p| self.notePending(shard, pipe, p, .kv, snk.target, 0),
                .held => self.holdForRoom(),
                .dropped => |why| self.noteSinkDrop(shard.metrics_registry, "kv", full_key, why),
            }
            return;
        }
        const message = (kv_handler_mod.KVHandler.SinkPut{ .namespace = snk.namespace, .key = full_key, .value = payload, .ttl_ms = snk.kv_ttl_ms }).encode(shard.allocator) orelse
            return self.noteSinkDrop(shard.metrics_registry, "kv", full_key, "out of memory");
        if (!shard.peer_mailboxes.?[owner].inbox.send(.{ .tag = .kv_sink_put, .src_shard = @intCast(shard.id), .payload_len = @intCast(message.len), .payload_ptr = message.ptr })) {
            shard.allocator.free(message);
            if (shard.shard_metrics) |sm| sm.recordCrossShardOverloaded(.engine);
            self.holdForRoom();
        }
    }

    /// The shard that owns KV `key` in `namespace`: this one when there are
    /// no others to send to.
    pub fn kvOwner(self: *const ProcessingHandler, shard: *const Shard, namespace: []const u8, key: []const u8) u16 {
        const mailboxes = shard.peer_mailboxes orelse return @intCast(shard.id);
        if (mailboxes.len <= 1 or self.peer_shard_count == 0) return @intCast(shard.id);
        const owner = self.peerShardId(namespace, key);
        return if (owner < mailboxes.len) owner else @intCast(shard.id);
    }

    /// Queue sink: enqueue the record payload into the configured queue (durable,
    /// via the queue handler's Raft-backed enqueue path), routed to the shard that
    /// owns the queue so a later `queue dequeue` finds it.
    fn writeQueueSink(self: *ProcessingHandler, pipe: *PipelineState, snk: SinkConfig, shard: *Shard, payload: []const u8) void {
        var opt_buf: [64]u8 = undefined;
        var ob = proto.OptionsBuilder.init(&opt_buf);
        if (snk.queue_priority > 0) ob.addU8(.priority, snk.queue_priority) catch {};

        const req = proto.Request{
            .header = makeSinkHeader(.queue_enqueue),
            .namespace = snk.namespace,
            // The enqueue registers the queue under this id, as the applier
            // files its messages: published before any sink writes
            // (`sinksReady`).
            .ns_id = self.namespaceIdOf(snk.namespace),
            .key = snk.target, // queue name
            .value = payload,
            .options = opt_buf[0..ob.offset],
        };
        const q = self.resolveQueueHandler(shard.queue_handler, snk.namespace, snk.target);
        // The applier labels the queue by resolving its namespace; only
        // this shard's handler may be touched from this thread, and a sink
        // on another shard writes only to "default" (`crossShardSink`,
        // `sinksReady`).
        if (q != shard.queue_handler and !std.mem.eql(u8, snk.namespace, "default"))
            return self.noteSinkDrop(shard.metrics_registry, "queue", snk.target, "the queue is on another shard");
        if (q == shard.queue_handler) shard.namespace_handler.proposeImplicitCreate(snk.namespace, shard);
        // Past the queue's caps, counting what this pipeline has proposed
        // to it and not yet applied, the enqueue would be refused at apply:
        // the source waits here instead.
        const held = queue_machine.heldBy(payload.len, 0);
        if (q == shard.queue_handler) {
            var msgs: u64 = 0;
            var bytes: u64 = 0;
            for (pipe.pending.items) |p| if (p.kind == .queue and std.mem.eql(u8, p.target, snk.target)) {
                msgs += 1;
                bytes += p.held;
            };
            if (!queue_handler_mod.sinkWouldFit(q.caps, shard, req.ns_id, snk.target, payload.len, msgs, bytes)) return self.holdForRoom();
        }
        const result = q.sinkEnqueue(req);
        defer q.freeResult(result);
        switch (result) {
            .err => |e| {
                if (e.code == .overloaded) return self.holdForRoom();
                self.noteSinkDrop(shard.metrics_registry, "queue", snk.target, e.message);
            },
            // Another shard's log: refused there, its leader counts it.
            .parked => |p| if (q == shard.queue_handler) self.notePending(shard, pipe, p, .queue, snk.target, held),
            else => {},
        }
    }

    /// A sink write this shard's log took: kept until it applies. One the
    /// list can't take is read again at once rather than go unwatched.
    pub fn notePending(self: *ProcessingHandler, shard: *Shard, pipe: *PipelineState, proposed: persistence_mod.ProposeResult, kind: SinkKind, target: []const u8, held: u64) void {
        // The first write of a new term: those of earlier terms were lost
        // with the leadership, or apply under their own term unwatched.
        // Either way their records are read again, and none is left to
        // count against a batch.
        if (proposed.term != self.pending_term) {
            self.rewindEveryPending(shard);
            self.pending_term = proposed.term;
        }
        pipe.pending.append(self.allocator, .{ .term = proposed.term, .index = proposed.index, .again = pipe.resume_here, .kind = kind, .target = target, .held = held }) catch {
            rewind(pipe, pipe.resume_here);
            self.holdForRoom();
            return;
        };
        self.pending_sinks += 1;
        self.pending_metrics = shard.metrics_registry;
        if (shard.metrics_registry) |m| m.processing.moveSinkPending(self.pending_sinks - 1, self.pending_sinks);
    }

    fn rewindEveryPending(self: *ProcessingHandler, shard: *Shard) void {
        if (self.pending_sinks == 0) return;
        const was = self.pending_sinks;
        var it = self.pipelines.valueIterator();
        while (it.next()) |pipe| {
            for (pipe.pending.items) |p| self.rewindHeld(shard, pipe, p.again);
            pipe.pending.clearRetainingCapacity();
        }
        self.pending_sinks = 0;
        if (shard.metrics_registry) |m| m.processing.moveSinkPending(was, 0);
    }

    /// The pipeline reads again from `at`, if that is before where it is.
    fn rewind(pipe: *PipelineState, at: Resume) void {
        switch (at) {
            .stream => |id| {
                const cur: StreamID = .{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };
                if (id.lessThan(cur)) {
                    pipe.stream_cursor_ts = id.timestamp_ms;
                    pipe.stream_cursor_seq = id.sequence;
                }
            },
            .ts => |t| pipe.ts_cursor_ns = @min(pipe.ts_cursor_ns, t),
        }
    }

    /// The earliest place a pending write's record is read again from: no
    /// checkpoint may pass it, or a restart would resume past a record still
    /// to be refused.
    fn earliestPending(pipe: *const PipelineState) ?Resume {
        var out: ?Resume = null;
        for (pipe.pending.items) |p| {
            const o = out orelse {
                out = p.again;
                continue;
            };
            out = switch (p.again) {
                .stream => |id| if (id.lessThan(o.stream)) p.again else o,
                .ts => |t| if (t < o.ts) p.again else o,
            };
        }
        return out;
    }

    /// A sink write applied, on any replica: the one this leader proposed is
    /// settled. Applied, it is done and the pipeline isn't held; refused for
    /// room, its record is read again (at least once: records after it may
    /// go twice); refused otherwise, it is counted dropped. An entry at its
    /// index from another term means the write was lost to a leader change:
    /// read again. Only ever this node's own bookkeeping and new proposals;
    /// applied state is untouched.
    ///
    /// A sink write no pipeline here is watching came from a pipeline on
    /// another shard, which can't see this apply, or one removed since:
    /// refused, it is lost, and this shard's leader counts it.
    pub fn afterApply(self: *ProcessingHandler, shard: *Shard, entry: *const entry_mod.Entry, result: *const persistence_mod.Result) void {
        const watched = self.settlePending(shard, entry, result);
        if (watched or entry.header.flags & Flags.SINK == 0) return;
        const r = result.refused() orelse return;
        const raft = shard.raft_node;
        if (raft.role != .leader or entry.header.term != raft.current_term) return;
        const kind: []const u8 = switch (@as(EntryType, @fromBackingInt(@intCast(entry.header.entry_type)))) {
            .queue_enqueue => "queue",
            .stream_append => "stream",
            else => "kv",
        };
        self.noteSinkDrop(shard.metrics_registry, kind, "(not watched here: another shard's pipeline, or one since removed)", r.refusal.message);
    }

    /// Settles each pipeline's pending writes up to this entry's index;
    /// true if one of them is this entry.
    fn settlePending(self: *ProcessingHandler, shard: *Shard, entry: *const entry_mod.Entry, result: *const persistence_mod.Result) bool {
        if (self.pending_sinks == 0) return false;
        const was = self.pending_sinks;
        defer if (shard.metrics_registry) |m| m.processing.moveSinkPending(was, self.pending_sinks);
        const index = entry.header.index;
        var watched = false;
        var it = self.pipelines.valueIterator();
        while (it.next()) |pipe| {
            // Every one at or before this index is settled, wherever it
            // sits: the list is a batch or two long.
            var keep: usize = 0;
            for (pipe.pending.items) |p| {
                if (p.index > index) {
                    pipe.pending.items[keep] = p;
                    keep += 1;
                    continue;
                }
                if (p.index < index or p.term != entry.header.term) {
                    self.rewindHeld(shard, pipe, p.again);
                    continue;
                }
                watched = true;
                if (result.refused()) |r| {
                    if (r.status == .overloaded) {
                        self.rewindHeld(shard, pipe, p.again);
                    } else self.noteSinkDrop(shard.metrics_registry, @tagName(p.kind), p.target, r.refusal.message);
                } else pipe.held_since_ms = 0;
            }
            self.pending_sinks -= pipe.pending.items.len - keep;
            pipe.pending.shrinkRetainingCapacity(keep);
        }
        return watched;
    }

    /// A write refused for room at apply: its record is read again, and the
    /// pipeline counts as held until a write of it applies.
    fn rewindHeld(self: *ProcessingHandler, shard: *Shard, pipe: *PipelineState, at: Resume) void {
        rewind(pipe, at);
        self.sink_rewinds += 1;
        if (shard.metrics_registry) |m| m.processing.recordSinkRewind();
        if (pipe.held_since_ms == 0) pipe.held_since_ms = @import("stdx").time.milliTimestamp();
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
    try h.replaySubmit(key, value, .default, created_at_ms, &out);
    return out;
}

fn applyLogged(h: *ProcessingHandler, e: *const entry_mod.Entry) persistence_mod.ApplyError!Result {
    var out = Result.start(e);
    try h.replayEntry(e, &out);
    return out;
}

test "processing: a sink's shard is its target's, any for a KV sink, and its own on one shard" {
    var h = ProcessingHandler.init(std.testing.allocator);
    defer h.deinit();
    h.own_shard = 1;
    try std.testing.expectEqual(@as(?u16, 1), h.sinkShard(.{ .kind = .kv }));
    h.peer_shard_count = 4;
    try std.testing.expectEqual(@as(?u16, null), h.sinkShard(.{ .kind = .kv }));
    try std.testing.expectEqual(@as(?u16, h.peerShardId("default", "cpu")), h.sinkShard(.{ .kind = .ts, .namespace = "default", .measurement = "cpu" }));
    const snk: ProcessingHandler.SinkConfig = .{ .kind = .stream, .target = "out", .namespace = "default" };
    try std.testing.expectEqual(@as(?u16, h.peerShardId("default", "out")), h.sinkShard(snk));
}

test "ProcessingHandler: all opcodes are registered" {
    const Dispatch = @import("../node/dispatcher.zig").Dispatcher;
    var d = Dispatch.init();
    ProcessingHandler.register(&d);

    try std.testing.expect(d.hasHandler(proto.OpCode.processing_submit));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_stop));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_cancel));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_status));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_list));
    try std.testing.expect(d.hasHandler(proto.OpCode.processing_remove));
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
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const result = try ProcessingHandler.applyOperatorChain(&ops, "test-value", 42, arena.allocator());
    try std.testing.expect(result != null);
    const records = result.?;

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
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const result = try ProcessingHandler.applyOperatorChain(&ops, "some-data", 0, arena.allocator());
    try std.testing.expect(result != null);
    const records = result.?;
    // key_not_empty filter rejects records with empty key
    try std.testing.expectEqual(@as(usize, 0), records.len);
}

test "ProcessingHandler: a flatmap, map, pass-through and aggregate chain frees all it allocates, over many inputs" {
    // Operators and the chain's arena on the leak-checking allocator: an
    // output left on an operator's own allocator leaks, and one freed twice
    // is caught.
    const allocator = std.testing.allocator;
    const Entry = definition.OperatorSpec.ConfigEntry;
    const flatmap_cfg = [_]Entry{ .{ .key = "array_field", .value = "$.items" }, .{ .key = "element_key", .value = "$.k" } };
    const map_cfg = [_]Entry{.{ .key = "v", .value = "$.v" }};
    const aggregate_cfg = [_]Entry{.{ .key = "function", .value = "count" }};
    var specs = [_]definition.OperatorSpec{
        .{ .type_name = "flatmap", .name = "fm", .config = &flatmap_cfg },
        .{ .type_name = "map", .name = "m", .config = &map_cfg },
        .{ .type_name = "passthrough", .name = "p", .config = null },
        .{ .type_name = "aggregate", .name = "a", .config = &aggregate_cfg },
    };
    var made: [specs.len]native_registry.CreateResult = undefined;
    var ops: [specs.len]Operator = undefined;
    var n: usize = 0;
    defer for (made[0..n]) |*m| m.deinit(allocator);
    for (&specs, &made, &ops) |*spec, *m, *op| {
        m.* = try native_registry.create(allocator, spec, null);
        n += 1;
        op.* = m.op;
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const input = "{\"items\":[{\"k\":\"a\",\"v\":1},{\"k\":\"b\",\"v\":2},{\"k\":\"a\",\"v\":3}]}";
    for (0..1000) |i| {
        defer _ = arena.reset(.retain_capacity);
        const records = (try ProcessingHandler.applyOperatorChain(&ops, input, @intCast(i), arena.allocator())).?;
        // One aggregate result per element, keyed by its element's key.
        try std.testing.expectEqual(@as(usize, 3), records.len);
        try std.testing.expectEqualStrings("a", records[2].key);
        if (i == 999) try std.testing.expect(std.mem.indexOf(u8, records[2].value, "\"count\":2000") != null);
    }
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
        _ = try applyLogged(&handler, &entry_mod.buildCommandEntry(.processing_submit, 0, 1, 1, @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms, .default, "job-1", value[0 .. 15 + yaml.len], &pbuf).?);
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
    // Cancelling it is refused: it stays failed, and counted.
    var out: Result = .{};
    try handler.replayStatusChange("job-r", .cancelled, &out);
    try std.testing.expectEqualStrings("job isn't running (it is failed)", out.refused().?.refusal.message);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, job.status);
    try std.testing.expectEqual(@as(u64, 1), handler.failed_jobs);
}

test "ProcessingHandler: stopping a stopped job succeeds and changes nothing; cancelling it is refused" {
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    _ = try applySubmit(&handler, "job-s", testSubmitValue(&buf, .running, "kind: Processing\nname: s\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n"), 1);
    var out: Result = .{};
    try handler.replayStatusChange("job-s", .stopped, &out);
    try std.testing.expect(out.refused() == null);

    // A second stop, raced with the first, is a no-op.
    out = .{};
    try handler.replayStatusChange("job-s", .stopped, &out);
    try std.testing.expect(out.refused() == null);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.stopped, handler.jobs.get("job-s").?.status);

    out = .{};
    try handler.replayStatusChange("job-s", .cancelled, &out);
    const refused = out.refused().?;
    try std.testing.expect(refused.status == .conflict);
    try std.testing.expectEqualStrings("job isn't running (it is stopped)", refused.refusal.message);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.stopped, handler.jobs.get("job-s").?.status);
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
        const entry = entry_mod.buildCommandEntry(.processing_submit, 0, 1, 1, 0, .default, c[0], value, &pbuf).?;
        var out = Result.start(&entry);
        try handler.replayEntry(&entry, &out);
        try std.testing.expectEqual(c[1], out.refused() != null);
        try std.testing.expectEqual(@as(usize, 1), handler.jobs.count());
    }
    try std.testing.expect(!handler.jobs.contains("job-2"));
}

/// Applies a keyed entry of `etype` for job `key`, as the log does.
fn applyKeyed(h: *ProcessingHandler, etype: EntryType, key: []const u8, value: []const u8) persistence_mod.ApplyError!Result {
    return applyKeyedIn(h, etype, .default, key, value);
}

fn applyKeyedIn(h: *ProcessingHandler, etype: EntryType, ns_id: NamespaceId, key: []const u8, value: []const u8) persistence_mod.ApplyError!Result {
    var pbuf: [1024]u8 = undefined;
    const entry = entry_mod.buildCommandEntry(etype, 0, 1, 1, 0, ns_id, key, value, &pbuf).?;
    return applyLogged(h, &entry);
}

test "ProcessingHandler: a remove is refused while its job runs, and frees the slot once it doesn't" {
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    handler.max_jobs = 1;
    var buf: [512]u8 = undefined;
    _ = try applySubmit(&handler, "job-1", testSubmitValue(&buf, .running, lookup_yaml), 1);
    try std.testing.expect(handler.pipelines.contains("job-1"));

    // Running: refused in log order, and nothing changes.
    const running = (try applyKeyed(&handler, .processing_remove, "job-1", "")).refused().?;
    try std.testing.expectEqual(proto.StatusCode.conflict, running.status);
    try std.testing.expectEqual(proto.Reason.job_running, running.refusal.reason);
    try std.testing.expect(handler.jobs.contains("job-1"));

    // At the limit, a cancelled job still holds its slot.
    try std.testing.expect((try applyKeyed(&handler, .processing_cancel, "job-1", &.{@backingInt(ProcessingHandler.JobStatus.cancelled)})).refused() == null);
    try std.testing.expect((try applySubmit(&handler, "job-2", testSubmitValue(&buf, .stopped, lookup_yaml), 2)).refused() != null);

    // Another namespace's remove finds no such job, on every replica.
    const elsewhere = (try applyKeyedIn(&handler, .processing_remove, .of(16), "job-1", "")).refused().?;
    try std.testing.expectEqual(proto.StatusCode.not_found, elsewhere.status);
    try std.testing.expect(handler.jobs.contains("job-1"));

    // Removed: the job, its pipeline and its slot are gone; "default" by
    // name is the bare namespace.
    try std.testing.expect((try applyKeyedIn(&handler, .processing_remove, .default, "job-1", "")).refused() == null);
    try std.testing.expectEqual(@as(usize, 0), handler.jobs.count());
    try std.testing.expectEqual(@as(usize, 0), handler.pipelines.count());
    try std.testing.expect((try applySubmit(&handler, "job-2", testSubmitValue(&buf, .stopped, lookup_yaml), 3)).refused() == null);
    try std.testing.expect(handler.jobs.contains("job-2"));

    // A second remove, or a stop or cancel taken before the remove applied,
    // finds no job: refused as not found, never a halt.
    for ([_]EntryType{ .processing_remove, .processing_stop, .processing_cancel }) |etype| {
        const gone = (try applyKeyed(&handler, etype, "job-1", &.{@backingInt(ProcessingHandler.JobStatus.stopped)})).refused().?;
        try std.testing.expectEqual(proto.StatusCode.not_found, gone.status);
    }
}

test "ProcessingHandler: removing a failed job un-counts it" {
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
    _ = try applySubmit(&handler, "job-f", testSubmitValue(&buf, .running, yaml), 1);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, handler.jobs.get("job-f").?.status);
    try std.testing.expectEqual(@as(u64, 1), handler.failed_jobs);
    try std.testing.expect((try applyKeyed(&handler, .processing_remove, "job-f", "")).refused() == null);
    try std.testing.expectEqual(@as(u64, 0), handler.failed_jobs);
    try std.testing.expect(!handler.jobs.contains("job-f"));
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
    handler.peer_shard_count = 2;
    handler.setPeerKvHandlers(&peers);

    // A key whose hash names shard 1, written here.
    var elsewhere: []const u8 = "";
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" }) |c| {
        if (handler.peerShardId("default", c) == 1) elsewhere = c;
    }
    try std.testing.expect(elsewhere.len > 0);
    const Key = @import("../kv/state.zig").Key;
    var kbuf: [@import("../protocol/limits.zig").MAX_KV_KEY]u8 = undefined;
    try t0.put(Key.init(&kbuf, .default, elsewhere).stored, "here", 0);
    try t1.put(Key.init(&kbuf, .default, "only-there").stored, "there", 0);

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
    var entry_buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&entry_buf);
    try ProcessingHandler.writeEntry(two.listedJob(.default, "job-1").?, &w);
    var r = @import("../util/wire.zig").WireReader.init(w.buffered());
    _ = r.readLengthPrefixed(u16).?;
    _ = r.readLengthPrefixed(u16).?;
    try std.testing.expectEqualStrings("RUNNING (held here: kv_lookup needs a one-shard server (--shards 1) until lookups can read other shards)", r.readLengthPrefixed(u16).?);

    // Stopped, it is no longer held.
    var out: Result = .{};
    try two.replayStatusChange("job-1", .stopped, &out);
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
    handler.own_shard = 0;
    // Routed by the id the node's table publishes for it.
    const table = try @import("../namespace/table.zig").Table.create(allocator);
    defer table.destroy();
    try table.publish(@import("../namespace/table.zig").FIRST_ALLOCATED, "acme", 1);
    handler.ns_table = table;
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
    var entry_buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&entry_buf);
    try ProcessingHandler.writeEntry(handler.listedJob(.default, "job-r").?, &w);
    var r = @import("../util/wire.zig").WireReader.init(w.buffered());
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    const status = r.readLengthPrefixed(u16).?;
    try std.testing.expect(std.mem.startsWith(u8, status, "FAILED: "));
    try std.testing.expectEqualStrings(reason, status["FAILED: ".len..]);
    try std.testing.expectEqual(@as(i64, 1), r.readI64().?);
}

test "ProcessingHandler: jobs that share a name are each listed" {
    var handler = ProcessingHandler.init(std.testing.allocator);
    defer handler.deinit();
    var buf: [512]u8 = undefined;
    const ids = [_][]const u8{ "job-a", "job-b", "job-c" };
    for (ids, 1..) |id, i| _ = try applySubmit(&handler, id, testSubmitValue(&buf, .stopped, lookup_yaml), @intCast(i));

    const page = ProcessingHandler.localScanJobs(@ptrCast(&handler), .{ .name = "", .id = .default }, "", null, 0);
    try std.testing.expectEqual(ids.len, page.items.len);
    var seen: [ids.len]bool = @splat(false);
    for (page.items) |item| {
        const job = handler.listedJob(.default, item).?;
        try std.testing.expectEqualStrings("lookup", job.name_owned);
        for (ids, 0..) |id, i| if (std.mem.eql(u8, id, job.job_id_owned)) {
            seen[i] = true;
        };
    }
    for (seen) |s| try std.testing.expect(s);
    try std.testing.expect(handler.listedJob(.of(16), "job-a") == null);
}
