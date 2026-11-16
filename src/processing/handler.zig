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
const sink_payload = @import("sink_payload.zig");
const sinks_machine = @import("sinks_machine.zig");
const pe = @import("../protocol/processing_entries.zig");
const ce = @import("../protocol/channel_entries.zig");
const schema = @import("../protocol/schema.zig");
const limits = @import("../protocol/limits.zig");
const view_mod = @import("../contract/view.zig");
const channels_mod = @import("../contract/channels.zig");
const channel_driver = @import("../contract/channel_driver.zig");
const source_page = @import("source_page.zig");
const TsPoint = source_page.TsPoint;
const ReadAt = ts_mod.TSProjection.ReadAt;
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
    /// The clock a TS source's idle window runs on; a test seam.
    mono_ms: *const fn () u64 = @import("stdx").time.monotonicMs,

    /// In-memory job store: job_id → JobRecord.
    jobs: std.StringHashMap(JobRecord),
    sink_drops: u64 = 0,
    chain_errors: u64 = 0,
    chain_warn_ms: i64 = 0,
    /// The job whose pipeline is ticking, so a drop or chain error names it.
    ticking_job: []const u8 = "",
    /// When a waiting pipeline was last logged (`noteWait`).
    held_warn_ms: i64 = 0,

    /// Per-job pipeline execution state (keyed by same job_id as jobs map).
    pipelines: std.StringHashMap(PipelineState),

    peer_shard_count: u16 = 1,
    /// The node's namespace table, wired by the shard: a sink routes by its
    /// namespace's id, as the dispatcher does.
    ns_table: ?*const @import("../namespace/table.zig").Table = null,

    /// This shard's KV, wired by the shard: all a kv_lookup reads.
    local_kv: ?*kv_handler_mod.KVHandler = null,
    /// The shard this handler runs on.
    own_shard: u16 = 0,

    /// The sink batch a page is building, and the buffers it and its
    /// entry are built in, allocated at the first batch.
    batch: Batch = .{ .w = .{ .buf = &.{} }, .r = .{ .buf = &.{} } },
    batch_buf: []u8 = &.{},
    rec_buf: []u8 = &.{},
    entry_buf: []u8 = &.{},
    msg_buf: []u8 = &.{},
    /// Records skipped because their writes don't fit one entry.
    too_large: u64 = 0,
    /// Pipelines here waiting for a sink's namespace, as last counted.
    waiting_ns: usize = 0,

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
        /// TS source: the last point read, by (time, position).
        ts_cursor: ReadAt,
        /// TS source: how far behind its field's newest point it reads
        /// (`holdBackNs`), and the field's write count as it last saw it
        /// (null: the field has no series), and when, by this leader's
        /// monotonic clock. Seen again after a restart or a leader change,
        /// so the window starts over and nothing is skipped.
        lateness_ms: u32 = 0,
        seen_writes: ?u64 = null,
        seen_known: bool = false,
        seen_at_ms: u64 = 0,
        stream_cursor_ts: u64, // Stream source: last StreamID timestamp_ms read
        stream_cursor_seq: u64, // Stream source: last StreamID sequence read
        last_poll_ms: i64, // last time this pipeline was ticked

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
        /// When its ticks began waiting for room; 0 while they go through.
        held_since_ms: i64 = 0,
        /// Why it last couldn't read (`waitOf`).
        wait: Wait = .none,
        /// Its cursor as its last applied batch left it: the `from` of its
        /// next batch. The batch it proposed and hasn't seen apply, by
        /// index and term (index 0: none), and when it last proposed one.
        from: sinks_machine.At = .{},
        /// Its read cursor is past `from` in term `ahead_term`, for records
        /// its operators have run on that no batch has logged yet: an idle
        /// page, or a record cut from a full batch, whose writes wait in
        /// `carry` for the next. Read again only if the term moves on, as
        /// operator state is.
        ahead: bool = false,
        ahead_term: u64 = 0,
        carry: std.ArrayListUnmanaged(u8) = .empty,
        carry_n: u32 = 0,
        carry_bytes: usize = 0,
        batch_index: u64 = 0,
        batch_term: u64 = 0,
        last_batch_ms: u64 = 0,
        /// Records this poll may still read, and whether the last page
        /// filled its batch with more to read: then the next page is read
        /// once that batch applies. `page_limit` is the last page's size.
        poll_left: u32 = 0,
        more: bool = false,
        /// When a poll last found it waiting, and counted it.
        wait_counted_ms: i64 = 0,
        page_limit: usize = 0,
        /// A read of a source another shard owns, in flight: its number
        /// (0 for none), the term it was asked in, and when.
        read_seq: u64 = 0,
        read_term: u64 = 0,
        read_sent_ms: i64 = 0,
        next_read_seq: u64 = 0,
        /// Per-tick processing time (ns) over productive ticks: last/max/sum/count.
        tick_ns_last: u64 = 0,
        tick_ns_max: u64 = 0,
        tick_ns_sum: u64 = 0,
        tick_count: u64 = 0,

        /// The field's write count as read now (null: it has no series).
        /// Any write, late or not, restarts the idle clock.
        pub fn noteWrites(self: *PipelineState, writes: ?u64, now_ms: u64) void {
            if (self.seen_known and std.meta.eql(self.seen_writes, writes)) return;
            self.seen_writes = writes;
            self.seen_known = true;
            self.seen_at_ms = now_ms;
        }

        /// How far behind its field's newest point the next read stops:
        /// `lateness_ms`, so a point that arrives up to that late is still
        /// ahead of the cursor; 0 once the field has had no write for that
        /// long, or has no series. The shard holding the field applies it
        /// to the newest time it holds (`throughFor`).
        /// The field's write count once it has had no write for the
        /// window, 0 while it hasn't: a read of another shard's field sends
        /// it, and that shard releases the window only if its own count is
        /// still the same.
        pub fn idleAt(self: *const PipelineState, now_ms: u64) u64 {
            if (self.lateness_ms == 0 or !self.seen_known) return 0;
            const writes = self.seen_writes orelse return 0;
            return if (now_ms -| self.seen_at_ms >= self.lateness_ms) writes else 0;
        }

        pub fn holdBackNs(self: *const PipelineState, now_ms: u64) u64 {
            const w: u64 = @as(u64, self.lateness_ms) * std.time.ns_per_ms;
            if (w == 0 or !self.seen_known) return w;
            if (self.seen_writes == null) return 0;
            if (now_ms -| self.seen_at_ms >= self.lateness_ms) return 0;
            return w;
        }
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
        /// Records this leader skipped for good, and the last one's place
        /// and reason, for its status. Not replicated: a new leader starts
        /// them again.
        drops: u64 = 0,
        last_drop: [160]u8 = undefined,
        last_drop_len: usize = 0,
        /// Single sink writes this leader dropped, and when one was last
        /// logged.
        write_drops: u64 = 0,
        drop_warn_ms: i64 = 0,
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
        for ([_][]u8{ self.batch_buf, self.rec_buf, self.entry_buf, self.msg_buf }) |b| if (b.len > 0) self.allocator.free(b);
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

    fn freeJobRecord(self: *ProcessingHandler, job: *JobRecord) void {
        self.allocator.free(job.job_id_owned);
        self.allocator.free(job.name_owned);
        self.allocator.free(job.yaml_owned);
        if (job.failure_owned.len > 0) self.allocator.free(job.failure_owned);
        self.freeHeldReason(job);
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
        pipe.carry.deinit(self.allocator);
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
        // Every op naming a job runs on group 0, whatever its key: one
        // group holds the job registry, so `max_jobs` bounds the server.
        // Pinned to the group, not routed by a hash or a job id's bits.
        inline for (.{ .processing_submit, .processing_stop, .processing_cancel, .processing_status }) |op|
            dispatcher.register(op, dispatchProcessing);
        dispatcher.register(.processing_remove, table_run.handler("processing.remove", ProcessingHandler, runRemove));
        inline for (.{ .processing_submit, .processing_stop, .processing_cancel, .processing_status, .processing_remove }) |op|
            dispatcher.setShardRoute(op, toGroup0);
        dispatcher.registerWalk(.processing_list, dispatchProcessing, localScanJobs);
    }

    fn toGroup0(_: Request) ?u16 {
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
        if (def.sinkWritesRefusal(&why_buf)) |why| {
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

        // A job id routes nothing (every processing op runs on group 0), so
        // its slot bits are 0.
        var id_buf: [32]u8 = undefined;
        const job_id = shard.run_id_gen.next(.job, 0, &id_buf);

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
            // <why>" for a running one this node holds, what a running
            // one's pipelines wait for and skipped (`liveNote`), or empty —
            // then this node's runtime of the job, `Runtime`, and
            // [yaml_len:u32][yaml], the submitted definition.
            // Sized to the job. The id is 32 bytes and the name came from a
            // submit entry under 64 KiB, so both lengths fit u16.
            var live_buf: [LIVE_NOTE_MAX]u8 = undefined;
            const note = noteOf(job);
            const live: []const u8 = if (note == .none) self.liveNote(job, &live_buf) orelse "" else "";
            const runtime = self.runtimeOf(job.job_id_owned, @import("stdx").time.milliTimestamp());
            const size = 2 + job.job_id_owned.len + 2 + job.name_owned.len + 1 + 4 + 8 + 8 + 2 + note.reasonLen() + live.len + Runtime.SIZE + 4 + job.yaml_owned.len;
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
            w.writeInt(u16, @intCast(note.reasonLen() + live.len), .little) catch unreachable;
            note.writeReason(w);
            w.writeAll(live) catch unreachable;
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

    const LIVE_NOTE_MAX = 512;

    /// What a running job's pipelines here are waiting for, and the last
    /// record skipped for good, if any: "waiting for namespace X",
    /// "waiting for room on shard N (target)"; "N records skipped, the
    /// last <where>: <why>".
    pub fn liveNote(self: *const ProcessingHandler, job: *const JobRecord, buf: []u8) ?[]const u8 {
        if (job.status != .running) return null;
        var w: std.Io.Writer = .fixed(buf);
        var it = self.pipelines.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            const base = if (std.mem.indexOfScalar(u8, key, 0)) |nul| key[0..nul] else key;
            if (!std.mem.eql(u8, base, job.job_id_owned) or e.value_ptr.wait == .none) continue;
            var wb: [160]u8 = undefined;
            w.writeAll(waitText(e.value_ptr.wait, &wb)) catch {};
            break;
        }
        if (job.drops > 0) {
            if (w.end > 0) w.writeAll("; ") catch {};
            w.print("{d} record{s} skipped, the last {s}", .{ job.drops, if (job.drops == 1) "" else "s", job.last_drop[0..job.last_drop_len] }) catch {};
        }
        return if (w.end == 0) null else w.buffered();
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
        // server now refuses, is kept as failed with its definition; only
        // running out of memory here drops it. Its pipelines' cursors are
        // the sinks machine's, untouched by any of this.
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

        // Replace an existing record (idempotent replay).
        if (self.jobs.fetchRemove(owned_id)) |old| {
            var rec = old.value;
            if (rec.status == .failed) self.failed_jobs -= 1;
            if (rec.held) self.held_jobs -= 1;
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
        }) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(owned_yaml);
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
        // Guard against a double build on idempotent replay. Each pipeline
        // reads from its cursor as the sinks machine holds it.
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
        const kv = self.local_kv orelse return null;
        const entry = kv.kv.getIn(self.namespaceIdOf(namespace), key) orelse return null;
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
            .lateness_ms = src.ts_lateness_ms,
            .batch_size = if (src.batch_size > 0) src.batch_size else 100,
            .sinks = &.{},
            .ts_cursor = .{ .ns = 0, .ual = 0 },
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
    /// not written. Writes already sent wait in its outbox until the
    /// halted shard is repaired.
    fn haltedSinkShard(self: *const ProcessingHandler, shard: *const Shard, pipe: *const PipelineState) ?u16 {
        var to: u16 = 0;
        while (to < @max(self.peer_shard_count, 1)) : (to += 1) {
            if (to != shard.id and self.sendsTo(pipe, to) and shard.shardHalted(to)) return to;
        }
        return null;
    }

    /// Why a pipeline reads nothing now, as its job's status says it.
    pub const Wait = union(enum) {
        none,
        /// A sink's namespace isn't in the node's table: no shard can tell
        /// where its targets are. Sinks don't create namespaces.
        namespace: []const u8,
        /// This sink shard is halted.
        halted: u16,
        /// A sink's shard answers `busy` (a target there is over its
        /// parked cap). `target` is the sink whose write waits, when the
        /// first unacked message is one of this pipeline's; "" otherwise.
        busy: struct { shard: u16, target: []const u8 },
        /// A whole window of this shard's writes to a sink's shard is
        /// unacked, with no `busy` answer: it is slow or can't be reached.
        unacked: struct { shard: u16, count: u64 },
        /// This shard's log has no room for another entry now.
        log,
    };

    /// Why `pipe` can't read now, or `.none`. A sink batch commits all its
    /// writes or none, so a pipeline waits whole: one full target delays
    /// every sink of the job, and every job sending to that shard, until it
    /// has room. Below its parked cap a full target parks what it can't
    /// take, so this waits only past it.
    fn waitOf(self: *const ProcessingHandler, shard: *Shard, pipe: *const PipelineState) Wait {
        for (pipe.sinks) |snk| if (self.namespaceIdOf(snk.namespace) == .invalid) return .{ .namespace = snk.namespace };
        if (self.haltedSinkShard(shard, pipe)) |to| return .{ .halted = to };
        const read: view_mod.ReadView = .{ .store = &shard.defaultPartition().store };
        var to: u16 = 0;
        const shards: u16 = @max(self.peer_shard_count, 1);
        while (to < shards) : (to += 1) {
            if (!self.sendsTo(pipe, to)) continue;
            const meta = channels_mod.metaOf(read, to);
            const unacked = meta.next_seq - 1 - meta.acked;
            // A `busy` answer holds only while something to it is unacked:
            // one with nothing outstanding has nothing to clear it.
            if (unacked > 0 and shard.channel.busySince(to) != null)
                return .{ .busy = .{ .shard = to, .target = self.waitingSink(read, pipe, to, meta.acked + 1) } };
            // The outbox has no cap of its own: a pipeline stops adding to
            // a channel a whole window behind.
            if (unacked >= channel_driver.WINDOW) return .{ .unacked = .{ .shard = to, .count = unacked } };
        }
        const raft = shard.raft_node;
        if (raft.log.lastIndex() -| raft.last_applied >= raft_node_mod.MAX_OUTSTANDING) return .log;
        return .none;
    }

    /// Whether one of `pipe`'s sinks may write to shard `to`.
    fn sendsTo(self: *const ProcessingHandler, pipe: *const PipelineState, to: u16) bool {
        for (pipe.sinks) |snk| {
            const s = self.sinkShard(snk) orelse return true;
            if (s == to) return true;
        }
        return false;
    }

    /// The sink of `pipe` the message at `seq` to `to` is for, by its
    /// slot, or "": a KV sink's keys are many, and the message may be
    /// another job's.
    fn waitingSink(self: *const ProcessingHandler, read: view_mod.ReadView, pipe: *const PipelineState, to: u16, seq: u64) []const u8 {
        var kb: [channels_mod.OUT_KEY]u8 = undefined;
        const out = (read.get(channels_mod.OutFam, channels_mod.outKey(&kb, to, seq)) catch null) orelse return "";
        for (pipe.sinks) |snk| {
            const name = if (snk.kind == .ts) snk.measurement else snk.target;
            if (snk.kind == .kv) continue;
            const slot = router.slot(router.routeNsKey(self.namespaceIdOf(snk.namespace), name));
            if (slot == out.v.slot) return name;
        }
        return "";
    }

    /// Drive all running pipelines this node leads: each reads a page from
    /// its source's cursor, runs it through its operators, and proposes the
    /// records' sink writes with the cursor they move it to, one batch at a
    /// time. Returns whether one was held for a halted sink shard.
    /// Called from Shard.tick() on each reactor iteration.
    pub fn tickPipelines(self: *ProcessingHandler, shard: *Shard) bool {
        var held = false;
        const now_ms = @import("stdx").time.milliTimestamp();
        const raft = shard.raft_node;
        const leads = raft.role == .leader and raft.leaderReady();
        var waiting_ns: usize = 0;
        defer self.noteWaitingNamespace(shard, waiting_ns);

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
            // What a pipeline waits for is its leader's to say: a node
            // that doesn't lead says nothing, and one led again starts over.
            if (!leads) {
                pipe.wait = .none;
                pipe.held_since_ms = 0;
                pipe.more = false;
                continue;
            }
            defer switch (pipe.wait) {
                .namespace => waiting_ns += 1,
                .halted => held = true,
                else => {},
            };

            // A poll reads up to `batch_size` records, a page at a time:
            // while a page fills its batch, the next is read as soon as
            // that batch applies, not at the next poll.
            const poll_ms: i64 = @intCast(pipe.src_poll_ms);
            const due = now_ms - pipe.last_poll_ms >= poll_ms;
            if (!due and !pipe.more) continue;
            // Its batch in the log: nothing more until it applies.
            if (pipe.batch_term == raft.current_term and pipe.batch_index > raft.last_applied) continue;
            self.loadCursor(shard, job.ns_id, pipeline_key, pipe);
            pipe.wait = self.waitOf(shard, pipe);
            if (pipe.wait != .none) {
                // Looked at again next tick, so the wait ends, and status
                // stops saying so, as soon as it can; counted once a poll.
                pipe.more = false;
                self.noteWait(shard, pipe, now_ms - pipe.wait_counted_ms >= poll_ms);
                continue;
            }
            pipe.held_since_ms = 0;
            if (!pipe.more) {
                pipe.last_poll_ms = now_ms;
                pipe.poll_left = pipe.batch_size;
            }
            pipe.more = false;

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

    /// Pipelines here waiting for a sink's namespace, into the node's gauge.
    fn noteWaitingNamespace(self: *ProcessingHandler, shard: *Shard, now: usize) void {
        if (now == self.waiting_ns) return;
        if (shard.metrics_registry) |m| m.processing.moveWaitingNamespace(self.waiting_ns, now);
        self.waiting_ns = now;
    }

    /// Its logged cursor, as its last applied batch left it: the `from` its
    /// next batch names, and where it reads from unless it read past it
    /// in this term. A batch a leader change lost is behind it.
    fn loadCursor(self: *ProcessingHandler, shard: *Shard, ns: NamespaceId, pipeline_key: []const u8, pipe: *PipelineState) void {
        const at = sinks_machine.cursorOf(.{ .store = &shard.defaultPartition().store }, ns, pipeline_key);
        pipe.from = at;
        pipe.batch_index = 0;
        // Read ahead in this term: it reads on from there, and its next
        // batch covers what it read.
        if (pipe.ahead and pipe.ahead_term == shard.raft_node.current_term) return;
        self.rewindTo(pipe, at);
    }

    /// Where it reads next.
    fn readAt(pipe: *const PipelineState) sinks_machine.At {
        return switch (pipe.source_kind) {
            .stream => .{ .a = pipe.stream_cursor_ts, .b = pipe.stream_cursor_seq },
            .ts => .{ .a = pipe.ts_cursor.ns, .b = pipe.ts_cursor.ual },
        };
    }

    /// The latest time a TS source's read reaches, on the shard holding
    /// its field: `behind` short of the field's newest point, or every
    /// point when `behind` is 0 or the field has no series.
    pub fn throughFor(f: ?@import("../ts/machine.zig").FieldValue, behind: u64) u64 {
        if (behind == 0) return std.math.maxInt(u64);
        const v = f orelse return std.math.maxInt(u64);
        return v.newest_ns -| behind;
    }

    /// Tick a TS source pipeline: read points, apply operators, write to sinks.
    /// It reads in time order, so a point written with a time behind its
    /// cursor (late, or from a writer whose clock lags) is never read.
    fn tickTsSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        const limit = @min(pipe.poll_left, source_page.TS_PAGE_POINTS);
        pipe.page_limit = limit;
        const owner = self.sinkOwner(shard, pipe.src_namespace, pipe.src_measurement);
        if (owner != shard.id) return self.askSource(pipeline_key, pipe, shard, owner, limit);
        var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
        const ns = shard.namespaceId(pipe.src_namespace);
        const now = self.mono_ms();
        const f = shard.ts_handler.ts.fieldRecord(ns, pipe.src_measurement, pipe.src_field);
        pipe.noteWrites(if (f) |v| v.writes else null, now);
        const n = source_page.readTsPage(shard.ts_handler.ts, ns, pipe.src_measurement, pipe.src_field, pipe.src_tag_hash, pipe.ts_cursor, throughFor(f, pipe.holdBackNs(now)), limit, &points);
        self.processTsPage(pipeline_key, pipe, shard, job, points[0..n]);
    }

    /// A page of a TS source, read here or sent by the shard that owns it,
    /// in (time, position) order after the cursor. Its points' writes go in
    /// one batch, as many whole points as fit.
    fn processTsPage(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord, points: []const TsPoint) void {
        const batch = self.beginBatch(pipe) orelse return;
        // Where the batch moves the cursor, when it ends before a record
        // its operators ran on; else where the read reached.
        var to: ?sinks_machine.At = null;
        var taken: u32 = 0;
        // One point at a time: its JSON and what its chain allocates are
        // freed once its outputs are written.
        var arena = std.heap.ArenaAllocator.init(shard.allocator);
        defer arena.deinit();
        for (points) |pt| {
            defer _ = arena.reset(.retain_capacity);
            // Format TS point as JSON record. Short of memory, the batch
            // stops here, the point still ahead of the cursor.
            const ts_ms: i64 = @intCast(pt.timestamp_ns / 1_000_000);
            const json = tsPointJson(arena.allocator(), pipe.src_measurement, pt.value, ts_ms) catch break;
            batch.beginRecord();
            // Apply operator chain (or pass through directly). A record
            // the chain failed on is counted and skipped: passing it through
            // raw would write what the operators were there to change.
            const output_records = applyOperatorChain(pipe.operators, json, ts_ms, arena.allocator()) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };
            if (output_records) |records| {
                for (records) |rec| self.writeSinkRecord(batch, pipe, shard, arena.allocator(), rec.value, pt.value, pt.timestamp_ns, pt.tags, rec.tags);
            } else {
                self.writeSinkRecord(batch, pipe, shard, arena.allocator(), json, pt.value, pt.timestamp_ns, pt.tags, 0);
            }
            var pb: [48]u8 = undefined;
            const before = readAt(pipe);
            const ended = self.endRecord(batch, pipe, shard, std.fmt.bufPrint(&pb, "point {d}/{d}", .{ pt.timestamp_ns, pt.ual }) catch "a point");
            if (ended == .cut) break;
            if (ended == .carried) to = before;
            pipe.ts_cursor = .{ .ns = pt.timestamp_ns, .ual = pt.ual };
            // Event-time watermark = highest source point timestamp seen.
            if (ts_ms > pipe.watermark_ms) pipe.watermark_ms = ts_ms;
            job.records_processed += 1;
            pipe.records_in += 1;
            taken += 1;
            if (ended == .carried) break;
        }
        self.notePage(pipe, batch, taken, points.len);
        self.proposeBatch(shard, job.ns_id, pipeline_key, pipe, batch, to orelse readAt(pipe));
    }

    /// Tick a stream source pipeline: read payloads from stream, apply operators, write to sink.
    fn tickStreamSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        const limit = @min(pipe.poll_left, channels_mod.MAX_SENDS);
        pipe.page_limit = limit;
        const owner = self.sinkOwner(shard, pipe.src_namespace, pipe.src_stream);
        if (owner != shard.id) return self.askSource(pipeline_key, pipe, shard, owner, limit);
        const cursor = StreamID{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };
        const result = shard.stream_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, cursor, limit);
        defer result.deinit(shard.stream_handler.allocator);
        self.processStreamPage(pipeline_key, pipe, shard, job, result.payloads, result.ids, result.last_id);
    }

    /// A page of a stream source read from the pipeline's cursor, here or
    /// sent by the shard that owns it: `last_id` is where the read reached.
    /// Its records' writes go in one batch, as many whole records as fit.
    fn processStreamPage(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job: *JobRecord, payloads: []const []const u8, ids: []const StreamID, last_id: StreamID) void {
        const batch = self.beginBatch(pipe) orelse return;
        var to: ?sinks_machine.At = null;
        var taken: u32 = 0;
        // One payload's chain at a time: what it allocates is freed once its
        // outputs are written.
        var arena = std.heap.ArenaAllocator.init(shard.allocator);
        defer arena.deinit();
        var all = true;
        for (payloads, ids) |payload, id| {
            defer _ = arena.reset(.retain_capacity);
            batch.beginRecord();
            const output_records = applyOperatorChain(pipe.operators, payload, @import("stdx").time.milliTimestamp(), arena.allocator()) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };
            if (output_records) |records| {
                for (records) |rec| self.writeSinkRecordFromStream(batch, pipe, shard, arena.allocator(), rec.key, rec.value, rec.tags);
            } else {
                self.writeSinkRecordFromStream(batch, pipe, shard, arena.allocator(), "", payload, 0);
            }
            var pb: [48]u8 = undefined;
            const before = readAt(pipe);
            const ended = self.endRecord(batch, pipe, shard, std.fmt.bufPrint(&pb, "record {d}-{d}", .{ id.timestamp_ms, id.sequence }) catch "a record");
            if (ended == .cut) {
                all = false;
                break;
            }
            if (ended == .carried) to = before;
            pipe.stream_cursor_ts = id.timestamp_ms;
            pipe.stream_cursor_seq = id.sequence;
            job.records_processed += 1;
            pipe.records_in += 1;
            taken += 1;
            if (ended == .carried) {
                all = false;
                break;
            }
        }
        // Every record taken: the cursor goes where the read reached, past
        // appends that couldn't be read (the stream handler logged them),
        // so the source doesn't stall on them.
        if (all) {
            pipe.stream_cursor_ts = last_id.timestamp_ms;
            pipe.stream_cursor_seq = last_id.sequence;
        }
        self.notePage(pipe, batch, taken, payloads.len);
        self.proposeBatch(shard, job.ns_id, pipeline_key, pipe, batch, to orelse readAt(pipe));
    }

    /// A page taken: `taken` records of the `got` it held count against
    /// the poll's `batch_size`; while the page filled its batch, or was as
    /// long as asked for, and the poll has records left, the next page is
    /// read once this batch applies.
    fn notePage(_: *ProcessingHandler, pipe: *PipelineState, batch: *const Batch, taken: u32, got: usize) void {
        pipe.poll_left -|= taken;
        pipe.more = pipe.poll_left > 0 and (batch.full or (got > 0 and got >= pipe.page_limit));
    }

    /// Most a read of another shard's source waits for its page before it
    /// is asked again: the answer may have found an inbox full.
    const READ_DEADLINE_MS: i64 = 5_000;

    /// A source another shard owns: ask that shard for a page from the
    /// cursor, at most one at a time. The page is processed when it comes
    /// (`takeSourcePage`); until then the pipeline reads nothing more.
    fn askSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, owner: u16, limit: usize) void {
        const raft = shard.raft_node;
        if (raft.role != .leader) return;
        const now = @import("stdx").time.milliTimestamp();
        if (pipe.read_seq != 0 and now - pipe.read_sent_ms < READ_DEADLINE_MS) return;
        const kind: source_page.Kind = switch (pipe.source_kind) {
            .stream => .stream,
            .ts => .ts,
        };
        const from_a: u64, const from_b: u64 = switch (pipe.source_kind) {
            .stream => .{ pipe.stream_cursor_ts, pipe.stream_cursor_seq },
            .ts => .{ pipe.ts_cursor.ns, pipe.ts_cursor.ual },
        };
        const seq = pipe.next_read_seq + 1;
        const bytes = (source_page.Read{
            .term = raft.current_term,
            .seq = seq,
            .kind = kind,
            .pipeline = pipeline_key,
            .namespace = pipe.src_namespace,
            .source = if (kind == .stream) pipe.src_stream else pipe.src_measurement,
            .field = pipe.src_field,
            .tag_hash = pipe.src_tag_hash,
            .from_a = from_a,
            .from_b = from_b,
            .limit = @intCast(limit),
            .behind_ns = if (kind == .ts) @as(u64, pipe.lateness_ms) * std.time.ns_per_ms else 0,
            .idle_at = if (kind == .ts) pipe.idleAt(self.mono_ms()) else 0,
        }).encode(shard.allocator) orelse return;
        if (!shard.peer_mailboxes.?[owner].inbox.send(.{ .tag = .source_read, .src_shard = @intCast(shard.id), .payload_len = @intCast(bytes.len), .payload_ptr = bytes.ptr })) {
            // A full inbox: asked again next tick.
            shard.allocator.free(bytes);
            return;
        }
        pipe.next_read_seq = seq;
        pipe.read_seq = seq;
        pipe.read_term = raft.current_term;
        pipe.read_sent_ms = now;
    }

    /// Another shard's pipeline asks for a page of a source this shard
    /// owns: read here, on this shard's thread, as a client's read is
    /// (applied data only, nothing logged), and sent back. An answer that
    /// finds the asker's inbox full is dropped; the asker asks again.
    pub fn answerSourceRead(shard: *Shard, to: u16, bytes: []const u8) void {
        const r = source_page.Read.decode(bytes) orelse return;
        const mailboxes = shard.peer_mailboxes orelse return;
        if (to >= mailboxes.len) return;
        var b: source_page.Page.Builder = .{};
        defer b.deinit(shard.allocator);
        const page = buildSourcePage(shard, r, &b) catch return;
        if (!mailboxes[to].inbox.send(.{ .tag = .source_page, .src_shard = @intCast(shard.id), .payload_len = @intCast(page.len), .payload_ptr = page.ptr }))
            shard.allocator.free(page);
    }

    fn buildSourcePage(shard: *Shard, r: source_page.Read, b: *source_page.Page.Builder) ![]u8 {
        try b.begin(shard.allocator, r);
        switch (r.kind) {
            .stream => {
                const from: StreamID = .{ .timestamp_ms = r.from_a, .sequence = r.from_b };
                const result = shard.stream_handler.readPayloadsForStream(r.source, r.namespace, from, r.limit);
                defer result.deinit(shard.stream_handler.allocator);
                for (result.payloads, result.ids) |p, id| try b.record(shard.allocator, id.timestamp_ms, id.sequence, p);
                return b.finish(shard.allocator, result.last_id.timestamp_ms, result.last_id.sequence);
            },
            .ts => {
                var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
                const ns = shard.namespaceId(r.namespace);
                const f = shard.ts_handler.ts.fieldRecord(ns, r.source, r.field);
                // The asker judged the field idle at a write count; a write
                // since, which it hasn't seen, holds the window again.
                const idle = r.idle_at != 0 and f != null and f.?.writes == r.idle_at;
                const n = source_page.readTsPage(shard.ts_handler.ts, ns, r.source, r.field, r.tag_hash, .{ .ns = r.from_a, .ual = r.from_b }, throughFor(f, if (idle) 0 else r.behind_ns), r.limit, &points);
                for (points[0..n]) |p| try b.point(shard.allocator, p);
                return b.finish(shard.allocator, if (f) |v| v.newest_ns else 0, if (f) |v| v.writes else 0);
            },
        }
    }

    /// A page another shard sent for a pipeline here. Processed only if it
    /// answers that pipeline's read in flight, this node still leads in the
    /// term it asked in, the job runs, the pipeline may write, and its
    /// cursor is still where the read began; dropped otherwise. The caller
    /// frees the bytes.
    pub fn takeSourcePage(self: *ProcessingHandler, shard: *Shard, bytes: []const u8) void {
        const page = source_page.Page.decode(bytes) orelse return;
        const entry = self.pipelines.getEntry(page.pipeline) orelse return;
        const pipeline_key = entry.key_ptr.*;
        const pipe = entry.value_ptr;
        if (pipe.read_seq != page.seq or pipe.read_term != page.term) return;
        pipe.read_seq = 0;
        const raft = shard.raft_node;
        if (raft.role != .leader or raft.current_term != page.term) return;
        const base_job_id = if (std.mem.indexOfScalar(u8, pipeline_key, 0)) |nul| pipeline_key[0..nul] else pipeline_key;
        const job = self.jobs.getPtr(base_job_id) orelse return;
        if (job.status != .running or job.held) return;
        if (pipe.batch_term == raft.current_term and pipe.batch_index > raft.last_applied) return;
        // A leader change since the read was asked moved the cursor back:
        // read again from there.
        self.loadCursor(shard, job.ns_id, pipeline_key, pipe);
        const read = readAt(pipe);
        if (read.a != page.from_a or read.b != page.from_b) return;
        // As a tick checks before it reads. Asked again next tick, from
        // the same cursor.
        pipe.wait = self.waitOf(shard, pipe);
        if (pipe.wait != .none) return;

        self.ticking_job = base_job_id;
        defer self.ticking_job = "";
        const before = job.records_processed;
        switch (pipe.source_kind) {
            .stream => {
                if (page.kind != .stream) return;
                const recs = shard.allocator.alloc(source_page.Page.Record, page.count) catch return;
                defer shard.allocator.free(recs);
                const got = page.records(recs) orelse return;
                const payloads = shard.allocator.alloc([]const u8, got.len) catch return;
                defer shard.allocator.free(payloads);
                const ids = shard.allocator.alloc(StreamID, got.len) catch return;
                defer shard.allocator.free(ids);
                for (got, payloads, ids) |rec, *p, *id| {
                    p.* = rec.payload;
                    id.* = .{ .timestamp_ms = rec.ms, .sequence = rec.seq };
                }
                self.processStreamPage(pipeline_key, pipe, shard, job, payloads, ids, .{ .timestamp_ms = page.last_a, .sequence = page.last_b });
            },
            .ts => {
                if (page.kind != .ts) return;
                var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
                const got = page.points(&points) orelse return;
                pipe.noteWrites(if (page.last_b != 0) page.last_b else null, self.mono_ms());
                self.processTsPage(pipeline_key, pipe, shard, job, got);
            },
        }
        const processed = job.records_processed - before;
        if (processed > 0) if (shard.metrics_registry) |m| m.processing.recordProcessed(processed);
    }

    /// Most often a pipeline logs a cursor move with no writes: records
    /// its operators emitted nothing for, or appends that couldn't be
    /// read. Until then it reads on, ahead of its logged cursor.
    const IDLE_BATCH_MS: u64 = 1000;

    /// The sink batch a page builds: its writes as channel messages. Each
    /// record's writes are built apart first (`r`), then go in whole, wait
    /// for the next batch, or are skipped whole. Its buffers are the
    /// handler's, allocated once.
    const Batch = struct {
        w: schema.Writer,
        /// The record being added: its writes, how many, and their bytes;
        /// `over` once they pass what one entry carries.
        r: schema.Writer,
        n: u32 = 0,
        bytes: usize = 0,
        rec_n: u32 = 0,
        rec_bytes: usize = 0,
        over: bool = false,
        /// The batch took no more records: the next one is the next batch's.
        full: bool = false,

        fn beginRecord(b: *Batch) void {
            b.r.pos = 0;
            b.rec_n = 0;
            b.rec_bytes = 0;
            b.over = false;
        }

        /// Adds one of the record's writes.
        fn add(b: *Batch, built: sink_payload.Built) void {
            if (b.over) return;
            if (b.rec_n + 1 > channels_mod.MAX_SENDS or b.rec_bytes + built.payload.len > channels_mod.MAX_SEND_BYTES) {
                b.over = true;
                return;
            }
            pe.SinkMessage.encode(&b.r, .{ .slot = built.slot, .entry_type = @backingInt(built.entry_type), .flags = Flags.SINK, .payload = built.payload }) catch {
                b.over = true;
                return;
            };
            b.rec_n += 1;
            b.rec_bytes += built.payload.len;
        }

        /// Whether the record's writes fit beside those in the batch.
        fn fits(b: *const Batch) bool {
            return b.n + b.rec_n <= channels_mod.MAX_SENDS and b.bytes + b.rec_bytes <= channels_mod.MAX_SEND_BYTES and b.r.pos <= b.w.buf.len - b.w.pos;
        }

        fn take(b: *Batch) void {
            @memcpy(b.w.buf[b.w.pos..][0..b.r.pos], b.r.buf[0..b.r.pos]);
            b.w.pos += b.r.pos;
            b.n += b.rec_n;
            b.bytes += b.rec_bytes;
        }
    };

    /// Room in a batch's message list beside its other fields.
    const BATCH_MESSAGES_MAX = pe.Messages.byte_bound.?;

    /// A new batch over the handler's buffers, or null out of memory:
    /// the tick reads nothing, its cursor where it was.
    fn beginBatch(self: *ProcessingHandler, pipe: *PipelineState) ?*Batch {
        if (self.rec_buf.len == 0) self.rec_buf = self.allocator.alloc(u8, BATCH_MESSAGES_MAX) catch return null;
        if (self.batch_buf.len == 0) {
            self.batch_buf = self.allocator.alloc(u8, BATCH_MESSAGES_MAX) catch return null;
            self.entry_buf = self.allocator.alloc(u8, limits.MAX_LOG_ENTRY_PAYLOAD) catch {
                self.allocator.free(self.batch_buf);
                self.batch_buf = &.{};
                return null;
            };
            self.msg_buf = self.allocator.alloc(u8, sink_payload.MAX_PAYLOAD) catch {
                self.allocator.free(self.batch_buf);
                self.allocator.free(self.entry_buf);
                self.batch_buf = &.{};
                self.entry_buf = &.{};
                return null;
            };
        }
        self.batch = .{ .w = .{ .buf = self.batch_buf }, .r = .{ .buf = self.rec_buf } };
        // A record cut from the last batch, its operators run: its writes
        // lead this one. They fit alone.
        if (pipe.carry_n > 0) {
            const c = pipe.carry.items;
            @memcpy(self.batch_buf[0..c.len], c);
            self.batch.w.pos = c.len;
            self.batch.n = pipe.carry_n;
            self.batch.bytes = pipe.carry_bytes;
            pipe.carry.clearRetainingCapacity();
            pipe.carry_n = 0;
            pipe.carry_bytes = 0;
        }
        return &self.batch;
    }

    /// What became of the record just added.
    const Ended = enum {
        /// In the batch.
        kept,
        /// Its writes go in the next batch: this one ends before it.
        carried,
        /// Not kept, and out of memory to carry it: the batch ends before
        /// it, and it is read again.
        cut,
        /// Too large even alone: skipped.
        skipped,
    };

    /// The record just added: in the batch if its writes fit beside the
    /// rest. If they don't, the batch ends before it, and its writes, built
    /// already, wait in `pipe.carry` to lead the next, so its operators
    /// don't run on it twice. If they don't fit an entry even alone, it is
    /// skipped and counted, loudly, and the cursor moves past it. `at`
    /// names it for that.
    fn endRecord(self: *ProcessingHandler, batch: *Batch, pipe: *PipelineState, shard: *Shard, at: []const u8) Ended {
        if (batch.over) return self.skipTooLarge(shard, at);
        if (batch.fits()) {
            batch.take();
            return .kept;
        }
        batch.full = true;
        pipe.carry.appendSlice(self.allocator, batch.r.written()) catch return .cut;
        pipe.carry_n = batch.rec_n;
        pipe.carry_bytes = batch.rec_bytes;
        return .carried;
    }

    fn skipTooLarge(self: *ProcessingHandler, shard: *Shard, at: []const u8) Ended {
        self.too_large += 1;
        if (shard.metrics_registry) |m| m.processing.recordTooLarge();
        self.noteDrop(shard, at, std.fmt.comptimePrint("its writes don't fit one entry (at most {d} writes and {d} bytes a record)", .{ channels_mod.MAX_SENDS, channels_mod.MAX_SEND_BYTES }));
        return .skipped;
    }

    /// Proposes `batch` with the cursor it moves the pipeline to: its
    /// writes and its cursor apply together, once. One with no writes and
    /// the cursor where it was is nothing to log; one with no writes that
    /// moves it is logged at most once a second, and until then the
    /// pipeline reads on from where it is, ahead of its logged cursor, so
    /// its operators never run on a record twice in a term. A batch not
    /// logged (rare: it didn't encode, or the log refused it) is read
    /// again, its operators run on it again.
    fn proposeBatch(self: *ProcessingHandler, shard: *Shard, ns: NamespaceId, pipeline_key: []const u8, pipe: *PipelineState, batch: *Batch, to: sinks_machine.At) void {
        const term = shard.raft_node.current_term;
        const read = readAt(pipe);
        const moved = to.a != pipe.from.a or to.b != pipe.from.b;
        if (batch.n == 0 and !moved) return;
        const now = self.mono_ms();
        if (batch.n == 0 and now -| pipe.last_batch_ms < IDLE_BATCH_MS) {
            pipe.ahead = true;
            pipe.ahead_term = term;
            return;
        }
        const entry = pe.SinkBatch.encodeInto(self.entry_buf, .{
            .ns = ns.int(),
            .pipeline = pipeline_key,
            .from_a = pipe.from.a,
            .from_b = pipe.from.b,
            .to_a = to.a,
            .to_b = to.b,
            .messages = .{ .bytes = batch.w.written(), .count = batch.n },
        }) catch {
            log.err("job '{s}': a sink batch didn't encode; read again", .{self.ticking_job});
            return self.rewindTo(pipe, pipe.from);
        };
        const r = shard.raft_node.propose(.processing_sink_batch, Flags.NONE, entry) catch |err| {
            if (!shard.quietHalted(err)) log.warn("job '{s}': sink batch not proposed: {s}; read again", .{ self.ticking_job, @errorName(err) });
            return self.rewindTo(pipe, pipe.from);
        };
        pipe.batch_index = r.index;
        pipe.batch_term = r.term;
        pipe.last_batch_ms = now;
        // Read past it: the record whose writes wait in `carry`.
        pipe.ahead = read.a != to.a or read.b != to.b;
        pipe.ahead_term = r.term;
        if (shard.metrics_registry) |m| m.processing.recordSinkBatch();
    }

    /// Back to where the pipeline's last applied batch left it: what it
    /// read since is read again, and its operators run on it again.
    fn rewindTo(_: *ProcessingHandler, pipe: *PipelineState, at: sinks_machine.At) void {
        pipe.ahead = false;
        pipe.carry.clearRetainingCapacity();
        pipe.carry_n = 0;
        pipe.carry_bytes = 0;
        switch (pipe.source_kind) {
            .stream => {
                pipe.stream_cursor_ts = at.a;
                pipe.stream_cursor_seq = at.b;
            },
            .ts => pipe.ts_cursor = .{ .ns = at.a, .ual = at.b },
        }
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

    /// A pipeline that can't read, at a poll: counted, and logged once a
    /// pipeline has waited 30 s, at most every 30 s. One waiting for a
    /// namespace is logged at once, with what to do, since only an
    /// operator ends that wait.
    fn noteWait(self: *ProcessingHandler, shard: *Shard, pipe: *PipelineState, count: bool) void {
        const now = @import("stdx").time.milliTimestamp();
        if (count) {
            pipe.wait_counted_ms = now;
            if (shard.metrics_registry) |m| m.processing.recordHeldTick();
        }
        if (pipe.held_since_ms == 0) pipe.held_since_ms = now;
        const held_for = now - pipe.held_since_ms;
        const prompt = pipe.wait == .namespace and held_for == 0;
        if (!prompt and held_for < HELD_WARN_MS) return;
        if (now - self.held_warn_ms < 30_000) return;
        self.held_warn_ms = now;
        var wb: [160]u8 = undefined;
        const what = waitText(pipe.wait, &wb);
        switch (pipe.wait) {
            .namespace => |ns| log.warn("job '{s}': {s}: sinks don't create namespaces; create it (flo namespace create {s})", .{ self.ticking_job, what, ns }),
            else => log.warn("job '{s}': a pipeline has waited {d} s: {s}", .{ self.ticking_job, @divTrunc(held_for, 1000), what }),
        }
    }

    const HELD_WARN_MS: i64 = 30_000;

    /// What a pipeline waits for, in words.
    fn waitText(w: Wait, buf: []u8) []const u8 {
        return switch (w) {
            .none => "",
            .namespace => |ns| std.fmt.bufPrint(buf, "waiting for namespace {s}", .{ns}) catch "waiting for a namespace",
            .halted => |to| std.fmt.bufPrint(buf, "waiting: sink shard {d} is halted", .{to}) catch "waiting: a sink's shard is halted",
            .busy => |r| if (r.target.len > 0)
                std.fmt.bufPrint(buf, "waiting for room on shard {d} ({s})", .{ r.shard, r.target }) catch "waiting for room on a sink's shard"
            else
                std.fmt.bufPrint(buf, "waiting for room on shard {d}", .{r.shard}) catch "waiting for room on a sink's shard",
            .unacked => |u| std.fmt.bufPrint(buf, "waiting: {d} writes to shard {d} unacked", .{ u.count, u.shard }) catch "waiting: writes to a sink's shard unacked",
            .log => "waiting for room in this shard's log",
        };
    }

    /// A sink write refused at the pipeline's shard, before it was sent:
    /// the record is lost to that sink. Counted, and said once per
    /// interval with the count.
    pub fn noteSinkDrop(self: *ProcessingHandler, metrics: ?*MetricsRegistry, kind: []const u8, target: []const u8, why: []const u8) void {
        self.sink_drops += 1;
        if (metrics) |m| m.processing.recordDropped();
        const job = self.jobs.getPtr(self.ticking_job) orelse return;
        job.write_drops += 1;
        const now = @import("stdx").time.milliTimestamp();
        if (now - job.drop_warn_ms < 30_000) return;
        job.drop_warn_ms = now;
        log.err("job '{s}': {s} sink '{s}': record dropped: {s} ({d} dropped by this job so far)", .{ self.ticking_job, kind, target, why, job.write_drops });
    }

    /// A whole record skipped by every sink: counted as dropped, said in
    /// the job's status with where and why, and logged at most once every
    /// 30 s per job.
    fn noteDrop(self: *ProcessingHandler, shard: *Shard, at: []const u8, why: []const u8) void {
        self.sink_drops += 1;
        if (shard.metrics_registry) |m| m.processing.recordDropped();
        const job = self.jobs.getPtr(self.ticking_job) orelse return;
        job.drops += 1;
        const text = std.fmt.bufPrint(&job.last_drop, "{s}: {s}", .{ at, why }) catch null;
        job.last_drop_len = if (text) |t| t.len else job.last_drop.len;
        const now = @import("stdx").time.milliTimestamp();
        if (now - job.drop_warn_ms < 30_000) return;
        job.drop_warn_ms = now;
        log.err("job '{s}': {s} skipped: {s} ({d} skipped by this job so far)", .{ self.ticking_job, at, why, job.drops });
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
    fn writeSinkRecord(self: *ProcessingHandler, batch: *Batch, pipe: *PipelineState, shard: *Shard, scratch: Allocator, payload: []const u8, field_value: f64, timestamp_ns: u64, src_tags: []const u8, record_tags: u32) void {
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            const ns = self.namespaceIdOf(snk.namespace);
            switch (snk.kind) {
                .stream => self.addWrite(batch, shard, snk, sink_payload.stream(self.msg_buf, scratch, ns, snk.target, payload)),
                .ts => self.sendTsPoint(batch, shard, snk, snk.value_field, field_value, timestamp_ns, src_tags),
                .kv => self.writeKvSink(batch, snk, shard, "", payload),
                .queue => self.addWrite(batch, shard, snk, sink_payload.queue(self.msg_buf, shard.queue_handler.caps, ns, snk.target, snk.queue_priority, payload)),
            }
        }
    }

    /// Write a processed record to all matching sinks (stream source variant).
    /// `record_key` is the record's partitioning key (set by `keyby`, empty otherwise);
    /// it is used to construct the KV sink key.
    fn writeSinkRecordFromStream(self: *ProcessingHandler, batch: *Batch, pipe: *PipelineState, shard: *Shard, scratch: Allocator, record_key: []const u8, payload: []const u8, record_tags: u32) void {
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            const ns = self.namespaceIdOf(snk.namespace);
            switch (snk.kind) {
                .stream => self.addWrite(batch, shard, snk, sink_payload.stream(self.msg_buf, scratch, ns, snk.target, payload)),
                .ts => self.writeTsSinkFromJson(batch, pipe, snk, shard, payload),
                .kv => self.writeKvSink(batch, snk, shard, record_key, payload),
                .queue => self.addWrite(batch, shard, snk, sink_payload.queue(self.msg_buf, shard.queue_handler.caps, ns, snk.target, snk.queue_priority, payload)),
            }
        }
    }

    /// One sink write into the batch; one its client path would refuse is
    /// counted dropped instead.
    fn addWrite(self: *ProcessingHandler, batch: *Batch, shard: *Shard, snk: SinkConfig, r: sink_payload.Result) void {
        switch (r) {
            .built => |b| batch.add(b),
            .refused => |why| self.noteSinkDrop(shard.metrics_registry, @tagName(snk.kind), if (snk.kind == .ts) snk.measurement else snk.target, why.why),
        }
    }

    /// TS sink (JSON source variant): honors the `fields:` map (one point per
    /// `field_name ← json_key` pair) and the `tags:` map (real tag hash). Falls back
    /// to the `value_field` shorthand (single "value" field) when no fields map is set.
    /// Each point takes the stamp of the entry its target applies.
    fn writeTsSinkFromJson(self: *ProcessingHandler, batch: *Batch, pipe: *PipelineState, snk: SinkConfig, shard: *Shard, payload: []const u8) void {
        _ = pipe;
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
                self.sendTsPoint(batch, shard, snk, pair.key, value, 0, sink_tags);
            }
            return;
        }

        // value_field shorthand — extract the named JSON field and store it as "value".
        const value = extractJsonFloat(payload, snk.value_field) orelse
            (std.fmt.parseFloat(f64, payload) catch return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the record is not a number"));
        if (!std.math.isFinite(value)) return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the value is not a finite number");
        self.sendTsPoint(batch, shard, snk, "value", value, 0, sink_tags);
    }

    /// One TS point, at `timestamp_ns` (0: its entry's stamp).
    fn sendTsPoint(self: *ProcessingHandler, batch: *Batch, shard: *Shard, snk: SinkConfig, field: []const u8, value: f64, timestamp_ns: u64, tags: []const u8) void {
        self.addWrite(batch, shard, snk, sink_payload.ts(self.msg_buf, self.namespaceIdOf(snk.namespace), snk.measurement, field, value, timestamp_ns, tags));
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

    /// KV sink: write the record under `key_prefix + separator + record_key`,
    /// on the shard that owns that key, so a later `kv get` finds it.
    fn writeKvSink(self: *ProcessingHandler, batch: *Batch, snk: SinkConfig, shard: *Shard, record_key: []const u8, payload: []const u8) void {
        var key_buf: [MAX_SINK_KEY]u8 = undefined;
        const full_key = self.kvSinkKeyOrDrop(&key_buf, snk, shard.metrics_registry, record_key) orelse return;
        self.addWrite(batch, shard, snk, sink_payload.kv(self.msg_buf, self.namespaceIdOf(snk.namespace), full_key, payload, snk.kv_ttl_ms));
    }

    /// The shard that owns `target` in `namespace`: this one when there are
    /// no others to send to.
    pub fn sinkOwner(self: *const ProcessingHandler, shard: *const Shard, namespace: []const u8, target: []const u8) u16 {
        const mailboxes = shard.peer_mailboxes orelse return @intCast(shard.id);
        if (mailboxes.len <= 1 or self.peer_shard_count == 0) return @intCast(shard.id);
        const owner = self.peerShardId(namespace, target);
        return if (owner < mailboxes.len) owner else @intCast(shard.id);
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

test "processing: what a pipeline waits for, in words" {
    var buf: [160]u8 = undefined;
    const W = ProcessingHandler.Wait;
    const cases = [_]struct { w: W, says: []const u8 }{
        .{ .w = .{ .namespace = "orders" }, .says = "waiting for namespace orders" },
        .{ .w = .{ .halted = 2 }, .says = "waiting: sink shard 2 is halted" },
        .{ .w = .{ .busy = .{ .shard = 3, .target = "jobs" } }, .says = "waiting for room on shard 3 (jobs)" },
        .{ .w = .{ .busy = .{ .shard = 3, .target = "" } }, .says = "waiting for room on shard 3" },
        .{ .w = .{ .unacked = .{ .shard = 1, .count = 1024 } }, .says = "waiting: 1024 writes to shard 1 unacked" },
        .{ .w = .log, .says = "waiting for room in this shard's log" },
    };
    for (cases) |c| try std.testing.expectEqualStrings(c.says, ProcessingHandler.waitText(c.w, &buf));
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

test "ProcessingHandler: every op naming a job is pinned to group 0, whatever its key" {
    const Dispatch = @import("../node/dispatcher.zig").Dispatcher;
    var d = Dispatch.init();
    ProcessingHandler.register(&d);
    var req: proto.Request = undefined;
    req.namespace = "acme";
    for ([_]proto.OpCode{ .processing_submit, .processing_stop, .processing_cancel, .processing_status, .processing_remove }) |op| {
        // No hash route: a job id's bits, or a key's slot, could be any group's.
        try std.testing.expect(d.pre_route[@backingInt(op)] == null);
        for ([_][]const u8{ "", "k", "job-1a2b-3", "another-key" }) |key| {
            req.key = key;
            try std.testing.expectEqual(@as(?u16, 0), d.shard_route[@backingInt(op)].?(req));
        }
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
    _ = &kv1;

    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    handler.peer_shard_count = 2;
    handler.local_kv = &h0;

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

test "processing: a TS source holds back its lateness window until its field has had no write, late or not, for that long" {
    var pipe: ProcessingHandler.PipelineState = undefined;
    pipe.lateness_ms = 3000;
    pipe.seen_writes = null;
    pipe.seen_known = false;
    pipe.seen_at_ms = 0;
    const w = 3 * std.time.ns_per_s;
    // Before the field is seen: held back, and the shard holding it says
    // from what (the first ask reads up to its newest less the window).
    try std.testing.expectEqual(w, pipe.holdBackNs(1000));
    // A field with no series has nothing to hold back.
    pipe.noteWrites(null, 1000);
    try std.testing.expectEqual(@as(u64, 0), pipe.holdBackNs(1000));
    pipe.noteWrites(1, 1000);
    try std.testing.expectEqual(w, pipe.holdBackNs(3999));
    try std.testing.expectEqual(@as(u64, 0), pipe.holdBackNs(4000));
    // Host a wrote t=100 s and stopped; host b uploads t=71..99 s, each one
    // late: every write restarts the clock, so the window holds and b's
    // points stay ahead of the cursor.
    for (0..29) |i| {
        const now: u64 = 5000 + 2000 * i;
        pipe.noteWrites(2 + i, now);
        try std.testing.expectEqual(w, pipe.holdBackNs(now + 2999));
    }
    // The same count again isn't a write.
    pipe.noteWrites(30, 70_000);
    try std.testing.expectEqual(@as(u64, 0), pipe.holdBackNs(61_000 + 3000));
    // A window of 0 never holds back.
    pipe.lateness_ms = 0;
    pipe.seen_known = false;
    try std.testing.expectEqual(@as(u64, 0), pipe.holdBackNs(0));

    const F = @import("../ts/machine.zig").FieldValue;
    const max = std.math.maxInt(u64);
    try std.testing.expectEqual(max, ProcessingHandler.throughFor(null, w));
    try std.testing.expectEqual(max, ProcessingHandler.throughFor(F{ .newest_ns = 100 * std.time.ns_per_s }, 0));
    try std.testing.expectEqual(@as(u64, 97 * std.time.ns_per_s), ProcessingHandler.throughFor(F{ .newest_ns = 100 * std.time.ns_per_s }, w));
    // Under the window: from time 0, not wrapped.
    try std.testing.expectEqual(@as(u64, 0), ProcessingHandler.throughFor(F{ .newest_ns = std.time.ns_per_s }, w));
}
