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
const record_fields = @import("record_fields.zig");
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
const ts_machine = @import("../ts/machine.zig");
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
const persistence = @import("../storage/persistence.zig");
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

const contract_store = @import("../contract/store.zig");

pub const ProcessingHandler = struct {
    allocator: Allocator,
    /// The clock a TS source's idle window runs on; a test seam.
    mono_ms: *const fn () u64 = @import("stdx").time.monotonicMs,

    /// What this node keeps of each job it runs, beside its record in
    /// `processing.job`: only while it leads, rebuilt from the family when
    /// the family changes (`reconcileAt`), and dropped at `stopLeading`.
    runs: std.StringHashMap(JobRun),
    /// This shard's store, wired by the shard: the jobs and cursors it reads.
    store: ?*contract_store.Store = null,
    /// `processing.job`'s commit count and the term when `runs` last matched
    /// it; maxInt for never.
    runs_commits: u64 = std.math.maxInt(u64),
    runs_term: u64 = 0,
    /// Whether a job here runs, as of `processing.job`'s commit count
    /// `running_commits` (`hasRunningJob`).
    running: bool = false,
    running_commits: u64 = std.math.maxInt(u64),
    /// A run is held for want of memory, as of `short_at_ms`: looked at
    /// again `SHORT_RETRY_MS` after.
    short_runs: bool = false,
    short_at_ms: u64 = 0,
    sink_drops: u64 = 0,
    /// See `sinkRead`.
    sink_read: ?*SinkRead = null,
    chain_errors: u64 = 0,
    chain_warn_ms: i64 = 0,
    /// The job whose pipeline is ticking, so a drop or chain error names it.
    ticking_job: []const u8 = "",
    /// When a waiting pipeline was last logged (`noteWait`).
    held_warn_ms: i64 = 0,

    /// Each running job's pipelines on this leader: under its job's id for
    /// its first source, `id\0n` for each other.
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
    /// A wait or a skipped record has been reported since this node last
    /// stopped leading: `stopLeading` has something to forget.
    reported: bool = false,

    /// Jobs this node, leading, holds as failed, for /status, as of its last
    /// `reconcileAt`: those stored as failed, and running ones whose
    /// definition doesn't build here. 0 on a node that doesn't lead.
    failed_jobs: u64 = 0,
    /// Running jobs this leader holds without pipelines, for /status.
    held_jobs: u64 = 0,

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
        /// TS source: its field's late points past the edge its window
        /// reports against (`lateEdge`), as last read.
        late_points: u64 = 0,
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
        /// When it began waiting; 0 while it reads.
        held_since_ms: i64 = 0,
        /// Why it last couldn't read (`waitOf`).
        wait: Wait = .none,
        /// Its cursor as its last applied batch left it: the `from` of its
        /// next batch.
        from: sinks_machine.At = .{},
        /// Its read cursor is past `from` in term `ahead_term`, for records
        /// its operators have run on that no batch has logged yet: an idle
        /// page, or a record cut from a full batch, whose writes wait in
        /// `carry` for the next. A new term reads them again, its operators
        /// built anew (`ops_term`).
        ahead: bool = false,
        ahead_term: u64 = 0,
        /// The records read past `from` in batches not logged (a batch with
        /// no writes waits up to `IDLE_BATCH_MS`): the next logged batch
        /// covers them, and counts them.
        unlogged_records: u32 = 0,
        carry: std.ArrayListUnmanaged(u8) = .empty,
        carry_n: u32 = 0,
        carry_bytes: usize = 0,
        /// The batch it proposed and hasn't seen apply, by index and term
        /// (index 0: none), and when it last proposed one.
        batch_index: u64 = 0,
        batch_term: u64 = 0,
        last_batch_ms: u64 = 0,
        /// Records this poll may still read, and whether the last page
        /// filled its batch with more to read: then the next page is read
        /// once that batch applies. `page_limit` is the last page's size.
        poll_left: u32 = 0,
        more: bool = false,
        page_limit: usize = 0,
        /// When a poll last found it waiting, and counted it.
        wait_counted_ms: i64 = 0,
        /// The leader term its operators' state is from (0: none yet).
        ops_term: u64 = 0,
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
        pub fn hasTsSink(self: *const PipelineState) bool {
            for (self.sinks) |snk| if (snk.kind == .ts) return true;
            return false;
        }

        pub fn idleAt(self: *const PipelineState, now_ms: u64) u64 {
            if (self.lateness_ms == 0 or !self.seen_known) return 0;
            const writes = self.seen_writes orelse return 0;
            return if (now_ms -| self.seen_at_ms >= self.lateness_ms) writes else 0;
        }

        /// The index of the late-point edge a TS source reports against:
        /// the smallest at or above its window. None for a stream source, or
        /// a window past every edge.
        pub fn lateEdge(self: *const PipelineState) ?usize {
            if (self.source_kind != .ts) return null;
            return ts_machine.lateEdgeFor(@as(u64, self.lateness_ms) * std.time.ns_per_ms);
        }

        pub fn holdBackNs(self: *const PipelineState, now_ms: u64) u64 {
            const w: u64 = @as(u64, self.lateness_ms) * std.time.ns_per_ms;
            if (w == 0 or !self.seen_known) return w;
            if (self.seen_writes == null) return 0;
            if (now_ms -| self.seen_at_ms >= self.lateness_ms) return 0;
            return w;
        }
    };

    pub const JobStatus = sinks_machine.JobStatus;

    /// A job as a request sees it: its record in `processing.job`, borrowed
    /// from the store until the next commit, with what this node keeps of
    /// it while it leads (`JobRun`).
    pub const JobRecord = struct {
        job_id_owned: []const u8,
        name_owned: []const u8,
        /// The namespace its submit was stamped with: the one every request
        /// about the job must name.
        ns_id: NamespaceId,
        status: JobStatus,
        batch_size: u32,
        yaml_owned: []const u8,
        /// The namespace its submit was parsed in, as the entry stores it:
        /// what its definition is parsed in again.
        home_owned: []const u8,
        created_at_ms: i64,
        /// The records its pipelines' batches covered (`processing.cursor`).
        records_processed: u64,
        /// Why it failed: at its submit, or, while this node leads, why its
        /// definition won't build here. Empty unless it failed.
        failure_owned: []const u8 = "",
        /// This node, leading, runs no pipelines for this running job, for
        /// a cause of its own (its shard count, or memory), and why.
        held: bool = false,
        held_owned: []const u8 = "",
        /// Records this leader skipped for good, and the last one's place
        /// and reason, for its status (`JobRun`).
        drops: u64 = 0,
        last_drop: []const u8 = "",
        /// Single sink writes dropped, and the last one's sink and reason.
        write_drops: u64 = 0,
        last_write_drop: []const u8 = "",
    };

    /// How long a run held for want of memory waits to be built again.
    const SHORT_RETRY_MS: u64 = 1_000;

    /// What this node keeps of a job while it leads: never replicated, and
    /// forgotten when it stops leading, so its next leader starts them again.
    pub const JobRun = struct {
        ns_id: NamespaceId,
        /// Its definition doesn't build here, and why: the job reads as
        /// failed while this node leads. Owned, or "".
        failure_owned: []const u8 = "",
        /// See `JobRecord.held`. Owned, or "".
        held: bool = false,
        held_owned: []const u8 = "",
        drops: u64 = 0,
        last_drop: [160]u8 = undefined,
        last_drop_len: usize = 0,
        /// Single sink writes this leader dropped, the last one's sink and
        /// reason, and when one was last logged.
        write_drops: u64 = 0,
        last_write_drop: [160]u8 = undefined,
        last_write_drop_len: usize = 0,
        drop_warn_ms: i64 = 0,
        /// Held for want of memory: built again at a later look
        /// (`reconcileAt`), not only when the jobs change.
        short: bool = false,
    };

    /// The most of a failure reason a job keeps.
    pub const MAX_FAILURE_REASON = sinks_machine.MAX_FAILURE_REASON;

    pub fn init(allocator: Allocator) ProcessingHandler {
        return .{
            .allocator = allocator,
            .runs = std.StringHashMap(JobRun).init(allocator),
            .pipelines = std.StringHashMap(PipelineState).init(allocator),
        };
    }

    pub fn deinit(self: *ProcessingHandler) void {
        if (self.sink_read) |r| self.allocator.destroy(r);
        self.dropRuns();
        self.runs.deinit();
        self.pipelines.deinit();
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

    fn freeRun(self: *ProcessingHandler, key: []const u8, run: *JobRun) void {
        if (run.failure_owned.len > 0) self.allocator.free(run.failure_owned);
        self.freeHeldReason(run);
        self.allocator.free(key);
    }

    fn dropRuns(self: *ProcessingHandler) void {
        var it = self.runs.iterator();
        while (it.next()) |e| self.freeRun(e.key_ptr.*, e.value_ptr);
        self.runs.clearRetainingCapacity();
        var pit = self.pipelines.iterator();
        while (pit.next()) |e| {
            self.freePipelineState(e.value_ptr);
            self.allocator.free(e.key_ptr.*);
        }
        self.pipelines.clearRetainingCapacity();
        self.failed_jobs = 0;
        self.held_jobs = 0;
        self.runs_commits = std.math.maxInt(u64);
        // A snapshot's load clears the store, its commit counts with it: a
        // count remembered from before could match one after.
        self.running_commits = std.math.maxInt(u64);
    }

    /// Note that this node holds running job `id` without pipelines, for
    /// `reason`; said once, when the hold starts. A leader change runs it on
    /// a node that can.
    fn holdJob(self: *ProcessingHandler, id: []const u8, run: *JobRun, reason: []const u8) void {
        if (!run.held) {
            run.held = true;
            log.warn("processing job {s} held on this node: {s}", .{ id, reason });
        }
        self.freeHeldReason(run);
        run.held_owned = self.allocator.dupe(u8, reason[0..@min(reason.len, MAX_FAILURE_REASON)]) catch HELD_NO_MEMORY;
    }

    /// Why a job is held when copying its reason ran out of memory.
    const HELD_NO_MEMORY = "out of memory";

    fn freeHeldReason(self: *ProcessingHandler, run: *JobRun) void {
        if (run.held_owned.len > 0 and run.held_owned.ptr != HELD_NO_MEMORY.ptr) self.allocator.free(run.held_owned);
        run.held_owned = "";
    }

    /// Job `id`'s definition doesn't build on this node, for `reason`.
    fn failRun(self: *ProcessingHandler, id: []const u8, run: *JobRun, reason: []const u8) void {
        log.err("processing job {s} not started: {s}", .{ id, reason });
        if (run.failure_owned.len > 0) self.allocator.free(run.failure_owned);
        run.failure_owned = self.allocator.dupe(u8, reason[0..@min(reason.len, MAX_FAILURE_REASON)]) catch "";
    }

    fn readView(self: *const ProcessingHandler) ?view_mod.ReadView {
        return .{ .store = self.store orelse return null };
    }

    /// Job `job_id` in namespace `ns`, if this shard holds it: its record,
    /// with what this node keeps of it while it leads. A job is seen,
    /// stopped and changed only from its own namespace, so from any other it
    /// is not found.
    pub fn viewOf(self: *const ProcessingHandler, ns: NamespaceId, job_id: []const u8) ?JobRecord {
        const read = self.readView() orelse return null;
        const j = sinks_machine.jobOf(read, ns, job_id) orelse return null;
        var v: JobRecord = .{
            .job_id_owned = job_id,
            .name_owned = j.name,
            .ns_id = ns,
            .status = sinks_machine.statusOf(j),
            .batch_size = j.batch_size,
            .yaml_owned = j.yaml,
            .home_owned = j.home,
            .created_at_ms = @intCast(@min(j.created_at_ms, std.math.maxInt(i64))),
            .records_processed = jobRecords(read, ns, job_id),
            .failure_owned = j.failure,
        };
        if (self.runs.getPtr(job_id)) |run| if (run.ns_id == ns) {
            if (v.status == .running and run.failure_owned.len > 0) {
                v.status = .failed;
                v.failure_owned = run.failure_owned;
            }
            v.held = run.held;
            v.held_owned = run.held_owned;
            v.drops = run.drops;
            v.last_drop = run.last_drop[0..run.last_drop_len];
            v.write_drops = run.write_drops;
            v.last_write_drop = run.last_write_drop[0..run.last_write_drop_len];
        };
        return v;
    }

    /// The job of this node's first run, for tests that run one.
    pub fn firstJob(self: *const ProcessingHandler) ?JobRecord {
        var it = self.runs.iterator();
        const e = it.next() orelse return null;
        return self.viewOf(e.value_ptr.ns_id, e.key_ptr.*);
    }

    /// The records job `job_id`'s pipelines' batches covered: its first
    /// source's cursor and each other source's (`id\0n`).
    fn jobRecords(read: view_mod.ReadView, ns: NamespaceId, job_id: []const u8) u64 {
        var total = sinks_machine.recordsOf(read, ns, job_id);
        var pb: [4 + pe.MAX_PIPELINE_KEY + 1]u8 = undefined;
        const k = view_mod.idKey(&pb, ns, job_id) catch return total;
        if (k.len + 1 > pb.len) return total;
        pb[k.len] = 0;
        var it = read.range(sinks_machine.CursorFam, pb[0 .. k.len + 1]);
        while (it.next() catch null) |c| total +|= c.value.v.records;
        return total;
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
        // A job lives on its home, its first source's shard, and its id
        // carries that shard's slot: every op naming a job routes by the id.
        // A submit is routed by the shard itself, which parses its
        // definition (`Shard.route`, `homeHash`).
        dispatcher.register(.processing_submit, dispatchProcessing);
        inline for (.{ .processing_stop, .processing_cancel, .processing_status }) |op|
            dispatcher.registerWithRoute(op, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_remove, table_run.handler("processing.remove", ProcessingHandler, runRemove), run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.processing_list, dispatchProcessing, localScanJobs);
    }

    /// The hash job `def` routes by, submitted in namespace `job_ns`: its
    /// first source's, from the very route function a stream or TS op on
    /// that source routes by, so the job's home is the shard owning that
    /// source by construction. Null with no source, or while the first
    /// source's namespace has no id here.
    pub fn homeHash(self: *const ProcessingHandler, def: *const definition.JobDefinition, job_ns: NamespaceId) ?u64 {
        if (def.sources.items.len == 0) return null;
        const src = def.sources.items[0];
        const ns = if (src.namespace.len == 0 or std.mem.eql(u8, src.namespace, def.namespace)) job_ns else self.namespaceIdOf(src.namespace);
        if (ns == .invalid) return null;
        var req: Request = .{ .header = undefined, .namespace = src.namespace, .key = "", .value = "", .ns_id = ns };
        return switch (src.kind) {
            .stream => blk: {
                req.key = src.stream;
                break :blk stream_handler_mod.StreamHandler.preRouteByStream(req);
            },
            .ts => blk: {
                req.key = src.ts_measurement;
                break :blk ts_handler_mod.TSHandler.preRouteByMeasurement(req);
            },
        };
    }

    /// A submit's home hash, from its definition: null if it doesn't parse
    /// (it is refused where it lands) or names no home yet.
    pub fn submitHash(self: *const ProcessingHandler, req: Request) ?u64 {
        if (req.ns_id == .invalid or req.value.len == 0) return null;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const req_ns: ?[]const u8 = if (req.namespace.len > 0) req.namespace else null;
        const def = parser.parseJobDefinitionWithNamespace(arena.allocator(), req.value, req_ns, null) catch return null;
        return self.homeHash(&def, req.ns_id);
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
        const read = handler.readView() orelse return .{ .items = &.{}, .next_cursor = null };
        const prefix = ns.id.bytes();
        var n: usize = 0;
        var counting = read.range(sinks_machine.JobFam, &prefix);
        while (counting.next() catch null) |_| n += 1;
        const name_buf = dispatcher_mod.scanScratch(n);
        var count: usize = 0;
        var it = read.range(sinks_machine.JobFam, &prefix);
        while (it.next() catch null) |item| {
            if (count >= name_buf.len) break;
            name_buf[count] = item.key[prefix.len..];
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
        const job_id = r.slice() orelse "";
        // Stored as failed: refused, but it ran, and holds its slot.
        if (sinks_machine.submitFailure(result)) |why| {
            var buf: [persistence_mod.Result.MESSAGE_MAX]u8 = undefined;
            const message = std.fmt.bufPrint(&buf, "processing job {s} failed: {s}; free its slot with `flo processing remove {s}`", .{ job_id, why, job_id }) catch buf[0..];
            return shard.sendRefusal(conn, request_id, .bad_request, .{ .reason = .unclassified, .ran = .yes, .message = message });
        }
        shard.sendOkResponse(conn, request_id, job_id);
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
        if (self.jobCount() >= sinks_machine.max_jobs) return self.refuseAtJobLimit(shard, conn, req.header.request_id);

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
        if (def.namesReservedStream()) {
            shard.sendRefusal(conn, req.header.request_id, .unauthorized, .{ .reason = .reserved_key, .ran = .no, .message = @import("../stream/names.zig").RESERVED });
            return;
        }
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

        // The definition has passed: propose the create of the job's own
        // namespace where this shard makes it (a lone shard's; shard 0 knows
        // the id it just proposed before its table does), so the submit has
        // its namespace's id. A node of several shards waited for it at the
        // edge.
        shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
        const ns_for_home = if (req.ns_id != .invalid) req.ns_id else shard.namespaceId(req.namespace);
        // The job's id carries its home's slot, so every later op on it
        // routes there, and the term, so no leader repeats another's. The
        // home is fixed at submit: a source stream's slot is a function of
        // its namespace's id and its name, so a stream made again under that
        // name in the same namespace is on the same shard. A first source in
        // another namespace that doesn't exist yet has no home: that
        // namespace isn't the job's to create.
        const home_hash = self.homeHash(&def, ns_for_home) orelse {
            const src_ns = if (def.sources.items.len > 0) def.sources.items[0].namespace else "";
            const own = src_ns.len == 0 or std.mem.eql(u8, src_ns, def.namespace);
            var mb: [256]u8 = undefined;
            const msg = if (own) "job not submitted: its first source names no stream or measurement" else std.fmt.bufPrint(&mb, "job not submitted: its first source's namespace '{s}' doesn't exist; create namespace {s} first, then submit", .{ src_ns, src_ns }) catch "job not submitted: its first source's namespace doesn't exist; create it first, then submit";
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, msg);
            return;
        };
        if (shard.shardCount() > 1 and shard.router.hashToShard(home_hash) != shard.id) {
            // Routed here without its home (`Shard.route` sends a submit to
            // its home): refused, never run on the wrong shard.
            shard.sendErrorResponse(conn, req.header.request_id, .unavailable, "job not submitted: it reached a shard other than its source's; try again");
            return;
        }
        var id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const job_id = shard.run_id_gen.next(.job, router.slot(home_hash), shard.raft_node.current_term, &id_buf);

        // Persist through Raft: the applier stores the job, and its
        // leader builds its pipelines from what it stored.
        const ns_id = ns_for_home;
        const proposed = self.proposeSubmit(shard, ns_id, job_id, .running, def.batch_size, home, yaml) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "job not persisted"));
            return;
        };
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        shard.park(conn, req, proposed, answerSubmit);
    }

    fn refuseAtJobLimit(_: *const ProcessingHandler, shard: *Shard, conn: *Connection, request_id: u64) void {
        var buf: [192]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "job not submitted: this shard holds at most {d} processing jobs; remove a stopped, cancelled or failed one with `flo processing remove <job_id>`", .{sinks_machine.max_jobs}) catch "job not submitted: this shard holds as many processing jobs as it may";
        shard.sendRefusal(conn, request_id, .overloaded, .{ .reason = .unclassified, .ran = .no, .message = msg });
    }

    // ── Stop ─────────────────────────────────────────────────────────────

    /// Whether this shard holds a running job: applied state, so a leader
    /// that can't run pipelines (its shard halted) still knows its duty has
    /// work it holds back. Looked at again only when the family changed.
    pub fn hasRunningJob(self: *ProcessingHandler) bool {
        const st = self.store orelse return false;
        const commits = st.family(sinks_machine.JobFam.ID).commits;
        if (commits == self.running_commits) return self.running;
        self.running_commits = commits;
        self.running = false;
        var it = (view_mod.ReadView{ .store = st }).range(sinks_machine.JobFam, "");
        while (it.next() catch null) |item| if (sinks_machine.statusOf(item.value.v) == .running) {
            self.running = true;
            break;
        };
        return self.running;
    }

    /// The jobs this shard holds, in every namespace.
    pub fn jobCount(self: *const ProcessingHandler) usize {
        const st = self.store orelse return 0;
        return st.family(sinks_machine.JobFam.ID).count;
    }

    /// The job `job_id`, if it lives in namespace `ns`: a job is seen,
    /// stopped and changed only from its own namespace, so from any other it
    /// is not found.
    fn ownedJob(self: *ProcessingHandler, job_id: []const u8, ns: NamespaceId) ?JobRecord {
        return self.viewOf(ns, job_id);
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
            const note = noteOf(&job);
            const live: []const u8 = if (note == .none) self.liveNote(&job, &live_buf) orelse "" else "";
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
        /// At least `late_points` of the source fields' points came more
        /// than `late_edge_ms` behind their field's newest: the largest edge
        /// the job's TS sources report against. `NO_LATE_EDGE` for none.
        late_edge_ms: u64 = NO_LATE_EDGE,
        late_points: u64 = 0,

        pub const NO_LATE_EDGE = std.math.maxInt(u64);

        /// `[running:u8][records_in:u64][records_out:u64][watermark_ms:i64]`
        /// `[tick_avg_us:u64][tick_max_us:u64][uptime_ms:u64]`
        /// `[late_edge_ms:u64][late_points:u64]`
        pub const SIZE = 1 + 8 * 8;

        fn write(self: Runtime, w: *std.Io.Writer) std.Io.Writer.Error!void {
            try w.writeByte(@intFromBool(self.running));
            try w.writeInt(u64, self.records_in, .little);
            try w.writeInt(u64, self.records_out, .little);
            try w.writeInt(i64, self.watermark_ms, .little);
            try w.writeInt(u64, self.tick_avg_us, .little);
            try w.writeInt(u64, self.tick_max_us, .little);
            try w.writeInt(u64, self.uptime_ms, .little);
            try w.writeInt(u64, self.late_edge_ms, .little);
            try w.writeInt(u64, self.late_points, .little);
        }
    };

    /// Job `job_id`'s pipelines on this node: the job's id, or `id\0n`
    /// for each of a multi-source job's sources.
    fn runtimeOf(self: *const ProcessingHandler, job_id: []const u8, now_ms: i64) Runtime {
        var r: Runtime = .{};
        var started: i64 = std.math.maxInt(i64);
        var tick_sum: u64 = 0;
        var ticks: u64 = 0;
        var late_edge: ?usize = null;
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
            if (p.lateEdge()) |edge| late_edge = @max(late_edge orelse edge, edge);
        }
        // Each field once: its count is the whole field's, however many of
        // the job's sources read it. A source on a smaller edge isn't
        // counted, as its count past the larger one isn't carried for it.
        if (late_edge) |edge| {
            r.late_edge_ms = ts_machine.LATE_EDGES_NS[edge] / std.time.ns_per_ms;
            var outer = self.pipelines.iterator();
            var at: usize = 0;
            while (outer.next()) |e| : (at += 1) {
                const p = e.value_ptr;
                if (!self.lateCounts(e.key_ptr.*, p, job_id, edge)) continue;
                var inner = self.pipelines.iterator();
                var seen = false;
                var i: usize = 0;
                while (inner.next()) |q| : (i += 1) {
                    if (i == at) break;
                    if (self.lateCounts(q.key_ptr.*, q.value_ptr, job_id, edge) and sameField(p, q.value_ptr)) {
                        seen = true;
                        break;
                    }
                }
                if (!seen) r.late_points += p.late_points;
            }
        }
        if (ticks > 0) r.tick_avg_us = tick_sum / ticks / std.time.ns_per_us;
        if (r.running) r.uptime_ms = @intCast(@max(0, now_ms - started));
        return r;
    }

    /// Whether pipeline `key` is one of job `job_id`'s, reporting late
    /// points against `edge`.
    fn lateCounts(_: *const ProcessingHandler, key: []const u8, p: *const PipelineState, job_id: []const u8, edge: usize) bool {
        const base = if (std.mem.indexOfScalar(u8, key, 0)) |nul| key[0..nul] else key;
        return std.mem.eql(u8, base, job_id) and p.lateEdge() == edge;
    }

    fn sameField(a: *const PipelineState, b: *const PipelineState) bool {
        return std.mem.eql(u8, a.src_namespace, b.src_namespace) and std.mem.eql(u8, a.src_measurement, b.src_measurement) and std.mem.eql(u8, a.src_field, b.src_field);
    }

    // ── List ─────────────────────────────────────────────────────────────

    fn handleList(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        // [limit:u32][cursor...]?
        var limit: u32 = 100;
        if (req.value.len >= 4) limit = std.mem.readInt(u32, req.value[0..4], .little);
        if (limit == 0) limit = 100;

        var jobs: std.ArrayListUnmanaged(JobRecord) = .empty;
        defer jobs.deinit(self.allocator);
        if (self.readView()) |read| {
            const prefix = req.ns_id.bytes();
            var jit = read.range(sinks_machine.JobFam, &prefix);
            while (jit.next() catch null) |item| {
                if (jobs.items.len >= limit) break;
                const v = self.viewOf(req.ns_id, item.key[prefix.len..]) orelse continue;
                jobs.append(self.allocator, v) catch {
                    shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                    return;
                };
            }
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
    fn encodeJobList(allocator: Allocator, jobs: []const JobRecord, next_cursor: ?[]const u8) ![]u8 {
        const cursor = next_cursor orelse "";
        var total: usize = 4 + 1 + 2 + cursor.len;
        for (jobs) |*j| total += entrySize(j);
        const buf = try allocator.alloc(u8, total);
        var w: std.Io.Writer = .fixed(buf);
        w.writeInt(u32, @intCast(jobs.len), .little) catch unreachable;
        for (jobs) |*j| writeEntry(j, &w) catch unreachable;
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
    pub fn listedJob(self: *const ProcessingHandler, ns: NamespaceId, job_id: []const u8) ?JobRecord {
        return self.viewOf(ns, job_id);
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
            w.print("{d} record{s} skipped, the last {s}", .{ job.drops, if (job.drops == 1) "" else "s", job.last_drop }) catch {};
        }
        if (job.write_drops > 0) {
            if (w.end > 0) w.writeAll("; ") catch {};
            w.print("{d} record{s} dropped by a sink, the last by {s}", .{ job.write_drops, if (job.write_drops == 1) "" else "s", job.last_write_drop }) catch {};
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
    /// entry's stamp; its value is `sinks_machine.encodeSubmit`'s.
    fn proposeSubmit(self: *ProcessingHandler, shard: *Shard, ns_id: NamespaceId, job_id: []const u8, status: JobStatus, batch_size: u32, job_namespace: []const u8, yaml: []const u8) !persistence_mod.ProposeResult {
        _ = self;
        var value_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
        const value = sinks_machine.encodeSubmit(&value_buf, status, batch_size, job_namespace, yaml) orelse return error.PayloadTooLarge;
        return persistence_mod.proposeEntryIn(shard, .processing_submit, Flags.NONE, ns_id, job_id, value);
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

    // ── Pipeline Execution ──────────────────────────────────────────────

    /// Build the tag registry + execution pipelines for a parsed job definition
    /// and register them under `job_id`: what a leader runs for each running
    /// job (`startRun`). Does not take ownership of `def` — createPipeline
    /// deep-copies everything it retains, so the caller may free `def` after.
    /// Returns why the job can't run, written into `why_buf` if need be:
    /// `failed` when its definition can't run on any node of this build,
    /// `held` when the cause is this node's own.
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
            .out_of_memory => .{ .short = "an operator couldn't be built: out of memory" },
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
                return .{ .short = "a pipeline couldn't be built: out of memory" };
            };
            if (self.pipelines.contains(owned_key)) {
                self.allocator.free(owned_key);
                continue;
            }
            if (!self.createPipeline(owned_key, src, def.sinks.items, def, tag_registry)) {
                self.dropPipelines(job_id);
                return .{ .short = "a pipeline couldn't be built: out of memory" };
            }
        }

        return null;
    }

    /// `short`: held, out of memory, and tried again later (`reconcileAt`).
    const StartRefusal = union(enum) { failed: []const u8, held: []const u8, short: []const u8 };

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
            var pairs_buf: [ts_mod.MAX_TAGS]ts_mod.TagPair = undefined;
            var n: usize = 0;
            var i: usize = 0;
            while (i + 1 < src.ts_tags.len and n < pairs_buf.len) : (i += 2) {
                pairs_buf[n] = .{ .key = src.ts_tags[i], .value = src.ts_tags[i + 1] };
                n += 1;
            }
            var tag_buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
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

        const count = self.fillOperators(job_id, op_specs, tags, ops, backings, true) orelse {
            self.allocator.free(backings);
            self.allocator.free(ops);
            return false;
        };

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

    /// Builds `specs` into `ops` and `backings`, each as long; how many, or
    /// null with none left built when one can't be.
    fn fillOperators(self: *ProcessingHandler, job_id: []const u8, specs: []const definition.OperatorSpec, tags: *const definition.TagRegistry, ops: []Operator, backings: []native_registry.CreateResult, loud: bool) ?usize {
        var count: usize = 0;
        for (specs) |*spec| {
            const result = native_registry.create(self.allocator, spec, tags) catch |err| {
                if (loud) log.err("processing job {s}: pipeline not started: operator '{s}' (type {s}) can't be built: {}", .{ job_id, spec.name, spec.type_name, err });
                for (backings[0..count]) |*b| b.deinit(self.allocator);
                return null;
            };
            if (result.backing == .kv_lookup) {
                result.backing.kv_lookup.setLookupFn(shardKvLookup, @ptrCast(self));
            }
            backings[count] = result;
            ops[count] = result.op;
            count += 1;
        }
        return count;
    }

    /// The pipeline's operators built again from its job's definition,
    /// their state empty: what a new leader term starts from, as it reads
    /// again from the logged cursor. Why not, the old ones kept, if they
    /// can't be; the caller waits a poll and tries again.
    fn resetOperators(self: *ProcessingHandler, ns: NamespaceId, job_id: []const u8, pipe: *PipelineState) ?[]const u8 {
        const read = self.readView() orelse return "its job isn't here";
        const job = sinks_machine.jobOf(read, ns, job_id) orelse return "its job isn't here";
        var diag: parser.Diagnostic = .{};
        var def = parser.parseJobDefinitionWithNamespace(self.allocator, job.yaml, job.home, &diag) catch |err|
            return if (err == error.OutOfMemory) "out of memory" else "its definition no longer parses";
        defer def.deinit(self.allocator);
        if (def.operators.items.len != pipe.operators.len) return "its definition no longer has the operators it ran";
        var tags = definition.TagRegistry{};
        fillTagRegistry(&tags, &def);
        const ops = self.allocator.alloc(Operator, pipe.operators.len) catch return "out of memory";
        const backings = self.allocator.alloc(native_registry.CreateResult, pipe.operators.len) catch {
            self.allocator.free(ops);
            return "out of memory";
        };
        _ = self.fillOperators(job_id, def.operators.items, &tags, ops, backings, false) orelse {
            self.allocator.free(ops);
            self.allocator.free(backings);
            return "an operator can't be built";
        };
        for (pipe.operator_backings) |*b| b.deinit(self.allocator);
        self.allocator.free(pipe.operator_backings);
        self.allocator.free(pipe.operators);
        pipe.operators = ops;
        pipe.operator_backings = backings;
        return null;
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

    /// `{"measurement":<name>,"field":<name>,"value":<v>,"timestamp_ms":<ms>,
    /// "tags":{<key>:<value>,...}}`: a point as its operators and its
    /// stream and queue sinks see it. Names and tags are JSON strings
    /// escaped so a sink's field read gives back their exact bytes; a
    /// non-finite value is null. `tags` is the point's canonical tag string,
    /// each pair split at its first `=` (a pair without one is a key whose
    /// value is empty).
    fn tsPointJson(allocator: Allocator, measurement: []const u8, field: []const u8, value: f64, ts_ms: i64, tags: []const u8) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(allocator);
        errdefer out.deinit();
        var jw: std.json.Stringify = .{ .writer = &out.writer };
        writeTsPoint(&jw, measurement, field, value, ts_ms, tags) catch return error.OutOfMemory;
        return out.toOwnedSlice();
    }

    fn writeTsPoint(jw: *std.json.Stringify, measurement: []const u8, field: []const u8, value: f64, ts_ms: i64, tags: []const u8) std.json.Stringify.Error!void {
        try jw.beginObject();
        try jw.objectField("measurement");
        try jw.write(measurement);
        try jw.objectField("field");
        try jw.write(field);
        try jw.objectField("value");
        if (std.math.isFinite(value)) try jw.write(value) else try jw.write(null);
        try jw.objectField("timestamp_ms");
        try jw.write(ts_ms);
        try jw.objectField("tags");
        try jw.beginObject();
        var it = std.mem.splitScalar(u8, tags, ',');
        while (it.next()) |pair| {
            if (pair.len == 0) continue;
            const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
            try jw.objectField(pair[0..eq]);
            try jw.write(if (eq < pair.len) pair[eq + 1 ..] else "");
        }
        try jw.endObject();
        try jw.endObject();
    }

    /// The longest record `tsPointJson` makes: every byte of the
    /// measurement, field and tags escaped as `\u00XX`, plus the keys and
    /// numbers. A stored point's field and tags together fit the TS write's
    /// value buffer.
    pub const MAX_TS_POINT_JSON: usize = 6 * (persistence.MAX_ENTRY_KEY + ts_mod.MAX_TAG_STRING + 512) + 256;
    comptime {
        // A point's record always fits a queue message and a stream sink's
        // payload, so a sink never refuses one for its size.
        std.debug.assert(MAX_TS_POINT_JSON <= @import("../protocol/queue_entries.zig").MAX_MESSAGE);
        std.debug.assert(MAX_TS_POINT_JSON <= sink_payload.MAX_PAYLOAD);
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
        /// Writes to a sink's shard are queued behind `stall`, as its
        /// answers say: a target there over its parked cap, a halted shard
        /// beyond it. `target` is the sink whose write waits, when the
        /// first unacked message is one of this pipeline's; "" otherwise.
        pressure: struct { shard: u16, stall: channel_driver.Stall.Value, target: []const u8 },
        /// This shard's outbox to a sink's shard (`to`), or all its
        /// outboxes together (`to` null), hold their cap: what it sent
        /// isn't taken as fast as it is made.
        outbox: struct { to: ?u16, bytes: u64, cap: u64, cause: ?channel_driver.Cause = null },
        /// A sink's namespace holds its share of this shard's outbox to
        /// `to`, or of all its outboxes together (`to` null)
        /// (`raft_node_mod.shareHeld`): others may still send there.
        share: struct { to: ?u16, namespace: []const u8, bytes: u64, cap: u64 },
        /// This shard's log has no room for another entry now.
        log,
        /// Its operators couldn't be built anew for this leader term, and
        /// why: tried again each poll.
        operators: []const u8,
    };

    /// Why `pipe` can't read now, or `.none`. A sink batch commits all its
    /// writes or none, so a pipeline waits whole: one full target delays
    /// every sink of the job, and every job sending to that shard, until it
    /// has room. Below its parked cap a full target parks what it can't
    /// take, so this waits only past it.
    fn waitOf(self: *const ProcessingHandler, shard: *Shard, ns: NamespaceId, pipe: *const PipelineState) Wait {
        for (pipe.sinks) |snk| if (self.namespaceIdOf(snk.namespace) == .invalid) return .{ .namespace = snk.namespace };
        if (self.haltedSinkShard(shard, pipe)) |to| return .{ .halted = to };
        const read: view_mod.ReadView = .{ .store = &shard.defaultPartition().store };
        // Writes are never refused at apply for an outbox's sake: the edge
        // stops adding to one at its cap, and each pipeline's batch passes
        // it by at most its own size. It goes on below the cap: no
        // hysteresis, since one batch is the most it overshoots by.
        const caps = shard.outbox_caps.sink;
        // The driver's clock moves only while it ticks; pressure is read
        // against the time now, so it expires though nothing is sent.
        const now_ms = @import("stdx").time.monotonicMs();
        const total = shard.outboxBytes(.sink);
        if (total >= caps.shard) return .{ .outbox = .{ .to = null, .bytes = total, .cap = caps.shard } };
        // One namespace's sinks may send to every shard, so its share of
        // the shard's cap is over all its outboxes together.
        for (pipe.sinks) |snk| {
            const held = shard.nsOutboxBytes(.sink, self.namespaceIdOf(snk.namespace));
            if (raft_node_mod.shareHeld(held, total, caps.shard)) return .{ .share = .{ .to = null, .namespace = snk.namespace, .bytes = held, .cap = caps.shard } };
        }
        var to: u16 = 0;
        const shards: u16 = @max(self.peer_shard_count, 1);
        while (to < shards) : (to += 1) {
            if (!self.sendsTo(pipe, to)) continue;
            const meta = channels_mod.metaOf(read, to, .sink);
            if (shard.channel.pressureOf(to, .sink, now_ms)) |stall|
                return .{ .pressure = .{ .shard = to, .stall = stall, .target = self.waitingSink(read, pipe, to, meta.acked + 1) } };
            const bytes = channels_mod.bytesTo(read, to, .sink);
            if (bytes >= caps.channel) return .{ .outbox = .{ .to = to, .bytes = bytes, .cap = caps.channel, .cause = shard.channel.causeOf(to, .sink, now_ms) } };
            for (pipe.sinks) |snk| {
                if (self.sinkShard(snk)) |s_to| if (s_to != to) continue;
                const held = channels_mod.nsBytesTo(read, to, .sink, self.namespaceIdOf(snk.namespace));
                if (raft_node_mod.shareHeld(held, bytes, caps.channel)) return .{ .share = .{ .to = to, .namespace = snk.namespace, .bytes = held, .cap = caps.channel } };
            }
        }
        const raft = shard.raft_node;
        if (raft.log.lastIndex() -| raft.last_applied >= raft_node_mod.MAX_OUTSTANDING) return .log;
        // Room for a whole batch within what the log takes of this shard's
        // own writes, so one isn't built only to be refused.
        const room = raft_node_mod.PROPOSAL_BYTES - raft_node_mod.PROPOSAL_RESERVE;
        if (raft.pendingBytes() + limits.MAX_LOG_ENTRY_PAYLOAD > room) return .log;
        // And within its namespace's share of that room, which its batches
        // are charged to as its clients' writes are.
        const in_ns = raft.pendingBytesIn(ns.int());
        if (in_ns > 0 and raft_node_mod.shareHeld(in_ns + limits.MAX_LOG_ENTRY_PAYLOAD, raft.pendingBytes() + limits.MAX_LOG_ENTRY_PAYLOAD, room)) return .log;
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
    /// another job's, whose full target this one can't name.
    fn waitingSink(self: *const ProcessingHandler, read: view_mod.ReadView, pipe: *const PipelineState, to: u16, seq: u64) []const u8 {
        var kb: [channels_mod.OUT_KEY]u8 = undefined;
        const out = (read.get(channels_mod.OutFam, channels_mod.outKey(&kb, to, .sink, seq)) catch null) orelse return "";
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
        if (leads) self.reconcileAt(shard.raft_node.current_term);

        var it = self.pipelines.iterator();
        while (it.next()) |entry| {
            const pipeline_key = entry.key_ptr.*;
            const pipe = entry.value_ptr;

            // Resolve base job ID (strip multi-source suffix "\0idx" if present)
            const base_job_id = if (std.mem.indexOfScalar(u8, pipeline_key, 0)) |nul_pos|
                pipeline_key[0..nul_pos]
            else
                pipeline_key;

            // Only a running job's: a run exists only for one (`reconcileAt`).
            const run = self.runs.getPtr(base_job_id) orelse continue;
            if (run.held or run.failure_owned.len > 0) continue;
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
            // A waiting one looks again each tick, so its wait ends as soon
            // as it can.
            if (!due and !pipe.more and (pipe.wait == .none or pipe.wait == .operators)) continue;
            // Its batch in the log: nothing more until it applies.
            if (pipe.batch_term == raft.current_term and pipe.batch_index > raft.last_applied) continue;
            // Operator state is a leader term's: a new term reads again
            // from the logged cursor, so its operators start empty.
            if (pipe.ops_term != raft.current_term) {
                if (pipe.ops_term != 0 and pipe.operators.len > 0) if (self.resetOperators(run.ns_id, base_job_id, pipe)) |why| {
                    // Tried again at the next poll, not every tick.
                    pipe.wait = .{ .operators = why };
                    pipe.last_poll_ms = now_ms;
                    self.reported = true;
                    self.noteWait(shard, pipe, true);
                    continue;
                };
                pipe.ops_term = raft.current_term;
            }
            self.loadCursor(shard, run.ns_id, pipeline_key, pipe);
            pipe.wait = self.waitOf(shard, run.ns_id, pipe);
            if (pipe.wait != .none) {
                self.reported = true;
                // Counted once a poll; a poll it was partway through goes
                // on once the wait ends.
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
            // the averages toward zero.
            const before = pipe.records_in;
            const t0 = @import("stdx").time.nanoTimestamp();
            self.ticking_job = base_job_id;
            defer self.ticking_job = "";
            switch (pipe.source_kind) {
                .ts => self.tickTsSource(pipeline_key, pipe, shard, run.ns_id),
                .stream => self.tickStreamSource(pipeline_key, pipe, shard, run.ns_id),
            }
            const processed = pipe.records_in - before;
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

    /// This node no longer runs its pipelines (it stopped leading, or its
    /// shard halted): what it reported of them, their waits and the
    /// records it skipped, is its next leader's to say. Forgotten, and out
    /// of the node's gauges.
    pub fn stopLeading(self: *ProcessingHandler, shard: *Shard) void {
        self.noteWaitingNamespace(shard, 0);
        self.reported = false;
        // A follower runs no pipelines and keeps no runs: what it would
        // need, it rebuilds from the jobs family if it leads again.
        if (self.runs.count() == 0 and self.pipelines.count() == 0) return;
        self.dropRuns();
    }

    /// While this node leads, its runs and pipelines match `processing.job`:
    /// a running job gets a run, its pipelines built from its definition;
    /// one stopped, cancelled or removed loses them. Looked at again only
    /// when the family changed or the term did.
    pub fn reconcileAt(self: *ProcessingHandler, term: u64) void {
        const st = self.store orelse return;
        const fam = st.family(sinks_machine.JobFam.ID);
        // A run held for want of memory is built again, a second after.
        const now = self.mono_ms();
        const retry = self.short_runs and now -| self.short_at_ms >= SHORT_RETRY_MS;
        if (fam.commits == self.runs_commits and term == self.runs_term and !retry) return;
        self.runs_commits = fam.commits;
        self.runs_term = term;
        // Timed from when a run first went short, or was last tried: jobs
        // changing more often than the wait don't put the retry off.
        const was_short = self.short_runs;
        self.short_runs = false;
        if (retry) self.short_at_ms = now;
        const read: view_mod.ReadView = .{ .store = st };
        // Removing invalidates the iterator: look again after each one.
        outer: while (true) {
            var it = self.runs.iterator();
            while (it.next()) |e| {
                const j = sinks_machine.jobOf(read, e.value_ptr.ns_id, e.key_ptr.*);
                if (j != null and sinks_machine.statusOf(j.?) == .running and !(retry and e.value_ptr.short)) continue;
                const key = e.key_ptr.*;
                var run = e.value_ptr.*;
                self.dropPipelines(key);
                _ = self.runs.remove(key);
                self.freeRun(key, &run);
                continue :outer;
            }
            break;
        }
        var failed: u64 = 0;
        var held: u64 = 0;
        var it = read.range(sinks_machine.JobFam, "");
        while (it.next() catch null) |item| {
            if (item.key.len <= 4) continue;
            const j = item.value.v;
            const status = sinks_machine.statusOf(j);
            if (status == .failed) failed += 1;
            if (status != .running) continue;
            const ns: NamespaceId = .of(std.mem.readInt(u32, item.key[0..4], .big));
            const id = item.key[4..];
            const run = self.runs.getPtr(id) orelse self.startRun(ns, id, j) orelse {
                self.short_runs = true;
                continue;
            };
            if (run.short) self.short_runs = true;
            if (run.held) held += 1;
            if (run.failure_owned.len > 0) failed += 1;
        }
        self.failed_jobs = failed;
        self.held_jobs = held;
        if (self.short_runs and !was_short) self.short_at_ms = now;
    }

    /// A run for running job `id`, its pipelines built from its definition
    /// here; one that won't build is held or failed, on this node only.
    /// Null only out of memory for the run itself: tried again at the next
    /// change.
    fn startRun(self: *ProcessingHandler, ns: NamespaceId, id: []const u8, j: sinks_machine.Job.Value) ?*JobRun {
        const key = self.allocator.dupe(u8, id) catch return null;
        self.runs.put(key, .{ .ns_id = ns }) catch {
            self.allocator.free(key);
            return null;
        };
        const run = self.runs.getPtr(key).?;
        var diag: parser.Diagnostic = .{};
        var def = parser.parseJobDefinitionWithNamespace(self.allocator, j.yaml, j.home, &diag) catch |err| {
            if (err == error.OutOfMemory) {
                self.holdJob(key, run, "its definition couldn't be parsed: out of memory");
                run.short = true;
            } else self.failRun(key, run, diag.message());
            return run;
        };
        defer def.deinit(self.allocator);
        var why_buf: [256]u8 = undefined;
        if (self.startPipelines(key, &def, &why_buf)) |refused| switch (refused) {
            .failed => |why| self.failRun(key, run, why),
            .held => |why| self.holdJob(key, run, why),
            .short => |why| {
                self.holdJob(key, run, why);
                run.short = true;
            },
        };
        return run;
    }

    /// Pipelines here waiting for a sink's namespace, into the node's gauge.
    fn noteWaitingNamespace(self: *ProcessingHandler, shard: *Shard, now: usize) void {
        if (now == self.waiting_ns) return;
        if (shard.metrics_registry) |m| m.processing.moveWaitingNamespace(self.waiting_ns, now);
        self.waiting_ns = now;
    }

    /// Its logged cursor, as its last applied batch left it: the `from` its
    /// next batch names, and where it reads from unless it read past it
    /// in this term. Forgets the batch in flight: a lost one never applies,
    /// and an applied one is already in the cursor.
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
    pub fn throughFor(f: ?ts_machine.FieldValue, behind: u64) u64 {
        if (behind == 0) return std.math.maxInt(u64);
        const v = f orelse return std.math.maxInt(u64);
        return v.newest_ns -| behind;
    }

    /// Tick a TS source pipeline: read points, apply operators, write to sinks.
    /// It reads in time order, so a point written with a time behind its
    /// cursor (late, or from a writer whose clock lags) is never read.
    fn tickTsSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job_ns: NamespaceId) void {
        const limit = @min(pipe.poll_left, source_page.TS_PAGE_POINTS);
        pipe.page_limit = limit;
        const owner = self.sinkOwner(shard, pipe.src_namespace, pipe.src_measurement);
        if (owner != shard.id) return self.askSource(pipeline_key, pipe, shard, owner, limit);
        var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
        const ns = shard.namespaceId(pipe.src_namespace);
        const now = self.mono_ms();
        const f = shard.ts_handler.ts.fieldRecord(ns, pipe.src_measurement, pipe.src_field);
        pipe.noteWrites(if (f) |v| v.writes else null, now);
        if (pipe.lateEdge()) |e| pipe.late_points = if (f) |v| v.late[e] else 0;
        const n = source_page.readTsPage(shard.ts_handler.ts, ns, pipe.src_measurement, pipe.src_field, pipe.src_tag_hash, pipe.ts_cursor, throughFor(f, pipe.holdBackNs(now)), limit, &points);
        self.processTsPage(pipeline_key, pipe, shard, job_ns, points[0..n]);
    }

    /// A page of a TS source, read here or sent by the shard that owns it,
    /// in (time, position) order after the cursor. Its points' writes go in
    /// one batch, as many whole points as fit.
    fn processTsPage(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job_ns: NamespaceId, points: []const TsPoint) void {
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
            const json = tsPointJson(arena.allocator(), pipe.src_measurement, pipe.src_field, pt.value, ts_ms, pt.tags) catch break;
            batch.beginRecord();
            // Apply operator chain (or pass through directly). A record
            // the chain failed on is counted and skipped: passing it through
            // raw would write what the operators were there to change.
            const output_records = applyOperatorChain(pipe.operators, json, ts_ms, arena.allocator()) catch |err| blk: {
                self.noteChainError(shard, err);
                break :blk @as(?[]const ProcessingRecord, &.{});
            };
            if (output_records) |records| {
                // A TS sink writes what the operators made of the point: its
                // record's `value`. Tags and time stay the point's, whatever
                // the record says.
                const ts_sink = pipe.hasTsSink();
                for (records) |rec| self.writeSinkRecord(batch, pipe, shard, arena.allocator(), rec.value, if (ts_sink) outputValue(rec.value) else null, pt.timestamp_ns, pt.tags, rec.tags);
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
            // The batch covers it unless it waits to lead the next.
            if (ended != .carried) batch.records += 1;
            pipe.records_in += 1;
            taken += 1;
            if (ended == .carried) break;
        }
        self.notePage(pipe, batch, taken, points.len);
        self.proposeBatch(shard, job_ns, pipeline_key, pipe, batch, to orelse readAt(pipe));
    }

    /// Tick a stream source pipeline: read payloads from stream, apply operators, write to sink.
    fn tickStreamSource(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job_ns: NamespaceId) void {
        const limit = @min(pipe.poll_left, channels_mod.MAX_SENDS);
        pipe.page_limit = limit;
        const owner = self.sinkOwner(shard, pipe.src_namespace, pipe.src_stream);
        if (owner != shard.id) return self.askSource(pipeline_key, pipe, shard, owner, limit);
        const cursor = StreamID{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };
        const result = shard.stream_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, cursor, limit);
        defer result.deinit(shard.stream_handler.allocator);
        self.processStreamPage(pipeline_key, pipe, shard, job_ns, result.payloads, result.ids, result.last_id);
    }

    /// A page of a stream source read from the pipeline's cursor, here or
    /// sent by the shard that owns it: `last_id` is where the read reached.
    /// Its records' writes go in one batch, as many whole records as fit.
    fn processStreamPage(self: *ProcessingHandler, pipeline_key: []const u8, pipe: *PipelineState, shard: *Shard, job_ns: NamespaceId, payloads: []const []const u8, ids: []const StreamID, last_id: StreamID) void {
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
            // The batch covers it unless it waits to lead the next.
            if (ended != .carried) batch.records += 1;
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
        self.proposeBatch(shard, job_ns, pipeline_key, pipe, batch, to orelse readAt(pipe));
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
            .asker = .pipeline,
            .key = pipeline_key,
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

    /// Another shard's pipeline or stream trigger asks for a page of a
    /// source this shard owns: read here, on this shard's thread, as a
    /// client's read is (applied data only, nothing logged), and sent back.
    /// A shard that doesn't serve its clients' reads now answers so, and
    /// reads nothing. An answer that finds the asker's inbox full is
    /// dropped; the asker asks again.
    pub fn answerSourceRead(shard: *Shard, to: u16, bytes: []const u8) void {
        const r = source_page.Read.decode(bytes) orelse return;
        const mailboxes = shard.peer_mailboxes orelse return;
        if (to >= mailboxes.len) return;
        var b: source_page.Page.Builder = .{};
        defer b.deinit(shard.allocator);
        const page = (if (shard.servesAsks()) buildSourcePage(shard, r, &b) else refusedPage(shard, r, &b, .not_ready)) catch return;
        if (!mailboxes[to].inbox.send(.{ .tag = .source_page, .src_shard = @intCast(shard.id), .payload_len = @intCast(page.len), .payload_ptr = page.ptr }))
            shard.allocator.free(page);
    }

    /// A page that reads nothing: `status` says why, and the asker's
    /// cursor stays where it asked from.
    fn refusedPage(shard: *Shard, r: source_page.Read, b: *source_page.Page.Builder, status: source_page.Status) ![]u8 {
        b.status = status;
        try b.begin(shard.allocator, r);
        return b.finish(shard.allocator, r.from_a, r.from_b, 0);
    }

    fn buildSourcePage(shard: *Shard, r: source_page.Read, b: *source_page.Page.Builder) ![]u8 {
        switch (r.kind) {
            .stream => {
                const sh = shard.stream_handler;
                const ns = shard.namespaceId(r.namespace);
                b.made_ns = sh.madeAt(ns, r.source) orelse return refusedPage(shard, r, b, .no_stream);
                b.ns = ns.int();
                try b.begin(shard.allocator, r);
                const from: StreamID = .{ .timestamp_ms = r.from_a, .sequence = r.from_b };
                // A read that stops before an append being read back from
                // disk reaches only past what it read or couldn't: asked
                // again, the append is there.
                const result = sh.readPayloadsForStream(r.source, r.namespace, from, r.limit);
                defer result.deinit(sh.allocator);
                for (result.payloads, result.ids) |p, id| try b.record(shard.allocator, id.timestamp_ms, id.sequence, p);
                return b.finish(shard.allocator, result.last_id.timestamp_ms, result.last_id.sequence, 0);
            },
            .ts => {
                try b.begin(shard.allocator, r);
                var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
                const ns = shard.namespaceId(r.namespace);
                const f = shard.ts_handler.ts.fieldRecord(ns, r.source, r.field);
                // The asker judged the field idle at a write count; a write
                // since, which it hasn't seen, holds the window again.
                const idle = r.idle_at != 0 and f != null and f.?.writes == r.idle_at;
                const n = source_page.readTsPage(shard.ts_handler.ts, ns, r.source, r.field, r.tag_hash, .{ .ns = r.from_a, .ual = r.from_b }, throughFor(f, if (idle) 0 else r.behind_ns), r.limit, &points);
                for (points[0..n]) |p| try b.point(shard.allocator, p);
                const late = if (f) |v| if (ts_machine.lateEdgeFor(r.behind_ns)) |e| v.late[e] else 0 else 0;
                return b.finish(shard.allocator, if (f) |v| v.newest_ns else 0, if (f) |v| v.writes else 0, late);
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
        const entry = self.pipelines.getEntry(page.key) orelse return;
        const pipeline_key = entry.key_ptr.*;
        const pipe = entry.value_ptr;
        if (pipe.read_seq != page.seq or pipe.read_term != page.term) return;
        pipe.read_seq = 0;
        // The owner read nothing: the cursor stays, and the next tick asks
        // again from it.
        if (page.status != .ok) return;
        const raft = shard.raft_node;
        if (raft.role != .leader or raft.current_term != page.term) return;
        const base_job_id = if (std.mem.indexOfScalar(u8, pipeline_key, 0)) |nul| pipeline_key[0..nul] else pipeline_key;
        const run = self.runs.getPtr(base_job_id) orelse return;
        if (run.held or run.failure_owned.len > 0) return;
        const ns = run.ns_id;
        if (pipe.batch_term == raft.current_term and pipe.batch_index > raft.last_applied) return;
        // A batch applied since the read was asked moved the cursor: read
        // again from there.
        self.loadCursor(shard, ns, pipeline_key, pipe);
        const read = readAt(pipe);
        if (read.a != page.from_a or read.b != page.from_b) return;
        // As a tick checks before it reads. Asked again next tick, from
        // the same cursor.
        pipe.wait = self.waitOf(shard, ns, pipe);
        if (pipe.wait != .none) {
            self.reported = true;
            return;
        }

        self.ticking_job = base_job_id;
        defer self.ticking_job = "";
        const before = pipe.records_in;
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
                self.processStreamPage(pipeline_key, pipe, shard, ns, payloads, ids, .{ .timestamp_ms = page.last_a, .sequence = page.last_b });
            },
            .ts => {
                if (page.kind != .ts) return;
                var points: [source_page.TS_PAGE_POINTS]TsPoint = undefined;
                const got = page.points(&points) orelse return;
                pipe.noteWrites(if (page.last_b != 0) page.last_b else null, self.mono_ms());
                pipe.late_points = page.late;
                self.processTsPage(pipeline_key, pipe, shard, ns, got);
            },
        }
        const processed = pipe.records_in - before;
        if (processed > 0) if (shard.metrics_registry) |m| m.processing.recordProcessed(processed);
    }

    /// At most this often a pipeline logs a cursor move with no writes: records
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
        /// The source records the batch covers.
        records: u32 = 0,
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
            pe.SinkMessage.encode(&b.r, .{ .slot = built.slot, .entry_type = @backingInt(built.entry_type), .flags = Flags.SINK, .ns = built.ns.int(), .payload = built.payload }) catch {
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
            // The record they are for: this batch's cursor passes it.
            self.batch.records = 1;
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
        const records = pipe.unlogged_records +| batch.records;
        if (batch.n == 0 and now -| pipe.last_batch_ms < IDLE_BATCH_MS) {
            pipe.ahead = true;
            pipe.ahead_term = term;
            pipe.unlogged_records = records;
            return;
        }
        const entry = pe.SinkBatch.encodeInto(self.entry_buf, .{
            .ns = ns.int(),
            .pipeline = pipeline_key,
            .from_a = pipe.from.a,
            .from_b = pipe.from.b,
            .to_a = to.a,
            .to_b = to.b,
            .records = records,
            .messages = .{ .bytes = batch.w.written(), .count = batch.n },
        }) catch {
            log.err("job '{s}': a sink batch didn't encode; read again", .{self.ticking_job});
            pipe.more = false;
            return self.rewindTo(pipe, pipe.from);
        };
        // A job's batches take its namespace's share of the log's room, so
        // one job can't take every namespace's clients' room.
        shard.raft_node.charge_ns = ns.int();
        defer shard.raft_node.charge_ns = 0;
        const r = shard.raft_node.propose(.processing_sink_batch, Flags.NONE, entry) catch |err| {
            if (!shard.quietHalted(err)) log.warn("job '{s}': sink batch not proposed: {s}; read again", .{ self.ticking_job, @errorName(err) });
            // Read again at the next poll, not every tick while the log
            // refuses it (a halted shard's does for good).
            pipe.more = false;
            return self.rewindTo(pipe, pipe.from);
        };
        pipe.batch_index = r.index;
        pipe.batch_term = r.term;
        pipe.last_batch_ms = now;
        pipe.unlogged_records = 0;
        // Read past it: the record whose writes wait in `carry`.
        pipe.ahead = read.a != to.a or read.b != to.b;
        pipe.ahead_term = r.term;
        if (shard.metrics_registry) |m| m.processing.recordSinkBatch();
    }

    /// Back to where the pipeline's last applied batch left it: what it
    /// read since is read again, and its operators run on it again.
    fn rewindTo(_: *ProcessingHandler, pipe: *PipelineState, at: sinks_machine.At) void {
        pipe.ahead = false;
        // Read again from the logged cursor: counted again as they are.
        pipe.unlogged_records = 0;
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
    /// namespace, or for its operators to be rebuilt, is logged when the
    /// wait begins.
    fn noteWait(self: *ProcessingHandler, shard: *Shard, pipe: *PipelineState, count: bool) void {
        const now = @import("stdx").time.milliTimestamp();
        if (count) {
            pipe.wait_counted_ms = now;
            if (shard.metrics_registry) |m| m.processing.recordHeldTick();
        }
        if (pipe.held_since_ms == 0) pipe.held_since_ms = now;
        const held_for = now - pipe.held_since_ms;
        // Only an operator, or memory, ends these: said at once.
        const prompt = (pipe.wait == .namespace or pipe.wait == .operators) and held_for == 0;
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
            .pressure => |r| blk: {
                // A full target on the shard it writes to: say which sink.
                if (r.stall.shard == r.shard and r.stall.cause == @backingInt(channel_driver.Cause.target_full)) break :blk (if (r.target.len > 0)
                    std.fmt.bufPrint(buf, "waiting for room on shard {d} ({s})", .{ r.shard, r.target })
                else
                    std.fmt.bufPrint(buf, "waiting for room on shard {d}", .{r.shard})) catch "waiting for room on a sink's shard";
                const cause = std.enums.fromInt(channel_driver.Cause, r.stall.cause) orelse break :blk "waiting: writes to a sink's shard are queued";
                break :blk std.fmt.bufPrint(buf, "waiting: writes to shard {d} are queued: it {s}", .{ r.shard, channel_driver.causeWords(cause) }) catch "waiting: writes to a sink's shard are queued";
            },
            .outbox => |o| blk: {
                var hb: [16]u8 = undefined;
                var cb: [16]u8 = undefined;
                const held = channel_driver.bytesText(&hb, o.bytes);
                const cap = channel_driver.bytesText(&cb, o.cap);
                const to = o.to orelse break :blk std.fmt.bufPrint(buf, "waiting: this shard's outboxes are full ({s} of {s})", .{ held, cap }) catch "waiting: an outbox is full";
                // Why it is full, as this leader sees the shard it goes to.
                if (o.cause) |c| break :blk std.fmt.bufPrint(buf, "waiting: the outbox to shard {d} is full ({s} of {s}): it {s}", .{ to, held, cap, channel_driver.causeWords(c) }) catch "waiting: an outbox is full";
                break :blk std.fmt.bufPrint(buf, "waiting: the outbox to shard {d} is full ({s} of {s})", .{ to, held, cap }) catch "waiting: an outbox is full";
            },
            .share => |r| blk: {
                var hb: [16]u8 = undefined;
                var cb: [16]u8 = undefined;
                const ns = if (r.namespace.len == 0) "default" else r.namespace;
                const held = channel_driver.bytesText(&hb, r.bytes);
                const cap = channel_driver.bytesText(&cb, r.cap);
                const to = r.to orelse break :blk std.fmt.bufPrint(buf, "waiting: namespace {s} holds its share of this shard's outboxes ({s} of {s})", .{ ns, held, cap }) catch "waiting: a namespace holds its share of the outboxes";
                break :blk std.fmt.bufPrint(buf, "waiting: namespace {s} holds its share of the outbox to shard {d} ({s} of {s})", .{ ns, to, held, cap }) catch "waiting: a namespace holds its share of an outbox";
            },
            .log => "waiting for room in this shard's log",
            .operators => |why| std.fmt.bufPrint(buf, "waiting: its operators couldn't be rebuilt: {s}", .{why}) catch "waiting: its operators couldn't be rebuilt",
        };
    }

    /// A sink write refused at the pipeline's shard, before it was sent:
    /// the record is lost to that sink. Counted, and said once per
    /// interval with the count.
    pub fn noteSinkDrop(self: *ProcessingHandler, metrics: ?*MetricsRegistry, kind: []const u8, target: []const u8, why: []const u8) void {
        self.sink_drops += 1;
        if (metrics) |m| m.processing.recordDropped();
        const job = self.runs.getPtr(self.ticking_job) orelse return;
        self.reported = true;
        job.write_drops += 1;
        const text = std.fmt.bufPrint(&job.last_write_drop, "{s} sink '{s}': {s}", .{ kind, target, why }) catch null;
        job.last_write_drop_len = if (text) |t| t.len else job.last_write_drop.len;
        const now = @import("stdx").time.milliTimestamp();
        if (now - job.drop_warn_ms < 30_000) return;
        job.drop_warn_ms = now;
        // Its records' doing, not the server's.
        log.warn("job '{s}': {s} sink '{s}': record dropped: {s} ({d} dropped by this job so far)", .{ self.ticking_job, kind, target, why, job.write_drops });
    }

    /// A whole record skipped by every sink: counted as dropped, said in
    /// the job's status with where and why, and logged at most once every
    /// 30 s per job.
    fn noteDrop(self: *ProcessingHandler, shard: *Shard, at: []const u8, why: []const u8) void {
        self.sink_drops += 1;
        if (shard.metrics_registry) |m| m.processing.recordDropped();
        const job = self.runs.getPtr(self.ticking_job) orelse return;
        self.reported = true;
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
    /// `field_value` is what a TS sink writes; null drops the record for
    /// TS sinks, counted, while the others still take it.
    fn writeSinkRecord(self: *ProcessingHandler, batch: *Batch, pipe: *PipelineState, shard: *Shard, scratch: Allocator, payload: []const u8, field_value: ?f64, timestamp_ns: u64, src_tags: []const u8, record_tags: u32) void {
        pipe.records_out += 1; // operator chain emitted a record into the sink stage
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            const ns = self.namespaceIdOf(snk.namespace);
            switch (snk.kind) {
                .stream => self.addWrite(batch, shard, snk, sink_payload.stream(self.msg_buf, scratch, ns, snk.target, payload)),
                .ts => if (field_value) |v|
                    self.sendTsPoint(batch, shard, snk, snk.value_field, v, timestamp_ns, src_tags)
                else
                    self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the operators' record has no finite number `value`"),
                .kv => self.writeKvSink(batch, snk, shard, "", payload),
                .queue => self.addWrite(batch, shard, snk, sink_payload.queue(self.msg_buf, shard.queue_handler.caps, ns, snk.target, snk.queue_priority, payload)),
            }
        }
    }

    /// The top-level `value` of an operator's output record, when it is a
    /// finite number; null otherwise. A number is never escaped, so the
    /// reader needs room only for its own state.
    fn outputValue(record: []const u8) ?f64 {
        var buf: [record_fields.STATE + 64]u8 = undefined;
        var out: [1]?record_fields.Field = .{null};
        record_fields.read(record, &.{"value"}, &out, &buf) catch return null;
        const v = (out[0] orelse return null).float() orelse return null;
        return if (std.math.isFinite(v)) v else null;
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
        // Every field the sink reads, in one pass over the record: the tags'
        // first, then the fields' or the value's. Its buffer is the
        // handler's, kept between records, not on the stack.
        const fr = self.sinkRead() orelse return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "out of memory");
        defer fr.release(self.allocator);
        const n_tags = flatPairCount(snk.ts_tag_keys);
        // A record that is a JSON number alone is its value, for a sink with
        // no tags or fields to read from it.
        if (snk.ts_tag_keys.len == 0 and snk.ts_field_keys.len == 0) if (record_fields.bareNumber(payload, fr.buf[0..SinkRead.STATE])) |v| {
            if (!std.math.isFinite(v)) return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the value is not a finite number");
            return self.sendTsPoint(batch, shard, snk, "value", v, 0, "");
        };
        const read = fr.read(self.allocator, payload, snk.ts_tag_keys, snk.ts_field_keys, snk.value_field) catch |err| return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, switch (err) {
            error.NotObject => "the record is not a JSON object",
            error.TooLong => std.fmt.comptimePrint("an escaped field it reads decodes past {d} bytes", .{SinkRead.BUF - SinkRead.STATE}),
            error.TooDeep => std.fmt.comptimePrint("the record nests past {d} levels", .{record_fields.MAX_DEPTH}),
            error.OutOfMemory => "out of memory",
        });
        var sink_tag_buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
        var missing: []const u8 = "";
        // Its tags name its series: a point without all of them would land
        // in another, which no read of these tags finds.
        const sink_tags = computeTsSinkTags(snk.ts_tag_keys, read[0..n_tags], &sink_tag_buf, &missing) catch |err| {
            var wb: [parser.MAX_SINK_FIELD_NAME + 64]u8 = undefined;
            return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, switch (err) {
                error.TagsTooLong => std.fmt.comptimePrint("its tags pass {d} bytes", .{ts_mod.MAX_TAG_STRING}),
                error.NoTagField => std.fmt.bufPrint(&wb, "the record has no string field \"{s}\" for a tag", .{missing}) catch "the record has no field for a tag",
                error.TagValueComma => std.fmt.bufPrint(&wb, "the record's field \"{s}\" holds ',', which would split its tag", .{missing}) catch "a tag's value holds ','",
                error.TagValueControl => std.fmt.bufPrint(&wb, "the record's field \"{s}\" holds a control character, which no read of a tag can name", .{missing}) catch "a tag's value holds a control character",
            });
        };

        if (snk.ts_field_keys.len > 0) {
            var it = FlatPairIterator.init(snk.ts_field_keys);
            var i: usize = n_tags;
            while (it.next()) |pair| : (i += 1) {
                // pair.key = TS field name, pair.value = JSON field to extract from.
                const value = (if (read[i]) |f| f.float() else null) orelse {
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

        // value_field shorthand: the named field, stored as "value".
        const value = (if (read[n_tags]) |f| f.float() else null) orelse
            return self.noteSinkDrop(shard.metrics_registry, "ts", snk.measurement, "the record has no numeric field for it");
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

    /// A point's tags, as a TS write takes them: `name=value` pairs joined by
    /// commas, from the record's fields that `tag_keys` names (flat pairs,
    /// tag name then field), for the projection to canonicalise. Every tag is
    /// the record's or none is: a field it lacks names `missing`. A value's
    /// trailing space is trimmed there, as for any TS write, so "a" and "a "
    /// are one series; a value holding a tab or any control character is
    /// dropped here.
    fn computeTsSinkTags(tag_keys: []const u8, values: []const ?record_fields.Field, buf: []u8, missing: *[]const u8) error{ TagsTooLong, NoTagField, TagValueComma, TagValueControl }![]const u8 {
        if (tag_keys.len == 0) return "";
        var pos: usize = 0;
        var it = FlatPairIterator.init(tag_keys);
        var i: usize = 0;
        while (it.next()) |pair| : (i += 1) {
            // pair.key = TS tag name, pair.value = the record field it reads.
            const tag_val = if (values[i]) |f| switch (f) {
                .string => |s| s,
                else => null,
            } else null;
            const v = tag_val orelse {
                missing.* = pair.value;
                return error.NoTagField;
            };
            // Tags are split again on `,`: one in a value would make two.
            if (std.mem.indexOfScalar(u8, v, ',') != null) {
                missing.* = pair.value;
                return error.TagValueComma;
            }
            // A decoded value can hold any character; a TS write keeps one
            // with a control character, but no read can name its series.
            for (v) |c| if (c < 0x20 or c == 0x7f) {
                missing.* = pair.value;
                return error.TagValueControl;
            };
            const kv = std.fmt.bufPrint(buf[pos..], "{s}{s}={s}", .{ if (pos > 0) "," else "", pair.key, v }) catch return error.TagsTooLong;
            pos += kv.len;
        }
        return buf[0..pos];
    }

    /// The pairs in a flat-pair string.
    fn flatPairCount(pairs: []const u8) usize {
        var n: usize = 0;
        var it = FlatPairIterator.init(pairs);
        while (it.next()) |_| n += 1;
        return n;
    }

    /// A TS sink's read of one record: each field it names, the tags'
    /// first, then its fields' (or its value's), all in one pass
    /// (`record_fields.read`). A sink naming more than `NAMES` fields asks
    /// the heap.
    const SinkRead = struct {
        const NAMES = 64;
        /// Room for the reader's own state and for decoding escaped values
        /// (an unescaped one is read in place): a tag past `MAX_TAG_STRING`
        /// is refused after anyway.
        const BUF = 8 * 1024;
        const STATE = record_fields.STATE;
        names_fixed: [NAMES][]const u8 = undefined,
        out_fixed: [NAMES]?record_fields.Field = undefined,
        names_heap: [][]const u8 = &.{},
        out_heap: []?record_fields.Field = &.{},
        buf: [BUF]u8 = undefined,

        /// What a record past `NAMES` took from the heap, given back.
        fn release(r: *SinkRead, a: Allocator) void {
            if (r.names_heap.len > 0) a.free(r.names_heap);
            if (r.out_heap.len > 0) a.free(r.out_heap);
            r.names_heap = &.{};
            r.out_heap = &.{};
        }

        fn read(r: *SinkRead, a: Allocator, payload: []const u8, tag_keys: []const u8, field_keys: []const u8, value_field: []const u8) (record_fields.Error || error{OutOfMemory})![]const ?record_fields.Field {
            const n = flatPairCount(tag_keys) + if (field_keys.len > 0) flatPairCount(field_keys) else 1;
            const names: [][]const u8 = if (n <= NAMES) r.names_fixed[0..n] else blk: {
                r.names_heap = try a.alloc([]const u8, n);
                r.out_heap = try a.alloc(?record_fields.Field, n);
                break :blk r.names_heap;
            };
            const out: []?record_fields.Field = if (n <= NAMES) r.out_fixed[0..n] else r.out_heap;
            var i: usize = 0;
            var tags = FlatPairIterator.init(tag_keys);
            while (tags.next()) |p| : (i += 1) names[i] = p.value;
            if (field_keys.len > 0) {
                var fields = FlatPairIterator.init(field_keys);
                while (fields.next()) |p| : (i += 1) names[i] = p.value;
            } else names[i] = value_field;
            try record_fields.read(payload, names, out, &r.buf);
            return out;
        }
    };
    /// The handler's one sink read, made on first use and kept: a record at
    /// a time, as pipelines tick on one thread.
    fn sinkRead(self: *ProcessingHandler) ?*SinkRead {
        if (self.sink_read) |r| return r;
        const r = self.allocator.create(SinkRead) catch return null;
        r.* = .{};
        self.sink_read = r;
        return r;
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

/// A handler over a store of its own, its job entries applied through the
/// processing machine as a shard applies them; `reconcile` (`reconcileAt`) stands in for
/// its leader's tick.
const JobsTest = struct {
    h: ProcessingHandler,
    store: contract_store.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init(t: *JobsTest, allocator: Allocator) void {
        t.* = .{ .h = ProcessingHandler.init(allocator), .store = contract_store.Store.init(std.testing.allocator), .scratch = std.heap.ArenaAllocator.init(std.testing.allocator) };
        t.store.strict_keys = true;
        t.h.store = &t.store;
    }

    fn deinit(t: *JobsTest) void {
        t.h.deinit();
        t.store.deinit();
        t.scratch.deinit();
    }

    fn applyIn(t: *JobsTest, etype: EntryType, ns: NamespaceId, key: []const u8, value: []const u8, stamp_ms: u64) persistence_mod.ApplyError!Result {
        t.index += 1;
        var pbuf: [2048]u8 = undefined;
        const e = entry_mod.buildCommandEntry(etype, 0, 1, t.index, stamp_ms * std.time.ns_per_ms, ns, key, value, &pbuf).?;
        var out = Result.start(&e);
        try @import("../contract/machine.zig").applyEntry(&sinks_machine.machine, &t.store, &t.scratch, &e, &out);
        return out;
    }

    fn apply(t: *JobsTest, etype: EntryType, key: []const u8, value: []const u8) persistence_mod.ApplyError!Result {
        return t.applyIn(etype, .default, key, value, 0);
    }

    fn submit(t: *JobsTest, key: []const u8, status: ProcessingHandler.JobStatus, yaml: []const u8, stamp_ms: u64) persistence_mod.ApplyError!Result {
        var buf: [1024]u8 = undefined;
        return t.applyIn(.processing_submit, .default, key, testSubmitValue(&buf, status, yaml), stamp_ms);
    }

    fn job(t: *JobsTest, key: []const u8) ?ProcessingHandler.JobRecord {
        return t.h.viewOf(.default, key);
    }

    fn jobs(t: *JobsTest) usize {
        return t.store.family(sinks_machine.JobFam.ID).count;
    }

    fn reconcile(t: *JobsTest) void {
        t.h.reconcileAt(1);
    }
};

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
    const host_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
    // One stamp far behind the host's clock and one a day ahead.
    for ([_]u64{ 1_000, host_ms + std.time.ms_per_day }) |stamp_ms| {
        var t: JobsTest = undefined;
        t.init(std.testing.allocator);
        defer t.deinit();
        _ = try t.submit("job-1", .stopped, yaml, stamp_ms);
        try std.testing.expectEqual(@as(i64, @intCast(stamp_ms)), (t.job("job-1") orelse return error.JobNotCreated).created_at_ms);
    }
}

test "ProcessingHandler: a submit with an unknown status halts, storing nothing" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    // [status=0xee][batch_size][ns_len=0]
    var val = @as([7]u8, @splat(0));
    val[0] = 0xee;
    try std.testing.expectError(error.Halt, t.apply(.processing_submit, "job-bad", &val));
    try std.testing.expectEqual(@as(usize, 0), t.jobs());
}

test "ProcessingHandler: a TS source record is JSON for any measurement name" {
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, "cp\"u\\", "value", 1.5, 7, "");
    defer std.testing.allocator.free(json);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("cp\"u\\", parsed.value.object.get("measurement").?.string);
    try std.testing.expectEqual(@as(f64, 1.5), parsed.value.object.get("value").?.float);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("timestamp_ms").?.integer);
}

test "ProcessingHandler: a TS source record carries the point's field and tags, each read back exact" {
    const tags = "a=x\"y,b=p\\q,c=\x01z\x7f,d,e=k=v,f=caf\xc3\xa9";
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, "m\"\\\n", "f\\\"", 2.5, 9, tags);
    defer std.testing.allocator.free(json);
    // The record's own reader, as a sink reads a field.
    var buf: [record_fields.STATE + 256]u8 = undefined;
    var out: [5]?record_fields.Field = undefined;
    try record_fields.read(json, &.{ "measurement", "field", "value", "timestamp_ms", "tags" }, &out, &buf);
    try std.testing.expectEqualStrings("m\"\\\n", out[0].?.string);
    try std.testing.expectEqualStrings("f\\\"", out[1].?.string);
    try std.testing.expectEqual(@as(?f64, 2.5), out[2].?.float());
    try std.testing.expectEqual(@as(?f64, 9), out[3].?.float());
    try std.testing.expect(out[4].? == .other);
    // The tags object, read the same way.
    const start = std.mem.indexOf(u8, json, "\"tags\":").? + "\"tags\":".len;
    const obj = json[start .. json.len - 1];
    var tout: [6]?record_fields.Field = undefined;
    try record_fields.read(obj, &.{ "a", "b", "c", "d", "e", "f" }, &tout, &buf);
    try std.testing.expectEqualStrings("x\"y", tout[0].?.string);
    try std.testing.expectEqualStrings("p\\q", tout[1].?.string);
    try std.testing.expectEqualStrings("\x01z\x7f", tout[2].?.string);
    try std.testing.expectEqualStrings("", tout[3].?.string);
    try std.testing.expectEqualStrings("k=v", tout[4].?.string);
    try std.testing.expectEqualStrings("caf\xc3\xa9", tout[5].?.string);
}

test "ProcessingHandler: a TS source record at its longest stays within MAX_TS_POINT_JSON" {
    // Every byte a control character, escaped six bytes wide.
    const ctl = "\x01";
    const measurement = comptime @import("stdx").repeat(ctl, persistence.MAX_ENTRY_KEY);
    const field = comptime @import("stdx").repeat(ctl, 512);
    const pair = "\x01=" ++ comptime @import("stdx").repeat(ctl, 30) ++ ",";
    const tags = comptime @import("stdx").repeat(pair, ts_mod.MAX_TAG_STRING / pair.len);
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, measurement, field, -std.math.floatMax(f64), std.math.minInt(i64), tags);
    defer std.testing.allocator.free(json);
    try std.testing.expect(json.len <= ProcessingHandler.MAX_TS_POINT_JSON);
}

test "ProcessingHandler: an operator's record gives a TS sink its value only as a finite top-level number" {
    try std.testing.expectEqual(@as(?f64, 2.5), ProcessingHandler.outputValue("{\"value\":2.5}"));
    try std.testing.expectEqual(@as(?f64, -3), ProcessingHandler.outputValue("{\"count\":1,\"value\":-3}"));
    for ([_][]const u8{ "{\"value\":\"2.5\"}", "{}", "{\"value\":null}", "{\"value\":1e400}", "{\"v\":{\"value\":1}}", "[1]", "2.5", "{\"value\":" }) |rec|
        try std.testing.expectEqual(@as(?f64, null), ProcessingHandler.outputValue(rec));
}

test "ProcessingHandler: a TS source record holds a measurement name of any length" {
    // Past the 512-byte buffer a point was built in, whose points were skipped.
    const name = comptime @import("stdx").repeat("\"", 600);
    const json = try ProcessingHandler.tsPointJson(std.testing.allocator, name, "value", 1.5, 7, "");
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
        const json = try ProcessingHandler.tsPointJson(std.testing.allocator, "m", "value", v, 7, "");
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings("{\"measurement\":\"m\",\"field\":\"value\",\"value\":null,\"timestamp_ms\":7,\"tags\":{}}", json);
    }
}

test "processing: what a pipeline waits for, in words" {
    var buf: [160]u8 = undefined;
    const W = ProcessingHandler.Wait;
    const cases = [_]struct { w: W, says: []const u8 }{
        .{ .w = .{ .namespace = "orders" }, .says = "waiting for namespace orders" },
        .{ .w = .{ .halted = 2 }, .says = "waiting: sink shard 2 is halted" },
        .{ .w = .{ .pressure = .{ .shard = 3, .stall = .{ .shard = 3, .cause = 4 }, .target = "jobs" } }, .says = "waiting for room on shard 3 (jobs)" },
        .{ .w = .{ .pressure = .{ .shard = 3, .stall = .{ .shard = 3, .cause = 4 }, .target = "" } }, .says = "waiting for room on shard 3" },
        .{ .w = .{ .pressure = .{ .shard = 3, .stall = .{ .shard = 3, .cause = 1 }, .target = "jobs" } }, .says = "waiting: writes to shard 3 are queued: it is halted" },
        .{ .w = .{ .pressure = .{ .shard = 3, .stall = .{ .shard = 3, .cause = 3 }, .target = "" } }, .says = "waiting: writes to shard 3 are queued: it cannot flush to disk" },
        .{ .w = .{ .outbox = .{ .to = 1, .bytes = 130 << 20, .cap = 128 << 20 } }, .says = "waiting: the outbox to shard 1 is full (130 MiB of 128 MiB)" },
        .{ .w = .{ .outbox = .{ .to = 1, .bytes = 70 << 10, .cap = 64 << 10, .cause = .no_leader } }, .says = "waiting: the outbox to shard 1 is full (70 KiB of 64 KiB): it hasn't answered for 10 s" },
        .{ .w = .{ .outbox = .{ .to = 2, .bytes = 9 << 20, .cap = 8 << 20, .cause = .target_full } }, .says = "waiting: the outbox to shard 2 is full (9 MiB of 8 MiB): it has a full target" },
        .{ .w = .{ .outbox = .{ .to = null, .bytes = 1 << 30, .cap = 1 << 30 } }, .says = "waiting: this shard's outboxes are full (1024 MiB of 1024 MiB)" },
        .{ .w = .{ .share = .{ .to = 3, .namespace = "orders", .bytes = 48 << 20, .cap = 64 << 20 } }, .says = "waiting: namespace orders holds its share of the outbox to shard 3 (48 MiB of 64 MiB)" },
        .{ .w = .{ .share = .{ .to = null, .namespace = "orders", .bytes = 192 << 20, .cap = 256 << 20 } }, .says = "waiting: namespace orders holds its share of this shard's outboxes (192 MiB of 256 MiB)" },
        .{ .w = .log, .says = "waiting for room in this shard's log" },
        .{ .w = .{ .operators = "out of memory" }, .says = "waiting: its operators couldn't be rebuilt: out of memory" },
    };
    for (cases) |c| try std.testing.expectEqualStrings(c.says, ProcessingHandler.waitText(c.w, &buf));
}

var test_why_buf: [256]u8 = undefined;

/// A processing_submit value for a job in "default", batch size 0.
fn testSubmitValue(buf: []u8, status: ProcessingHandler.JobStatus, yaml: []const u8) []const u8 {
    return sinks_machine.encodeSubmit(buf, status, 0, "default", yaml).?;
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
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    _ = try t.submit("job-r", .running, yaml, 1);
    t.reconcile();
    const job = t.job("job-r").?;
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, job.status);
    try std.testing.expect(std.mem.indexOf(u8, job.failure_owned, "partitions") != null);
    try std.testing.expectEqualStrings(yaml, job.yaml_owned);
    try std.testing.expectEqual(@as(usize, 0), t.h.pipelines.count());
    try std.testing.expectEqual(@as(u64, 1), t.h.failed_jobs);

    // The same submit again is still one failed job.
    _ = try t.submit("job-r", .running, yaml, 1);
    t.reconcile();
    try std.testing.expectEqual(@as(u64, 1), t.h.failed_jobs);
    // Cancelling it is refused: it stays failed, and counted.
    const out = try t.apply(.processing_cancel, "job-r", &.{@backingInt(ProcessingHandler.JobStatus.cancelled)});
    try std.testing.expectEqualStrings("job isn't running (it is failed)", out.refused().?.refusal.message);
    t.reconcile();
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, t.job("job-r").?.status);
    try std.testing.expectEqual(@as(u64, 1), t.h.failed_jobs);
}

test "ProcessingHandler: stopping a stopped job succeeds and changes nothing; cancelling it is refused" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    _ = try t.submit("job-s", .running, "kind: Processing\nname: s\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n", 1);
    const stop = &[_]u8{@backingInt(ProcessingHandler.JobStatus.stopped)};
    try std.testing.expect((try t.apply(.processing_stop, "job-s", stop)).refused() == null);
    // A second stop, raced with the first, is a no-op.
    try std.testing.expect((try t.apply(.processing_stop, "job-s", stop)).refused() == null);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.stopped, t.job("job-s").?.status);

    const out = try t.apply(.processing_cancel, "job-s", &.{@backingInt(ProcessingHandler.JobStatus.cancelled)});
    const refused = out.refused().?;
    try std.testing.expect(refused.status == .conflict);
    try std.testing.expectEqualStrings("job isn't running (it is stopped)", refused.refusal.message);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.stopped, t.job("job-s").?.status);
}

test "ProcessingHandler: a failure reason is kept up to 1 KiB, cut on a character and marked" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectEqualStrings("bad key", try sinks_machine.boundedReason(arena.allocator(), "bad key"));

    // A 2-byte character straddles the cut.
    var long: [2000]u8 = @splat('x');
    const cut_at = ProcessingHandler.MAX_FAILURE_REASON - "... (cut)".len;
    long[cut_at - 1] = 0xC3;
    long[cut_at] = 0xA9;
    const cut = try sinks_machine.boundedReason(arena.allocator(), &long);
    try std.testing.expect(cut.len <= ProcessingHandler.MAX_FAILURE_REASON);
    try std.testing.expect(std.unicode.utf8ValidateSlice(cut));
    try std.testing.expect(std.mem.endsWith(u8, cut, "x... (cut)"));
}

test "ProcessingHandler: a submit past the shard's job limit is refused in log order" {
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
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    sinks_machine.max_jobs = 1;
    defer sinks_machine.max_jobs = 10_000;
    for ([_]struct { []const u8, bool }{ .{ "job-1", false }, .{ "job-2", true }, .{ "job-1", false } }) |c| {
        const out = try t.submit(c[0], .stopped, yaml, 0);
        try std.testing.expectEqual(c[1], out.refused() != null);
        try std.testing.expectEqual(@as(usize, 1), t.jobs());
    }
    try std.testing.expect(t.job("job-2") == null);
}

test "ProcessingHandler: a remove is refused while its job runs, and frees the slot once it doesn't" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    sinks_machine.max_jobs = 1;
    defer sinks_machine.max_jobs = 10_000;
    _ = try t.submit("job-1", .running, lookup_yaml, 1);
    t.reconcile();
    try std.testing.expect(t.h.pipelines.contains("job-1"));

    // Running: refused in log order, and nothing changes.
    const running = (try t.apply(.processing_remove, "job-1", "")).refused().?;
    try std.testing.expectEqual(proto.StatusCode.conflict, running.status);
    try std.testing.expectEqual(proto.Reason.job_running, running.refusal.reason);
    try std.testing.expect(t.job("job-1") != null);

    // At the limit, a cancelled job still holds its slot.
    try std.testing.expect((try t.apply(.processing_cancel, "job-1", &.{@backingInt(ProcessingHandler.JobStatus.cancelled)})).refused() == null);
    try std.testing.expect((try t.submit("job-2", .stopped, lookup_yaml, 2)).refused() != null);
    // Its pipeline goes once the leader sees it cancelled.
    t.reconcile();
    try std.testing.expectEqual(@as(usize, 0), t.h.pipelines.count());

    // Another namespace's remove finds no such job, on every replica.
    const elsewhere = (try t.applyIn(.processing_remove, .of(16), "job-1", "", 0)).refused().?;
    try std.testing.expectEqual(proto.StatusCode.not_found, elsewhere.status);
    try std.testing.expect(t.job("job-1") != null);

    // Removed: the job and its slot are gone.
    try std.testing.expect((try t.apply(.processing_remove, "job-1", "")).refused() == null);
    try std.testing.expectEqual(@as(usize, 0), t.jobs());
    try std.testing.expect((try t.submit("job-2", .stopped, lookup_yaml, 3)).refused() == null);
    try std.testing.expect(t.job("job-2") != null);

    // A second remove, or a stop or cancel taken before the remove applied,
    // finds no job: refused as not found, never a halt.
    for ([_]EntryType{ .processing_remove, .processing_stop, .processing_cancel }) |etype| {
        const gone = (try t.apply(etype, "job-1", &.{@backingInt(ProcessingHandler.JobStatus.stopped)})).refused().?;
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
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    _ = try t.submit("job-f", .running, yaml, 1);
    t.reconcile();
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, t.job("job-f").?.status);
    try std.testing.expectEqual(@as(u64, 1), t.h.failed_jobs);
    try std.testing.expect((try t.apply(.processing_remove, "job-f", "")).refused() == null);
    t.reconcile();
    try std.testing.expectEqual(@as(u64, 0), t.h.failed_jobs);
    try std.testing.expect(t.job("job-f") == null);
}

test "ProcessingHandler: every op naming a job routes by its id's slot; a submit is routed by the shard, from its definition" {
    const Dispatch = @import("../node/dispatcher.zig").Dispatcher;
    var d = Dispatch.init();
    ProcessingHandler.register(&d);
    for ([_]proto.OpCode{ .processing_stop, .processing_cancel, .processing_status, .processing_remove }) |op| {
        try std.testing.expect(d.pre_route[@backingInt(op)].? == run_id_mod.preRouteByRunId);
        try std.testing.expect(d.shard_route[@backingInt(op)] == null);
    }
    try std.testing.expect(d.pre_route[@backingInt(proto.OpCode.processing_submit)] == null);
    try std.testing.expect(d.shard_route[@backingInt(proto.OpCode.processing_submit)] == null);
}

test "ProcessingHandler: a job's home is its first source's shard, hashed as a stream or TS op on it routes" {
    var h = ProcessingHandler.init(std.testing.allocator);
    defer h.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const stream_def = try parser.parseJobDefinitionWithNamespace(a, "kind: Processing\nsources: [{stream: {name: orders}}, {stream: {name: refunds}}]\nsinks: [{stream: {name: out}}]\n", "app", null);
    try std.testing.expectEqual(router.routeNsKey(.of(7), "orders"), h.homeHash(&stream_def, .of(7)).?);
    const ts_def = try parser.parseJobDefinitionWithNamespace(a, "kind: Processing\nsources: [{ts: {measurement: cpu}}]\nsinks: [{stream: {name: out}}]\n", "app", null);
    try std.testing.expectEqual(router.routeNsKey(.of(7), "cpu"), h.homeHash(&ts_def, .of(7)).?);
    // No id for the job's namespace yet, or for another namespace its first
    // source reads that this node doesn't have: no home yet.
    try std.testing.expect(h.homeHash(&stream_def, .invalid) == null);
    const other = try parser.parseJobDefinitionWithNamespace(a, "kind: Processing\nsources: [{stream: {name: orders, namespace: billing}}]\nsinks: [{stream: {name: out}}]\n", "app", null);
    try std.testing.expect(h.homeHash(&other, .of(7)) == null);
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

test "ProcessingHandler: replicas of different shard counts store the same status; only a leader that can't run it holds it" {
    var one: JobsTest = undefined;
    one.init(std.testing.allocator);
    defer one.deinit();
    var two: JobsTest = undefined;
    two.init(std.testing.allocator);
    defer two.deinit();
    two.h.peer_shard_count = 2;
    for ([_]*JobsTest{ &one, &two }) |t| {
        const out = try t.submit("job-1", .running, lookup_yaml, 1);
        // Stored and running on both: the entry is all either read.
        try std.testing.expect(out.refused() == null);
        t.reconcile();
        try std.testing.expectEqual(ProcessingHandler.JobStatus.running, t.job("job-1").?.status);
        try std.testing.expectEqual(@as(u64, 0), t.h.failed_jobs);
    }
    try std.testing.expect(one.h.pipelines.contains("job-1"));
    try std.testing.expect(!one.job("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 0), one.h.held_jobs);

    try std.testing.expect(!two.h.pipelines.contains("job-1"));
    const held = two.job("job-1").?;
    try std.testing.expect(held.held);
    try std.testing.expectEqualStrings("kv_lookup needs a one-shard server (--shards 1) until lookups can read other shards", held.held_owned);
    try std.testing.expectEqual(@as(u64, 1), two.h.held_jobs);

    // Applied again, it is still one held job.
    _ = try two.submit("job-1", .running, lookup_yaml, 2);
    two.reconcile();
    try std.testing.expect(two.job("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 1), two.h.held_jobs);

    // The list says so, beside the status.
    var entry_buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&entry_buf);
    const listed = two.h.listedJob(.default, "job-1").?;
    try ProcessingHandler.writeEntry(&listed, &w);
    var r = @import("../util/wire.zig").WireReader.init(w.buffered());
    _ = r.readLengthPrefixed(u16).?;
    _ = r.readLengthPrefixed(u16).?;
    try std.testing.expectEqualStrings("RUNNING (held here: kv_lookup needs a one-shard server (--shards 1) until lookups can read other shards)", r.readLengthPrefixed(u16).?);

    // Stopped, it is no longer held.
    _ = try two.apply(.processing_stop, "job-1", &.{@backingInt(ProcessingHandler.JobStatus.stopped)});
    two.reconcile();
    try std.testing.expect(!two.job("job-1").?.held);
    try std.testing.expectEqual(@as(u64, 0), two.h.held_jobs);
}

test "ProcessingHandler: a submit stored as failed commits, and its answer says why" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    const out = try t.submit("job-f", .running, "kind: Processing\nname: bad\nsources: [{stream: {name: in, partitions: \"0-1\"}}]\nsinks: [{stream: {name: out}}]\n", 1);
    // Stored, so it ran: its edge refuses it as failed (`answerSubmit`).
    try std.testing.expect(out.refused() == null);
    try std.testing.expect(std.mem.indexOf(u8, sinks_machine.submitFailure(&out).?, "partitions") != null);
    try std.testing.expectEqual(ProcessingHandler.JobStatus.failed, t.job("job-f").?.status);
    // A good one says nothing.
    const good = try t.submit("job-g", .running, "kind: Processing\nname: good\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n", 1);
    try std.testing.expect(sinks_machine.submitFailure(&good) == null);
}

test "ProcessingHandler: memory running out while a leader builds a job's pipelines never fails the job" {
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
        var t: JobsTest = undefined;
        t.init(std.testing.allocator);
        defer t.deinit();
        var yaml_buf: [512]u8 = undefined;
        const yaml = try std.fmt.bufPrint(&yaml_buf, "kind: Processing\nname: oom\nsources: [{{stream: {{name: a}}}}, {{stream: {{name: b}}}}]\noperators: [{s}]\nsinks: [{{stream: {{name: out}}}}]\n", .{op});
        try std.testing.expect((try t.submit(job_id, .running, yaml, 1)).refused() == null);
        // Each of the leader's allocations in turn fails; past the last, it
        // builds clean.
        var fail_index: usize = 0;
        while (true) : (fail_index += 1) {
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
            var h = ProcessingHandler.init(failing.allocator());
            defer h.deinit();
            h.store = &t.store;
            h.reconcileAt(1);
            // Running, held here or not, never failed; held with no
            // pipeline left, or running both. Or no run at all, for now.
            if (h.viewOf(.default, job_id)) |job| {
                try std.testing.expectEqual(ProcessingHandler.JobStatus.running, job.status);
                try std.testing.expectEqual(@as(u64, 0), h.failed_jobs);
                if (h.runs.contains(job_id)) try std.testing.expectEqual(@as(u32, if (job.held) 0 else 2), h.pipelines.count());
            }
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

test "ProcessingHandler: a run held for want of memory is built again a second later, though no job changed" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    const Clock = struct {
        var ms: u64 = 10_000;
        fn now() u64 {
            return ms;
        }
    };
    t.h.mono_ms = Clock.now;
    _ = try t.submit("job-m", .running, "kind: Processing\nname: m\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n", 1);
    t.reconcile();
    // As if its build had run out of memory: held, its pipelines gone.
    t.h.dropPipelines("job-m");
    const run = t.h.runs.getPtr("job-m").?;
    t.h.holdJob("job-m", run, "a pipeline couldn't be built: out of memory");
    run.short = true;
    t.h.short_runs = true;
    t.h.short_at_ms = Clock.ms;
    // Within the second, though the jobs change: left as it is.
    _ = try t.submit("job-other", .stopped, "kind: Processing\nname: o\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n", 1);
    Clock.ms += ProcessingHandler.SHORT_RETRY_MS / 2;
    t.reconcile();
    try std.testing.expect(t.job("job-m").?.held);
    // A second after it went short: built again.
    Clock.ms += ProcessingHandler.SHORT_RETRY_MS / 2;
    t.reconcile();
    try std.testing.expect(!t.job("job-m").?.held);
    try std.testing.expect(t.h.pipelines.contains("job-m"));
    try std.testing.expect(!t.h.short_runs);
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
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    for (0..2) |_| {
        _ = try t.submit("job-twice", .running, yaml, 1);
        t.reconcile();
    }
    try std.testing.expectEqual(@as(usize, 1), t.h.pipelines.count());
    try std.testing.expect(t.h.pipelines.contains("job-twice"));
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
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    _ = try t.submit("job-r", .running, yaml, 1);
    const reason = t.job("job-r").?.failure_owned;
    try std.testing.expect(reason.len > 0);

    // A definition that doesn't parse has no name of its own: the job's id stands in.
    var entry_buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&entry_buf);
    const listed = t.h.listedJob(.default, "job-r").?;
    try ProcessingHandler.writeEntry(&listed, &w);
    var r = @import("../util/wire.zig").WireReader.init(w.buffered());
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    try std.testing.expectEqualStrings("job-r", r.readLengthPrefixed(u16).?);
    const status = r.readLengthPrefixed(u16).?;
    try std.testing.expect(std.mem.startsWith(u8, status, "FAILED: "));
    try std.testing.expectEqualStrings(reason, status["FAILED: ".len..]);
    try std.testing.expectEqual(@as(i64, 1), r.readI64().?);
}

test "ProcessingHandler: jobs that share a name are each listed" {
    var t: JobsTest = undefined;
    t.init(std.testing.allocator);
    defer t.deinit();
    const ids = [_][]const u8{ "job-a", "job-b", "job-c" };
    for (ids, 1..) |id, i| _ = try t.submit(id, .stopped, lookup_yaml, i);

    const page = ProcessingHandler.localScanJobs(@ptrCast(&t.h), .{ .name = "", .id = .default }, "", null, 0);
    try std.testing.expectEqual(ids.len, page.items.len);
    var seen: [ids.len]bool = @splat(false);
    for (page.items) |item| {
        const job = t.h.listedJob(.default, item).?;
        try std.testing.expectEqualStrings("lookup", job.name_owned);
        for (ids, 0..) |id, i| if (std.mem.eql(u8, id, job.job_id_owned)) {
            seen[i] = true;
        };
    }
    for (seen) |b| try std.testing.expect(b);
    try std.testing.expect(t.h.listedJob(.of(16), "job-a") == null);
}

test "processing: a job's status counts its TS sources' late points at the largest edge they report against, each field once" {
    const allocator = std.testing.allocator;
    var handler = ProcessingHandler.init(allocator);
    defer handler.deinit();
    const Src = struct {
        fn ts(measurement: []const u8, lateness_ms: u32) definition.SourceSpec {
            return .{ .kind = .ts, .name = "t", .stream = "", .namespace = "default", .batch_size = 0, .ts_measurement = measurement, .ts_field = "value", .ts_lateness_ms = lateness_ms };
        }
    };
    // Windows of 30 s and 20 s both report against the 30 s edge; the
    // 3 s one reports against 5 s, and its count past 30 s isn't carried
    // for it, so it isn't counted. Two sources reading cpu.value (say,
    // one host each) count its late points once: the count is the field's.
    const sources = [_]struct { key: []const u8, measurement: []const u8, lateness_ms: u32, late: u64 }{
        .{ .key = "job-a\x000", .measurement = "cpu", .lateness_ms = 30_000, .late = 5 },
        .{ .key = "job-a\x001", .measurement = "cpu", .lateness_ms = 3_000, .late = 9 },
        .{ .key = "job-a\x002", .measurement = "mem", .lateness_ms = 20_000, .late = 2 },
        .{ .key = "job-a\x003", .measurement = "cpu", .lateness_ms = 30_000, .late = 5 },
        .{ .key = "job-h", .measurement = "cpu", .lateness_ms = 2 * 3_600_000, .late = 4 },
    };
    for (sources) |s| {
        const src = Src.ts(s.measurement, s.lateness_ms);
        var pipe = try ProcessingHandler.makePipeState(allocator, &src, &.{}, 0);
        pipe.late_points = s.late;
        try handler.pipelines.put(try allocator.dupe(u8, s.key), pipe);
    }
    const stream_src = definition.SourceSpec{ .kind = .stream, .name = "s", .stream = "in", .namespace = "default", .batch_size = 0 };
    try handler.pipelines.put(try allocator.dupe(u8, "job-s"), try ProcessingHandler.makePipeState(allocator, &stream_src, &.{}, 0));

    const a = handler.runtimeOf("job-a", 0);
    try std.testing.expectEqual(@as(u64, 30_000), a.late_edge_ms);
    try std.testing.expectEqual(@as(u64, 7), a.late_points);
    // A window past every edge, or no TS source: no count.
    for ([_][]const u8{ "job-h", "job-s" }) |id| {
        const r = handler.runtimeOf(id, 0);
        try std.testing.expect(r.running);
        try std.testing.expectEqual(ProcessingHandler.Runtime.NO_LATE_EDGE, r.late_edge_ms);
        try std.testing.expectEqual(@as(u64, 0), r.late_points);
    }
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

    const F = ts_machine.FieldValue;
    const max = std.math.maxInt(u64);
    try std.testing.expectEqual(max, ProcessingHandler.throughFor(null, w));
    try std.testing.expectEqual(max, ProcessingHandler.throughFor(F{ .newest_ns = 100 * std.time.ns_per_s }, 0));
    try std.testing.expectEqual(@as(u64, 97 * std.time.ns_per_s), ProcessingHandler.throughFor(F{ .newest_ns = 100 * std.time.ns_per_s }, w));
    // Under the window: from time 0, not wrapped.
    try std.testing.expectEqual(@as(u64, 0), ProcessingHandler.throughFor(F{ .newest_ns = std.time.ns_per_s }, w));
}

test "processing: a namespace may hold three quarters of an outbox, and past a quarter waits once the outbox is three quarters full" {
    const cap: u64 = 64 << 20;
    // Alone: up to three quarters.
    try std.testing.expect(!raft_node_mod.shareHeld(40 << 20, 40 << 20, cap));
    try std.testing.expect(raft_node_mod.shareHeld(48 << 20, 48 << 20, cap));
    // Two at 30 MiB and 20 MiB, past three quarters together: the heavier,
    // past its quarter, waits; the lighter, at its quarter, goes on.
    try std.testing.expect(raft_node_mod.shareHeld(30 << 20, 50 << 20, cap));
    try std.testing.expect(!raft_node_mod.shareHeld(16 << 20, 50 << 20, cap));
    // Below three quarters together, both go on.
    try std.testing.expect(!raft_node_mod.shareHeld(30 << 20, 40 << 20, cap));
    // Holding nothing never waits, however small the cap or full the outbox.
    try std.testing.expect(!raft_node_mod.shareHeld(0, 0, 1));
    try std.testing.expect(!raft_node_mod.shareHeld(0, 1 << 20, 1));
    try std.testing.expect(raft_node_mod.shareHeld(1, 1, 1));
}

/// A TS sink's tags for `record`, its fields read as the sink reads them.
fn sinkTagsOf(keys: []const u8, record: []const u8, buf: []u8, missing: *[]const u8) ![]const u8 {
    var r: ProcessingHandler.SinkRead = .{};
    defer r.release(std.testing.allocator);
    const read = try r.read(std.testing.allocator, record, keys, "", "value");
    return ProcessingHandler.computeTsSinkTags(keys, read[0 .. read.len - 1], buf, missing);
}

test "processing: a TS sink's tags are the record's, every one or none, and never cut short" {
    var buf: [ts_mod.MAX_TAG_STRING]u8 = undefined;
    var missing: []const u8 = "";
    const keys = "host\x00h\x00region\x00r";
    try std.testing.expectEqualStrings("host=a,region=eu", try sinkTagsOf(keys, "{\"h\":\"a\",\"r\":\"eu\"}", &buf, &missing));
    // A record without one of them: no point, naming the field it lacks.
    try std.testing.expectError(error.NoTagField, sinkTagsOf(keys, "{\"h\":\"a\"}", &buf, &missing));
    try std.testing.expectEqualStrings("r", missing);
    // A tag's field that isn't a string is none.
    try std.testing.expectError(error.NoTagField, sinkTagsOf(keys, "{\"h\":\"a\",\"r\":7}", &buf, &missing));
    // Tags up to what a read can name: `host=` and 1019 bytes is 1024, kept;
    // one more is dropped, rather than written under fewer.
    var long: [ts_mod.MAX_TAG_STRING + 16]u8 = undefined;
    const at_limit = try std.fmt.bufPrint(&long, "{{\"h\":\"{s}\"}}", .{@as([ts_mod.MAX_TAG_STRING - 5]u8, @splat('a'))});
    try std.testing.expectEqual(ts_mod.MAX_TAG_STRING, (try sinkTagsOf("host\x00h", at_limit, &buf, &missing)).len);
    const past = try std.fmt.bufPrint(&long, "{{\"h\":\"{s}\"}}", .{@as([ts_mod.MAX_TAG_STRING - 4]u8, @splat('a'))});
    try std.testing.expectError(error.TagsTooLong, sinkTagsOf("host\x00h", past, &buf, &missing));
    // A comma in a value would read as another tag.
    try std.testing.expectError(error.TagValueComma, sinkTagsOf(keys, "{\"h\":\"a,zz=q\",\"r\":\"eu\"}", &buf, &missing));
    try std.testing.expectEqualStrings("h", missing);
    // An `=` in a value is the value's.
    try std.testing.expectEqualStrings("host=a=b,region=eu", try sinkTagsOf(keys, "{\"h\":\"a=b\",\"r\":\"eu\"}", &buf, &missing));
    // Escapes decoded, nesting passed over: two values that differ by an
    // escaped quote stay two, and a nested `h` isn't the record's.
    try std.testing.expectEqualStrings("host=web\"1", try sinkTagsOf("host\x00h", "{\"h\":\"web\\\"1\"}", &buf, &missing));
    try std.testing.expectEqualStrings("host=y", try sinkTagsOf("host\x00h", "{\"meta\":{\"h\":\"x\"},\"h\":\"y\"}", &buf, &missing)); // A decoded control character: no read of a tag can name it.
    try std.testing.expectError(error.TagValueControl, sinkTagsOf("host\x00h", "{\"h\":\"a\\u0000b\"}", &buf, &missing));
    try std.testing.expectError(error.TagValueControl, sinkTagsOf("host\x00h", "{\"h\":\"a\\nb\"}", &buf, &missing));
    // Two tags reading one field each get it.
    try std.testing.expectEqualStrings("a=x,b=x", try sinkTagsOf("a\x00h\x00b\x00h", "{\"h\":\"x\"}", &buf, &missing));
}
