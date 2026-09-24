//! Workflow Handler — registers workflow opcodes with Dispatcher.
//!
//! Workflows are a Layer 2 "Intelligent Layer" that compose Layer 1 primitives
//! for durable, multi-step orchestration. The handler manages workflow
//! definition CRUD, run lifecycle, signals, and disable/enable.
//!
//! ## Opcode Range
//!
//!   Commands:   0x80–0x87, 0x8E–0x8F, 0x92
//!   Responses:  0x88–0x8D, 0x90–0x91, 0x93
//!
//! ## Wire Format
//!
//! Each command uses the standard Request format (namespace, key, value):
//!
//! | Command             | key               | value                               |
//! |---------------------|-------------------|-------------------------------------|
//! | workflow_create      | (unused)          | YAML definition                     |
//! | workflow_start       | workflow_name     | [ver_len:u16][ver][idem?][rid?][in]  |
//! | workflow_signal      | run_id            | [sig_len:u16][sig_type][payload...]  |
//! | workflow_cancel      | run_id            | reason (optional)                   |
//! | workflow_status      | run_id            | (empty)                             |
//! | workflow_history     | run_id            | [limit:u32]                         |
//! | workflow_list_runs   | workflow_name     | [limit:u32][status_len:u16][status][cursor_len:u16][cursor][search_len:u16][search] |
//! | workflow_get_def     | workflow_name     | version (optional)                  |
//! | workflow_disable     | workflow_name     | version (optional)                  |
//! | workflow_enable      | workflow_name     | version (optional)                  |
//! | workflow_list_defs   | (unused)          | (empty)                             |

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const parser = @import("parser.zig");
const definition = @import("definition.zig");
const plan_types = @import("plan_types.zig");
const validator = @import("validator.zig");
const jsonpath = @import("jsonpath.zig");
const wf_types = @import("types.zig");
const StepOutputMap = wf_types.StepOutputMap;

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const StreamID = @import("../projection/stream.zig").StreamID;
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Partition = @import("../storage/partition.zig").Partition;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const router = @import("../node/router.zig");
const run_id_mod = @import("../node/run_id.zig");

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

/// Cluster-local count of registered stream triggers across all shards on this
/// node. Lets the (cross-shard) stream-append path skip the trigger push-wake
/// broadcast entirely when no workflow watches any stream — zero overhead for
/// deployments that don't use stream triggers.
var global_stream_trigger_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

// ═══════════════════════════════════════════════════════════════════════════════
// WorkflowHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const WorkflowHandler = struct {
    /// A run waiting for its first step: its "namespace:run_id" key and the
    /// log index of its start.
    const Started = struct { ns_key: []u8, index: u64 };

    allocator: Allocator,

    /// Mutex for cross-shard thread safety. Acquired when this handler is
    /// accessed from a different shard's thread via forwardToShard dispatch.
    mu: @import("stdx").Mutex,
    /// The run the last applied start created; a key of `runs` (see
    /// `Shard.answering_index`).
    last_started_key: ?[]const u8 = null,
    /// The last applied start named a run that already existed under its
    /// idempotency key; `last_started_key` is that run.
    last_start_existed: bool = false,
    /// The last applied start named a run id that already existed.
    last_start_collided: bool = false,
    /// Runs started on this leader, in the order they were queued (not log
    /// order: a client's start is queued once it applies, a producer's when
    /// it is proposed), for the tick to take
    /// their first step once the start has applied (`advanceStartedRuns`).
    /// No start takes its first step where it is proposed or answered: a
    /// step proposes, and the shard applies nothing in the middle of a
    /// module's work.
    started_to_advance: std.ArrayListUnmanaged(Started) = .empty,

    /// In-memory definition store: "namespace:name" → DefinitionRecord.
    /// Key is namespace-qualified (allocated separately from record fields).
    definitions: std.StringHashMap(DefinitionRecord),

    /// In-memory run store: "namespace:run_id" → RunRecord.
    runs: std.StringHashMap(RunRecord),

    /// (namespace, workflow, idempotency key) → the key in `runs` of the
    /// run that key started (see `idemKey`). Kept by the start applier, so every node
    /// and every replay decides duplicates alike, without a scan.
    idem_index: std.StringHashMapUnmanaged([]const u8) = .{},

    /// Disabled workflows: "namespace:name" → void.
    disabled: std.StringHashMap(void),

    /// Active stream triggers: "namespace:workflow_name" → StreamTriggerState.
    /// Registered when a workflow with a `trigger:` block is created.
    stream_triggers: std.StringHashMap(StreamTriggerState),

    /// Set when a stream this shard watches was just appended to (via a
    /// `stream_event` inbox notification or a local append). The next
    /// `tickStreamTriggers` force-polls all triggers, bypassing their poll
    /// timer — turning a polled (≤ batch_timeout_ms) latency into ~one tick.
    /// Single-threaded per shard (set/read only on the shard's reactor thread).
    triggers_dirty: bool = false,
    /// Whether an applied definition installs its trigger and schedule here.
    /// Cleared by the shard while it applies entries replicated from a peer:
    /// those producers run on the node that owns the definition.
    install_producers: bool = true,

    /// Active interval/cron schedules: "namespace:workflow_name" → ScheduleState.
    /// Registered when a workflow with a `schedule:` block is created.
    schedules: std.StringHashMap(ScheduleState),

    /// Plan executor health tracking: "namespace:workflow:plan:executor" → ExecutorHealth.
    /// Used by health-weighted selection to route to the healthiest executor and
    /// by circuit breakers to skip failing executors.
    plan_health: std.StringHashMap(plan_types.ExecutorHealth),

    const MAX_WORKFLOW_NAME_LEN: usize = 256;
    const MAX_DEFINITIONS: usize = 10_000;
    const MAX_RUNS: usize = 100_000;

    pub const RunStatus = enum(u8) {
        pending = 0,
        running = 1,
        waiting = 2,
        completed = 3,
        failed = 4,
        cancelled = 5,
        timed_out = 6,

        pub fn toString(self: RunStatus) []const u8 {
            return switch (self) {
                .pending => "pending",
                .running => "running",
                .waiting => "waiting",
                .completed => "completed",
                .failed => "failed",
                .cancelled => "cancelled",
                .timed_out => "timed_out",
            };
        }

        pub fn isTerminal(self: RunStatus) bool {
            return switch (self) {
                .pending, .running, .waiting => false,
                .completed, .failed, .cancelled, .timed_out => true,
            };
        }
    };

    pub const DefinitionRecord = struct {
        name_owned: []const u8,
        version_owned: []const u8,
        yaml_owned: []const u8,
        created_at_ms: i64,
        /// Cached idempotency mode (parsed once at create time) so the start
        /// path can enforce `required` without re-parsing the YAML.
        idempotency: definition.IdempotencyMode = .none,
    };

    pub const RunRecord = struct {
        run_id_owned: []const u8,
        workflow_name_owned: []const u8,
        workflow_version_owned: []const u8,
        status: RunStatus,
        input_owned: []const u8,
        created_at_ms: i64,
        started_at_ms: ?i64,
        completed_at_ms: ?i64,
        idempotency_key_owned: ?[]const u8,

        /// Current step name in the workflow graph (null = at start step).
        current_step_name_owned: ?[]const u8 = null,

        /// Signal type the run is waiting for (non-null when status == .waiting).
        wait_signal_type_owned: ?[]const u8 = null,

        /// Signals received by this run.
        signals: std.ArrayList(Signal),

        /// History events for this run.
        history: std.ArrayList(HistoryEvent),

        /// Final workflow output (resolved from `output` mapping when completed, null if not declared).
        output_owned: ?[]const u8 = null,

        /// Per-step outputs for JSONPath resolution ($.steps.*).
        step_outputs: ?StepOutputMap = null,

        /// Action run ID when parked waiting for async action completion.
        pending_action_run_id_owned: ?[]const u8 = null,

        /// Step name that triggered the pending action.
        pending_step_name_owned: ?[]const u8 = null,

        /// Shard ID where the pending action run was created (for cross-shard lookup).
        pending_action_shard_id: ?u16 = null,

        /// Child workflow run ID when parked waiting for a `@workflow/<name>` step.
        pending_child_run_id_owned: ?[]const u8 = null,

        /// Shard ID where the pending child workflow run lives (cross-shard lookup).
        pending_child_shard_id: ?u16 = null,

        /// The index, in this shard's log, of the entry that creates the
        /// action or child run this run waits for; 0 when that run is
        /// created on another shard. Once this shard has applied that index
        /// and the run does not exist, the log dropped it (a leadership lost
        /// before commit) and the step fails rather than waiting for ever.
        pending_index: u64 = 0,

        /// Number of poll attempts taken for the current step (a `poll:` step that
        /// keeps returning `pending`). Reset on step transition.
        poll_attempt: u32 = 0,

        /// Absolute time the next poll re-invocation is due (ms; 0 = not polling).
        poll_next_at_ms: i64 = 0,

        /// Retry attempt counter for the current step (reset on step transition).
        retry_count: u32 = 0,

        /// Index of the current executor within a plan (reset on step transition).
        plan_executor_idx: u8 = 0,

        /// Natural index into plan.executors[] for the active executor.
        /// In health-weighted mode, plan_executor_idx tracks position in
        /// the sorted order while this holds the actual executor index.
        plan_executor_natural_idx: u8 = 0,

        /// Retry counter for the current plan executor (reset on executor advance).
        plan_executor_retry_count: u32 = 0,

        /// Absolute deadline for wait_for_signal timeout (ms since epoch, 0 = none).
        wait_timeout_at_ms: i64 = 0,

        /// Transition target to follow when wait times out.
        wait_timeout_target_owned: ?[]const u8 = null,

        /// Pre-computed search attribute JSON (e.g. {"customer_id":"C-789"}).
        /// Built at run start from $.input.* paths, updated at completion for
        /// $.steps.* and $.flo.* paths. Enables fast substring search without
        /// re-parsing definition YAML on every list request.
        search_tags_owned: ?[]const u8 = null,
    };

    pub const Signal = struct {
        signal_type_owned: []const u8,
        payload_owned: ?[]const u8,
        received_at_ms: i64,
    };

    pub const HistoryEvent = struct {
        event_type_owned: []const u8,
        detail_owned: []const u8,
        timestamp_ms: i64,
    };

    /// Per-trigger polling state for stream-triggered workflows.
    pub const StreamTriggerState = struct {
        workflow_name_owned: []const u8,
        namespace_owned: []const u8,
        stream_name_owned: []const u8,
        stream_namespace_owned: []const u8,
        batch_size: u32,
        poll_interval_ms: u32,
        stream_cursor_ts: u64,
        stream_cursor_seq: u64,
        last_poll_ms: i64,
    };

    pub const ScheduleState = struct {
        workflow_name_owned: []const u8,
        namespace_owned: []const u8,
        interval_ms: i64,
        max_concurrent: u32,
        input_owned: ?[]const u8,
        last_trigger_ms: i64,
    };

    pub fn init(allocator: Allocator) WorkflowHandler {
        return .{
            .allocator = allocator,
            .mu = .{},
            .definitions = std.StringHashMap(DefinitionRecord).init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
            .disabled = std.StringHashMap(void).init(allocator),
            .stream_triggers = std.StringHashMap(StreamTriggerState).init(allocator),
            .schedules = std.StringHashMap(ScheduleState).init(allocator),
            .plan_health = std.StringHashMap(plan_types.ExecutorHealth).init(allocator),
        };
    }

    pub fn deinit(self: *WorkflowHandler) void {
        // Free all definition records (ns-qualified key + record fields)
        var dit = self.definitions.iterator();
        while (dit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // ns-qualified key
            self.allocator.free(entry.value_ptr.name_owned);
            self.allocator.free(entry.value_ptr.version_owned);
            self.allocator.free(entry.value_ptr.yaml_owned);
        }
        self.definitions.deinit();

        // Free all run records (ns-qualified key + record fields)
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // ns-qualified key
            self.freeRunRecord(entry.value_ptr);
        }
        self.runs.deinit();
        var ik = self.idem_index.keyIterator();
        while (ik.next()) |k| self.allocator.free(k.*);
        self.idem_index.deinit(self.allocator);
        for (self.started_to_advance.items) |s| self.allocator.free(s.ns_key);
        self.started_to_advance.deinit(self.allocator);

        // Free disabled keys (ns-qualified)
        var diit = self.disabled.iterator();
        while (diit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.disabled.deinit();

        // Free stream trigger state
        var tit = self.stream_triggers.iterator();
        while (tit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeTriggerState(entry.value_ptr);
            _ = global_stream_trigger_count.fetchSub(1, .monotonic);
        }
        self.stream_triggers.deinit();

        // Free schedule state
        var sit = self.schedules.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeScheduleState(entry.value_ptr);
        }
        self.schedules.deinit();

        // Free plan health state
        var phit = self.plan_health.iterator();
        while (phit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.plan_health.deinit();
    }

    fn freeRunRecord(self: *WorkflowHandler, run: *RunRecord) void {
        self.allocator.free(run.run_id_owned);
        self.allocator.free(run.workflow_name_owned);
        self.allocator.free(run.workflow_version_owned);
        self.allocator.free(run.input_owned);
        if (run.idempotency_key_owned) |k| self.allocator.free(k);
        if (run.current_step_name_owned) |s| self.allocator.free(s);
        if (run.wait_signal_type_owned) |s| self.allocator.free(s);
        if (run.pending_action_run_id_owned) |a| self.allocator.free(a);
        if (run.pending_child_run_id_owned) |a| self.allocator.free(a);
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        if (run.wait_timeout_target_owned) |t| self.allocator.free(t);
        if (run.output_owned) |o| self.allocator.free(o);
        if (run.search_tags_owned) |t| self.allocator.free(t);
        if (run.step_outputs) |*so| {
            var mutable = so.*;
            mutable.deinit(self.allocator);
        }

        // Free signals
        for (run.signals.items) |sig| {
            self.allocator.free(sig.signal_type_owned);
            if (sig.payload_owned) |p| self.allocator.free(p);
        }
        run.signals.deinit(self.allocator);

        // Free history events
        for (run.history.items) |evt| {
            self.allocator.free(evt.event_type_owned);
            self.allocator.free(evt.detail_owned);
        }
        run.history.deinit(self.allocator);
    }

    fn freeTriggerState(self: *WorkflowHandler, state: *StreamTriggerState) void {
        self.allocator.free(state.workflow_name_owned);
        self.allocator.free(state.namespace_owned);
        self.allocator.free(state.stream_name_owned);
        self.allocator.free(state.stream_namespace_owned);
    }

    fn freeScheduleState(self: *WorkflowHandler, state: *ScheduleState) void {
        self.allocator.free(state.workflow_name_owned);
        self.allocator.free(state.namespace_owned);
        if (state.input_owned) |inp| self.allocator.free(inp);
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        // Name-based ops: pre-routed by hash(namespace, workflow_name) so that
        // definitions and runs land on the same deterministic shard.
        dispatcher.registerWithRoute(.workflow_create, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_start, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_list_runs, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_get_definition, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_disable, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_enable, dispatchWorkflow, preRouteByWorkflow);
        // Run-based ops: key is run_id with embedded partition bits.
        // Pre-routed by extracting partition from the run ID.
        dispatcher.registerWithRoute(.workflow_status, dispatchWorkflow, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.workflow_signal, dispatchWorkflow, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.workflow_cancel, dispatchWorkflow, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.workflow_history, dispatchWorkflow, run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.workflow_list_definitions, dispatchWorkflow, localScanWorkflowDefs);
    }

    /// Route workflow requests by hash(namespace, workflow_name) so that all
    /// operations for a given workflow land on the same shard.
    fn preRouteByWorkflow(req: proto.Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    /// ShardWalker LocalScanFn for workflow_list_definitions — returns
    /// workflow definition names from one shard's WorkflowHandler registry.
    fn localScanWorkflowDefs(
        ctx: *anyopaque,
        namespace: []const u8,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *WorkflowHandler = @ptrCast(@alignCast(ctx));
        handler.mu.lock();
        defer handler.mu.unlock();

        const S = struct {
            threadlocal var name_buf: [256][]const u8 = undefined;
        };

        var count: usize = 0;
        var dit = handler.definitions.iterator();
        while (dit.next()) |entry| {
            if (count >= S.name_buf.len) break;
            if (namespace.len > 0) {
                const map_key = entry.key_ptr.*;
                // map_key format is "namespace:name"
                if (!std.mem.startsWith(u8, map_key, namespace)) continue;
                if (map_key.len <= namespace.len or map_key[namespace.len] != ':') continue;
            }
            S.name_buf[count] = entry.value_ptr.name_owned;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    fn dispatchWorkflow(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        // Parked outside the handler's lock: on a single node the responder
        // runs straight away and takes the lock itself.
        if (shard.workflow_handler.handleCommand(shard, conn, req)) |proposed| shard.park(conn, req, proposed, respondWorkflow);
    }

    /// A parked create or start applied: answer, and for a start, begin.
    fn respondWorkflow(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const h = shard.workflow_handler;
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        h.mu.lock();
        defer h.mu.unlock();
        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .workflow_create => h.respondCreate(shard, conn, req),
            .workflow_start => h.respondStart(shard, conn, req),
            else => shard.sendOkResponse(conn, req.header.request_id, ""),
        }
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    /// Answers the client, or returns the write the client is parked on.
    pub fn handleCommand(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        self.mu.lock();
        defer self.mu.unlock();
        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .workflow_create => return self.handleCreate(shard, conn, req),
            .workflow_start => return self.handleStart(shard, conn, req),
            .workflow_signal => self.handleSignal(shard, conn, req),
            .workflow_cancel => self.handleCancel(shard, conn, req),
            .workflow_status => self.handleStatus(shard, conn, req),
            .workflow_history => self.handleHistory(shard, conn, req),
            .workflow_list_runs => self.handleListRuns(shard, conn, req),
            .workflow_get_definition => self.handleGetDefinition(shard, conn, req),
            .workflow_disable => self.handleDisable(shard, conn, req),
            .workflow_enable => self.handleEnable(shard, conn, req),
            .workflow_list_definitions => self.handleListDefinitions(shard, conn, req),
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown workflow opcode");
            },
        }
        return null;
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    /// Runs and definitions are keyed "namespace:name", and every node
    /// reads the namespace back up to the first ':'. Checked where a
    /// definition is created and a client starts a run: every producer's
    /// start comes from a definition.
    fn keyableNamespace(namespace: []const u8) bool {
        return std.mem.indexOfAny(u8, namespace, ":\x00") == null;
    }

    fn handleCreate(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        const yaml = req.value;
        if (!keyableNamespace(req.namespace)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid namespace name");
            return null;
        }

        if (yaml.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow definition is required");
            return null;
        }

        // Parse the YAML/JSON definition
        var def = parser.parseWorkflow(self.allocator, yaml) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid workflow definition");
            return null;
        };
        defer def.deinit(self.allocator);

        // Validate the definition
        var validation = validator.validateWorkflow(self.allocator, &def) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "validation failed");
            return null;
        };
        defer validation.deinit();

        if (validation.hasErrors()) {
            // Build error message from first error
            if (validation.items().len > 0) {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, validation.items()[0].message);
            } else {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow validation failed");
            }
            return null;
        }

        // Workflow name must be sent as req.key by all callers (CLI extracts
        // it from YAML client-side). The server validates it matches the
        // parsed definition to catch mismatches early.
        if (req.key.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required as key");
            return null;
        }
        const name = req.key;

        // The applier stores the definition and (re)registers its trigger
        // and schedule from the entry; the same applier runs at boot, so a
        // restart keeps them too.
        const proposed = self.proposeCreate(shard, req.namespace, name, yaml) catch |err| {
            shard.sendErrorResponse(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureMessage(err, "workflow not persisted"));
            return null;
        };
        return proposed;
    }

    /// The create applied: the definition is stored under its key.
    fn respondCreate(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;
        const ns_key = self.makeNsKey(req.namespace, name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(ns_key);
        if (!self.definitions.contains(ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
            return;
        }

        shard.sendOkResponse(conn, req.header.request_id, name);
    }

    // ── START ───────────────────────────────────────────────────────────

    fn handleStart(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        const workflow_name = req.key;

        if (workflow_name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return null;
        }
        if (!keyableNamespace(req.namespace)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid namespace name");
            return null;
        }

        // Build namespace-qualified key for definition/disabled lookups
        const def_ns_key = self.makeNsKey(req.namespace, workflow_name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };
        defer self.allocator.free(def_ns_key);

        // Check workflow exists
        if (!self.definitions.contains(def_ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return null;
        }

        // Check workflow is not disabled
        if (self.disabled.contains(def_ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow is disabled");
            return null;
        }

        // Parse the value: [ver_len:u16][ver][has_idem:u8][key_len:u16]?[key]?[has_rid:u8][rid_len:u16]?[rid]?[input...]
        var version: []const u8 = "latest";
        var idempotency_key: ?[]const u8 = null;
        var explicit_run_id: ?[]const u8 = null;
        var input: []const u8 = "{}";

        if (req.value.len >= 2) {
            var offset: usize = 0;
            const value = req.value;

            // Read version
            if (offset + 2 > value.len) {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                return null;
            }
            const ver_len = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;
            if (offset + ver_len > value.len) {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                return null;
            }
            if (ver_len > 0) {
                version = value[offset .. offset + ver_len];
            }
            offset += ver_len;

            // Read optional idempotency key
            if (offset < value.len) {
                const has_idem = value[offset];
                offset += 1;
                if (has_idem == 1) {
                    if (offset + 2 > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return null;
                    }
                    const key_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                    offset += 2;
                    if (offset + key_len > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return null;
                    }
                    idempotency_key = value[offset .. offset + key_len];
                    offset += key_len;
                }
            }

            // Read optional run_id
            if (offset < value.len) {
                const has_rid = value[offset];
                offset += 1;
                if (has_rid == 1) {
                    if (offset + 2 > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return null;
                    }
                    const rid_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                    offset += 2;
                    if (offset + rid_len > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return null;
                    }
                    explicit_run_id = value[offset .. offset + rid_len];
                    offset += rid_len;
                }
            }

            // Remaining bytes are input
            if (offset < value.len) {
                input = value[offset..];
            }
        }

        // Enforce idempotency mode: `required` rejects starts with no key.
        if (idempotency_key == null) {
            if (self.definitions.get(def_ns_key)) |def_rec| {
                if (def_rec.idempotency == .required) {
                    shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "idempotency key is required for this workflow");
                    return null;
                }
            }
        }

        // A key that already started a run answers with it. A start that
        // passes this while another with its key is uncommitted is decided
        // by the applier.
        if (idempotency_key) |idem_key| {
            if (self.runForIdempotencyKey(req.namespace, workflow_name, idem_key)) |existing| {
                shard.sendOkResponse(conn, req.header.request_id, existing.run_id_owned);
                return null;
            }
        }

        // Generate or use explicit run ID.
        // Server-generated IDs have embedded partition bits
        // for O(1) cross-shard routing on status/signal/cancel/history.
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = if (explicit_run_id) |rid|
            rid
        else blk: {
            const partition_id = shard.router.keyToPartitionNs(req.namespace, workflow_name);
            break :blk shard.run_id_gen.next(.workflow, partition_id, &run_id_buf) catch "wfr-0";
        };

        // Build namespace-qualified key for the runs map
        const run_ns_key = self.makeNsKey(req.namespace, run_id_str) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };

        // Guard against ID collision (e.g. replayed entry with same key)
        if (self.runs.contains(run_ns_key)) {
            self.allocator.free(run_ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .conflict, "run id already exists");
            return null;
        }
        self.allocator.free(run_ns_key);

        // The applier creates the run from the entry.
        const proposed = self.proposeStart(shard, req.namespace, run_id_str, workflow_name, version, input, idempotency_key, "workflow_started") catch |err| {
            shard.sendErrorResponse(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureMessage(err, "run not persisted"));
            return null;
        };
        return proposed;
    }

    /// The start applied: answer with the id of the run it created, and
    /// leave its first step to the tick.
    fn respondStart(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const stored_key = self.last_started_key orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
            return;
        };
        const run = self.runs.get(stored_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
            return;
        };
        if (self.last_start_collided) {
            shard.sendErrorResponse(conn, req.header.request_id, .conflict, "run id already exists");
            return;
        }
        if (self.last_start_existed) {
            shard.sendOkResponse(conn, req.header.request_id, run.run_id_owned);
            return;
        }
        if (shard.metrics_registry) |m| m.workflow.recordStarted();
        shard.sendOkResponse(conn, req.header.request_id, run.run_id_owned);
        self.queueFirstStep(stored_key, shard.answering_index);
    }

    /// The index key for an idempotency key, any length the wire allows:
    /// each part length-prefixed, so no byte in a name can make two
    /// different triples the same key.
    fn idemKey(self: *WorkflowHandler, namespace: []const u8, wf_name: []const u8, key: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{d}:{s}{d}:{s}{s}", .{ namespace.len, namespace, wf_name.len, wf_name, key });
    }

    /// The key in `runs` of the run an idempotency key started for this
    /// workflow in this namespace, if one has applied.
    fn runKeyForIdempotencyKey(self: *WorkflowHandler, namespace: []const u8, wf_name: []const u8, key: []const u8) ?[]const u8 {
        const ik = self.idemKey(namespace, wf_name, key) catch return null;
        defer self.allocator.free(ik);
        return self.idem_index.get(ik);
    }

    fn runForIdempotencyKey(self: *WorkflowHandler, namespace: []const u8, wf_name: []const u8, key: []const u8) ?*RunRecord {
        const run_key = self.runKeyForIdempotencyKey(namespace, wf_name, key) orelse return null;
        return self.runs.getPtr(run_key);
    }

    fn indexIdempotencyKey(self: *WorkflowHandler, namespace: []const u8, wf_name: []const u8, key: []const u8, run_key: []const u8) void {
        const ik = self.idemKey(namespace, wf_name, key) catch {
            log.err("workflow {s}: idempotency key of run {s} not indexed (out of memory); a retry with it may start a second run", .{ wf_name, run_key });
            return;
        };
        if (self.idem_index.contains(ik)) {
            self.allocator.free(ik);
            return;
        }
        self.idem_index.put(self.allocator, ik, run_key) catch {
            self.allocator.free(ik);
            log.err("workflow {s}: idempotency key of run {s} not indexed (out of memory); a retry with it may start a second run", .{ wf_name, run_key });
        };
    }

    /// The caller holds `mu`.
    fn queueFirstStep(self: *WorkflowHandler, ns_key: []const u8, index: u64) void {
        const owned = self.allocator.dupe(u8, ns_key) catch {
            log.err("workflow run {s} started but cannot be queued for its first step; it stays at its start", .{ns_key});
            return;
        };
        self.started_to_advance.append(self.allocator, .{ .ns_key = owned, .index = index }) catch {
            self.allocator.free(owned);
            log.err("workflow run {s} started but cannot be queued for its first step; it stays at its start", .{ns_key});
        };
    }

    /// Take the first step of each queued run whose start has applied, in
    /// queue order; one not applied yet waits for a later tick, one the log
    /// dropped is forgotten. Leader only: a step proposes.
    pub fn advanceStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        if (shard.raft_node.role != .leader) return;
        self.mu.lock();
        defer self.mu.unlock();
        var keep: usize = 0;
        var i: usize = 0;
        // A step can start a child run here, which joins the queue behind
        // this pass and is kept, its start not yet applied.
        while (i < self.started_to_advance.items.len) : (i += 1) {
            const s = self.started_to_advance.items[i];
            if (shard.raft_node.last_applied < s.index) {
                self.started_to_advance.items[keep] = s;
                keep += 1;
                continue;
            }
            defer self.allocator.free(s.ns_key);
            if (!self.runs.contains(s.ns_key)) {
                log.warn("shard {d}: workflow run {s} was started but its start created no run; nothing will step it", .{ shard.id, s.ns_key });
                continue;
            }
            const ns_end = std.mem.indexOfScalar(u8, s.ns_key, ':') orelse continue;
            self.advanceWorkflow(shard, s.ns_key, s.ns_key[0..ns_end]);
        }
        self.started_to_advance.shrinkRetainingCapacity(keep);
    }

    /// This node stopped leading: the runs it had not taken a first step
    /// for stay at their start. Logs how many had applied (they exist and
    /// stay `running`) and how many had not (the log may drop them).
    pub fn dropStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();
        var applied: usize = 0;
        for (self.started_to_advance.items) |s| {
            if (s.index <= shard.raft_node.last_applied) {
                // A few by name, for an operator to look up; the rest counted.
                if (applied < 3) log.warn("shard {d}: workflow run {s} started but had not taken its first step; it stays at its start", .{ shard.id, s.ns_key });
                applied += 1;
            }
            self.allocator.free(s.ns_key);
        }
        const unapplied = self.started_to_advance.items.len - applied;
        if (applied + unapplied > 0) log.warn("shard {d}: stopped leading with {d} started workflow runs that had not taken their first step, and {d} starts not yet applied", .{ shard.id, applied, unapplied });
        self.started_to_advance.clearRetainingCapacity();
    }

    // ── SIGNAL ──────────────────────────────────────────────────────────

    fn handleSignal(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.makeNsKey(req.namespace, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.getPtr(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // Parse signal: [signal_len:u16][signal_type][payload...]
        if (req.value.len < 2) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "signal type is required");
            return;
        }

        const sig_len = std.mem.readInt(u16, req.value[0..2], .little);
        if (2 + sig_len > req.value.len) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed signal");
            return;
        }

        const signal_type = req.value[2 .. 2 + sig_len];
        const payload_data = if (2 + sig_len < req.value.len) req.value[2 + sig_len ..] else null;

        // Store signal
        const owned_sig_type = self.allocator.dupe(u8, signal_type) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        const owned_payload: ?[]const u8 = if (payload_data) |p|
            self.allocator.dupe(u8, p) catch {
                self.allocator.free(owned_sig_type);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            }
        else
            null;

        const now_ms: i64 = @import("stdx").time.milliTimestamp();

        run.signals.append(self.allocator, .{
            .signal_type_owned = owned_sig_type,
            .payload_owned = owned_payload,
            .received_at_ms = now_ms,
        }) catch {
            self.allocator.free(owned_sig_type);
            if (owned_payload) |p| self.allocator.free(p);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "signal store failed");
            return;
        };

        // Add history event
        self.addHistoryEvent(run, "signal_received", signal_type, now_ms);
        if (shard.metrics_registry) |m| m.workflow.recordSignalDelivered();

        // If the run is waiting for this signal type, resume execution
        if (run.status == .waiting) {
            if (run.wait_signal_type_owned) |expected| {
                if (std.mem.eql(u8, expected, signal_type)) {
                    run.status = .running;
                    // Clear wait state
                    self.allocator.free(expected);
                    run.wait_signal_type_owned = null;
                    self.addHistoryEvent(run, "signal_matched", signal_type, now_ms);

                    // Resume: need a non-deferred ns_key copy for advanceWorkflow
                    const resume_key = self.allocator.dupe(u8, run_ns_key) catch {
                        shard.sendOkResponse(conn, req.header.request_id, "");
                        return;
                    };
                    defer self.allocator.free(resume_key);

                    // Follow the "success" transition from the current wait step
                    self.advanceWorkflow(shard, resume_key, req.namespace);
                }
            }
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── CANCEL ──────────────────────────────────────────────────────────

    fn handleCancel(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.makeNsKey(req.namespace, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.getPtr(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        const reason = if (req.value.len > 0) req.value else "cancelled by user";
        self.completeRun(shard, run_ns_key, run, .cancelled, reason, now_ms);

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── STATUS ──────────────────────────────────────────────────────────

    fn handleStatus(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.makeNsKey(req.namespace, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.get(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // Binary wire format:
        // [run_id_len:u16][run_id][workflow_len:u16][workflow][version_len:u16][version]
        // [status:u8][current_step_len:u16][current_step][input_len:u32][input]
        // [created_at:i64][has_started:u8][started_at?:i64][has_completed:u8][completed_at?:i64]
        // [has_wait_signal:u8][wait_signal_len:u16][wait_signal]?
        const current_step = run.current_step_name_owned orelse "start";
        var wire_buf: [16384]u8 = undefined;
        var fbs: std.Io.Writer = .fixed(&wire_buf);
        const w = &fbs;
        w.writeInt(u16, @intCast(run.run_id_owned.len), .little) catch return;
        w.writeAll(run.run_id_owned) catch return;
        w.writeInt(u16, @intCast(run.workflow_name_owned.len), .little) catch return;
        w.writeAll(run.workflow_name_owned) catch return;
        w.writeInt(u16, @intCast(run.workflow_version_owned.len), .little) catch return;
        w.writeAll(run.workflow_version_owned) catch return;
        w.writeByte(@intFromEnum(run.status)) catch return;
        w.writeInt(u16, @intCast(current_step.len), .little) catch return;
        w.writeAll(current_step) catch return;
        w.writeInt(u32, @intCast(run.input_owned.len), .little) catch return;
        w.writeAll(run.input_owned) catch return;
        w.writeInt(i64, run.created_at_ms, .little) catch return;
        if (run.started_at_ms) |v| {
            w.writeByte(1) catch return;
            w.writeInt(i64, v, .little) catch return;
        } else {
            w.writeByte(0) catch return;
        }
        if (run.completed_at_ms) |v| {
            w.writeByte(1) catch return;
            w.writeInt(i64, v, .little) catch return;
        } else {
            w.writeByte(0) catch return;
        }
        if (run.wait_signal_type_owned) |sig| {
            w.writeByte(1) catch return;
            w.writeInt(u16, @intCast(sig.len), .little) catch return;
            w.writeAll(sig) catch return;
        } else {
            w.writeByte(0) catch return;
        }
        // Optional: output (composed from definition's output mapping)
        if (run.output_owned) |output| {
            w.writeByte(1) catch return;
            w.writeInt(u32, @intCast(output.len), .little) catch return;
            w.writeAll(output) catch return;
        } else {
            w.writeByte(0) catch return;
        }

        shard.sendOkResponse(conn, req.header.request_id, fbs.buffered());
    }

    // ── HISTORY ─────────────────────────────────────────────────────────

    fn handleHistory(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.makeNsKey(req.namespace, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.get(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // Parse limit from value
        var limit: u32 = 100;
        if (req.value.len >= 4) {
            limit = std.mem.readInt(u32, req.value[0..4], .little);
        }

        // Serialize to binary wire format:
        // [count:u32]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
        // [has_more:u8][cursor_len:u16]
        const events = run.history.items;
        const count: u32 = @intCast(@min(events.len, limit));

        var total: usize = 4; // count
        for (events[0..count]) |evt| {
            total += 2 + evt.event_type_owned.len; // type_len:u16 + type
            total += 2 + evt.detail_owned.len; // detail_len:u16 + detail
            total += 8; // timestamp:i64
        }
        total += 1 + 2; // trailer

        const buf = self.allocator.alloc(u8, total) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);

        std.mem.writeInt(u32, buf[0..4], count, .little);
        var pos: usize = 4;
        for (events[0..count]) |evt| {
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(evt.event_type_owned.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..evt.event_type_owned.len], evt.event_type_owned);
            pos += evt.event_type_owned.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(evt.detail_owned.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..evt.detail_owned.len], evt.detail_owned);
            pos += evt.detail_owned.len;
            std.mem.writeInt(i64, buf[pos..][0..8], evt.timestamp_ms, .little);
            pos += 8;
        }
        buf[pos] = 0; // has_more
        pos += 1;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little); // cursor_len

        shard.sendOkResponse(conn, req.header.request_id, buf);
    }

    // ── LIST RUNS ───────────────────────────────────────────────────────

    const RunInfo = struct {
        run_id: []const u8,
        workflow: []const u8,
        status: []const u8,
        created_at: i64,
    };

    /// Collect runs matching filters from this handler's local runs map.
    /// Called under mu.lock() — safe for both local and cross-shard use.
    fn collectMatchingRuns(
        self: *WorkflowHandler,
        runs_list: *std.ArrayListUnmanaged(RunInfo),
        namespace: []const u8,
        workflow_name: []const u8,
        status_filter: ?[]const u8,
        search_lower: ?[]const u8,
    ) !void {
        const ns_prefix = self.makeNsKey(namespace, "") orelse return error.OutOfMemory;
        defer self.allocator.free(ns_prefix);

        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            const run = entry.value_ptr;
            const map_key = entry.key_ptr.*;

            if (!std.mem.startsWith(u8, map_key, ns_prefix)) continue;
            if (workflow_name.len > 0 and !std.mem.eql(u8, run.workflow_name_owned, workflow_name)) continue;
            if (status_filter) |sf| {
                if (!std.mem.eql(u8, run.status.toString(), sf)) continue;
            }

            // Free-text search: match against run_id, workflow, step, input, search_tags
            if (search_lower) |sq| {
                const matched = containsLower(run.run_id_owned, sq) or
                    containsLower(run.workflow_name_owned, sq) or
                    (if (run.current_step_name_owned) |step| containsLower(step, sq) else false) or
                    containsLower(run.input_owned, sq) or
                    (if (run.search_tags_owned) |tags| containsLower(tags, sq) else false);
                if (!matched) continue;
            }

            try runs_list.append(self.allocator, .{
                .run_id = run.run_id_owned,
                .workflow = run.workflow_name_owned,
                .status = run.status.toString(),
                .created_at = run.started_at_ms orelse run.created_at_ms,
            });
        }
    }

    fn handleListRuns(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const workflow_name = req.key;

        // Parse value: [limit:u32][status_len:u16][status][cursor_len:u16][cursor][search_len:u16][search]
        if (req.value.len < 10) { // 4 + 2 + 2 + 2 minimum
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid list-runs request");
            return;
        }

        const limit = std.mem.readInt(u32, req.value[0..4], .little);
        var offset: usize = 4;

        // Status filter
        const sf_len = std.mem.readInt(u16, req.value[offset..][0..2], .little);
        offset += 2;
        const status_filter: ?[]const u8 = if (sf_len > 0 and offset + sf_len <= req.value.len) blk: {
            const s = req.value[offset .. offset + sf_len];
            offset += sf_len;
            break :blk s;
        } else null;

        // Cursor
        const c_len = std.mem.readInt(u16, req.value[offset..][0..2], .little);
        offset += 2;
        const cursor_filter: ?[]const u8 = if (c_len > 0 and offset + c_len <= req.value.len) blk: {
            const c = req.value[offset .. offset + c_len];
            offset += c_len;
            break :blk c;
        } else null;

        // Search query
        const sq_len = std.mem.readInt(u16, req.value[offset..][0..2], .little);
        offset += 2;
        const search_query: ?[]const u8 = if (sq_len > 0 and offset + sq_len <= req.value.len) blk: {
            const s = req.value[offset .. offset + sq_len];
            offset += sq_len;
            break :blk s;
        } else null;

        // Lowercase the search query once for case-insensitive matching
        var search_lower_buf: [256]u8 = undefined;
        const search_lower: ?[]const u8 = if (search_query) |sq| blk: {
            const len = @min(sq.len, search_lower_buf.len);
            for (0..len) |idx| {
                search_lower_buf[idx] = std.ascii.toLower(sq[idx]);
            }
            break :blk search_lower_buf[0..len];
        } else null;

        // Collect matching runs with sort key
        var runs_list: std.ArrayListUnmanaged(RunInfo) = .empty;
        defer runs_list.deinit(self.allocator);

        // Cross-shard aggregation: when workflow_name is empty, search all shards.
        // When workflow_name is set, all runs for that workflow are on this shard.
        if (workflow_name.len == 0) {
            // Search all shards via peer_shards
            self.collectMatchingRuns(&runs_list, req.namespace, workflow_name, status_filter, search_lower) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
            if (shard.peer_shards) |peers| {
                for (peers) |peer| {
                    if (peer.id == shard.id) continue; // skip self — already collected
                    const peer_handler = peer.workflow_handler;
                    peer_handler.mu.lock();
                    defer peer_handler.mu.unlock();
                    peer_handler.collectMatchingRuns(&runs_list, req.namespace, workflow_name, status_filter, search_lower) catch {
                        shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                        return;
                    };
                }
            }
        } else {
            // Single-shard: all runs for this workflow are local
            self.collectMatchingRuns(&runs_list, req.namespace, workflow_name, status_filter, search_lower) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }

        // Sort by created_at descending (newest first)
        std.mem.sortUnstable(RunInfo, runs_list.items, {}, struct {
            fn lessThan(_: void, a: RunInfo, b: RunInfo) bool {
                return a.created_at > b.created_at;
            }
        }.lessThan);

        // Apply cursor-based pagination: skip past the cursor run_id
        var start_idx: usize = 0;
        if (cursor_filter) |c| {
            for (runs_list.items, 0..) |item, idx| {
                if (std.mem.eql(u8, item.run_id, c)) {
                    start_idx = idx + 1;
                    break;
                }
            }
        }

        const end_idx = @min(start_idx + limit, runs_list.items.len);
        const page = runs_list.items[start_idx..end_idx];
        const has_more = end_idx < runs_list.items.len;
        const last_run_id: []const u8 = if (page.len > 0) page[page.len - 1].run_id else "";

        // Serialize to binary wire format:
        // [count:u32]([run_id_len:u16][run_id][workflow_len:u16][workflow]
        //  [status_len:u16][status][created_at:i64])*
        // [has_more:u8][cursor_len:u16][cursor]?
        var total: usize = 4; // count
        for (page) |r| {
            total += 2 + r.run_id.len + 2 + r.workflow.len + 2 + r.status.len + 8;
        }
        total += 1 + 2 + (if (has_more) last_run_id.len else 0); // trailer

        const buf = self.allocator.alloc(u8, total) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);

        std.mem.writeInt(u32, buf[0..4], @intCast(page.len), .little);
        var pos: usize = 4;
        for (page) |r| {
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(r.run_id.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..r.run_id.len], r.run_id);
            pos += r.run_id.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(r.workflow.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..r.workflow.len], r.workflow);
            pos += r.workflow.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(r.status.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..r.status.len], r.status);
            pos += r.status.len;
            std.mem.writeInt(i64, buf[pos..][0..8], r.created_at, .little);
            pos += 8;
        }
        buf[pos] = if (has_more) 1 else 0;
        pos += 1;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(if (has_more) last_run_id.len else 0), .little);
        pos += 2;
        if (has_more) {
            @memcpy(buf[pos..][0..last_run_id.len], last_run_id);
        }

        shard.sendOkResponse(conn, req.header.request_id, buf);
    }

    // ── GET DEFINITION ──────────────────────────────────────────────────

    fn handleGetDefinition(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        const ns_key = self.makeNsKey(req.namespace, name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(ns_key);

        const def = self.definitions.get(ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // If a specific version is requested, check it matches
        if (req.value.len > 0) {
            if (!std.mem.eql(u8, def.version_owned, req.value)) {
                shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
                return;
            }
        }

        shard.sendOkResponse(conn, req.header.request_id, def.yaml_owned);
    }

    // ── DISABLE ─────────────────────────────────────────────────────────

    fn handleDisable(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        const ns_key = self.makeNsKey(req.namespace, name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        // Check workflow exists
        if (!self.definitions.contains(ns_key)) {
            self.allocator.free(ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return;
        }

        if (!self.disabled.contains(ns_key)) {
            self.disabled.put(ns_key, {}) catch {
                self.allocator.free(ns_key);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "disable failed");
                return;
            };
        } else {
            // Already disabled — free the temp key
            self.allocator.free(ns_key);
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── ENABLE ──────────────────────────────────────────────────────────

    fn handleEnable(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        const ns_key = self.makeNsKey(req.namespace, name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(ns_key);

        if (self.disabled.fetchRemove(ns_key)) |old| {
            self.allocator.free(old.key);
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── LIST DEFINITIONS ────────────────────────────────────────────────

    fn handleListDefinitions(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        // Build namespace prefix for filtering ("namespace:")
        const ns_prefix = self.makeNsKey(req.namespace, "") orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(ns_prefix);

        // Collect matching definitions
        const DefInfo = struct { name: []const u8, version: []const u8, created_at: i64 };
        var defs: std.ArrayListUnmanaged(DefInfo) = .empty;
        defer defs.deinit(self.allocator);

        var dit = self.definitions.iterator();
        while (dit.next()) |entry| {
            const def = entry.value_ptr;
            const map_key = entry.key_ptr.*;
            if (!std.mem.startsWith(u8, map_key, ns_prefix)) continue;
            defs.append(self.allocator, .{
                .name = def.name_owned,
                .version = def.version_owned,
                .created_at = def.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }

        // Serialize to binary wire format:
        // [count:u32]([name_len:u16][name][version_len:u16][version][created_at:i64])*
        // [has_more:u8][cursor_len:u16]
        var total: usize = 4; // count
        for (defs.items) |d| {
            total += 2 + d.name.len + 2 + d.version.len + 8;
        }
        total += 1 + 2; // trailer

        const buf = self.allocator.alloc(u8, total) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);

        std.mem.writeInt(u32, buf[0..4], @intCast(defs.items.len), .little);
        var pos: usize = 4;
        for (defs.items) |d| {
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(d.name.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..d.name.len], d.name);
            pos += d.name.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(d.version.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..d.version.len], d.version);
            pos += d.version.len;
            std.mem.writeInt(i64, buf[pos..][0..8], d.created_at, .little);
            pos += 8;
        }
        buf[pos] = 0; // has_more
        pos += 1;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little); // cursor_len

        shard.sendOkResponse(conn, req.header.request_id, buf);
    }

    // ── Step Executor ───────────────────────────────────────────────────

    /// Maximum steps a single advanceWorkflow call may execute before bailing
    /// out as a safety net against infinite-loop workflow definitions.
    const MAX_ADVANCE_STEPS: u32 = 256;

    /// Drive a run through the workflow step graph.
    ///
    /// Starting from the run's current position (start step if null),
    /// execute `.run` steps by invoking actions via the ActionsHandler,
    /// resolve input mappings via JSONPath, and follow transitions.
    /// When a `.wait_for_signal` step is reached the run enters `.waiting`
    /// and this method returns. Async (user-hosted) actions also park the
    /// run in `.waiting` until the action completes. When a terminal
    /// transition is reached the run is completed/failed accordingly.
    ///
    /// `shard` provides access to the ActionsHandler for action invocation.
    /// `run_ns_key` must be a key that is valid in `self.runs`.
    /// `namespace` is used only for definition lookups.
    fn advanceWorkflow(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, namespace: []const u8) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        if (run.status.isTerminal()) return;

        // Look up the definition
        const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);

        const def_record = self.definitions.get(def_ns_key) orelse return;

        // Parse the definition to access the step graph
        var def = parser.parseWorkflow(self.allocator, def_record.yaml_owned) catch return;
        defer def.deinit(self.allocator);

        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        var steps_executed: u32 = 0;

        // Initialize step_outputs if needed
        if (run.step_outputs == null) {
            run.step_outputs = StepOutputMap.init();
        }

        while (steps_executed < MAX_ADVANCE_STEPS) {
            steps_executed += 1;

            // Determine current step
            const step: definition.Step = if (run.current_step_name_owned) |step_name|
                def.getStep(step_name) orelse {
                    // Step not found in definition — fail the run
                    self.completeRun(shard, run_ns_key, run, .failed, "step not found in definition", now_ms);
                    return;
                }
            else
                def.start; // first step

            switch (step) {
                .run => |run_step| {
                    // Record step start
                    const step_label = run.current_step_name_owned orelse "start";
                    self.addHistoryEvent(run, "step_started", step_label, now_ms);

                    // Resolve input mapping via JSONPath
                    var resolved_input: ?[]u8 = null;
                    if (run_step.input_mapping) |mapping| {
                        resolved_input = jsonpath.resolveInput(
                            self.allocator,
                            mapping,
                            run.input_owned,
                            if (run.step_outputs) |*so| so else null,
                            run.run_id_owned,
                        ) catch null;
                    }
                    const step_input: []const u8 = resolved_input orelse run.input_owned;

                    // Execute the step and determine outcome
                    const outcome_or_park = self.executeRunStep(shard, run, &def, run_step, step_input, step_label, namespace, now_ms);

                    // Free resolved input after action invocation
                    if (resolved_input) |ri| self.allocator.free(ri);

                    // null outcome means run is parked (async action)
                    const raw_outcome = outcome_or_park orelse return;

                    // Polling: a `pending` outcome on a `poll:` step parks for a
                    // backoff re-invocation instead of transitioning. On exhaustion
                    // the effective outcome becomes `timeout`.
                    const outcome = switch (self.applyPollPolicy(run, run_step, raw_outcome, step_label, now_ms)) {
                        .parked => return,
                        .proceed => |o| o,
                    };

                    // Check retry on failure
                    if (std.mem.eql(u8, outcome, definition.StepOutcome.failure) or
                        std.mem.eql(u8, outcome, definition.StepOutcome.execution_failure))
                    {
                        if (run_step.retry) |retry| {
                            if (run.retry_count < retry.max_attempts) {
                                run.retry_count += 1;
                                self.addHistoryEvent(run, "step_retry", step_label, now_ms);
                                continue; // retry same step immediately
                            }
                        }
                    }

                    self.addHistoryEvent(run, "step_completed", step_label, now_ms);
                    if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();

                    // Reset retry + poll counters on step transition
                    run.retry_count = 0;
                    run.poll_attempt = 0;
                    run.poll_next_at_ms = 0;

                    // Follow transition
                    const transition = run_step.resolveTransition(outcome) orelse {
                        // No matching transition — fail
                        self.completeRun(shard, run_ns_key, run, .failed, "no transition for outcome", now_ms);
                        return;
                    };

                    // Check if target is a terminal (builtin or custom)
                    if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                        self.completeRun(shard, run_ns_key, run, status, transition.target, now_ms);
                        return;
                    }

                    // Transition to next step
                    self.setCurrentStep(run, transition.target);
                },

                .wait_for_signal => |wait_step| {
                    const step_label = run.current_step_name_owned orelse "start";
                    self.addHistoryEvent(run, "waiting_for_signal", wait_step.signal_type, now_ms);

                    // Check if a matching signal has already been received
                    var signal_found = false;
                    for (run.signals.items) |sig| {
                        if (std.mem.eql(u8, sig.signal_type_owned, wait_step.signal_type)) {
                            signal_found = true;
                            break;
                        }
                    }

                    if (signal_found) {
                        // Signal already received — follow "success" transition
                        self.addHistoryEvent(run, "step_completed", step_label, now_ms);
                        if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();
                        const transition = wait_step.getTransition(definition.StepOutcome.success) orelse {
                            self.completeRun(shard, run_ns_key, run, .failed, "no success transition for wait step", now_ms);
                            return;
                        };

                        if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                            self.completeRun(shard, run_ns_key, run, status, transition.target, now_ms);
                            return;
                        }
                        self.setCurrentStep(run, transition.target);
                        continue; // advance to next step
                    }

                    // No signal yet — park the run in waiting state
                    run.status = .waiting;
                    const owned_sig = self.allocator.dupe(u8, wait_step.signal_type) catch return;
                    if (run.wait_signal_type_owned) |old| self.allocator.free(old);
                    run.wait_signal_type_owned = owned_sig;

                    // Record timeout deadline if configured
                    if (wait_step.timeout_ms) |timeout_ms| {
                        run.wait_timeout_at_ms = now_ms + timeout_ms;
                        if (wait_step.on_timeout) |target| {
                            if (run.wait_timeout_target_owned) |old| self.allocator.free(old);
                            run.wait_timeout_target_owned = self.allocator.dupe(u8, target) catch null;
                        }
                    }
                    return; // will resume when signal arrives or timeout fires
                },
            }
        }

        // Safety: too many steps — possible infinite loop in definition
        self.completeRun(shard, run_ns_key, run, .failed, "max step limit reached", now_ms);
    }

    /// Execute a single run step. Returns the step outcome string, or null
    /// if the run was parked waiting for an async action.
    fn executeRunStep(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        def: *const definition.WorkflowDefinition,
        run_step: definition.RunStep,
        step_input: []const u8,
        step_label: []const u8,
        namespace: []const u8,
        now_ms: i64,
    ) ?[]const u8 {
        if (run_step.isAction()) {
            return self.invokeAction(shard, run, run_step.targetName(), step_input, step_label, namespace, now_ms);
        } else if (run_step.isPlan()) {
            return self.executePlan(shard, run, def, run_step.targetName(), step_input, step_label, namespace, now_ms);
        } else if (run_step.isChildWorkflow()) {
            return self.executeChildWorkflow(shard, run, run_step, step_input, step_label, namespace, now_ms);
        }
        return definition.StepOutcome.execution_failure;
    }

    /// Invoke a child workflow (`run: "@workflow/<name>[:version]"`).
    /// Creates a child run on the shard that owns the child workflow, parks the
    /// parent, and returns null. The parent resumes via checkPendingActions when
    /// the child reaches a terminal state (completed → success, otherwise failure).
    fn executeChildWorkflow(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        run_step: definition.RunStep,
        step_input: []const u8,
        step_label: []const u8,
        namespace: []const u8,
        now_ms: i64,
    ) ?[]const u8 {
        // Strip the optional ":version" suffix — definitions are keyed by name.
        const raw = run_step.targetName();
        const child_name = if (std.mem.indexOfScalar(u8, raw, ':')) |i| raw[0..i] else raw;

        // The parent's run key, to find the run again after the child's
        // start is proposed.
        const parent_ns_key = self.makeNsKey(namespace, run.run_id_owned) orelse
            return definition.StepOutcome.execution_failure;
        defer self.allocator.free(parent_ns_key);

        const child_ns_key = self.makeNsKey(namespace, child_name) orelse
            return definition.StepOutcome.execution_failure;
        defer self.allocator.free(child_ns_key);

        // Resolve the shard/handler that owns the child workflow.
        const target_shard_id = self.resolveWorkflowShardId(shard, namespace, child_name);
        const is_local = (target_shard_id == shard.id);
        const target_shard: *Shard = if (is_local) shard else blk: {
            const peers = shard.peer_shards orelse break :blk shard;
            break :blk peers[target_shard_id];
        };
        const target_handler: *WorkflowHandler = if (is_local) self else target_shard.workflow_handler;

        // Cross-shard: serialize access to the target handler's run map.
        if (!is_local) target_handler.mu.lock();
        defer if (!is_local) target_handler.mu.unlock();

        if (!target_handler.definitions.contains(child_ns_key)) {
            self.addHistoryEvent(run, "child_not_found", child_name, now_ms);
            return definition.StepOutcome.target_not_found;
        }

        var child_id_buf: [32]u8 = undefined;
        var child_index: u64 = 0;
        const child_rid = target_handler.spawnRun(target_shard, namespace, child_name, step_input, "child_started", &child_id_buf, &child_index) orelse
            return definition.StepOutcome.execution_failure;

        const parent = self.runs.getPtr(parent_ns_key) orelse return definition.StepOutcome.execution_failure;
        // The index is in the target shard's log: known only when that is
        // this shard.
        self.parkForChild(parent, child_rid, step_label, child_name, target_shard_id, now_ms, if (is_local) child_index else 0);
        return null; // parked
    }

    /// Compute the shard ID that owns the given workflow (definition + runs).
    fn resolveWorkflowShardId(self: *WorkflowHandler, shard: *Shard, namespace: []const u8, workflow_name: []const u8) u16 {
        _ = self;
        const hash = router.hashKeyWithNamespace(namespace, workflow_name);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard.id,
            .shard => |t| t.shard_id,
            .remote => shard.id,
        };
    }

    /// Start a child run. Must be called with this handler's `mu` held.
    /// Returns the new run's id, written into `id_buf`, and its start's log
    /// index in `index`; the run exists once its start applies, and until
    /// then the parent waits as for any child that has not finished.
    fn spawnRun(
        self: *WorkflowHandler,
        shard: *Shard,
        namespace: []const u8,
        wf_name: []const u8,
        input: []const u8,
        evt_type: []const u8,
        id_buf: *[32]u8,
        index: *u64,
    ) ?[]const u8 {
        const partition_id = shard.router.keyToPartitionNs(namespace, wf_name);
        const run_id_str = shard.run_id_gen.next(.workflow, partition_id, id_buf) catch return null;
        index.* = self.proposeRun(shard, namespace, run_id_str, wf_name, "latest", input, null, evt_type) orelse return null;
        return run_id_str;
    }

    /// Whether the run `run` waits for, which the caller did not find, was
    /// lost: this shard proposed it and has applied that entry's index
    /// without creating it.
    fn pendingLost(shard: *Shard, run: *const RunRecord, on_shard: ?u16) bool {
        if (run.pending_index == 0 or (on_shard orelse shard.id) != shard.id) return false;
        return shard.raft_node.last_applied >= run.pending_index;
    }

    /// Park a workflow run waiting for a child workflow to reach a terminal state.
    fn parkForChild(self: *WorkflowHandler, run: *RunRecord, child_run_id: []const u8, step_label: []const u8, child_name: []const u8, target_shard_id: u16, now_ms: i64, index: u64) void {
        run.status = .waiting;
        run.pending_index = index;

        if (run.pending_child_run_id_owned) |old| self.allocator.free(old);
        run.pending_child_run_id_owned = self.allocator.dupe(u8, child_run_id) catch null;

        if (run.pending_step_name_owned) |old| self.allocator.free(old);
        run.pending_step_name_owned = self.allocator.dupe(u8, step_label) catch null;

        run.pending_child_shard_id = target_shard_id;

        const sig_type = std.fmt.allocPrint(self.allocator, "_child_done:{s}", .{child_run_id}) catch null;
        if (run.wait_signal_type_owned) |old| self.allocator.free(old);
        run.wait_signal_type_owned = sig_type;

        self.addHistoryEvent(run, "awaiting_child", child_name, now_ms);
    }

    /// Execute a plan step: iterate executors with selection strategy awareness.
    /// For health-weighted selection, executors are sorted by health score and
    /// circuit breakers are consulted before each attempt. For static-order
    /// (and other strategies), executors are tried in definition order.
    /// Returns the outcome string, or null if parked waiting for an async action.
    fn executePlan(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        def: *const definition.WorkflowDefinition,
        plan_name: []const u8,
        step_input: []const u8,
        step_label: []const u8,
        namespace: []const u8,
        now_ms: i64,
    ) ?[]const u8 {
        const plan = def.getPlan(plan_name) orelse {
            self.addHistoryEvent(run, "plan_not_found", plan_name, now_ms);
            return definition.StepOutcome.execution_failure;
        };

        // Build executor iteration order — health-weighted sorts by score,
        // all other strategies use definition order.
        var order_buf: [32]u8 = undefined;
        const exec_count: u8 = @intCast(@min(plan.executors.len, 32));
        const exec_order = order_buf[0..exec_count];
        for (exec_order, 0..) |*slot, i| slot.* = @intCast(i);

        if (plan.selection == .health_weighted) {
            self.sortExecutorsByHealth(exec_order, plan, namespace, run.workflow_name_owned);
        }

        while (run.plan_executor_idx < exec_count) {
            const natural_idx = exec_order[run.plan_executor_idx];
            const executor = plan.executors[natural_idx];

            // Health-weighted: check circuit breaker before attempting
            if (plan.selection == .health_weighted) {
                if (executor.breaker) |breaker_cfg| {
                    if (self.getOrCreateHealth(namespace, run.workflow_name_owned, plan_name, executor.name)) |health| {
                        if (!health.shouldAllowRequest(breaker_cfg, now_ms)) {
                            self.addHistoryEvent(run, "plan_breaker_skip", executor.name, now_ms);
                            run.plan_executor_idx += 1;
                            run.plan_executor_retry_count = 0;
                            continue;
                        }
                    }
                }
            }

            // Track natural index for async resume health recording
            run.plan_executor_natural_idx = natural_idx;

            self.addHistoryEvent(run, "plan_executor_start", executor.name, now_ms);

            // Plan executor action_name stores the raw YAML "run" value
            // (e.g. "@actions/process-payment").  Strip the prefix so the
            // actions handler can look it up by its registered name.
            const resolved_action = if (std.mem.startsWith(u8, executor.action_name, "@actions/"))
                executor.action_name["@actions/".len..]
            else
                executor.action_name;

            const outcome = self.invokeAction(shard, run, resolved_action, step_input, step_label, namespace, now_ms);

            // Parked for async action — will resume via resumeFromAction
            if (outcome == null) return null;

            const success = std.mem.eql(u8, outcome.?, definition.StepOutcome.success);

            // Record health metrics for synchronous completions
            if (plan.selection == .health_weighted) {
                self.recordPlanHealth(namespace, run.workflow_name_owned, plan_name, executor, success, now_ms);
            }

            // Success — reset plan state and return
            if (success) {
                self.addHistoryEvent(run, "plan_executor_success", executor.name, now_ms);
                run.plan_executor_idx = 0;
                run.plan_executor_natural_idx = 0;
                run.plan_executor_retry_count = 0;
                return outcome;
            }

            // Failure — check per-executor retry
            if (executor.retry) |retry| {
                if (run.plan_executor_retry_count < retry.max_attempts) {
                    run.plan_executor_retry_count += 1;
                    self.addHistoryEvent(run, "plan_executor_retry", executor.name, now_ms);
                    continue;
                }
            }

            // Executor exhausted — advance to next
            self.addHistoryEvent(run, "plan_executor_exhausted", executor.name, now_ms);
            run.plan_executor_idx += 1;
            run.plan_executor_retry_count = 0;
        }

        // All executors exhausted
        run.plan_executor_idx = 0;
        run.plan_executor_natural_idx = 0;
        run.plan_executor_retry_count = 0;
        return definition.StepOutcome.failure;
    }

    /// Invoke an action by name via the ActionsHandler.
    /// Returns the step outcome, or null if parked for async completion.
    fn invokeAction(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        action_name: []const u8,
        input: []const u8,
        step_label: []const u8,
        namespace: []const u8,
        now_ms: i64,
    ) ?[]const u8 {
        // Determine target shard via hash routing (deterministic: same as
        // action_register and action_invoke pre_route). This replaces the
        // old findActionShard scan.
        const target_shard_id = self.resolveActionShardId(shard, namespace, action_name);
        const is_local = (target_shard_id == shard.id);

        if (is_local) {
            // Local shard: use invokeByName directly (same thread, no threading concerns)
            return self.invokeActionLocal(shard, run, action_name, input, step_label, target_shard_id, now_ms);
        }

        // Cross-shard: the target shard creates the run on its own thread,
        // through its own log and applier, from a message we hand it.
        var run_id_buf: [32]u8 = undefined;
        const partition_id = shard.router.keyToPartitionNs(namespace, action_name);
        const pre_run_id = shard.run_id_gen.next(.action, partition_id, &run_id_buf) catch {
            return definition.StepOutcome.execution_failure;
        };
        const peer_inboxes = shard.peer_inboxes orelse return definition.StepOutcome.execution_failure;
        if (target_shard_id >= peer_inboxes.len) return definition.StepOutcome.execution_failure;
        const message = ActionsHandler.encodeStartRunMessage(shard.allocator, pre_run_id, action_name, input, run.run_id_owned, run.workflow_name_owned) orelse {
            return definition.StepOutcome.execution_failure;
        };
        if (!peer_inboxes[target_shard_id].send(.{
            .tag = .action_start,
            .src_shard = @intCast(shard.id),
            .payload_len = @intCast(message.len),
            .payload_ptr = message.ptr,
        })) {
            shard.allocator.free(message);
            return definition.StepOutcome.execution_failure;
        }

        // Park the workflow run, awaiting action completion. The owning shard
        // proposes the run and wakes workers when it applies.
        self.parkForAction(run, pre_run_id, step_label, action_name, target_shard_id, now_ms, 0);
        return null; // signals: parked
    }

    /// Same-shard action invocation (no threading concerns).
    fn invokeActionLocal(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        action_name: []const u8,
        input: []const u8,
        step_label: []const u8,
        target_shard_id: u16,
        now_ms: i64,
    ) ?[]const u8 {
        const action = shard.actions_handler.actions.get(action_name) orelse {
            self.addHistoryEvent(run, "action_not_found", action_name, now_ms);
            return definition.StepOutcome.target_not_found;
        };

        if (!action.enabled) {
            self.addHistoryEvent(run, "action_disabled", action_name, now_ms);
            return definition.StepOutcome.target_disabled;
        }

        var action_id_buf: [32]u8 = undefined;
        const invoked = shard.actions_handler.invokeByName(shard, action_name, input, run.run_id_owned, run.workflow_name_owned, &action_id_buf) orelse {
            return definition.StepOutcome.execution_failure;
        };
        // The run exists once the invoke applies; `checkPendingActions`
        // resumes this one when it finishes.
        self.parkForAction(run, invoked.id, step_label, action_name, target_shard_id, now_ms, invoked.index);
        return null;
    }

    /// Resolve the ActionsHandler responsible for the given action name.
    /// Uses peer_shards for cross-shard access, falls back to local.
    fn resolveActionHandler(self: *WorkflowHandler, shard: *Shard, namespace: []const u8, action_name: []const u8) *ActionsHandler {
        _ = self;
        const hash = router.hashKeyWithNamespace(namespace, action_name);
        const target = shard.router.route(hash);
        switch (target) {
            .local => return shard.actions_handler,
            .shard => |t| {
                if (shard.peer_shards) |peers| {
                    return peers[t.shard_id].actions_handler;
                }
                return shard.actions_handler;
            },
            .remote => return shard.actions_handler,
        }
    }

    /// Resolve the StreamHandler that owns `stream_name` in `namespace`, using
    /// the same composite hash the dispatcher uses to route stream_append. The
    /// stream's data lives on that shard, so stream triggers must read from it
    /// rather than the local shard (which is merely wherever the triggering
    /// workflow definition happened to be created). Falls back to local.
    fn resolveStreamHandler(shard: *Shard, namespace: []const u8, stream_name: []const u8) @TypeOf(shard.stream_handler) {
        const hash = router.hashKeyWithNamespace(namespace, stream_name);
        const target = shard.router.route(hash);
        switch (target) {
            .local => return shard.stream_handler,
            .shard => |t| {
                if (shard.peer_shards) |peers| {
                    return peers[t.shard_id].stream_handler;
                }
                return shard.stream_handler;
            },
            .remote => return shard.stream_handler,
        }
    }

    /// Compute the shard ID that owns the given action.
    fn resolveActionShardId(self: *WorkflowHandler, shard: *Shard, namespace: []const u8, action_name: []const u8) u16 {
        _ = self;
        const hash = router.hashKeyWithNamespace(namespace, action_name);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard.id,
            .shard => |t| t.shard_id,
            .remote => shard.id,
        };
    }

    /// Get an action run result, checking the correct shard's ActionsHandler.
    /// For local shard: direct access. For remote shard: mutex-protected access.
    fn getActionRunResult(shard: *Shard, target_shard_id: ?u16, action_rid: []const u8) ?ActionsHandler.InternalRunResult {
        const tid = target_shard_id orelse shard.id;
        if (tid == shard.id) {
            // Local shard
            return shard.actions_handler.getRunResult(action_rid);
        }
        // Cross-shard: use peer shard's handler with mutex
        if (shard.peer_shards) |peers| {
            const handler = peers[tid].actions_handler;
            handler.runs_mu.lock();
            defer handler.runs_mu.unlock();
            return handler.getRunResult(action_rid);
        }
        return shard.actions_handler.getRunResult(action_rid);
    }

    /// Park a workflow run waiting for an async action to complete.
    fn parkForAction(self: *WorkflowHandler, run: *RunRecord, action_run_id: []const u8, step_label: []const u8, action_name: []const u8, target_shard_id: u16, now_ms: i64, index: u64) void {
        run.status = .waiting;
        run.pending_index = index;

        // Store tracking info for checkPendingActions
        if (run.pending_action_run_id_owned) |old| self.allocator.free(old);
        run.pending_action_run_id_owned = self.allocator.dupe(u8, action_run_id) catch null;

        if (run.pending_step_name_owned) |old| self.allocator.free(old);
        run.pending_step_name_owned = self.allocator.dupe(u8, step_label) catch null;

        // Store which shard the action run lives on
        run.pending_action_shard_id = target_shard_id;

        // Set a synthetic signal type so handleSignal can also resume this run
        const sig_type = std.fmt.allocPrint(self.allocator, "_action_done:{s}", .{action_run_id}) catch null;
        if (run.wait_signal_type_owned) |old| self.allocator.free(old);
        run.wait_signal_type_owned = sig_type;

        self.addHistoryEvent(run, "awaiting_action", action_name, now_ms);
    }

    /// Decision returned by applyPollPolicy for a step outcome.
    const PollDecision = union(enum) {
        /// A poll was armed; the run is parked. Caller should return immediately.
        parked,
        /// Caller should follow the transition for this (effective) outcome.
        proceed: []const u8,
    };

    /// Apply `poll:` semantics to a step outcome. When an action returns the
    /// `pending` business outcome and the step has a `poll:` block, the run is
    /// parked for a backoff delay and re-invoked later (up to `maxAttempts`).
    /// On exhaustion the effective outcome becomes `timeout` so the caller follows
    /// the documented `timeout:` transition. Non-pending outcomes pass through.
    fn applyPollPolicy(self: *WorkflowHandler, run: *RunRecord, run_step: definition.RunStep, outcome: []const u8, step_label: []const u8, now_ms: i64) PollDecision {
        if (!std.mem.eql(u8, outcome, definition.StepOutcome.pending)) return .{ .proceed = outcome };
        const poll_cfg = run_step.poll orelse return .{ .proceed = outcome };

        if (run.poll_attempt < poll_cfg.max_attempts) {
            const delay: i64 = if (run.poll_attempt == 0)
                poll_cfg.initial_delay_ms
            else
                @intCast(poll_cfg.calculateDelay(run.poll_attempt - 1));
            run.poll_attempt += 1;
            run.poll_next_at_ms = now_ms + delay;
            run.status = .waiting;
            self.addHistoryEvent(run, "poll_scheduled", step_label, now_ms);
            return .parked;
        }

        // Max attempts exceeded — fall through to the `timeout` transition.
        run.poll_attempt = 0;
        run.poll_next_at_ms = 0;
        self.addHistoryEvent(run, "poll_exhausted", step_label, now_ms);
        return .{ .proceed = definition.StepOutcome.timeout };
    }

    /// Check all waiting runs for completed async actions and timed-out signals.
    /// Called periodically by the shard's task scheduler.
    pub fn checkPendingActions(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now_ms: i64 = @import("stdx").time.milliTimestamp();

        // Collect keys of runs that need resuming (can't modify map while iterating)
        var resume_keys: [64][]const u8 = undefined;
        var timeout_keys: [64][]const u8 = undefined;
        var child_keys: [64][]const u8 = undefined;
        var poll_keys: [64][]const u8 = undefined;
        var resume_count: usize = 0;
        var timeout_count: usize = 0;
        var child_count: usize = 0;
        var poll_count: usize = 0;

        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (run.status != .waiting) continue;

            // Check due poll timers (a `poll:` step awaiting backoff re-invocation)
            if (run.poll_next_at_ms > 0 and now_ms >= run.poll_next_at_ms) {
                if (poll_count < poll_keys.len) {
                    poll_keys[poll_count] = entry.key_ptr.*;
                    poll_count += 1;
                }
                continue; // a polling run has no pending action/child/signal to check
            }

            // Check async action completion
            if (run.pending_action_run_id_owned) |action_rid| {
                const done = if (getActionRunResult(shard, run.pending_action_shard_id, action_rid)) |result|
                    result.status == .completed or result.status == .failed
                else
                    pendingLost(shard, run, run.pending_action_shard_id);
                if (done and resume_count < resume_keys.len) {
                    resume_keys[resume_count] = entry.key_ptr.*;
                    resume_count += 1;
                }
            }

            // Check child workflow completion
            if (run.pending_child_run_id_owned) |child_rid| {
                if (self.childRunTerminal(shard, entry.key_ptr.*, child_rid, run)) {
                    if (child_count < child_keys.len) {
                        child_keys[child_count] = entry.key_ptr.*;
                        child_count += 1;
                    }
                }
            }

            // Check wait_for_signal timeouts
            if (run.wait_timeout_at_ms > 0 and now_ms >= run.wait_timeout_at_ms) {
                // Only timeout if not already handled as an action resume
                if (run.pending_action_run_id_owned == null) {
                    if (timeout_count < timeout_keys.len) {
                        timeout_keys[timeout_count] = entry.key_ptr.*;
                        timeout_count += 1;
                    }
                }
            }
        }

        // Resume action-completed runs
        for (resume_keys[0..resume_count]) |ns_key| {
            self.resumeFromAction(shard, ns_key, now_ms);
        }

        // Resume child-workflow-completed runs
        for (child_keys[0..child_count]) |ns_key| {
            self.resumeFromChild(shard, ns_key, now_ms);
        }

        // Fire due poll timers — re-invoke the current step's action.
        for (poll_keys[0..poll_count]) |ns_key| {
            if (self.runs.getPtr(ns_key)) |r| {
                r.poll_next_at_ms = 0;
                r.status = .running;
                if (r.wait_signal_type_owned) |s| self.allocator.free(s);
                r.wait_signal_type_owned = null;
            }
            const ns_end = std.mem.indexOfScalar(u8, ns_key, ':') orelse continue;
            self.advanceWorkflow(shard, ns_key, ns_key[0..ns_end]);
        }

        // Handle signal timeouts
        for (timeout_keys[0..timeout_count]) |ns_key| {
            self.handleWaitTimeout(shard, ns_key, now_ms);
        }
    }

    /// Whether the child run referenced by a parent is in a terminal state.
    /// `parent_ns_key` is "namespace:parent_run_id"; the child shares the namespace.
    fn childRunTerminal(self: *WorkflowHandler, shard: *Shard, parent_ns_key: []const u8, child_run_id: []const u8, parent: *const RunRecord) bool {
        const child_shard_id = parent.pending_child_shard_id;
        const ns_end = std.mem.indexOfScalar(u8, parent_ns_key, ':') orelse return false;
        const namespace = parent_ns_key[0..ns_end];
        const child_ns_key = self.makeNsKey(namespace, child_run_id) orelse return false;
        defer self.allocator.free(child_ns_key);

        const tid = child_shard_id orelse shard.id;
        if (tid == shard.id) {
            const child = self.runs.getPtr(child_ns_key) orelse return pendingLost(shard, parent, child_shard_id);
            return child.status.isTerminal();
        }
        const peers = shard.peer_shards orelse return false;
        if (tid >= peers.len) return false;
        const handler = peers[tid].workflow_handler;
        handler.mu.lock();
        defer handler.mu.unlock();
        const child = handler.runs.getPtr(child_ns_key) orelse return false;
        return child.status.isTerminal();
    }

    /// Resume a parent workflow run after its child workflow reached a terminal
    /// state. Child `completed` → parent step `success`; any other terminal → `failure`.
    fn resumeFromChild(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, now_ms: i64) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        const child_rid = run.pending_child_run_id_owned orelse return;
        const step_label = run.pending_step_name_owned orelse "unknown";

        const ns_end = std.mem.indexOfScalar(u8, run_ns_key, ':') orelse return;
        const namespace = run_ns_key[0..ns_end];

        const child_ns_key = self.makeNsKey(namespace, child_rid) orelse return;
        defer self.allocator.free(child_ns_key);

        // Read the child's terminal status + a private copy of its output.
        const tid = run.pending_child_shard_id orelse shard.id;
        var child_status: RunStatus = undefined;
        var child_output: ?[]u8 = null;
        var lost = false;
        if (tid == shard.id) {
            if (self.runs.getPtr(child_ns_key)) |child| {
                if (!child.status.isTerminal()) return;
                child_status = child.status;
                if (child.output_owned) |o| child_output = self.allocator.dupe(u8, o) catch null;
            } else {
                // Its start's index applied without creating it: the log
                // dropped it, and the step fails.
                if (!pendingLost(shard, run, run.pending_child_shard_id)) return;
                log.warn("workflow run {s}: the child run {s} its step waits for was never created (its start was dropped); the step fails", .{ run_ns_key, child_rid });
                lost = true;
                child_status = .failed;
            }
        } else {
            const peers = shard.peer_shards orelse return;
            if (tid >= peers.len) return;
            const handler = peers[tid].workflow_handler;
            handler.mu.lock();
            if (handler.runs.getPtr(child_ns_key)) |child| {
                if (!child.status.isTerminal()) {
                    handler.mu.unlock();
                    return;
                }
                child_status = child.status;
                if (child.output_owned) |o| child_output = self.allocator.dupe(u8, o) catch null;
            } else {
                handler.mu.unlock();
                return;
            }
            handler.mu.unlock();
        }
        defer if (child_output) |o| self.allocator.free(o);

        const outcome: []const u8 = if (lost)
            definition.StepOutcome.execution_failure
        else if (child_status == .completed)
            definition.StepOutcome.success
        else
            definition.StepOutcome.failure;

        if (!lost) {
            if (run.step_outputs) |*so| {
                so.put(self.allocator, step_label, child_output orelse "{}", outcome) catch {};
            }
        }

        run.status = .running;
        self.addHistoryEvent(run, if (lost) "child_lost" else "child_completed", outcome, now_ms);
        self.addHistoryEvent(run, "step_completed", step_label, now_ms);
        if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();

        // Clear pending child state.
        if (run.pending_child_run_id_owned) |a| self.allocator.free(a);
        run.pending_child_run_id_owned = null;
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        run.pending_step_name_owned = null;
        run.pending_child_shard_id = null;
        run.pending_index = 0;
        if (run.wait_signal_type_owned) |s| self.allocator.free(s);
        run.wait_signal_type_owned = null;

        // Follow the transition for the parent's current step.
        const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return;
        var def = parser.parseWorkflow(self.allocator, def_record.yaml_owned) catch return;
        defer def.deinit(self.allocator);

        const step: definition.Step = if (run.current_step_name_owned) |sn|
            def.getStep(sn) orelse return
        else
            def.start;

        switch (step) {
            .run => |run_step| {
                const transition = run_step.resolveTransition(outcome) orelse {
                    self.completeRun(shard, run_ns_key, run, .failed, "no transition for outcome", now_ms);
                    return;
                };
                if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                    self.completeRun(shard, run_ns_key, run, status, transition.target, now_ms);
                    return;
                }
                self.setCurrentStep(run, transition.target);
                const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                defer self.allocator.free(key_copy);
                self.advanceWorkflow(shard, key_copy, namespace);
            },
            else => {},
        }
    }

    /// Resume a workflow run after its async action completed.
    fn resumeFromAction(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, now_ms: i64) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        const action_rid = run.pending_action_run_id_owned orelse return;
        const step_label = run.pending_step_name_owned orelse "unknown";

        // No run yet: wait, unless this shard has applied the invoke's index
        // without creating it. Then the log dropped it, and the step fails.
        const found = getActionRunResult(shard, run.pending_action_shard_id, action_rid);
        if (found == null) {
            if (!pendingLost(shard, run, run.pending_action_shard_id)) return;
            log.warn("workflow run {s}: the action run {s} its step waits for was never created (its invoke was dropped); the step fails", .{ run_ns_key, action_rid });
        }

        const outcome: []const u8 = if (found) |result| switch (result.status) {
            .completed => blk: {
                // Use the named outcome from the action if provided, otherwise default to "success"
                const action_outcome = result.outcome orelse definition.StepOutcome.success;
                if (run.step_outputs) |*so| {
                    so.put(self.allocator, step_label, result.output orelse "{}", action_outcome) catch {};
                }
                break :blk action_outcome;
            },
            .failed => blk: {
                if (run.step_outputs) |*so| {
                    so.put(self.allocator, step_label, result.output orelse "{}", definition.StepOutcome.failure) catch {};
                }
                break :blk definition.StepOutcome.failure;
            },
            else => definition.StepOutcome.execution_failure,
        } else definition.StepOutcome.execution_failure;

        // Record completion before clearing state (step_label points into pending_step_name_owned)
        run.status = .running;
        self.addHistoryEvent(run, if (found == null) "action_lost" else "action_completed", outcome, now_ms);
        self.addHistoryEvent(run, "step_completed", step_label, now_ms);
        if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();

        // Clear pending action state
        if (run.pending_action_run_id_owned) |a| self.allocator.free(a);
        run.pending_action_run_id_owned = null;
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        run.pending_step_name_owned = null;
        run.pending_action_shard_id = null;
        run.pending_index = 0;
        if (run.wait_signal_type_owned) |s| self.allocator.free(s);
        run.wait_signal_type_owned = null;

        // Look up definition and resolve transition for the completed step
        // We need the namespace from the run_ns_key ("namespace:run_id")
        const ns_end = std.mem.indexOfScalar(u8, run_ns_key, ':') orelse return;
        const namespace = run_ns_key[0..ns_end];

        const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return;

        var def = parser.parseWorkflow(self.allocator, def_record.yaml_owned) catch return;
        defer def.deinit(self.allocator);

        const step: definition.Step = if (run.current_step_name_owned) |sn|
            def.getStep(sn) orelse return
        else
            def.start;

        switch (step) {
            .run => |run_step| {
                // Polling: a `pending` async outcome on a `poll:` step re-arms a
                // backoff poll instead of transitioning. On exhaustion → `timeout`.
                const poll_label = run.current_step_name_owned orelse "start";
                const eff_outcome = switch (self.applyPollPolicy(run, run_step, outcome, poll_label, now_ms)) {
                    .parked => return,
                    .proceed => |o| o,
                };

                // Record health for health-weighted plan executors (on async completion)
                if (run_step.isPlan()) {
                    if (def.getPlan(run_step.targetName())) |plan| {
                        if (plan.selection == .health_weighted and run.plan_executor_natural_idx < plan.executors.len) {
                            const attempted_executor = plan.executors[run.plan_executor_natural_idx];
                            const is_success = std.mem.eql(u8, outcome, definition.StepOutcome.success);
                            self.recordPlanHealth(namespace, run.workflow_name_owned, run_step.targetName(), attempted_executor, is_success, now_ms);
                        }
                    }
                }

                // Per-step retry for non-plan steps (mirrors advanceWorkflow sync retry logic).
                // Re-invokes the same step by calling advanceWorkflow, which sees the unchanged
                // current_step_name_owned and invokes the action again.
                if (!run_step.isPlan() and
                    (std.mem.eql(u8, outcome, definition.StepOutcome.failure) or
                        std.mem.eql(u8, outcome, definition.StepOutcome.execution_failure)))
                {
                    if (run_step.retry) |retry_cfg| {
                        if (run.retry_count < retry_cfg.max_attempts) {
                            run.retry_count += 1;
                            const retry_label = run.current_step_name_owned orelse "start";
                            self.addHistoryEvent(run, "step_retry", retry_label, now_ms);
                            const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                            defer self.allocator.free(key_copy);
                            self.advanceWorkflow(shard, key_copy, namespace);
                            return;
                        }
                    }
                }

                // Plan executor fallback: if an async action failed and this is a plan step,
                // try the next executor before following the failure transition.
                if (run_step.isPlan() and (std.mem.eql(u8, outcome, definition.StepOutcome.failure) or
                    std.mem.eql(u8, outcome, definition.StepOutcome.execution_failure)))
                {
                    if (def.getPlan(run_step.targetName())) |plan| {
                        // Use natural index — in health-weighted mode plan_executor_idx
                        // is the position in sorted order, not the index into plan.executors[].
                        const exec_count: u8 = @intCast(@min(plan.executors.len, 32));

                        // Check per-executor retry first
                        if (run.plan_executor_idx < exec_count and
                            run.plan_executor_natural_idx < plan.executors.len)
                        {
                            const executor = plan.executors[run.plan_executor_natural_idx];
                            if (executor.retry) |retry| {
                                if (run.plan_executor_retry_count < retry.max_attempts) {
                                    run.plan_executor_retry_count += 1;
                                    self.addHistoryEvent(run, "plan_executor_retry", executor.name, now_ms);
                                    const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                                    defer self.allocator.free(key_copy);
                                    self.advanceWorkflow(shard, key_copy, namespace);
                                    return;
                                }
                            }

                            // Executor exhausted — advance to next
                            self.addHistoryEvent(run, "plan_executor_exhausted", executor.name, now_ms);
                            run.plan_executor_idx += 1;
                            run.plan_executor_retry_count = 0;

                            if (run.plan_executor_idx < exec_count) {
                                // More executors available — re-enter advance loop
                                const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                                defer self.allocator.free(key_copy);
                                self.advanceWorkflow(shard, key_copy, namespace);
                                return;
                            }
                        }
                        // All executors exhausted — reset and fall through to failure transition
                        run.plan_executor_idx = 0;
                        run.plan_executor_natural_idx = 0;
                        run.plan_executor_retry_count = 0;
                    }
                }

                const transition = run_step.resolveTransition(eff_outcome) orelse {
                    self.completeRun(shard, run_ns_key, run, .failed, "no transition for outcome", now_ms);
                    return;
                };
                if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                    self.completeRun(shard, run_ns_key, run, status, transition.target, now_ms);
                    return;
                }
                self.setCurrentStep(run, transition.target);
                // Continue advancing — pass a durable copy of ns_key
                const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                defer self.allocator.free(key_copy);
                self.advanceWorkflow(shard, key_copy, namespace);
            },
            else => {},
        }
    }

    /// Handle a wait_for_signal timeout — follow the timeout transition.
    fn handleWaitTimeout(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, now_ms: i64) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;

        if (run.wait_timeout_target_owned) |target| {
            // Clear wait state
            if (run.wait_signal_type_owned) |s| self.allocator.free(s);
            run.wait_signal_type_owned = null;
            run.wait_timeout_at_ms = 0;

            self.addHistoryEvent(run, "signal_timeout", target, now_ms);

            // Parse definition to resolve custom terminals
            const ns_end = std.mem.indexOfScalar(u8, run_ns_key, ':') orelse return;
            const namespace = run_ns_key[0..ns_end];

            const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
            defer self.allocator.free(def_ns_key);
            const def_record = self.definitions.get(def_ns_key) orelse return;

            var def = parser.parseWorkflow(self.allocator, def_record.yaml_owned) catch return;
            defer def.deinit(self.allocator);

            // Check if timeout target is a terminal (builtin or custom)
            if (resolveTerminalStatus(target, def.terminals)) |status| {
                // Free before completing since completeRun doesn't touch these fields
                run.status = .running; // transition to running briefly
                self.allocator.free(target);
                run.wait_timeout_target_owned = null;
                self.completeRun(shard, run_ns_key, run, status, "signal timeout", now_ms);
                return;
            }

            // Transition to the timeout target step
            run.status = .running;
            self.setCurrentStep(run, target);
            self.allocator.free(target);
            run.wait_timeout_target_owned = null;

            // Continue advancing
            const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
            defer self.allocator.free(key_copy);
            self.advanceWorkflow(shard, key_copy, namespace);
        } else {
            // No timeout target configured — just time out the run
            run.status = .timed_out;
            run.completed_at_ms = now_ms;
            if (run.wait_signal_type_owned) |s| self.allocator.free(s);
            run.wait_signal_type_owned = null;
            run.wait_timeout_at_ms = 0;
            self.addHistoryEvent(run, "workflow_timed_out", "signal timeout", now_ms);
        }
    }

    /// Map a terminal name (builtin or custom) to handler RunStatus.
    /// Checks builtin terminals first, then custom terminals from the definition.
    fn resolveTerminalStatus(name: []const u8, custom_terminals: []const definition.Terminal) ?RunStatus {
        // Check builtins first
        if (definition.BuiltinTerminal.statusFor(name)) |status| {
            return switch (status) {
                .completed => .completed,
                .failed => .failed,
                .cancelled => .cancelled,
                .timed_out => .timed_out,
                else => null,
            };
        }
        // Check custom terminals
        for (custom_terminals) |t| {
            if (std.mem.eql(u8, t.name, name)) {
                return switch (t.status) {
                    .completed => .completed,
                    .failed => .failed,
                    .cancelled => .cancelled,
                    .timed_out => .timed_out,
                    else => .failed,
                };
            }
        }
        return null;
    }

    /// Transition the run to a terminal status and persist to UAL.
    fn completeRun(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) void {
        run.status = status;
        run.completed_at_ms = now_ms;

        // Every terminal transition passes through here, so this is the one
        // place the outcome counters need to be recorded.
        if (shard.metrics_registry) |m| switch (status) {
            .completed => m.workflow.recordCompleted(),
            .failed => m.workflow.recordFailed(),
            .cancelled => m.workflow.recordCancelled(),
            .timed_out => m.workflow.recordTimedOut(),
            else => {},
        };

        // Resolve explicit output mapping from definition (if declared)
        if (status == .completed) {
            self.resolveWorkflowOutput(run, run_ns_key);
        }

        // Re-compute search tags now that $.steps.* and $.flo.* are available
        {
            const sep = std.mem.indexOfScalar(u8, run_ns_key, ':') orelse return;
            const namespace = run_ns_key[0..sep];
            const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
            defer self.allocator.free(def_ns_key);
            if (self.buildSearchTags(def_ns_key, run)) |tags| {
                if (run.search_tags_owned) |old| self.allocator.free(old);
                run.search_tags_owned = tags;
            }
        }

        const event_type = switch (status) {
            .completed => "workflow_completed",
            .failed => "workflow_failed",
            .cancelled => "workflow_cancelled",
            .timed_out => "workflow_timed_out",
            else => "workflow_ended",
        };
        self.addHistoryEvent(run, event_type, detail, now_ms);
        // completeRun mutates the run first because persistComplete
        // serializes it; the applier then rebuilds the same state from the
        // entry.
        self.persistComplete(shard, run_ns_key, run, status, now_ms);
    }

    /// Resolve the workflow's `output` mapping (same format as step inputMapping).
    /// Looks up the definition, parses the YAML, and if `output:` is declared,
    /// resolves the JSON mapping via PathResolver and stores it on the run.
    ///
    /// Two modes:
    ///  1. `output: "$.steps.ship.output"` → direct path passthrough (raw bytes).
    ///  2. `output: '{"key": "$.path"}'` → JSON mapping with interpolation.
    /// If `output` is not declared, workflow output remains null.
    fn resolveWorkflowOutput(self: *WorkflowHandler, run: *RunRecord, run_ns_key: []const u8) void {
        // Extract namespace from run_ns_key ("namespace:run_id")
        const sep = std.mem.indexOfScalar(u8, run_ns_key, ':') orelse return;
        const namespace = run_ns_key[0..sep];

        // Look up the workflow definition
        const def_ns_key = self.makeNsKey(namespace, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return;

        // Parse definition to access the output mapping
        var def = parser.parseWorkflow(self.allocator, def_record.yaml_owned) catch |err| {
            log.warn("Failed to parse workflow definition for output resolution: {}", .{err});
            return;
        };
        defer def.deinit(self.allocator);

        const output_expr = def.output orelse return;

        const trimmed = std.mem.trim(u8, output_expr, " \t");
        if (trimmed.len < 2) return;

        // Mode 1: Direct path passthrough ("$.steps.ship.output" or "$.input")
        if (trimmed[0] == '$' and trimmed[1] == '.') {
            var resolver = jsonpath.PathResolver.init(
                self.allocator,
                run.input_owned,
                if (run.step_outputs) |*so| so else null,
                run.run_id_owned,
            );
            const resolved = resolver.resolve(trimmed) catch |err| {
                log.warn("Workflow output path resolution failed for '{s}': {}", .{ trimmed, err });
                return;
            } orelse return;

            if (run.output_owned) |old| self.allocator.free(old);
            run.output_owned = resolved;
            return;
        }

        // Mode 2: JSON mapping — resolve all $.path references within the object
        const resolved = jsonpath.resolveInput(
            self.allocator,
            output_expr,
            run.input_owned,
            if (run.step_outputs) |*so| so else null,
            run.run_id_owned,
        ) catch |err| {
            log.warn("Workflow output mapping resolution failed: {}", .{err});
            return;
        };

        // Store on the run (free any previous output)
        if (run.output_owned) |old| self.allocator.free(old);
        run.output_owned = resolved;
    }

    /// Build pre-computed search attribute JSON from definition + run data.
    /// Returns an owned JSON string like `{"customer_id":"C-789","order_amount":149.99}`
    /// or null if the definition has no search attributes.
    fn buildSearchTags(self: *WorkflowHandler, def_ns_key: []const u8, run: *RunRecord) ?[]const u8 {
        const def_rec = self.definitions.get(def_ns_key) orelse return null;
        var def = parser.parseWorkflow(self.allocator, def_rec.yaml_owned) catch return null;
        defer def.deinit(self.allocator);
        if (def.search_attributes.len == 0) return null;

        var resolver = jsonpath.PathResolver.init(
            self.allocator,
            run.input_owned,
            if (run.step_outputs) |*so| so else null,
            run.run_id_owned,
        );

        var aw: std.Io.Writer.Allocating = .init(self.allocator);
        const w: *std.Io.Writer = &aw.writer;
        w.writeByte('{') catch {
            aw.deinit();
            return null;
        };

        var first = true;
        for (def.search_attributes) |attr| {
            const resolved = resolver.resolve(attr.from) catch null;
            defer if (resolved) |r| self.allocator.free(r);

            if (!first) w.writeByte(',') catch continue;
            w.writeByte('"') catch continue;
            writeJsonEscaped(w, attr.name) catch continue;
            w.writeAll("\":") catch continue;

            if (resolved) |val| {
                // val is already JSON-formatted (strings quoted, numbers raw)
                w.writeAll(val) catch continue;
            } else {
                w.writeAll("null") catch continue;
            }
            first = false;
        }

        w.writeByte('}') catch {
            aw.deinit();
            return null;
        };
        return aw.toOwnedSlice() catch {
            aw.deinit();
            return null;
        };
    }

    /// Update the run's current step pointer.
    fn setCurrentStep(self: *WorkflowHandler, run: *RunRecord, step_name: []const u8) void {
        if (run.current_step_name_owned) |old| self.allocator.free(old);
        run.current_step_name_owned = self.allocator.dupe(u8, step_name) catch null;
        // Reset plan state on step transition
        run.plan_executor_idx = 0;
        run.plan_executor_natural_idx = 0;
        run.plan_executor_retry_count = 0;
        // Reset poll state — a new step starts its own poll budget
        run.poll_attempt = 0;
        run.poll_next_at_ms = 0;
    }

    // ── Stream Trigger Polling ─────────────────────────────────────────

    /// True if any stream trigger is registered on this node (across all shards).
    /// Read by the stream-append path to decide whether to push-wake pollers, so
    /// streams that nobody watches incur zero cross-shard notification overhead.
    pub fn anyStreamTriggers() bool {
        return global_stream_trigger_count.load(.monotonic) > 0;
    }

    /// Register a stream trigger for a workflow definition.
    /// Called from handleCreate when the parsed definition has a `trigger:` block.
    fn registerStreamTrigger(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow_name: []const u8,
        trigger: definition.StreamTriggerDef,
    ) void {
        const trigger_key = self.makeNsKey(namespace, workflow_name) orelse return;

        // A re-created definition keeps its place in the stream: starting
        // over at zero would run every consumed event again. A trigger moved
        // to another stream starts that stream from its beginning.
        var cursor_ts: u64 = 0;
        var cursor_seq: u64 = 0;
        const stream_ns = trigger.namespace orelse namespace;
        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            if (std.mem.eql(u8, state.stream_name_owned, trigger.stream) and std.mem.eql(u8, state.stream_namespace_owned, stream_ns)) {
                cursor_ts = state.stream_cursor_ts;
                cursor_seq = state.stream_cursor_seq;
            }
            self.freeTriggerState(&state);
            _ = global_stream_trigger_count.fetchSub(1, .monotonic);
        }

        const owned_wf = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(trigger_key);
            return;
        };
        const owned_ns = self.allocator.dupe(u8, namespace) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            return;
        };
        const owned_stream = self.allocator.dupe(u8, trigger.stream) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ns);
            return;
        };
        const owned_stream_ns = self.allocator.dupe(u8, stream_ns) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ns);
            self.allocator.free(owned_stream);
            return;
        };

        self.stream_triggers.put(trigger_key, .{
            .workflow_name_owned = owned_wf,
            .namespace_owned = owned_ns,
            .stream_name_owned = owned_stream,
            .stream_namespace_owned = owned_stream_ns,
            .batch_size = trigger.batch_size,
            .poll_interval_ms = trigger.batch_timeout_ms,
            .stream_cursor_ts = cursor_ts,
            .stream_cursor_seq = cursor_seq,
            .last_poll_ms = 0,
        }) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ns);
            self.allocator.free(owned_stream);
            self.allocator.free(owned_stream_ns);
            return;
        };
        _ = global_stream_trigger_count.fetchAdd(1, .monotonic);
    }

    /// Remove the stream trigger for a workflow definition.
    fn unregisterStreamTrigger(self: *WorkflowHandler, namespace: []const u8, workflow_name: []const u8) void {
        const trigger_key = self.makeNsKey(namespace, workflow_name) orelse return;
        defer self.allocator.free(trigger_key);

        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeTriggerState(&state);
            _ = global_stream_trigger_count.fetchSub(1, .monotonic);
        }
    }

    // ── Schedule Registration ───────────────────────────────────────────

    fn registerSchedule(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow_name: []const u8,
        schedule: definition.ScheduleDef,
    ) void {
        // Only interval-based schedules are supported for now
        const interval_ms = schedule.interval_ms orelse return;
        if (interval_ms <= 0) return;
        if (schedule.paused) return;

        const sched_key = self.makeNsKey(namespace, workflow_name) orelse return;

        // Remove old schedule if re-creating workflow
        if (self.schedules.fetchRemove(sched_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeScheduleState(&state);
        }

        const owned_wf = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(sched_key);
            return;
        };
        const owned_ns = self.allocator.dupe(u8, namespace) catch {
            self.allocator.free(sched_key);
            self.allocator.free(owned_wf);
            return;
        };
        const owned_input: ?[]const u8 = if (schedule.input) |inp|
            self.allocator.dupe(u8, inp) catch null
        else
            null;

        self.schedules.put(sched_key, .{
            .workflow_name_owned = owned_wf,
            .namespace_owned = owned_ns,
            .interval_ms = interval_ms,
            .max_concurrent = schedule.max_concurrent,
            .input_owned = owned_input,
            .last_trigger_ms = 0,
        }) catch {
            self.allocator.free(sched_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ns);
            if (owned_input) |inp| self.allocator.free(inp);
        };
    }

    fn unregisterSchedule(self: *WorkflowHandler, namespace: []const u8, workflow_name: []const u8) void {
        const sched_key = self.makeNsKey(namespace, workflow_name) orelse return;
        defer self.allocator.free(sched_key);

        if (self.schedules.fetchRemove(sched_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeScheduleState(&state);
        }
    }

    /// Check all active schedules and start runs when the interval has elapsed.
    /// Called from shard.tick() on every reactor cycle.
    pub fn tickSchedules(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now_ms: i64 = @import("stdx").time.milliTimestamp();

        var it = self.schedules.iterator();
        while (it.next()) |entry| {
            const schedule = entry.value_ptr;

            // Enforce interval
            if (now_ms - schedule.last_trigger_ms < schedule.interval_ms) continue;

            // Skip disabled workflows
            if (self.disabled.contains(entry.key_ptr.*)) continue;

            // Check max_concurrent: count active (non-terminal) runs for this workflow
            if (schedule.max_concurrent > 0) {
                var active_count: u32 = 0;
                var rit = self.runs.iterator();
                while (rit.next()) |rentry| {
                    const run = rentry.value_ptr;
                    if (!run.status.isTerminal() and
                        std.mem.eql(u8, run.workflow_name_owned, schedule.workflow_name_owned))
                    {
                        active_count += 1;
                    }
                }
                if (active_count >= schedule.max_concurrent) continue;
            }

            schedule.last_trigger_ms = now_ms;
            self.startRunFromSchedule(shard, schedule);
        }
    }

    /// Start a workflow run from a schedule trigger.
    fn startRunFromSchedule(
        self: *WorkflowHandler,
        shard: *Shard,
        schedule: *const ScheduleState,
    ) void {
        const input = schedule.input_owned orelse "{}";

        // Generate run ID with embedded partition bits
        var run_id_buf: [32]u8 = undefined;
        const partition_id = shard.router.keyToPartitionNs(schedule.namespace_owned, schedule.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, partition_id, &run_id_buf) catch return;

        _ = self.proposeRun(shard, schedule.namespace_owned, run_id_str, schedule.workflow_name_owned, "latest", input, null, "schedule_started");
    }

    /// Poll all active stream triggers and start workflow runs for new events.
    /// Called from shard.tick() on every reactor cycle.
    pub fn tickStreamTriggers(self: *WorkflowHandler, shard: *Shard) void {
        const now_ms = @import("stdx").time.milliTimestamp();

        // A `stream_event` notification (push-wake) forces an immediate poll of
        // every trigger this tick, bypassing each trigger's poll-interval timer.
        // The timer remains as a fallback for events that arrive without a
        // notification (e.g. appended before the trigger registered).
        const forced = self.triggers_dirty;
        self.triggers_dirty = false;

        var it = self.stream_triggers.iterator();
        while (it.next()) |entry| {
            const trigger = entry.value_ptr;

            // Enforce poll interval unless a push-wake forced this poll.
            if (!forced) {
                const poll_ms: i64 = @intCast(trigger.poll_interval_ms);
                if (now_ms - trigger.last_poll_ms < poll_ms) continue;
            }
            trigger.last_poll_ms = now_ms;

            // Skip disabled workflows
            if (self.disabled.contains(entry.key_ptr.*)) continue;

            // Read from the source stream
            const batch_limit: usize = @intCast(trigger.batch_size);
            const cursor = StreamID{ .timestamp_ms = trigger.stream_cursor_ts, .sequence = trigger.stream_cursor_seq };
            // Read from the shard that owns the stream's data, not the local
            // shard (which is just where this workflow definition was created).
            const stream_handler = resolveStreamHandler(shard, trigger.stream_namespace_owned, trigger.stream_name_owned);
            const result = stream_handler.readPayloadsForStream(
                trigger.stream_name_owned,
                trigger.stream_namespace_owned,
                cursor,
                @max(batch_limit, 1) * 10, // read ahead for batching
            );
            if (result.payloads.len == 0) continue;
            defer stream_handler.allocator.free(result.payloads);
            defer stream_handler.allocator.free(result.ids);

            // Start one run per event (or per batch if batch_size > 1)
            if (trigger.batch_size <= 1) {
                // One run per event
                for (result.payloads, result.ids) |payload, event_id| {
                    self.startRunFromTrigger(shard, trigger, payload, event_id);
                }
            } else {
                // Batch mode: collect batch_size events into a JSON array per run
                var i: usize = 0;
                while (i < result.payloads.len) {
                    const end = @min(i + batch_limit, result.payloads.len);
                    const batch = result.payloads[i..end];
                    const batch_json = self.buildBatchJson(batch) orelse {
                        i = end;
                        continue;
                    };
                    defer self.allocator.free(batch_json);
                    self.startRunFromTrigger(shard, trigger, batch_json, result.ids[end - 1]);
                    i = end;
                }
            }

            // Advance cursor to last StreamID read
            trigger.stream_cursor_ts = result.last_id.timestamp_ms;
            trigger.stream_cursor_seq = result.last_id.sequence;
        }
    }

    /// Build a JSON array from a batch of payloads: ["payload1","payload2",...]
    fn buildBatchJson(self: *WorkflowHandler, payloads: []const []const u8) ?[]u8 {
        // Estimate size: 2 (brackets) + payloads * (payload.len + 3 for quotes + comma)
        var total: usize = 2;
        for (payloads) |p| total += p.len + 3;
        var buf = self.allocator.alloc(u8, total) catch return null;
        var pos: usize = 0;
        buf[pos] = '[';
        pos += 1;
        for (payloads, 0..) |p, idx| {
            if (idx > 0) {
                buf[pos] = ',';
                pos += 1;
            }
            // If the payload already looks like JSON, embed it directly
            if (p.len > 0 and (p[0] == '{' or p[0] == '[' or p[0] == '"')) {
                @memcpy(buf[pos .. pos + p.len], p);
                pos += p.len;
            } else {
                // Wrap as string
                buf[pos] = '"';
                pos += 1;
                @memcpy(buf[pos .. pos + p.len], p);
                pos += p.len;
                buf[pos] = '"';
                pos += 1;
            }
        }
        buf[pos] = ']';
        pos += 1;
        // Shrink to actual size
        if (pos < buf.len) {
            const result = self.allocator.realloc(buf, pos) catch return buf[0..pos];
            return result;
        }
        return buf;
    }

    /// Start a workflow run programmatically from a stream trigger event.
    /// A trigger-started run records the event it came from as its
    /// idempotency key ("trigger:<stream>@<ts>:<seq>", the last event of a
    /// batch). The applier reads it back to restore the trigger's cursor at
    /// boot, so a restart neither re-runs consumed events nor skips
    /// unconsumed ones.
    fn startRunFromTrigger(
        self: *WorkflowHandler,
        shard: *Shard,
        trigger: *const StreamTriggerState,
        input: []const u8,
        event_id: StreamID,
    ) void {
        // Generate run ID with embedded partition bits
        var run_id_buf: [32]u8 = undefined;
        const partition_id = shard.router.keyToPartitionNs(trigger.namespace_owned, trigger.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, partition_id, &run_id_buf) catch return;

        var idem_buf: [512]u8 = undefined;
        const idem = std.fmt.bufPrint(&idem_buf, "trigger:{s}@{d}:{d}", .{ trigger.stream_name_owned, event_id.timestamp_ms, event_id.sequence }) catch return;

        // The first-step queue is shared with a parent run on another
        // shard starting a child here, which holds this handler's lock.
        self.mu.lock();
        defer self.mu.unlock();
        _ = self.proposeRun(shard, trigger.namespace_owned, run_id_str, trigger.workflow_name_owned, "latest", input, idem, "trigger_started");
    }

    /// Advance a stream trigger's cursor to the event a restored run came
    /// from. Runs apply in log order after their definition, so by the end
    /// of replay the cursor sits at the last event that started a run.
    fn restoreTriggerCursor(self: *WorkflowHandler, namespace: []const u8, wf_name: []const u8, idem: []const u8) void {
        const at = std.mem.lastIndexOfScalar(u8, idem, '@') orelse return;
        const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
        if (colon <= at) return;
        const ts = std.fmt.parseInt(u64, idem[at + 1 .. colon], 10) catch return;
        const seq = std.fmt.parseInt(u64, idem[colon + 1 ..], 10) catch return;
        const key = self.makeNsKey(namespace, wf_name) orelse return;
        defer self.allocator.free(key);
        const trigger = self.stream_triggers.getPtr(key) orelse return;
        const event = StreamID{ .timestamp_ms = ts, .sequence = seq };
        const cursor = StreamID{ .timestamp_ms = trigger.stream_cursor_ts, .sequence = trigger.stream_cursor_seq };
        if (event.greaterThan(cursor)) {
            trigger.stream_cursor_ts = ts;
            trigger.stream_cursor_seq = seq;
        }
    }

    // ── Plan Health Helpers ─────────────────────────────────────────────

    /// Get or create the health record for a plan executor.
    /// Returns null on allocation failure; callers must handle gracefully.
    fn getOrCreateHealth(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow: []const u8,
        plan_name: []const u8,
        executor_name: []const u8,
    ) ?*plan_types.ExecutorHealth {
        const key = std.fmt.allocPrint(self.allocator, "{s}:{s}:{s}:{s}", .{ namespace, workflow, plan_name, executor_name }) catch return null;

        if (self.plan_health.getPtr(key)) |health| {
            self.allocator.free(key);
            return health;
        }

        const owned_name = self.allocator.dupe(u8, executor_name) catch {
            self.allocator.free(key);
            return null;
        };

        self.plan_health.put(key, plan_types.ExecutorHealth.initWithName(owned_name)) catch {
            self.allocator.free(key);
            self.allocator.free(owned_name);
            return null;
        };
        return self.plan_health.getPtr(key);
    }

    /// Record an API attempt outcome against a plan executor's health state.
    /// Updates circuit breaker state if the executor has breaker config.
    fn recordPlanHealth(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow: []const u8,
        plan_name: []const u8,
        executor: definition.ExecutorConfig,
        success: bool,
        now_ms: i64,
    ) void {
        const health = self.getOrCreateHealth(namespace, workflow, plan_name, executor.name) orelse return;
        health.recordApiAttempt(success, 0, now_ms);
        if (executor.breaker) |breaker_cfg| {
            health.updateBreakerState(breaker_cfg, success, now_ms);
        }
    }

    /// Sort executor indices by health score (descending). Used by health-weighted
    /// selection to try the healthiest executor first.
    fn sortExecutorsByHealth(
        self: *WorkflowHandler,
        order: []u8,
        plan: definition.InlinePlan,
        namespace: []const u8,
        workflow: []const u8,
    ) void {
        // Insertion sort — max 32 executors, O(n²) is fine
        var i: usize = 1;
        while (i < order.len) : (i += 1) {
            var j = i;
            while (j > 0) {
                const a_score = self.getExecutorHealthScore(namespace, workflow, plan.name, plan.executors[order[j]].name);
                const b_score = self.getExecutorHealthScore(namespace, workflow, plan.name, plan.executors[order[j - 1]].name);
                if (a_score > b_score) {
                    const tmp = order[j];
                    order[j] = order[j - 1];
                    order[j - 1] = tmp;
                    j -= 1;
                } else break;
            }
        }
    }

    /// Return the health score for a plan executor (0.0–1.0).
    /// Returns 1.0 if no health data exists (assume healthy).
    fn getExecutorHealthScore(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow: []const u8,
        plan_name: []const u8,
        executor_name: []const u8,
    ) f64 {
        const key = std.fmt.allocPrint(self.allocator, "{s}:{s}:{s}:{s}", .{ namespace, workflow, plan_name, executor_name }) catch return 1.0;
        defer self.allocator.free(key);
        if (self.plan_health.get(key)) |health| {
            return health.healthScore();
        }
        return 1.0;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// Build a namespace-qualified key: "namespace:name" for map lookups.
    fn makeNsKey(self: *WorkflowHandler, namespace: []const u8, name: []const u8) ?[]const u8 {
        return std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ namespace, name }) catch null;
    }

    fn addHistoryEvent(self: *WorkflowHandler, run: *RunRecord, event_type: []const u8, detail: []const u8, timestamp_ms: i64) void {
        const owned_type = self.allocator.dupe(u8, event_type) catch return;
        const owned_detail = self.allocator.dupe(u8, detail) catch {
            self.allocator.free(owned_type);
            return;
        };
        run.history.append(self.allocator, .{
            .event_type_owned = owned_type,
            .detail_owned = owned_detail,
            .timestamp_ms = timestamp_ms,
        }) catch {
            self.allocator.free(owned_type);
            self.allocator.free(owned_detail);
        };
    }

    // ── UAL Persistence ────────────────────────────────────────────────

    /// Persist a workflow_create entry to the UAL so the definition survives restart.
    /// The key stored is "namespace:name" so replay can directly use it as the ns-qualified map key.
    fn proposeCreate(self: *WorkflowHandler, shard: *Shard, namespace: []const u8, name: []const u8, yaml: []const u8) !persistence_mod.ProposeResult {
        _ = self;
        // Build ns-qualified key: "namespace:name"
        var key_buf: [600]u8 = undefined;
        const ns_key = try std.fmt.bufPrint(&key_buf, "{s}:{s}", .{ namespace, name });
        return persistence_mod.proposeEntry(shard, .workflow_create, entry_mod.Flags.NONE, namespace, ns_key, yaml);
    }

    /// Start a run for a producer (a schedule, a trigger, a parent run):
    /// the start is proposed and the tick takes its first step once it
    /// applies. Returns the start's log index. The caller holds `mu`.
    fn proposeRun(
        self: *WorkflowHandler,
        shard: *Shard,
        namespace: []const u8,
        run_id: []const u8,
        wf_name: []const u8,
        version: []const u8,
        input: []const u8,
        idempotency_key: ?[]const u8,
        event_type: []const u8,
    ) ?u64 {
        const proposed = self.proposeStart(shard, namespace, run_id, wf_name, version, input, idempotency_key, event_type) catch |err| {
            log.err("workflow {s}: run {s} not started: {s}", .{ wf_name, run_id, @errorName(err) });
            return null;
        };
        var key_buf: [600]u8 = undefined;
        const ns_key = std.fmt.bufPrint(&key_buf, "{s}:{s}", .{ namespace, run_id }) catch return null;
        self.queueFirstStep(ns_key, proposed.index);
        return proposed.index;
    }

    /// Start entry. Key is "namespace:run_id".
    /// Value: [wf_name_len:u16][wf_name][ver_len:u16][ver][status:u8][created_at_ms:i64]
    ///   [event_len:u16][event_type][idem_len:u16][idempotency_key][input...]
    /// The event type is the run's first history event (manual, schedule,
    /// trigger or child start); search tags are derived by the applier.
    fn proposeStart(
        self: *WorkflowHandler,
        shard: *Shard,
        namespace: []const u8,
        run_id: []const u8,
        wf_name: []const u8,
        version: []const u8,
        input: []const u8,
        idempotency_key: ?[]const u8,
        event_type: []const u8,
    ) !persistence_mod.ProposeResult {
        _ = self;
        var ns_key_buf: [600]u8 = undefined;
        const ns_key = try std.fmt.bufPrint(&ns_key_buf, "{s}:{s}", .{ namespace, run_id });
        const idem = idempotency_key orelse "";
        const value_len = 2 + wf_name.len + 2 + version.len + 1 + 8 + 2 + event_type.len + 2 + idem.len + input.len;
        if (value_len > 65000) return error.PayloadTooLarge;
        var value_buf: [65536]u8 = undefined;
        var off: usize = 0;
        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(wf_name.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + wf_name.len], wf_name);
        off += wf_name.len;
        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(version.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + version.len], version);
        off += version.len;
        value_buf[off] = @intFromEnum(RunStatus.running);
        off += 1;
        std.mem.writeInt(i64, value_buf[off..][0..8], @import("stdx").time.milliTimestamp(), .little);
        off += 8;
        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(event_type.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + event_type.len], event_type);
        off += event_type.len;
        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(idem.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + idem.len], idem);
        off += idem.len;
        @memcpy(value_buf[off .. off + input.len], input);
        off += input.len;
        return persistence_mod.proposeEntry(shard, .workflow_start, entry_mod.Flags.NONE, namespace, ns_key, value_buf[0..off]);
    }

    /// Persist a workflow_complete entry to the UAL so terminal state survives restarts.
    /// Value format: [status:u8][completed_at_ms:i64]
    ///   [has_output:u8][output_len:u32][output]?
    ///   [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
    ///   [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
    ///   [tags_len:u16][search_tags]?
    fn persistComplete(
        self: *WorkflowHandler,
        shard: *Shard,
        ns_key: []const u8,
        run: *const RunRecord,
        status: RunStatus,
        completed_at_ms: i64,
    ) void {
        // Extract namespace from "namespace:run_id"
        const colon = std.mem.indexOfScalar(u8, ns_key, ':') orelse return;
        const namespace = ns_key[0..colon];

        // Calculate total size needed
        var total: usize = 9; // status + completed_at_ms
        // Output
        total += 1; // has_output flag
        if (run.output_owned) |out| {
            total += 4 + out.len; // output_len + output
        }
        // Step outputs
        total += 2; // step_count
        if (run.step_outputs) |so| {
            for (so.entries) |entry| {
                total += 2 + entry.step_name.len + 2 + entry.outcome.len + 4 + entry.output.len;
            }
        }
        // History events
        total += 2; // history_count
        for (run.history.items) |ev| {
            total += 2 + ev.event_type_owned.len + 2 + ev.detail_owned.len + 8;
        }
        // Search tags
        const tags = run.search_tags_owned orelse "";
        total += 2 + tags.len;

        const buf = self.allocator.alloc(u8, total) catch return;
        defer self.allocator.free(buf);
        var off: usize = 0;

        // [status:u8][completed_at_ms:i64]
        buf[off] = @intFromEnum(status);
        off += 1;
        std.mem.writeInt(i64, buf[off..][0..8], completed_at_ms, .little);
        off += 8;

        // [has_output:u8][output_len:u32][output]?
        if (run.output_owned) |out| {
            buf[off] = 1;
            off += 1;
            std.mem.writeInt(u32, buf[off..][0..4], @intCast(out.len), .little);
            off += 4;
            @memcpy(buf[off .. off + out.len], out);
            off += out.len;
        } else {
            buf[off] = 0;
            off += 1;
        }

        // [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
        const step_count: u16 = if (run.step_outputs) |so| @intCast(so.entries.len) else 0;
        std.mem.writeInt(u16, buf[off..][0..2], step_count, .little);
        off += 2;
        if (run.step_outputs) |so| {
            for (so.entries) |entry| {
                std.mem.writeInt(u16, buf[off..][0..2], @intCast(entry.step_name.len), .little);
                off += 2;
                @memcpy(buf[off .. off + entry.step_name.len], entry.step_name);
                off += entry.step_name.len;
                std.mem.writeInt(u16, buf[off..][0..2], @intCast(entry.outcome.len), .little);
                off += 2;
                @memcpy(buf[off .. off + entry.outcome.len], entry.outcome);
                off += entry.outcome.len;
                std.mem.writeInt(u32, buf[off..][0..4], @intCast(entry.output.len), .little);
                off += 4;
                @memcpy(buf[off .. off + entry.output.len], entry.output);
                off += entry.output.len;
            }
        }

        // [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
        const hist_count: u16 = @intCast(run.history.items.len);
        std.mem.writeInt(u16, buf[off..][0..2], hist_count, .little);
        off += 2;
        for (run.history.items) |ev| {
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(ev.event_type_owned.len), .little);
            off += 2;
            @memcpy(buf[off .. off + ev.event_type_owned.len], ev.event_type_owned);
            off += ev.event_type_owned.len;
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(ev.detail_owned.len), .little);
            off += 2;
            @memcpy(buf[off .. off + ev.detail_owned.len], ev.detail_owned);
            off += ev.detail_owned.len;
            std.mem.writeInt(i64, buf[off..][0..8], ev.timestamp_ms, .little);
            off += 8;
        }

        // [tags_len:u16][search_tags]?
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(tags.len), .little);
        off += 2;
        if (tags.len > 0) {
            @memcpy(buf[off .. off + tags.len], tags);
            off += tags.len;
        }

        _ = persistence_mod.proposeEntry(shard, .workflow_complete, entry_mod.Flags.NONE, namespace, ns_key, buf[0..off]) catch |err| {
            log.err("workflow run {s}: completion not persisted: {s}", .{ ns_key, @errorName(err) });
            return;
        };
    }

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *WorkflowHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.workflow_create, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_start, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_complete, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *WorkflowHandler = @ptrCast(@alignCast(ctx));
        self.replayEntry(entry);
    }

    /// Apply a workflow entry: live commit, replicated entry or boot replay alike.
    pub fn replayEntry(self: *WorkflowHandler, entry: *const entry_mod.Entry) void {
        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);
        // Other shard threads walk these maps under this lock. Nothing
        // holds it across an apply, so taking it here cannot nest.
        self.mu.lock();
        defer self.mu.unlock();
        if (etype == .workflow_start) {
            self.last_started_key = null;
            self.last_start_existed = false;
            self.last_start_collided = false;
        }
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;

        switch (etype) {
            .workflow_create => self.replayCreate(cmd.key, cmd.value, @intCast(entry.header.timestamp_ns / 1_000_000)),
            .workflow_start => self.replayStart(cmd.key, cmd.value),
            .workflow_complete => self.replayComplete(cmd.key, cmd.value),
            else => {},
        }
    }

    /// Apply a workflow_create entry. The key is "namespace:name" (ns-qualified).
    fn replayCreate(self: *WorkflowHandler, ns_key_raw: []const u8, yaml: []const u8, created_at_ms: i64) void {
        // "namespace:name"
        const sep = std.mem.indexOfScalar(u8, ns_key_raw, ':');
        const namespace = if (sep) |i| ns_key_raw[0..i] else "default";
        const raw_name = if (sep) |i| ns_key_raw[i + 1 ..] else ns_key_raw;

        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return;
        // Remove old definition if exists
        if (self.definitions.fetchRemove(ns_key)) |old| {
            self.allocator.free(old.key); // old ns-qualified key
            self.allocator.free(old.value.name_owned);
            self.allocator.free(old.value.version_owned);
            self.allocator.free(old.value.yaml_owned);
        }

        var def = parser.parseWorkflow(self.allocator, yaml) catch {
            self.allocator.free(ns_key);
            return;
        };
        defer def.deinit(self.allocator);

        const version = self.allocator.dupe(u8, def.version) catch {
            self.allocator.free(ns_key);
            return;
        };
        const owned_name = self.allocator.dupe(u8, raw_name) catch {
            self.allocator.free(ns_key);
            self.allocator.free(version);
            return;
        };
        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(version);
            return;
        };
        self.definitions.put(ns_key, .{
            .name_owned = owned_name,
            .version_owned = version,
            .yaml_owned = owned_yaml,
            .created_at_ms = created_at_ms,
            .idempotency = def.idempotency,
        }) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(version);
            self.allocator.free(owned_yaml);
            return;
        };

        // A definition's trigger and schedule are part of it: (re)register
        // them here so a restart keeps them and a redefinition without one
        // drops it.
        if (!self.install_producers) return;
        if (def.trigger) |trigger| {
            self.registerStreamTrigger(namespace, raw_name, trigger);
        } else {
            self.unregisterStreamTrigger(namespace, raw_name);
        }
        if (def.schedule) |schedule| {
            self.registerSchedule(namespace, raw_name, schedule);
        } else {
            self.unregisterSchedule(namespace, raw_name);
        }
    }

    /// Apply a workflow_start entry. The key is "namespace:run_id" (ns-qualified).
    fn replayStart(self: *WorkflowHandler, ns_key_raw: []const u8, value: []const u8) void {
        // "namespace:run_id"
        const sep = std.mem.indexOfScalar(u8, ns_key_raw, ':');
        const namespace = if (sep) |i| ns_key_raw[0..i] else "default";
        const raw_run_id = if (sep) |i| ns_key_raw[i + 1 ..] else ns_key_raw;

        var off: usize = 0;
        if (off + 2 > value.len) return;
        const wf_name_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + wf_name_len > value.len) return;
        const wf_name = value[off .. off + wf_name_len];
        off += wf_name_len;

        if (off + 2 > value.len) return;
        const ver_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + ver_len > value.len) return;
        const version = value[off .. off + ver_len];
        off += ver_len;

        if (off + 1 > value.len) return;
        const status: RunStatus = @enumFromInt(value[off]);
        off += 1;

        if (off + 8 > value.len) return;
        const created_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);
        off += 8;

        if (off + 2 > value.len) return;
        const evt_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + evt_len > value.len) return;
        const event_type = value[off .. off + evt_len];
        off += evt_len;

        if (off + 2 > value.len) return;
        const idem_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + idem_len > value.len) return;
        const idem: ?[]const u8 = if (idem_len > 0) value[off .. off + idem_len] else null;
        off += idem_len;

        const input = if (off < value.len) value[off..] else "{}";

        // Two starts with one idempotency key can both pass the handler's
        // check before either applies; the log's order decides. The later
        // one creates nothing, and its client is answered with the earlier
        // run. Checked before the run id, as the handler does: a retry
        // carrying both its key and its own run id is the same start.
        if (idem) |k| {
            if (self.runKeyForIdempotencyKey(namespace, wf_name, k)) |run_key| {
                self.last_started_key = run_key;
                self.last_start_existed = true;
                return;
            }
        }

        // Already present: a re-apply, or a second start with a run id
        // the client chose, which the handler let through while the first
        // was uncommitted. The log's order decides; the later is refused.
        if (self.runs.getEntry(ns_key_raw)) |e| {
            self.last_started_key = e.key_ptr.*;
            self.last_start_collided = true;
            return;
        }

        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return;
        var run = RunRecord{
            .run_id_owned = self.allocator.dupe(u8, raw_run_id) catch {
                self.allocator.free(ns_key);
                return;
            },
            .workflow_name_owned = "",
            .workflow_version_owned = "",
            .status = status,
            .input_owned = "",
            .created_at_ms = created_at_ms,
            .started_at_ms = created_at_ms,
            .completed_at_ms = null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
        };
        run.workflow_name_owned = self.allocator.dupe(u8, wf_name) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(ns_key);
            return;
        };
        run.workflow_version_owned = self.allocator.dupe(u8, version) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(run.workflow_name_owned);
            self.allocator.free(ns_key);
            return;
        };
        run.input_owned = self.allocator.dupe(u8, input) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(run.workflow_name_owned);
            self.allocator.free(run.workflow_version_owned);
            self.allocator.free(ns_key);
            return;
        };
        run.idempotency_key_owned = if (idem) |k| self.allocator.dupe(u8, k) catch null else null;
        // The run's first history event, as the start path recorded it.
        if (event_type.len > 0) self.addHistoryEvent(&run, event_type, input, created_at_ms);
        if (idem) |k| {
            if (std.mem.eql(u8, event_type, "trigger_started") and std.mem.startsWith(u8, k, "trigger:")) {
                self.restoreTriggerCursor(namespace, wf_name, k);
            }
        }

        self.runs.put(ns_key, run) catch {
            self.freeRunRecord(&run);
            self.allocator.free(ns_key);
            return;
        };
        self.last_started_key = ns_key;
        if (idem) |k| self.indexIdempotencyKey(namespace, wf_name, k, ns_key);
        // Search tags come from the definition and the input; derived here so
        // every node computes the same ones.
        if (self.runs.getPtr(ns_key)) |stored| {
            if (self.makeNsKey(namespace, wf_name)) |def_ns_key| {
                defer self.allocator.free(def_ns_key);
                stored.search_tags_owned = self.buildSearchTags(def_ns_key, stored);
            }
        }
    }

    /// Apply a workflow_complete entry. Updates the run's terminal status,
    /// output, step_outputs, and history events.
    /// Value format: [status:u8][completed_at_ms:i64]
    ///   [has_output:u8][output_len:u32][output]?
    ///   [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
    ///   [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
    fn replayComplete(self: *WorkflowHandler, ns_key_raw: []const u8, value: []const u8) void {
        if (value.len < 9) return;
        const status: RunStatus = @enumFromInt(value[0]);
        const completed_at_ms = std.mem.readInt(i64, value[1..9], .little);

        // Look up existing run (must have been replayed via workflow_start first)
        const run = self.runs.getPtr(ns_key_raw) orelse return;
        run.status = status;
        run.completed_at_ms = completed_at_ms;
        // The entry carries the whole terminal state; whatever the run held
        // (the live producer's copy, or an earlier apply) is replaced.
        if (run.output_owned) |o| {
            self.allocator.free(o);
            run.output_owned = null;
        }
        if (run.step_outputs) |*so| {
            var mutable = so.*;
            mutable.deinit(self.allocator);
            run.step_outputs = null;
        }
        for (run.history.items) |evt| {
            self.allocator.free(evt.event_type_owned);
            self.allocator.free(evt.detail_owned);
        }
        run.history.clearRetainingCapacity();

        var off: usize = 9;

        // [has_output:u8][output_len:u32][output]?
        const has_output = value[off];
        off += 1;
        if (has_output == 1) {
            if (off + 4 > value.len) return;
            const out_len = std.mem.readInt(u32, value[off..][0..4], .little);
            off += 4;
            if (off + out_len > value.len) return;
            run.output_owned = self.allocator.dupe(u8, value[off .. off + out_len]) catch return;
            off += out_len;
        }

        // [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
        if (off + 2 > value.len) return;
        const step_count = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (step_count > 0) {
            var so = StepOutputMap.init();
            var i: u16 = 0;
            while (i < step_count) : (i += 1) {
                if (off + 2 > value.len) return;
                const name_len = std.mem.readInt(u16, value[off..][0..2], .little);
                off += 2;
                if (off + name_len > value.len) return;
                const name = value[off .. off + name_len];
                off += name_len;

                if (off + 2 > value.len) return;
                const outcome_len = std.mem.readInt(u16, value[off..][0..2], .little);
                off += 2;
                if (off + outcome_len > value.len) return;
                const outcome = value[off .. off + outcome_len];
                off += outcome_len;

                if (off + 4 > value.len) return;
                const output_len = std.mem.readInt(u32, value[off..][0..4], .little);
                off += 4;
                if (off + output_len > value.len) return;
                const output = value[off .. off + output_len];
                off += output_len;

                so.put(self.allocator, name, output, outcome) catch return;
            }
            run.step_outputs = so;
        }

        // [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
        if (off + 2 > value.len) return;
        const hist_count = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        var h: u16 = 0;
        while (h < hist_count) : (h += 1) {
            if (off + 2 > value.len) return;
            const type_len = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (off + type_len > value.len) return;
            const event_type = self.allocator.dupe(u8, value[off .. off + type_len]) catch return;
            off += type_len;

            if (off + 2 > value.len) return;
            const detail_len = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (off + detail_len > value.len) return;
            const detail = self.allocator.dupe(u8, value[off .. off + detail_len]) catch return;
            off += detail_len;

            if (off + 8 > value.len) return;
            const timestamp_ms = std.mem.readInt(i64, value[off..][0..8], .little);
            off += 8;

            run.history.append(self.allocator, .{
                .event_type_owned = event_type,
                .detail_owned = detail,
                .timestamp_ms = timestamp_ms,
            }) catch return;
        }

        // [tags_len:u16][search_tags]? — optional, may be absent in old entries
        if (off + 2 <= value.len) {
            const tags_len = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (tags_len > 0 and off + tags_len <= value.len) {
                if (run.search_tags_owned) |old| self.allocator.free(old);
                run.search_tags_owned = self.allocator.dupe(u8, value[off .. off + tags_len]) catch null;
            }
        }
    }

    pub fn definitionCount(self: *const WorkflowHandler) usize {
        return self.definitions.count();
    }

    pub fn runCount(self: *const WorkflowHandler) usize {
        return self.runs.count();
    }
};

/// Write a string with JSON escaping (escapes ", \, and control chars).
/// Caller writes surrounding quotes.
fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => if (c < 0x20) {
                try writer.print("\\u{x:0>4}", .{c});
            } else {
                try writer.writeByte(c);
            },
        }
    }
}

/// Case-insensitive substring search. needle_lower must already be lowercase.
fn containsLower(haystack: []const u8, needle_lower: []const u8) bool {
    if (needle_lower.len == 0) return true;
    if (haystack.len < needle_lower.len) return false;
    const end = haystack.len - needle_lower.len + 1;
    outer: for (0..end) |i| {
        for (0..needle_lower.len) |j| {
            if (std.ascii.toLower(haystack[i + j]) != needle_lower[j]) continue :outer;
        }
        return true;
    }
    return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeRequest(op: OpCode, key: []const u8, value: []const u8) Request {
    return .{
        .header = .{
            .magic = proto.MAGIC,
            .payload_length = 0,
            .request_id = 1,
            .crc32 = 0,
            .version = proto.VERSION,
            .op_code = @intFromEnum(op),
            .flags = 0,
            .reserved = .{0} ** 8,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = "",
    };
}

test "workflow handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    WorkflowHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_create)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_start)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_signal)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_cancel)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_status)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_history)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_list_runs)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_get_definition)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_disable)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_enable)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.workflow_list_definitions)] != null);

    try testing.expectEqual(@as(u16, 11), dispatcher.handler_count);
}

test "workflow handler: init and deinit" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    try testing.expectEqual(@as(usize, 0), handler.definitionCount());
    try testing.expectEqual(@as(usize, 0), handler.runCount());
}

test "workflow handler: a re-created trigger keeps its cursor on the same stream only" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    handler.registerStreamTrigger("ns", "wf", .{ .stream = "events" });
    const key = try std.fmt.allocPrint(allocator, "ns:wf", .{});
    defer allocator.free(key);
    handler.stream_triggers.getPtr(key).?.stream_cursor_ts = 1700;
    handler.stream_triggers.getPtr(key).?.stream_cursor_seq = 3;

    handler.registerStreamTrigger("ns", "wf", .{ .stream = "events", .batch_size = 2 });
    const same = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(u64, 1700), same.stream_cursor_ts);
    try testing.expectEqual(@as(u64, 3), same.stream_cursor_seq);
    try testing.expectEqual(@as(u32, 2), same.batch_size);

    handler.registerStreamTrigger("ns", "wf", .{ .stream = "other" });
    const moved = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(u64, 0), moved.stream_cursor_ts);
    try testing.expectEqual(@as(u64, 0), moved.stream_cursor_seq);
}

// ── Step Executor Tests ─────────────────────────────────────────────────

/// Minimal 2-step workflow: start → step_b → flo.Completed
const test_workflow_json =
    \\{"kind":"Workflow","name":"test-wf","version":"1.0.0",
    \\"start":{"run":"@actions/step-a","transitions":{"success":"step_b"}},
    \\"steps":{"step_b":{"run":"@actions/step-b","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}}
;

/// Workflow with a wait_for_signal step
const test_wait_workflow_json =
    \\{"kind":"Workflow","name":"wait-wf","version":"1.0.0",
    \\"start":{"run":"@actions/init","transitions":{"success":"wait_approval"}},
    \\"steps":{"wait_approval":{"waitForSignal":{"type":"approval"},"transitions":{"success":"flo.Completed"}}}}
;

/// Workflow with input mapping
const test_input_mapping_json =
    \\{"kind":"Workflow","name":"map-wf","version":"1.0.0",
    \\"start":{"run":"@actions/step-a","inputMapping":"{\"user\":\"$.input.name\"}","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

/// Workflow with retry policy
const test_retry_workflow_json =
    \\{"kind":"Workflow","name":"retry-wf","version":"1.0.0",
    \\"start":{"run":"@actions/flaky","retry":{"maxAttempts":3},"transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

// Poll workflow: action returns `pending`; the engine should re-arm a backoff
// poll (delays 0 for test speed) and follow `timeout` after maxAttempts=2.
const test_poll_workflow_json =
    \\{"kind":"Workflow","name":"poll-wf","version":"1.0.0",
    \\"start":{"run":"@actions/poller",
    \\"poll":{"maxAttempts":2,"initialDelayMs":0,"baseDelayMs":0,"maxDelayMs":0,"backoff":"constant"},
    \\"transitions":{"success":"flo.Completed","failure":"flo.Failed","timeout":"flo.TimedOut"}}}
;

/// Register a test action for workflow step executor tests.
fn registerTestAction(actions: *ActionsHandler, name: []const u8) void {
    const alloc = actions.allocator;
    const owned_name = alloc.dupe(u8, name) catch return;
    const owned_ns = alloc.dupe(u8, "default") catch {
        alloc.free(owned_name);
        return;
    };
    const owned_owner = alloc.dupe(u8, "") catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        return;
    };
    actions.actions.put(owned_name, .{
        .name_owned = owned_name,
        .namespace_owned = owned_ns,
        .owner_owned = owned_owner,
        .action_type = .user,
        .version = 1,
        .enabled = true,
        .created_at_ns = 0,
    }) catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        alloc.free(owned_owner);
    };
}

/// Register a test action that simulates failure on invocation.
fn registerFailingAction(actions: *ActionsHandler, name: []const u8) void {
    const alloc = actions.allocator;
    const owned_name = alloc.dupe(u8, name) catch return;
    const owned_ns = alloc.dupe(u8, "default") catch {
        alloc.free(owned_name);
        return;
    };
    const owned_owner = alloc.dupe(u8, "") catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        return;
    };
    actions.actions.put(owned_name, .{
        .name_owned = owned_name,
        .namespace_owned = owned_ns,
        .owner_owned = owned_owner,
        .action_type = .user,
        .version = 1,
        .enabled = true,
        .created_at_ns = 0,
    }) catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        alloc.free(owned_owner);
    };
}

/// Create a minimal test shard with an actions handler for unit tests.
/// Only `actions_handler` is usable — all other fields are undefined.
fn createTestShard(actions: *ActionsHandler) !Shard {
    const waiter_pool_mod = @import("../node/waiter_pool.zig");
    const RaftNode = @import("../raft/node.zig").RaftNode;
    var shard: Shard = undefined;
    shard.id = 0;
    shard.actions_handler = actions;
    shard.peer_shards = null;
    shard.peer_inboxes = null;
    // `Shard = undefined` means field defaults do not apply — anything the code
    // under test reads must be assigned here or it holds garbage.
    shard.metrics_registry = null;
    shard.shard_metrics = null;
    const raft_node = try std.testing.allocator.create(RaftNode);
    raft_node.* = try RaftNode.init(std.testing.allocator, 1, 0, 64 * 1024, .{});
    try raft_node.bootstrap();
    shard.raft_node = raft_node;
    shard.raft_network = null;
    shard.router = router.Router.init(1, 1, 0);
    shard.run_id_gen = .{};
    shard.waiter_pool = waiter_pool_mod.WaiterPool.init();
    // The one applier needs a partition to append to, the registry that
    // owns action entries, and the copy buffer it reads committed entries
    // through.
    const partition = try std.testing.allocator.create(Partition);
    partition.* = try Partition.init(std.testing.allocator, 0, 256 * 1024, 0);
    const partitions = try std.testing.allocator.alloc(*Partition, 1);
    partitions[0] = partition;
    shard.partitions = partitions;
    shard.replay_registry = .{};
    actions.registerReplay(&shard.replay_registry);
    shard.apply_buf = try std.testing.allocator.alloc(u8, @import("../kv/handler.zig").MAX_APPLY_PAYLOAD);
    shard.durability = .async_flush;
    shard.pending = try std.testing.allocator.alloc(shard_mod.Pending, shard_mod.PENDING_SLOTS);
    @memset(shard.pending, .{});
    shard.pending_count = 0;
    shard.applying = false;
    shard.last_entry_applied = true;
    shard.wake_workers = false;
    shard.replies_held = 0;
    shard.forward_count = 0;
    return shard;
}

fn destroyTestShard(shard: *Shard) void {
    shard.raft_node.deinit();
    std.testing.allocator.destroy(shard.raft_node);
    shard.partitions[0].deinit();
    std.testing.allocator.destroy(shard.partitions[0]);
    std.testing.allocator.free(shard.partitions);
    std.testing.allocator.free(shard.apply_buf);
    std.testing.allocator.free(shard.pending);
}

/// Complete a pending test action run with outcome "success".
/// Call this after advanceWorkflow parks, then call checkPendingActions to resume.
fn completeTestAction(actions: *ActionsHandler, run_id: []const u8) void {
    completeTestActionWith(actions, run_id, "success");
}

/// Complete a pending test action run with a specific outcome.
fn completeTestActionWith(actions: *ActionsHandler, run_id: []const u8, outcome: []const u8) void {
    const run = actions.runs.getPtr(run_id) orelse return;
    run.status = .completed;
    run.completed_at_ms = @import("stdx").time.milliTimestamp();
    if (run.outcome_owned) |old| actions.allocator.free(old);
    run.outcome_owned = actions.allocator.dupe(u8, outcome) catch null;
}

fn createTestRun(handler: *WorkflowHandler, ns_key: []const u8, run_id: []const u8, wf_name: []const u8) void {
    const alloc = handler.allocator;
    const owned_ns = alloc.dupe(u8, ns_key) catch return;
    const owned_rid = alloc.dupe(u8, run_id) catch {
        alloc.free(owned_ns);
        return;
    };
    const owned_wf = alloc.dupe(u8, wf_name) catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        return;
    };
    const owned_ver = alloc.dupe(u8, "1.0.0") catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        return;
    };
    const owned_input = alloc.dupe(u8, "{}") catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        alloc.free(owned_ver);
        return;
    };

    handler.runs.put(owned_ns, .{
        .run_id_owned = owned_rid,
        .workflow_name_owned = owned_wf,
        .workflow_version_owned = owned_ver,
        .status = .running,
        .input_owned = owned_input,
        .created_at_ms = 0,
        .started_at_ms = 0,
        .completed_at_ms = null,
        .idempotency_key_owned = null,
        .signals = .empty,
        .history = .empty,
    }) catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        alloc.free(owned_ver);
        alloc.free(owned_input);
    };
}

fn createTestRunWithInput(handler: *WorkflowHandler, ns_key: []const u8, run_id: []const u8, wf_name: []const u8, input: []const u8) void {
    const alloc = handler.allocator;
    const owned_ns = alloc.dupe(u8, ns_key) catch return;
    const owned_rid = alloc.dupe(u8, run_id) catch {
        alloc.free(owned_ns);
        return;
    };
    const owned_wf = alloc.dupe(u8, wf_name) catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        return;
    };
    const owned_ver = alloc.dupe(u8, "1.0.0") catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        return;
    };
    const owned_input = alloc.dupe(u8, input) catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        alloc.free(owned_ver);
        return;
    };

    handler.runs.put(owned_ns, .{
        .run_id_owned = owned_rid,
        .workflow_name_owned = owned_wf,
        .workflow_version_owned = owned_ver,
        .status = .running,
        .input_owned = owned_input,
        .created_at_ms = 0,
        .started_at_ms = 0,
        .completed_at_ms = null,
        .idempotency_key_owned = null,
        .signals = .empty,
        .history = .empty,
    }) catch {
        alloc.free(owned_ns);
        alloc.free(owned_rid);
        alloc.free(owned_wf);
        alloc.free(owned_ver);
        alloc.free(owned_input);
    };
}

fn createTestDef(handler: *WorkflowHandler, ns_key: []const u8, name: []const u8, yaml: []const u8) void {
    const alloc = handler.allocator;
    const owned_ns = alloc.dupe(u8, ns_key) catch return;
    const owned_name = alloc.dupe(u8, name) catch {
        alloc.free(owned_ns);
        return;
    };
    const owned_ver = alloc.dupe(u8, "1.0.0") catch {
        alloc.free(owned_ns);
        alloc.free(owned_name);
        return;
    };
    const owned_yaml = alloc.dupe(u8, yaml) catch {
        alloc.free(owned_ns);
        alloc.free(owned_name);
        alloc.free(owned_ver);
        return;
    };

    handler.definitions.put(owned_ns, .{
        .name_owned = owned_name,
        .version_owned = owned_ver,
        .yaml_owned = owned_yaml,
        .created_at_ms = 0,
    }) catch {
        alloc.free(owned_ns);
        alloc.free(owned_name);
        alloc.free(owned_ver);
        alloc.free(owned_yaml);
    };
}

test "a producer's start waits in the first-step queue until it applies, then takes its step; one whose index applied without creating a run is dropped" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "step-a");
    registerTestAction(&actions, "step-b");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);

    // Proposed, not yet applied: there is no run to step, so it waits.
    try testing.expect(handler.proposeRun(&shard, "default", "run-p", "test-wf", "latest", "{}", null, "schedule_started") != null);
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 1), handler.started_to_advance.items.len);
    try testing.expect(handler.runs.get("default:run-p") == null);

    _ = shard.applyCommitted();
    // A start whose index applied without creating a run (dropped with the
    // log's tail) is dropped from the queue, not kept for ever.
    handler.queueFirstStep("default:ghost", shard.raft_node.last_applied);
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 0), handler.started_to_advance.items.len);
    const run = handler.runs.get("default:run-p").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expect(run.pending_action_run_id_owned != null);
}

test "a step whose child run the log dropped fails once the child's start index has applied" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-c", "run-c", "test-wf");

    // Parked on a child whose start is at an index this shard has applied
    // but which created no run, as when a lost leadership truncated it.
    const proposed = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k", "v");
    _ = shard.applyCommitted();
    const run = handler.runs.getPtr("default:run-c").?;
    handler.parkForChild(run, "gone", "start", "child-wf", shard.id, 0, proposed.index);
    handler.checkPendingActions(&shard);
    const after = handler.runs.get("default:run-c").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, after.status);
    var lost = false;
    for (after.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "child_lost")) lost = true;
    }
    try testing.expect(lost);
}

test "a step whose action run the log dropped fails once the invoke's index has applied" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "step-a");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-l", "run-l", "test-wf");

    handler.advanceWorkflow(&shard, "default:run-l", "default");
    const run = handler.runs.getPtr("default:run-l").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    // Not applied yet: the run waits.
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get("default:run-l").?.status);

    // Point the run at an action run no invoke created, then apply the
    // invoke's index: the run it names does not exist, as when a lost
    // leadership truncated the invoke from the log.
    allocator.free(run.pending_action_run_id_owned.?);
    run.pending_action_run_id_owned = try allocator.dupe(u8, "gone");
    _ = shard.applyCommitted();
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.runs.get("default:run-l").?.status);
}

// The step executor tests step runs directly: after each step they apply
// what it proposed, as the shard's tick does.
test "step executor: linear workflow completes via action invocation" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    // Set up actions handler with test WASM actions
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "step-a");
    registerTestAction(&actions, "step-b");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-1", "run-1", "test-wf");

    try testing.expectEqual(@as(usize, 1), handler.runCount());

    // Phase 1: advance invokes step-a asynchronously, workflow parks
    handler.advanceWorkflow(&shard, "default:run-1", "default");
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-1").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-a with "success"
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes step-a, advances to step-b (parks again)
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-1").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "success"
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }

    // Phase 3: checkPendingActions resumes step-b, workflow completes
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-1").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expect(run.completed_at_ms != null);
    // History should have: step_started(start), action_completed, step_completed(start),
    // step_started(step_b), action_completed, step_completed(step_b), workflow_completed
    try testing.expect(run.history.items.len >= 5);
    // Step outputs should be tracked
    try testing.expect(run.step_outputs != null);
}

test "step executor: wait_for_signal parks run" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "init");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:wait-wf", "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, "default:run-2", "run-2", "wait-wf");

    // Phase 1: advance invokes init action, parks
    handler.advanceWorkflow(&shard, "default:run-2", "default");
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-2").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete init with success so workflow transitions to wait_approval
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes init, workflow transitions to wait_approval (parks for signal)
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-2").?;
    // Should be waiting for approval signal after init → wait_approval
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expect(run.wait_signal_type_owned != null);
    try testing.expectEqualStrings("approval", run.wait_signal_type_owned.?);
}

test "step executor: signal resumes waiting workflow" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "init");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:wait-wf", "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, "default:run-3", "run-3", "wait-wf");

    // Advance until init parks, then complete it
    handler.advanceWorkflow(&shard, "default:run-3", "default");
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-3").?;
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }
    // Resume init → transitions to wait_approval → parks for signal
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.get("default:run-3").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    }

    // Simulate signal delivery: set up matching signal, clear wait, resume
    {
        const run = handler.runs.getPtr("default:run-3").?;
        const sig_type = try allocator.dupe(u8, "approval");
        try run.signals.append(allocator, .{
            .signal_type_owned = sig_type,
            .payload_owned = null,
            .received_at_ms = 0,
        });
        if (run.wait_signal_type_owned) |old| allocator.free(old);
        run.wait_signal_type_owned = null;
        run.status = .running;
    }

    // Resume execution — signal found, follow success transition → flo.Completed
    handler.advanceWorkflow(&shard, "default:run-3", "default");
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-3").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
}

test "step executor: missing definition does not crash" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    // No definition registered
    createTestRun(&handler, "default:run-4", "run-4", "nonexistent-wf");

    // Should gracefully no-op (no definition found)
    handler.advanceWorkflow(&shard, "default:run-4", "default");
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-4").?;
    // Still running since we couldn't find the definition to advance
    try testing.expectEqual(WorkflowHandler.RunStatus.running, run.status);
}

test "step executor: missing action yields target_not_found" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    // Actions handler with NO actions registered
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-5", "run-5", "test-wf");

    handler.advanceWorkflow(&shard, "default:run-5", "default");
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-5").?;
    // Should fail because action "step-a" is not registered
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
}

test "step executor: failing action follows failure transition" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "step-a");
    registerFailingAction(&actions, "step-b"); // step-b will fail
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-6", "run-6", "test-wf");

    // Phase 1: advance invokes step-a, parks
    handler.advanceWorkflow(&shard, "default:run-6", "default");
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-6").?;
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }
    // Phase 2: step-a completes (success) → transitions to step_b → step-b parks
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-6").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "failure"
        completeTestActionWith(&actions, run.pending_action_run_id_owned.?, "failure");
    }
    // Phase 3: step-b fails → follows "failure" transition → flo.Failed
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-6").?;
    // step-a succeeds → step_b → step-b fails → "failure" → flo.Failed
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
}

test "step executor: retry on failure" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerFailingAction(&actions, "flaky"); // always fails
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:retry-wf", "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, "default:run-7", "run-7", "retry-wf");

    // Initial attempt: flaky action parks
    handler.advanceWorkflow(&shard, "default:run-7", "default");
    _ = shard.applyCommitted();

    // Drive attempts: 1 original + 3 retries (maxAttempts=3) = 4 total
    // Each iteration: complete current action with "failure", checkPendingActions
    // (which either retries→re-parks, or exhausts retries→flo.Failed)
    var attempt: u32 = 0;
    while (attempt < 5) : (attempt += 1) {
        const run_ptr = handler.runs.getPtr("default:run-7").?;
        if (run_ptr.status.isTerminal()) break;
        const rid = run_ptr.pending_action_run_id_owned orelse break;
        completeTestActionWith(&actions, rid, "failure");
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }

    const run = handler.runs.get("default:run-7").?;
    // Should fail after retries exhausted (max_attempts=3 means up to 3 retries)
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    // History should include retry events
    var retry_count: u32 = 0;
    for (run.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "step_retry")) {
            retry_count += 1;
        }
    }
    try testing.expect(retry_count >= 2); // at least 2 retries before final failure
}

// An action that returns the `pending` business outcome on a `poll:` step must
// re-arm a backoff poll (not fail with "no transition"), and after maxAttempts
// is exceeded must follow the `timeout` transition. Driven at the handler layer
// because the worker CLI cannot emit a `pending` outcome.
test "step executor: poll re-arms on pending and times out after maxAttempts" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "poller");
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:poll-wf", "poll-wf", test_poll_workflow_json);
    createTestRun(&handler, "default:run-poll", "run-poll", "poll-wf");

    // Initial: poller invoked, parks for async completion.
    handler.advanceWorkflow(&shard, "default:run-poll", "default");
    _ = shard.applyCommitted();

    // First `pending` must ARM a poll (not fail): status waiting, attempt incremented.
    {
        const run = handler.runs.getPtr("default:run-poll").?;
        try testing.expect(run.pending_action_run_id_owned != null);
        completeTestActionWith(&actions, run.pending_action_run_id_owned.?, "pending");
    }
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr("default:run-poll").?;
        try testing.expect(!run.status.isTerminal()); // pre-fix bug: would be .failed
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        try testing.expectEqual(@as(u32, 1), run.poll_attempt);
    }

    // Drive remaining poll cycles: each tick either fires the due poll timer
    // (re-invoking the action) or processes the re-invoked action's `pending`
    // completion (re-arming). maxAttempts=2 → exhausted → `timeout` → flo.TimedOut.
    var i: u32 = 0;
    while (i < 12) : (i += 1) {
        const run = handler.runs.getPtr("default:run-poll").?;
        if (run.status.isTerminal()) break;
        if (run.pending_action_run_id_owned) |rid| {
            completeTestActionWith(&actions, rid, "pending");
        }
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }

    const run = handler.runs.get("default:run-poll").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, run.status);

    var polls: u32 = 0;
    for (run.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "poll_scheduled")) polls += 1;
    }
    try testing.expect(polls >= 1);
}

test "step executor: checkPendingActions handles completed async action" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);

    // Manually create a run that's parked waiting for an action
    {
        const alloc = handler.allocator;
        const ns_key = alloc.dupe(u8, "default:run-8") catch return;
        const rid = alloc.dupe(u8, "run-8") catch {
            alloc.free(ns_key);
            return;
        };
        const wf = alloc.dupe(u8, "test-wf") catch {
            alloc.free(ns_key);
            alloc.free(rid);
            return;
        };
        const ver = alloc.dupe(u8, "1.0.0") catch {
            alloc.free(ns_key);
            alloc.free(rid);
            alloc.free(wf);
            return;
        };
        const inp = alloc.dupe(u8, "{}") catch {
            alloc.free(ns_key);
            alloc.free(rid);
            alloc.free(wf);
            alloc.free(ver);
            return;
        };
        const arid = alloc.dupe(u8, "action-42") catch {
            alloc.free(ns_key);
            alloc.free(rid);
            alloc.free(wf);
            alloc.free(ver);
            alloc.free(inp);
            return;
        };
        const step_name = alloc.dupe(u8, "start") catch {
            alloc.free(ns_key);
            alloc.free(rid);
            alloc.free(wf);
            alloc.free(ver);
            alloc.free(inp);
            alloc.free(arid);
            return;
        };

        handler.runs.put(ns_key, .{
            .run_id_owned = rid,
            .workflow_name_owned = wf,
            .workflow_version_owned = ver,
            .status = .waiting,
            .input_owned = inp,
            .created_at_ms = 0,
            .started_at_ms = 0,
            .completed_at_ms = null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
            .pending_action_run_id_owned = arid,
            .pending_step_name_owned = step_name,
        }) catch {
            alloc.free(ns_key);
            alloc.free(rid);
            alloc.free(wf);
            alloc.free(ver);
            alloc.free(inp);
            alloc.free(arid);
            alloc.free(step_name);
            return;
        };
    }

    // Create a completed action run in the actions handler
    {
        const arid = actions.allocator.dupe(u8, "action-42") catch return;
        const aname = actions.allocator.dupe(u8, "step-a") catch {
            actions.allocator.free(arid);
            return;
        };
        actions.runs.put(arid, .{
            .run_id_owned = arid,
            .action_name_owned = aname,
            .input_owned = null,
            .status = .completed,
            .created_at_ms = 0,
            .started_at_ms = 0,
            .completed_at_ms = 1000,
        }) catch {
            actions.allocator.free(arid);
            actions.allocator.free(aname);
            return;
        };

        // Also register the action so transition resolution works
        registerTestAction(&actions, "step-a");
        registerTestAction(&actions, "step-b");
    }

    // checkPendingActions should detect the completed action and resume step-a
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    // After step-a resumes, workflow advances to step_b (async) — parks again
    {
        const run = handler.runs.getPtr("default:run-8").?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b so the workflow can finish
        completeTestAction(&actions, run.pending_action_run_id_owned.?);
    }

    // Second checkPendingActions: step-b done → flo.Completed
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get("default:run-8").?;
    // After resuming start → step_b (success) → flo.Completed
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
}
