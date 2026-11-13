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
const raft_node_mod = @import("../raft/node.zig");
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const system_ops = @import("../protocol/system_ops.zig");
const list_runs = @import("list_runs.zig");
const parser = @import("parser.zig");

const definition = @import("definition.zig");
const cron_mod = @import("cron.zig");
const plan_types = @import("plan_types.zig");
const validator = @import("validator.zig");
const jsonpath = @import("jsonpath.zig");
const json_depth = @import("../util/json_depth.zig");
const wf_types = @import("types.zig");
const StepOutputMap = wf_types.StepOutputMap;

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const StreamID = @import("../projection/stream.zig").StreamID;
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
const Partition = @import("../storage/partition.zig").Partition;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const router = @import("../node/router.zig");
const run_id_mod = @import("../node/run_id.zig");
const NamespaceId = @import("../namespace/table.zig").Id;
const NamespaceHandler = @import("../namespace/handler.zig").NamespaceHandler;

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

/// Cluster-local count of registered stream triggers across all shards on this
/// node. Lets the (cross-shard) stream-append path skip the trigger push-wake
/// broadcast entirely when no workflow watches any stream — zero overhead for
/// deployments that don't use stream triggers.
var global_stream_trigger_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

/// A workflow's key, in the handler's maps and its log entries alike: its
/// namespace's id, fixed width, then a workflow's name or a run's id. No
/// name can make one namespace's key another's.
pub const KEY_PREFIX: usize = 4;

/// `id ‖ name` into `buf`, which holds at least `KEY_PREFIX + name.len`.
pub fn writeKey(buf: []u8, id: NamespaceId, name: []const u8) []u8 {
    buf[0..KEY_PREFIX].* = id.bytes();
    @memcpy(buf[KEY_PREFIX..][0..name.len], name);
    return buf[0 .. KEY_PREFIX + name.len];
}

/// The namespace a key names. Every key leads with one (`writeKey`).
pub fn keyNamespace(key: []const u8) NamespaceId {
    return .of(std.mem.readInt(u32, key[0..KEY_PREFIX], .big));
}

/// The workflow name or run id a key holds after its namespace.
pub fn keyName(key: []const u8) []const u8 {
    return key[KEY_PREFIX..];
}

/// A key as a log line shows it: its name, then its namespace's id.
const ShownKey = struct {
    key: []const u8,

    pub fn format(self: ShownKey, w: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.key.len < KEY_PREFIX) return w.writeAll("(no key)");
        try w.print("{s} (namespace {d})", .{ keyName(self.key), keyNamespace(self.key).int() });
    }
};

fn shown(key: []const u8) ShownKey {
    return .{ .key = key };
}

// ═══════════════════════════════════════════════════════════════════════════════
// WorkflowHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const WorkflowHandler = struct {
    /// A run waiting for its first step: its key in `runs` and the log
    /// index of its start.
    const Started = struct { ns_key: []u8, index: u64 };

    allocator: Allocator,

    /// Mutex for cross-shard thread safety. Acquired when this handler is
    /// accessed from a different shard's thread via forwardToShard dispatch.
    mu: @import("stdx").Mutex,
    /// Runs whose step_started this leader applied in its term, in log
    /// order, for the tick to take their first step (`advanceStartedRuns`).
    /// No run takes its first step where its entry applies: a step
    /// proposes, and the shard applies nothing in the middle of a module's
    /// work.
    started_to_advance: std.ArrayListUnmanaged(Started) = .empty,
    /// Runs started and not yet stepped, by their keys in `runs`: kept by
    /// the appliers on every replica, so whichever node leads steps them.
    /// Their first steps go in this map's order, not their starts'.
    to_step: std.StringHashMapUnmanaged(void) = .empty,
    /// Where the last pass over `to_step` stopped, so a burst of starts is
    /// walked once rather than from the top each pass.
    to_step_cursor: u32 = 0,
    /// Runs this leader has proposed a step_started for in `stepping_term`,
    /// not yet applied. Leader memory: a new term starts it empty.
    stepping: std.StringHashMapUnmanaged(void) = .empty,
    stepping_term: u64 = 0,
    /// Runs parked on an action or a child, by their keys in `runs`: what
    /// the duty looks at, so its cost follows the work in flight and not
    /// every run ever started. Leader memory, like a step's state.
    parked: std.StringHashMapUnmanaged(void) = .empty,
    /// When a parked run's poll or signal wait is due, earliest first. An
    /// entry is checked against the run when it comes due, so one outdated
    /// by the run moving on is dropped then.
    timers: std.PriorityQueue(Timer, void, timerCompare) = .empty,
    /// An insert into `parked` or `timers` failed, and when to try again.
    rewatch: bool = false,
    rewatch_at_ms: i64 = 0,
    /// Runs not yet finished, by their workflow's key: kept by the appliers,
    /// so a schedule's `max_concurrent` needn't count them.
    active_runs: std.StringHashMapUnmanaged(u32) = .empty,

    /// Completions a busy log refused, retried by the tick (`retryCompletions`).
    /// It drains only while this node leads; after a step-down and
    /// re-election the entries fire against the run's local state, and a run
    /// that turned terminal meanwhile is skipped. A completion refused with
    /// NotLeader or WritesStopped isn't kept here: the run stays non-terminal
    /// on this node until replay. Leader-only run state across failover is
    /// for the delivery-leases work and the state-machine contract to settle.
    completion_retries: std.ArrayListUnmanaged(PendingCompletion) = .empty,
    /// Logs a completion record. A seam so a test can make the log refuse.
    propose: *const fn (shard: *Shard, run_ns_key: []const u8, value: []const u8) anyerror!persistence_mod.ProposeResult = proposeCompletion,

    /// Definitions, by key (`writeKey`: namespace id ‖ workflow name), each
    /// key allocated apart from its record.
    definitions: std.StringHashMap(DefinitionRecord),

    /// Runs, by key (namespace id ‖ run id).
    runs: std.StringHashMap(RunRecord),

    /// (namespace id, workflow, idempotency key) → the key in `runs` of the
    /// run that key started (see `idemKey`). Kept by the start applier, so every node
    /// and every replay decides duplicates alike, without a scan.
    idem_index: std.StringHashMapUnmanaged([]const u8) = .{},

    /// Disabled workflows, by their definitions' keys.
    disabled: std.StringHashMap(void),

    /// Active stream triggers, by their definitions' keys.
    /// Registered when a workflow with a `trigger:` block is created.
    stream_triggers: std.StringHashMap(StreamTriggerState),

    /// Set when a stream this shard watches was just appended to (via a
    /// peer's `stream_appended` wake flag or a local append). The next
    /// `tickStreamTriggers` force-polls all triggers, bypassing their poll
    /// timer — turning a polled (≤ batch_timeout_ms) latency into ~one tick.
    /// Single-threaded per shard (set/read only on the shard's reactor thread).
    triggers_dirty: bool = false,
    /// Whether an applied definition installs its trigger and schedule here.
    /// Cleared by the shard while it applies entries replicated from a peer:
    /// those producers run on the node that owns the definition.
    install_producers: bool = true,
    /// This shard's namespaces, as its applied entries bind them: what an
    /// applier reads a namespace's name from, the same on every replica.
    namespaces: ?*const NamespaceHandler = null,

    /// Active interval/cron schedules, by their definitions' keys.
    /// Registered when a workflow with a `schedule:` block is created.
    schedules: std.StringHashMap(ScheduleState),

    /// Plan executor health, by (namespace id, workflow, plan, executor) (`healthKey`).
    /// Used by health-weighted selection to route to the healthiest executor and
    /// by circuit breakers to skip failing executors.
    plan_health: std.StringHashMap(plan_types.ExecutorHealth),

    const MAX_WORKFLOW_NAME_LEN: usize = 256;
    /// The longest key a create or start entry carries.
    pub const MAX_START_KEY: usize = 600;
    /// The longest run id a start may choose: its key is the namespace's id
    /// and the run id.
    pub const MAX_RUN_ID: usize = MAX_START_KEY - KEY_PREFIX;
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

        /// The current step's start was refused for room and is retried on
        /// the poll timer; the retries add nothing more to history.
        waiting_for_room: bool = false,

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

        /// The order this pass through the plan tries its executors in,
        /// fixed when the pass starts (0 = not started). Health-weighted
        /// order is a sort by health, and a failure changes health: sorting
        /// again mid-pass would put a different executor under
        /// `plan_executor_idx`, so a retry or the next attempt could start
        /// one that already ran out while another is never tried.
        plan_order: [plan_types.MAX_PLAN_EXECUTORS]u8 = undefined,
        plan_order_len: u8 = 0,

        /// Absolute deadline for wait_for_signal timeout (ms since epoch, 0 = none).
        wait_timeout_at_ms: i64 = 0,

        /// The entry of a completion or cancel this leader proposed and that
        /// hasn't applied, and its term: nothing else moves the run
        /// meanwhile (`endingInFlight`). The run changes only when the
        /// entry applies, so no node shows an ending the log may drop.
        ending_index: u64 = 0,
        ending_term: u64 = 0,
        /// Whether that entry is a client's cancel: another cancel may
        /// follow it, and the last one proposed is the one answered.
        ending_cancel: bool = false,

        /// This run's key in `runs`, which owns it.
        ns_key: []const u8 = "",

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
        /// The namespace the workflow is defined in.
        ns: NamespaceId,
        stream_name_owned: []const u8,
        /// The namespace the trigger's definition names for its stream,
        /// looked up as it polls; null reads the workflow's own.
        stream_namespace_owned: ?[]const u8,
        batch_size: u32,
        poll_interval_ms: u32,
        stream_cursor_ts: u64,
        stream_cursor_seq: u64,
        last_poll_ms: i64,
    };

    pub const ScheduleState = struct {
        workflow_name_owned: []const u8,
        ns: NamespaceId,
        /// Slots every `interval_ms` from the anchor, or at `cron`'s fire
        /// times (UTC) from the anchor on.
        interval_ms: i64,
        cron: ?cron_mod.Cron = null,
        max_concurrent: u32,
        input_owned: ?[]const u8,
        /// A cron's latest fire time changes on the minute: the last one
        /// found, until `cron_until_ms`.
        cron_latest_ms: ?i64 = null,
        cron_until_ms: i64 = std.math.minInt(i64),
        /// Slots count from here: the stamp of the entry that created or
        /// last enabled the workflow, the same on every replica.
        anchor_ms: i64,
        /// The latest slot a run started for, set as that start applies
        /// on every replica, so a new leader fires only later slots.
        fired_ms: i64 = std.math.minInt(i64),
        /// The slot this leader proposed a start for, in `proposed_term`;
        /// not proposed again while that term lasts.
        proposed_ms: i64 = std.math.minInt(i64),
        proposed_term: u64 = 0,
    };

    const Timer = struct { at_ms: i64, ns_key: []const u8 };

    fn timerCompare(_: void, a: Timer, b: Timer) std.math.Order {
        return std.math.order(a.at_ms, b.at_ms);
    }

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
        for (self.completion_retries.items) |p| {
            self.allocator.free(p.run_ns_key);
            self.allocator.free(p.detail);
        }
        self.completion_retries.deinit(self.allocator);
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
        self.parked.deinit(self.allocator);
        self.to_step.deinit(self.allocator);
        self.stepping.deinit(self.allocator);
        self.timers.deinit(self.allocator);
        var ak = self.active_runs.keyIterator();
        while (ak.next()) |k| self.allocator.free(k.*);
        self.active_runs.deinit(self.allocator);

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
        self.allocator.free(state.stream_name_owned);
        if (state.stream_namespace_owned) |n| self.allocator.free(n);
    }

    fn freeScheduleState(self: *WorkflowHandler, state: *ScheduleState) void {
        self.allocator.free(state.workflow_name_owned);
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
        if (req.key.len == 0) return null;
        return router.routeNsKey(req.ns_id, req.key);
    }

    /// ShardWalker LocalScanFn for workflow_list_definitions — returns
    /// workflow definition names from one shard's WorkflowHandler registry.
    fn localScanWorkflowDefs(
        ctx: *anyopaque,
        ns: dispatcher_mod.WalkNamespace,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *WorkflowHandler = @ptrCast(@alignCast(ctx));
        handler.mu.lock();
        defer handler.mu.unlock();
        // Sized under the lock, so an insert can't outgrow it mid-scan.
        const name_buf = dispatcher_mod.scanScratch(handler.definitions.count());

        var count: usize = 0;
        var dit = handler.definitions.iterator();
        while (dit.next()) |entry| {
            if (count >= name_buf.len) break;
            if (keyNamespace(entry.key_ptr.*) != ns.id) continue;
            name_buf[count] = entry.value_ptr.name_owned;
            count += 1;
        }

        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    fn dispatchWorkflow(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        // Parked outside the handler's lock: on a single node the entry
        // applies straight away, and its applier takes the lock itself.
        const proposed = shard.workflow_handler.handleCommand(shard, conn, req) orelse return;
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        shard.park(conn, req, proposed, switch (op) {
            .workflow_create => answerCreate,
            .workflow_start => answerStart,
            else => answerOk,
        });
    }

    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        shard.sendOkResponse(conn, request_id, "");
    }

    /// The create applied: answered with the definition's name.
    fn answerCreate(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        shard.sendOkResponse(conn, request_id, r.slice() orelse "");
    }

    /// The start applied: answered with its run's id, the one it created or
    /// the earlier run its idempotency key names. The first step is the
    /// tick's (`startApplied`).
    fn answerStart(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        _ = r.int(u8);
        shard.sendOkResponse(conn, request_id, r.slice() orelse "");
    }

    /// After a start applies on the leader that proposed it: a run it
    /// created is queued for its first step. Never on a follower (a step
    /// proposes, and only the leader does), and never on a later leader
    /// applying an earlier term's start: that start's own leader may have
    /// stepped it already, so it stays at its start (`dropStartedRuns`).
    pub fn startApplied(self: *WorkflowHandler, shard: *Shard, entry: *const entry_mod.Entry, result: *const Result) void {
        if (entry.header.entry_type == @backingInt(entry_mod.EntryType.workflow_complete)) return self.endingApplied(shard, entry);
        const etype: entry_mod.EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        if (etype != .workflow_start and etype != .workflow_step_started) return;
        if (shard.raft_node.role != .leader or entry.header.term != shard.raft_node.current_term) return;
        // The run's key is the entry's.
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        self.mu.lock();
        defer self.mu.unlock();
        if (etype == .workflow_start) {
            if (result.refused() != null) return;
            var r = result.reader();
            if ((r.int(u8) orelse 0) == 1) if (shard.metrics_registry) |m| m.workflow.recordStarted();
            return;
        }
        // Of this leader's term, so this leader proposed it.
        _ = self.stepping.remove(cmd.key);
        // Refused: another entry took its first step already.
        if (result.refused() == null) self.queueFirstStep(cmd.key, result.index);
    }

    /// A completion applied: if it was the one this leader proposed last
    /// for its run, its outcome is counted, once however many cancels and
    /// answers there were. A completion an earlier leader proposed counts
    /// on none.
    fn endingApplied(self: *WorkflowHandler, shard: *Shard, entry: *const entry_mod.Entry) void {
        if (shard.raft_node.role != .leader) return;
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        self.mu.lock();
        defer self.mu.unlock();
        const run = self.runs.getPtr(cmd.key) orelse return;
        if (run.status.isTerminal()) self.dropCompletionRetry(cmd.key);
        // Its index and term: a node elected again and still behind may
        // apply another leader's entry at the index it once proposed at.
        if (run.ending_index != entry.header.index or run.ending_term != entry.header.term) return;
        run.ending_index = 0;
        if (shard.metrics_registry) |m| switch (run.status) {
            .completed => m.workflow.recordCompleted(),
            .failed => m.workflow.recordFailed(),
            .cancelled => m.workflow.recordCancelled(),
            .timed_out => m.workflow.recordTimedOut(),
            else => {},
        };
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    /// Answers the client, or returns the write the client is parked on.
    fn handleCommand(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        self.mu.lock();
        defer self.mu.unlock();
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        switch (op) {
            .workflow_create => return self.handleCreate(shard, conn, req),
            .workflow_start => return self.handleStart(shard, conn, req),
            .workflow_signal => self.handleSignal(shard, conn, req),
            .workflow_cancel => return self.handleCancel(shard, conn, req),
            .workflow_status => self.handleStatus(shard, conn, req),
            .workflow_history => self.handleHistory(shard, conn, req),
            .workflow_list_runs => self.handleListRuns(shard, conn, req),
            .workflow_get_definition => self.handleGetDefinition(shard, conn, req),
            .workflow_disable => return self.handleSetEnabled(shard, conn, req, false),
            .workflow_enable => return self.handleSetEnabled(shard, conn, req, true),
            .workflow_list_definitions => self.handleListDefinitions(shard, conn, req),
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown workflow opcode");
            },
        }
        return null;
    }

    /// A stored definition parsed again. One that no longer parses (the
    /// parser's rules changed under it) is logged by name, never dropped in
    /// silence: its runs stop advancing.
    fn parseStored(self: *WorkflowHandler, yaml: []const u8, def_key: []const u8) parser.ParseError!parser.WorkflowDefinition {
        var diag: parser.Diagnostic = .{};
        return parser.parseWorkflow(self.allocator, yaml, &diag) catch |err| {
            log.err("workflow {f} not loaded: {s}", .{ shown(def_key), if (err == error.OutOfMemory) "out of memory" else diag.message() });
            return err;
        };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        const yaml = req.value;

        if (yaml.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow definition is required");
            return null;
        }

        // Parse the YAML/JSON definition
        var diag: parser.Diagnostic = .{};
        var def = parser.parseWorkflow(self.allocator, yaml, &diag) catch |err| {
            if (err == error.OutOfMemory) {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "out of memory");
            } else {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, diag.message());
            }
            return null;
        };
        defer def.deinit(self.allocator);

        // A trigger reads from its stream without passing a client's
        // request check, so the namespace it names is checked here.
        if (def.trigger) |t| if (t.namespace) |ns| if (ns.len > 0) if (@import("../namespace/handler.zig").nameRefusal(ns)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return null;
        };

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
        const proposed = self.proposeCreate(shard, req.ns_id, name, yaml) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "workflow not persisted"));
            return null;
        };
        return proposed;
    }

    // ── START ───────────────────────────────────────────────────────────

    fn handleStart(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        const workflow_name = req.key;

        if (workflow_name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return null;
        }

        // Build namespace-qualified key for definition/disabled lookups
        const def_ns_key = self.nsKey(req.ns_id, workflow_name) orelse {
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
                    if (run_id_mod.looksGenerated(.workflow, explicit_run_id.?)) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: run ids of the form wfr-<hex>-<hex> are the server's; choose another");
                        return null;
                    }
                    if (explicit_run_id.?.len > MAX_RUN_ID) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, std.fmt.comptimePrint("bad request: a run id is at most {d} bytes", .{MAX_RUN_ID}));
                        return null;
                    }
                }
            }

            // Remaining bytes are input
            if (offset < value.len) {
                input = value[offset..];
            }
        }

        if (!json_depth.within(input)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "input: " ++ json_depth.TOO_DEEP_MESSAGE);
            return null;
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
            if (self.runForIdempotencyKey(req.ns_id, workflow_name, idem_key)) |existing| {
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
            const slot = router.nsKeySlot(req.ns_id, workflow_name);
            break :blk shard.run_id_gen.next(.workflow, slot, &run_id_buf);
        };

        // Build namespace-qualified key for the runs map
        const run_ns_key = self.nsKey(req.ns_id, run_id_str) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };

        // Guard against ID collision (e.g. replayed entry with same key)
        if (self.runs.contains(run_ns_key)) {
            self.allocator.free(run_ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .conflict, "conflict: a run with this id already exists");
            return null;
        }
        self.allocator.free(run_ns_key);

        // The applier creates the run from the entry.
        const proposed = self.proposeStart(shard, req.ns_id, run_id_str, workflow_name, version, input, idempotency_key, "workflow_started") catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "run not persisted"));
            return null;
        };
        return proposed;
    }

    /// The index key for an idempotency key, any length the wire allows:
    /// the namespace's id, then the workflow's name length-prefixed, so no
    /// byte in a name can make two different triples the same key.
    fn idemKey(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, key: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}{d}:{s}{s}", .{ &ns.bytes(), wf_name.len, wf_name, key });
    }

    /// The key in `runs` of the run an idempotency key started for this
    /// workflow in this namespace, if one has applied.
    fn runKeyForIdempotencyKey(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, key: []const u8) ?[]const u8 {
        const ik = self.idemKey(ns, wf_name, key) catch return null;
        defer self.allocator.free(ik);
        return self.idem_index.get(ik);
    }

    fn runForIdempotencyKey(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, key: []const u8) ?*RunRecord {
        const run_key = self.runKeyForIdempotencyKey(ns, wf_name, key) orelse return null;
        return self.runs.getPtr(run_key);
    }

    fn indexIdempotencyKey(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, key: []const u8, run_key: []const u8) void {
        const ik = self.idemKey(ns, wf_name, key) catch {
            log.err("workflow {s}: idempotency key of run {f} not indexed (out of memory); a retry with it may start a second run", .{ wf_name, shown(run_key) });
            return;
        };
        if (self.idem_index.contains(ik)) {
            self.allocator.free(ik);
            return;
        }
        self.idem_index.put(self.allocator, ik, run_key) catch {
            self.allocator.free(ik);
            log.err("workflow {s}: idempotency key of run {f} not indexed (out of memory); a retry with it may start a second run", .{ wf_name, shown(run_key) });
        };
    }

    /// The caller holds `mu`.
    fn queueFirstStep(self: *WorkflowHandler, ns_key: []const u8, index: u64) void {
        const owned = self.allocator.dupe(u8, ns_key) catch {
            log.err("workflow run {f} started but cannot be queued for its first step; it stays at its start", .{shown(ns_key)});
            return;
        };
        self.started_to_advance.append(self.allocator, .{ .ns_key = owned, .index = index }) catch {
            self.allocator.free(owned);
            log.err("workflow run {f} started but cannot be queued for its first step; it stays at its start", .{shown(ns_key)});
        };
    }

    /// Whether the stepping duty has anything to do.
    pub fn hasDutyWork(self: *const WorkflowHandler) bool {
        return self.to_step.count() > 0 or self.started_to_advance.items.len > 0 or self.parked.count() > 0 or self.timers.count() > 0 or self.completion_retries.items.len > 0;
    }

    /// Most step_started entries one pass proposes, and most runs it looks
    /// at doing so.
    const STEPS_PER_PASS: usize = 64;
    const VISITS_PER_PASS: usize = 4 * STEPS_PER_PASS;

    /// Propose a step_started for runs started and not yet stepped, then
    /// take the first step of each run whose step_started applied, in log
    /// order; one whose cancel is in flight waits. Leader only: a step
    /// proposes.
    pub fn advanceStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        if (shard.raft_node.role != .leader) return;
        self.mu.lock();
        defer self.mu.unlock();
        self.proposeFirstSteps(shard);
        var keep: usize = 0;
        var i: usize = 0;
        // A step can start a child run here, which joins the queue behind
        // this pass and is kept, its start not yet applied.
        while (i < self.started_to_advance.items.len) : (i += 1) {
            const s = self.started_to_advance.items[i];
            const cancelling = if (self.runs.getPtr(s.ns_key)) |r| endingInFlight(shard, r) else false;
            if (shard.raft_node.last_applied < s.index or cancelling) {
                self.started_to_advance.items[keep] = s;
                keep += 1;
                continue;
            }
            defer self.allocator.free(s.ns_key);
            if (!self.runs.contains(s.ns_key)) {
                log.warn("shard {d}: workflow run {f} was started but its start created no run; nothing will step it", .{ shard.id, shown(s.ns_key) });
                continue;
            }
            self.advanceWorkflow(shard, s.ns_key);
        }
        self.started_to_advance.shrinkRetainingCapacity(keep);
    }

    /// A step_started for each run started and not yet stepped, up to
    /// `STEPS_PER_PASS`, once per term. Picks up where the last pass
    /// stopped. The caller holds `mu`.
    fn proposeFirstSteps(self: *WorkflowHandler, shard: *Shard) void {
        const term = shard.raft_node.current_term;
        if (self.stepping_term != term) {
            self.stepping.clearRetainingCapacity();
            self.stepping_term = term;
        }
        if (self.to_step.count() == 0) return;
        var proposed: usize = 0;
        var visited: usize = 0;
        var it = self.to_step.iterator();
        // A cursor past the end (the map grew or shrank) starts over.
        const start: u32 = if (self.to_step_cursor <= self.to_step.capacity()) self.to_step_cursor else 0;
        it.index = start;
        var wrapped = start == 0;
        defer self.to_step_cursor = it.index;
        while (proposed < STEPS_PER_PASS and visited < VISITS_PER_PASS) {
            if (wrapped and start != 0 and it.index >= start) break;
            const entry = it.next() orelse {
                // At the end: once more from the top, up to where it began.
                if (wrapped) break;
                wrapped = true;
                it = self.to_step.iterator();
                continue;
            };
            visited += 1;
            const ns_key = entry.key_ptr.*;
            if (self.stepping.contains(ns_key)) continue;
            if (self.runs.getPtr(ns_key)) |run| if (endingInFlight(shard, run)) continue;
            _ = persistence_mod.proposeEntryIn(shard, .workflow_step_started, entry_mod.Flags.NONE, keyNamespace(ns_key), ns_key, "") catch |err| {
                if (!shard.quietHalted(err)) log.warn("shard {d}: workflow run {f}: first step not logged: {s}; tried again next tick", .{ shard.id, shown(ns_key), @errorName(err) });
                break;
            };
            self.stepping.put(self.allocator, ns_key, {}) catch break;
            proposed += 1;
        }
    }

    /// This node stopped leading. A run whose first step was logged but not
    /// taken stays at its start: the next leader won't step it again. One
    /// whose step_started wasn't logged is the next leader's to step.
    pub fn dropStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.started_to_advance.items.len > 0 or self.stepping.count() > 0) log.warn("shard {d}: stopped leading with {d} workflow runs whose first step was logged and not taken, and {d} whose step_started was proposed and not applied; those whose entry commits stay at their start", .{ shard.id, self.started_to_advance.items.len, self.stepping.count() });
        for (self.started_to_advance.items) |s| self.allocator.free(s.ns_key);
        self.started_to_advance.clearRetainingCapacity();
        self.stepping.clearRetainingCapacity();
    }

    // ── SIGNAL ──────────────────────────────────────────────────────────

    fn handleSignal(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.nsKey(req.ns_id, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.getPtr(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // A finished run's history is its logged completion record.
        if (run.status.isTerminal()) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run already finished");
            return;
        }
        if (endingInFlight(shard, run)) {
            // Retried, it's delivered if the cancel never applies, and told
            // the run finished if it does.
            shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun("run is ending — retry"));
            return;
        }

        // Parse signal: [signal_len:u16][signal_type][payload...]
        if (req.value.len < 2) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "signal type is required");
            return;
        }

        const sig_len: usize = std.mem.readInt(u16, req.value[0..2], .little);
        if (req.value.len - 2 < sig_len) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed signal");
            return;
        }

        const signal_type = req.value[2 .. 2 + sig_len];
        const payload_data = if (2 + sig_len < req.value.len) req.value[2 + sig_len ..] else null;

        // A signal adds up to two history events. Refused once they'd leave
        // the completion record no room for its final event, output and tags.
        const history_after = completionBase(run) + 2 * Completion.eventSize("signal_received", signal_type);
        if (history_after + COMPLETION_HEADROOM > completionLimit(run_ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run history is full");
            return;
        }

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
                    self.advanceWorkflow(shard, resume_key);
                }
            }
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── CANCEL ──────────────────────────────────────────────────────────

    fn handleCancel(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return null;
        }

        const run_ns_key = self.nsKey(req.ns_id, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };
        defer self.allocator.free(run_ns_key);

        const run = self.runs.getPtr(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return null;
        };

        if (run.status.isTerminal()) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run already finished");
            return null;
        }
        if (endingInFlight(shard, run) and !run.ending_cancel) {
            // Retried, it's cancelled if the completion never applies, and
            // told the run finished if it does.
            shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun("run is ending — retry"));
            return null;
        }
        if (req.value.len > MAX_CANCEL_REASON) {
            var msg: [96]u8 = undefined;
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, std.fmt.bufPrint(&msg, "a cancel reason is at most {d} bytes, not {d}", .{ MAX_CANCEL_REASON, req.value.len }) catch "cancel reason too long");
            return null;
        }
        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        const reason = if (req.value.len > 0) req.value else "cancelled by user";
        const held_action = run.pending_action_run_id_owned;
        const held_on = run.pending_action_shard_id orelse shard.id;
        const proposed = self.proposeComplete(shard, run_ns_key, run, .cancelled, reason, now_ms) catch |err| {
            const status: proto.StatusCode, const text: []const u8 = switch (err) {
                error.OutOfMemory => .{ .internal_error, "out of memory" },
                error.BadKey => .{ .internal_error, "internal error: a run key without its namespace" },
                else => .{ persistence_mod.failureCode(err).toStatus(), persistence_mod.failureMessage(err, "the cancel was not persisted") },
            };
            // tryComplete fails only before or at the propose.
            shard.sendRefusal(conn, req.header.request_id, status, proto.notRun(text));
            return null;
        };
        defer self.allocator.free(proposed.value);
        // The run ends when the entry applies, and the client is answered
        // then. If it never does, the client is told the leader changed, and
        // the run carries on.
        run.ending_index = proposed.result.index;
        run.ending_term = proposed.result.term;
        run.ending_cancel = true;
        if (held_action) |rid| shard.actions_handler.cancelHeld(shard, held_on, rid);
        return proposed.result;
    }

    /// Whether a completion or cancel of this run may still apply: its entry is in this
    /// leader's log, as it was proposed, and hasn't applied. A node elected
    /// again keeps its entries, so a new term alone doesn't end it; an entry
    /// cut and replaced, or one that applied without ending the run, does.
    pub fn endingInFlight(shard: *const Shard, run: *const RunRecord) bool {
        if (run.ending_index == 0 or run.status.isTerminal()) return false;
        const raft = shard.raft_node;
        if (raft.role != .leader or raft.last_applied >= run.ending_index) return false;
        return (raft.log.entryTerm(run.ending_index) orelse return false) == run.ending_term;
    }

    // ── STATUS ──────────────────────────────────────────────────────────

    fn handleStatus(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.nsKey(req.ns_id, run_id) orelse {
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
        // Sized to the run. Each field is bounded where the run was made: the
        // start entry (key < 600 bytes, value <= 65000) and the completion
        // record (under 64 KiB) hold every one, so each length fits its field.
        const size = 2 + run.run_id_owned.len + 2 + run.workflow_name_owned.len + 2 + run.workflow_version_owned.len +
            1 + 2 + current_step.len + 4 + run.input_owned.len + 8 +
            1 + (if (run.started_at_ms != null) @as(usize, 8) else 0) + 1 + (if (run.completed_at_ms != null) @as(usize, 8) else 0) +
            1 + (if (run.wait_signal_type_owned) |sig| 2 + sig.len else 0) +
            1 + (if (run.output_owned) |o| 4 + o.len else 0);
        const wire_buf = self.allocator.alloc(u8, size) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "out of memory");
            return;
        };
        defer self.allocator.free(wire_buf);
        var fbs: std.Io.Writer = .fixed(wire_buf);
        const w = &fbs;
        w.writeInt(u16, @intCast(run.run_id_owned.len), .little) catch unreachable;
        w.writeAll(run.run_id_owned) catch unreachable;
        w.writeInt(u16, @intCast(run.workflow_name_owned.len), .little) catch unreachable;
        w.writeAll(run.workflow_name_owned) catch unreachable;
        w.writeInt(u16, @intCast(run.workflow_version_owned.len), .little) catch unreachable;
        w.writeAll(run.workflow_version_owned) catch unreachable;
        w.writeByte(@backingInt(run.status)) catch unreachable;
        w.writeInt(u16, @intCast(current_step.len), .little) catch unreachable;
        w.writeAll(current_step) catch unreachable;
        w.writeInt(u32, @intCast(run.input_owned.len), .little) catch unreachable;
        w.writeAll(run.input_owned) catch unreachable;
        w.writeInt(i64, run.created_at_ms, .little) catch unreachable;
        for ([_]?i64{ run.started_at_ms, run.completed_at_ms }) |at| {
            w.writeByte(@intFromBool(at != null)) catch unreachable;
            if (at) |v| w.writeInt(i64, v, .little) catch unreachable;
        }
        if (run.wait_signal_type_owned) |sig| {
            w.writeByte(1) catch unreachable;
            w.writeInt(u16, @intCast(sig.len), .little) catch unreachable;
            w.writeAll(sig) catch unreachable;
        } else {
            w.writeByte(0) catch unreachable;
        }
        // Optional: output (composed from definition's output mapping)
        if (run.output_owned) |output| {
            w.writeByte(1) catch unreachable;
            w.writeInt(u32, @intCast(output.len), .little) catch unreachable;
            w.writeAll(output) catch unreachable;
        } else {
            w.writeByte(0) catch unreachable;
        }
        std.debug.assert(fbs.end == size);

        shard.sendOkResponse(conn, req.header.request_id, fbs.buffered());
    }

    // ── HISTORY ─────────────────────────────────────────────────────────

    fn handleHistory(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run_ns_key = self.nsKey(req.ns_id, run_id) orelse {
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
        // Event types are literals; details are a signal type (u16 on the
        // wire), a cancel reason (MAX_CANCEL_REASON), step or terminal names
        // from a definition under 64 KiB, or, once complete, the completion
        // record's, itself under 64 KiB. So each length fits u16.
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

    pub const RunInfo = struct {
        run_id: []const u8,
        workflow: []const u8,
        status: []const u8,
        created_at: i64,
    };

    /// Collect runs matching filters from this handler's own runs map, on
    /// its own thread.
    pub fn collectMatchingRuns(
        self: *WorkflowHandler,
        runs_list: *std.ArrayListUnmanaged(RunInfo),
        ns: NamespaceId,
        workflow_name: []const u8,
        status_filter: ?[]const u8,
        search_lower: ?[]const u8,
    ) !void {
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            const run = entry.value_ptr;
            if (keyNamespace(entry.key_ptr.*) != ns) continue;
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

    const ListRunsQuery = struct {
        limit: u32,
        status: ?[]const u8,
        cursor: ?[]const u8,
        search: ?[]const u8,
    };

    /// Value: [limit:u32]([len:u16][bytes]) for status, cursor and search; an
    /// empty field is no filter. Null when any field runs past the value.
    fn parseListRunsQuery(value: []const u8) ?ListRunsQuery {
        if (value.len < 4) return null;
        var offset: usize = 4;
        var fields: [3]?[]const u8 = undefined;
        for (&fields) |*field| {
            if (value.len - offset < 2) return null;
            const len = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;
            if (value.len - offset < len) return null;
            field.* = if (len > 0) value[offset .. offset + len] else null;
            offset += len;
        }
        return .{
            .limit = std.mem.readInt(u32, value[0..4], .little),
            .status = fields[0],
            .cursor = fields[1],
            .search = fields[2],
        };
    }

    /// A workflow's runs, or every workflow's when the request names none,
    /// newest first, a page at a time (`list_runs`).
    fn handleListRuns(_: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const query = parseListRunsQuery(req.value) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid list-runs request");
            return;
        };
        const status = query.status orelse "";
        const search = query.search orelse "";
        if (status.len > system_ops.MAX_RUN_STATUS) return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: no run has a status that long");
        if (search.len > system_ops.MAX_RUN_SEARCH) return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: a run search is at most 4096 bytes");
        const after: ?list_runs.RunAt.Value = if (query.cursor) |c|
            list_runs.decodeCursor(c) orelse return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: a malformed cursor")
        else
            null;
        // Lowercased once for case-insensitive matching: all of it, so a
        // long query matches no more than it says.
        var search_buf: [system_ops.MAX_RUN_SEARCH]u8 = undefined;
        const q: list_runs.Query = .{
            .limit = if (query.limit == 0) list_runs.DEFAULT_LIMIT else @min(query.limit, list_runs.MAX_LIMIT),
            .status = status,
            .search = std.ascii.lowerString(search_buf[0..search.len], search),
            .after = after,
        };
        // One workflow's runs are on the shard that took the request.
        if (req.key.len > 0 or shard.shardCount() == 1) return list_runs.answerOwn(shard, conn, req, q);
        switch (list_runs.start(shard, conn, req, q)) {
            .asked => conn.response_deferred = true,
            .refused => |r| shard.sendRefusal(conn, req.header.request_id, r.status, r.refusal),
        }
    }

    // ── GET DEFINITION ──────────────────────────────────────────────────

    fn handleGetDefinition(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        const ns_key = self.nsKey(req.ns_id, name) orelse {
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

    // ── DISABLE / ENABLE ────────────────────────────────────────────────

    /// Logged, so every replica holds it and a restart or a new leader
    /// keeps it; answered when it applies.
    fn handleSetEnabled(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request, enabled: bool) ?persistence_mod.ProposeResult {
        if (req.key.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return null;
        }
        const ns_key = self.nsKey(req.ns_id, req.key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };
        defer self.allocator.free(ns_key);
        if (!self.definitions.contains(ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return null;
        }
        return persistence_mod.proposeEntryIn(shard, .workflow_enabled, entry_mod.Flags.NONE, req.ns_id, ns_key, &.{@intFromBool(enabled)}) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureCode(err).toStatus(), proto.notRun(persistence_mod.failureMessage(err, "not persisted")));
            return null;
        };
    }

    /// Apply a workflow_step_started entry: the run's first step is now
    /// its leader's to take, once. A run already stepped, finished or
    /// unknown is refused, and changes nothing.
    fn replayStepStarted(self: *WorkflowHandler, ns_key: []const u8, value: []const u8, out: *Result) persistence_mod.ApplyError!void {
        if (value.len != 0) return error.Halt;
        if (!self.to_step.remove(ns_key)) return out.refuse(.conflict, .unclassified, "conflict: the run's first step was taken");
    }

    /// Apply a workflow_enabled entry. The definition may have gone since it
    /// was proposed; then nothing changes.
    fn replayEnabled(self: *WorkflowHandler, ns_key: []const u8, value: []const u8, stamp_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        if (value.len != 1 or value[0] > 1) return error.Halt;
        if (!self.definitions.contains(ns_key)) return out.refuse(.not_found, .not_found, "workflow not found");
        if (value[0] == 1) {
            // Enabled again, its schedule counts slots from now: those
            // missed while disabled don't fire.
            if (self.disabled.fetchRemove(ns_key)) |old| {
                self.allocator.free(old.key);
                if (self.schedules.getPtr(ns_key)) |schedule| schedule.anchor_ms = stamp_ms;
            }
            return;
        }
        if (self.disabled.contains(ns_key)) return;
        const owned = self.allocator.dupe(u8, ns_key) catch return error.OutOfMemory;
        self.disabled.put(owned, {}) catch {
            self.allocator.free(owned);
            return error.OutOfMemory;
        };
    }

    // ── LIST DEFINITIONS ────────────────────────────────────────────────

    fn handleListDefinitions(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const DefInfo = struct { name: []const u8, version: []const u8, created_at: i64 };
        var defs: std.ArrayListUnmanaged(DefInfo) = .empty;
        defer defs.deinit(self.allocator);

        var dit = self.definitions.iterator();
        while (dit.next()) |entry| {
            const def = entry.value_ptr;
            if (keyNamespace(entry.key_ptr.*) != req.ns_id) continue;
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

        // Fewer definitions than a hash map counts (u32); each name and
        // version came from a create entry under 64 KiB.
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
    fn advanceWorkflow(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        if (run.status.isTerminal() or endingInFlight(shard, run)) return;
        const ns = keyNamespace(run_ns_key);

        // Look up the definition
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);

        const def_record = self.definitions.get(def_ns_key) orelse return;

        // Parse the definition to access the step graph
        var def = self.parseStored(def_record.yaml_owned, def_ns_key) catch return;
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
                        ) catch |err| switch (err) {
                            error.JsonTooDeep => {
                                self.completeRun(shard, run_ns_key, run, .failed, "step input: " ++ json_depth.TOO_DEEP_MESSAGE, now_ms);
                                return;
                            },
                            else => null,
                        };
                    }
                    const step_input: []const u8 = resolved_input orelse run.input_owned;

                    // Execute the step and determine outcome
                    const outcome_or_park = self.executeRunStep(shard, run, &def, run_step, step_input, step_label, ns, now_ms);

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

                    // Check retry on failure. A plan's executors retry by
                    // their own policies (the parser refuses a plan step's).
                    if (!run_step.isPlan() and
                        (std.mem.eql(u8, outcome, definition.StepOutcome.failure) or
                            std.mem.eql(u8, outcome, definition.StepOutcome.execution_failure)))
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

                    // A signal's match brings the run back here, so this is
                    // how any wait ends too.
                    self.clearWaitTimeout(run);
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
                        // Saturating: a timeout past the clock never fires.
                        run.wait_timeout_at_ms = now_ms +| timeout_ms;
                        self.armTimer(run, run.wait_timeout_at_ms);
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
        ns: NamespaceId,
        now_ms: i64,
    ) ?[]const u8 {
        if (run_step.isAction()) {
            return self.invokeAction(shard, run, run_step.targetName(), step_input, step_label, ns, true, now_ms);
        } else if (run_step.isPlan()) {
            return self.executePlan(shard, run, def, run_step.targetName(), step_input, step_label, ns, now_ms);
        } else if (run_step.isChildWorkflow()) {
            return self.executeChildWorkflow(shard, run, run_step, step_input, step_label, ns, now_ms);
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
        ns: NamespaceId,
        now_ms: i64,
    ) ?[]const u8 {
        // Strip the optional ":version" suffix — definitions are keyed by name.
        const raw = run_step.targetName();
        const child_name = if (std.mem.indexOfScalar(u8, raw, ':')) |i| raw[0..i] else raw;

        // The parent's run key, to find the run again after the child's
        // start is proposed.
        const parent_ns_key = self.nsKey(ns, run.run_id_owned) orelse
            return definition.StepOutcome.execution_failure;
        defer self.allocator.free(parent_ns_key);

        const child_ns_key = self.nsKey(ns, child_name) orelse
            return definition.StepOutcome.execution_failure;
        defer self.allocator.free(child_ns_key);

        // Resolve the shard/handler that owns the child workflow.
        const target_shard_id = self.resolveWorkflowShardId(shard, ns, child_name);
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
        const child_rid = switch (target_handler.spawnRun(target_shard, ns, child_name, step_input, "child_started", &child_id_buf, &child_index)) {
            .started => |rid| rid,
            .held => {
                const held_parent = self.runs.getPtr(parent_ns_key) orelse return definition.StepOutcome.execution_failure;
                return self.holdStepForRoom(held_parent, step_label, now_ms);
            },
            .failed => {
                if (self.runs.getPtr(parent_ns_key)) |p| p.waiting_for_room = false;
                return definition.StepOutcome.execution_failure;
            },
        };

        const parent = self.runs.getPtr(parent_ns_key) orelse return definition.StepOutcome.execution_failure;
        parent.waiting_for_room = false;
        // The index is in the target shard's log: known only when that is
        // this shard.
        self.parkForChild(parent, child_rid, step_label, child_name, target_shard_id, now_ms, if (is_local) child_index else 0);
        return null; // parked
    }

    /// Compute the shard ID that owns the given workflow (definition + runs).
    fn resolveWorkflowShardId(self: *WorkflowHandler, shard: *Shard, ns: NamespaceId, workflow_name: []const u8) u16 {
        _ = self;
        const hash = router.routeNsKey(ns, workflow_name);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard.id,
            .shard => |t| t.shard_id,
        };
    }

    /// Start a child run. Must be called with this handler's `mu` held.
    /// Returns the new run's id, written into `id_buf`, and its start's log
    /// index in `index`; the run exists once its start applies, and until
    /// then the parent waits as for any child that has not finished.
    fn spawnRun(
        self: *WorkflowHandler,
        shard: *Shard,
        ns: NamespaceId,
        wf_name: []const u8,
        input: []const u8,
        evt_type: []const u8,
        id_buf: *[32]u8,
        index: *u64,
    ) union(enum) { started: []const u8, held, failed } {
        const slot = router.nsKeySlot(ns, wf_name);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, id_buf);
        const proposed = self.proposeStart(shard, ns, run_id_str, wf_name, "latest", input, null, evt_type) catch |err| {
            if (err == error.Overloaded) return .held;
            if (!shard.quietHalted(err)) log.err("workflow {s}: run {s} not started: {s}", .{ wf_name, run_id_str, @errorName(err) });
            return .failed;
        };
        index.* = proposed.index;
        return .{ .started = run_id_str };
    }

    /// How long a step whose start the log was too full to take waits
    /// before it's tried again.
    pub const STEP_ROOM_RETRY_MS: i64 = 100;

    /// A step's start the log was too full to take: the run waits on its
    /// poll timer and runs the step again, rather than failing it. Its
    /// history says so once per wait.
    fn holdStepForRoom(self: *WorkflowHandler, run: *RunRecord, step_label: []const u8, now_ms: i64) ?[]const u8 {
        if (!run.waiting_for_room) self.addHistoryEvent(run, "step_waiting_for_room", step_label, now_ms);
        run.waiting_for_room = true;
        run.status = .waiting;
        run.poll_next_at_ms = now_ms + STEP_ROOM_RETRY_MS;
        self.armTimer(run, run.poll_next_at_ms);
        return null;
    }

    /// Whether the run `run` waits for, which the caller did not find, was
    /// lost: this shard proposed it and has applied that entry's index
    /// without creating it.
    fn pendingLost(shard: *Shard, run: *const RunRecord, on_shard: ?u16) bool {
        if ((on_shard orelse shard.id) != shard.id) return false;
        // No index: this shard held the start, and a step-down may have
        // dropped it. A start it has let go of is `starting` until it applies.
        if (run.pending_index == 0) {
            const rid = run.pending_action_run_id_owned orelse return false;
            return !shard.actions_handler.startPending(rid);
        }
        return shard.raft_node.last_applied >= run.pending_index;
    }

    /// Park a workflow run waiting for a child workflow to reach a terminal state.
    fn parkForChild(self: *WorkflowHandler, run: *RunRecord, child_run_id: []const u8, step_label: []const u8, child_name: []const u8, target_shard_id: u16, now_ms: i64, index: u64) void {
        run.status = .waiting;
        self.watchParked(run);
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
        ns: NamespaceId,
        now_ms: i64,
    ) ?[]const u8 {
        const plan = def.getPlan(plan_name) orelse {
            self.addHistoryEvent(run, "plan_not_found", plan_name, now_ms);
            return definition.StepOutcome.execution_failure;
        };

        // The pass's executor order: health-weighted sorts by score, all
        // other strategies use definition order.
        // The parser refuses a longer list.
        std.debug.assert(plan.executors.len <= plan_types.MAX_PLAN_EXECUTORS);
        const exec_count: u8 = @intCast(plan.executors.len);
        if (run.plan_order_len != exec_count) {
            for (run.plan_order[0..exec_count], 0..) |*slot, i| slot.* = @intCast(i);
            if (plan.selection == .health_weighted) {
                self.sortExecutorsByHealth(run.plan_order[0..exec_count], plan, ns, run.workflow_name_owned);
            }
            run.plan_order_len = exec_count;
        }
        const exec_order = run.plan_order[0..exec_count];

        while (run.plan_executor_idx < exec_count) {
            const natural_idx = exec_order[run.plan_executor_idx];
            const executor = plan.executors[natural_idx];

            // Health-weighted: check circuit breaker before attempting
            if (plan.selection == .health_weighted) {
                if (executor.breaker) |breaker_cfg| {
                    if (self.getOrCreateHealth(ns, run.workflow_name_owned, plan_name, executor.name)) |health| {
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

            // A retry for room is the same start, already recorded.
            if (!run.waiting_for_room) self.addHistoryEvent(run, "plan_executor_start", executor.name, now_ms);

            // Plan executor action_name stores the raw YAML "run" value
            // (e.g. "@actions/process-payment").  Strip the prefix so the
            // actions handler can look it up by its registered name.
            const resolved_action = if (std.mem.startsWith(u8, executor.action_name, "@actions/"))
                executor.action_name["@actions/".len..]
            else
                executor.action_name;

            // With another executor to try, a missing action doesn't hold the
            // step up; the last that can run waits, as a step with no plan does.
            const can_wait = !self.anotherExecutorCanRun(plan, exec_order[run.plan_executor_idx + 1 ..], ns, run.workflow_name_owned, plan_name, now_ms);
            const outcome = self.invokeAction(shard, run, resolved_action, step_input, step_label, ns, can_wait, now_ms);

            // Parked for async action — will resume via resumeFromAction
            if (outcome == null) return null;

            const success = std.mem.eql(u8, outcome.?, definition.StepOutcome.success);

            // Record health metrics for synchronous completions
            if (plan.selection == .health_weighted) {
                self.recordPlanHealth(ns, run.workflow_name_owned, plan_name, executor, success, now_ms);
            }

            // Success — reset plan state and return
            if (success) {
                self.addHistoryEvent(run, "plan_executor_success", executor.name, now_ms);
                resetPlanPass(run);
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
        resetPlanPass(run);
        return definition.StepOutcome.failure;
    }

    /// Whether one of `order`'s executors could run: its breaker, if it has
    /// one, isn't holding it back.
    fn anotherExecutorCanRun(self: *WorkflowHandler, plan: definition.InlinePlan, order: []const u8, ns: NamespaceId, workflow: []const u8, plan_name: []const u8, now_ms: i64) bool {
        for (order) |i| {
            if (plan.selection != .health_weighted) return true;
            const executor = plan.executors[i];
            const cfg = executor.breaker orelse return true;
            const health = self.getOrCreateHealth(ns, workflow, plan_name, executor.name) orelse return true;
            if (health.wouldAllowRequest(cfg, now_ms)) return true;
        }
        return false;
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
        ns: NamespaceId,
        can_wait: bool,
        now_ms: i64,
    ) ?[]const u8 {
        // Determine target shard via hash routing (deterministic: same as
        // action_register and action_invoke pre_route). This replaces the
        // old findActionShard scan.
        const target_shard_id = self.resolveActionShardId(shard, ns, action_name);
        const is_local = (target_shard_id == shard.id);

        if (is_local and ActionsHandler.actionExists(shard, ns, action_name)) {
            // Local shard: use invokeByName directly (same thread, no threading concerns)
            return self.invokeActionLocal(shard, run, ns, action_name, input, step_label, target_shard_id, now_ms);
        }

        // The action's shard creates the run on its own thread, through its
        // own log and applier, from a message we hand it. It holds the
        // message while the action isn't registered, so a step waits for
        // its action wherever the two are placed.
        var run_id_buf: [32]u8 = undefined;
        const slot = router.nsKeySlot(ns, action_name);
        const pre_run_id = shard.run_id_gen.next(.action, slot, &run_id_buf);
        const message = ActionsHandler.encodeStartRunMessage(shard.allocator, ns, pre_run_id, action_name, input, run.run_id_owned, run.workflow_name_owned, can_wait) orelse {
            return definition.StepOutcome.execution_failure;
        };
        if (is_local) {
            defer shard.allocator.free(message);
            shard.actions_handler.startRunFromInbox(shard, shard.id, message);
            self.parkForAction(run, pre_run_id, step_label, action_name, target_shard_id, now_ms, 0);
            return null;
        }
        const peer_mailboxes = shard.peer_mailboxes orelse {
            shard.allocator.free(message);
            return definition.StepOutcome.execution_failure;
        };
        if (target_shard_id >= peer_mailboxes.len) {
            shard.allocator.free(message);
            return definition.StepOutcome.execution_failure;
        }
        if (!peer_mailboxes[target_shard_id].inbox.send(.{
            .tag = .action_start,
            .src_shard = @intCast(shard.id),
            .payload_len = @intCast(message.len),
            .payload_ptr = message.ptr,
        })) {
            if (shard.shard_metrics) |sm| sm.recordCrossShardOverloaded(.engine);
            shard.allocator.free(message);
            return definition.StepOutcome.execution_failure;
        }

        // Park the workflow run, awaiting action completion. The owning shard
        // proposes the run and wakes workers when it applies.
        self.parkForAction(run, pre_run_id, step_label, action_name, target_shard_id, now_ms, 0);
        return null; // signals: parked
    }

    /// Same-shard action invocation (no threading concerns). The action is
    /// the one in the workflow's namespace.
    fn invokeActionLocal(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        ns: NamespaceId,
        action_name: []const u8,
        input: []const u8,
        step_label: []const u8,
        target_shard_id: u16,
        now_ms: i64,
    ) ?[]const u8 {
        var action_id_buf: [32]u8 = undefined;
        const invoked = switch (ActionsHandler.invokeByName(shard, ns, action_name, input, run.run_id_owned, run.workflow_name_owned, &action_id_buf)) {
            .invoked => |i| i,
            .held => return self.holdStepForRoom(run, step_label, now_ms),
            .missing, .failed => {
                run.waiting_for_room = false;
                return definition.StepOutcome.execution_failure;
            },
        };
        run.waiting_for_room = false;
        // The run exists once the invoke applies; `checkPendingActions`
        // resumes this one when it finishes.
        self.parkForAction(run, invoked.id, step_label, action_name, target_shard_id, now_ms, invoked.index);
        return null;
    }

    /// Resolve the StreamHandler that owns `stream_name` in namespace `ns`, using
    /// the same composite hash the dispatcher uses to route stream_append. The
    /// stream's data lives on that shard, so stream triggers must read from it
    /// rather than the local shard (which is merely wherever the triggering
    /// workflow definition happened to be created). Falls back to local.
    fn resolveStreamHandler(shard: *Shard, ns: NamespaceId, stream_name: []const u8) @TypeOf(shard.stream_handler) {
        const hash = router.routeNsKey(ns, stream_name);
        const target = shard.router.route(hash);
        switch (target) {
            .local => return shard.stream_handler,
            .shard => |t| {
                if (shard.peer_shards) |peers| {
                    return peers[t.shard_id].stream_handler;
                }
                return shard.stream_handler;
            },
        }
    }

    /// Compute the shard ID that owns the given action.
    fn resolveActionShardId(self: *WorkflowHandler, shard: *Shard, ns: NamespaceId, action_name: []const u8) u16 {
        _ = self;
        const hash = router.routeNsKey(ns, action_name);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard.id,
            .shard => |t| t.shard_id,
        };
    }

    /// How the action run a workflow run waits on ended, or null while it
    /// hasn't: this shard's store, or what the run's own shard told this
    /// one. A run on another shard not heard of yet is asked about, in case
    /// the message was lost.
    fn getActionRunResult(shard: *Shard, run_ns_key: []const u8, target_shard_id: ?u16, action_rid: []const u8) ?ActionsHandler.InternalRunResult {
        const tid = target_shard_id orelse shard.id;
        const ns = keyNamespace(run_ns_key);
        if (tid == shard.id) {
            if (shard.actions_handler.finished.getPtr(action_rid)) |f| return finishedResult(f.*);
            return ActionsHandler.getRunResult(shard, ns, action_rid);
        }
        const f = shard.actions_handler.finished.getPtr(action_rid) orelse {
            shard.actions_handler.askResult(shard, tid, ns, action_rid);
            return null;
        };
        return finishedResult(f.*);
    }

    /// A run its shard said ended, as a workflow reads it. Its invoke lost
    /// on that shard reads as `cancelled`: the step fails as for one lost
    /// here. One whose action never came says so in its outcome.
    fn finishedResult(f: ActionsHandler.Finished) ActionsHandler.InternalRunResult {
        return .{ .status = f.status, .output = f.output, .outcome = if (f.outcome.len > 0) f.outcome else if (f.status == .completed) definition.StepOutcome.success else null };
    }

    /// Park a workflow run waiting for an async action to complete.
    fn parkForAction(self: *WorkflowHandler, run: *RunRecord, action_run_id: []const u8, step_label: []const u8, action_name: []const u8, target_shard_id: u16, now_ms: i64, index: u64) void {
        run.status = .waiting;
        self.watchParked(run);
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
    /// parked for a backoff delay and re-invoked later (up to `max_attempts`).
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
            run.poll_next_at_ms = now_ms +| delay;
            self.armTimer(run, run.poll_next_at_ms);
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

    fn watchParked(self: *WorkflowHandler, run: *const RunRecord) void {
        self.parked.put(self.allocator, run.ns_key, {}) catch |err| {
            log.err("workflow run {f}: can't watch its step: {s}; trying again", .{ shown(run.ns_key), @errorName(err) });
            self.rewatch = true;
        };
    }

    fn armTimer(self: *WorkflowHandler, run: *const RunRecord, at_ms: i64) void {
        self.timers.push(self.allocator, .{ .at_ms = at_ms, .ns_key = run.ns_key }) catch |err| {
            log.err("workflow run {f}: can't keep its timer: {s}; trying again", .{ shown(run.ns_key), @errorName(err) });
            self.rewatch = true;
        };
    }

    /// Check parked runs for finished actions and children, and the timers
    /// now due. Called periodically by the shard's task scheduler.
    pub fn checkPendingActions(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.retryCompletions(shard, @import("stdx").time.milliTimestamp());

        const now_ms: i64 = @import("stdx").time.milliTimestamp();

        // Collect keys of runs that need resuming (can't modify map while iterating)
        var resume_keys: [64][]const u8 = undefined;
        var due_keys: [64][]const u8 = undefined;
        var child_keys: [64][]const u8 = undefined;
        var resume_count: usize = 0;
        var due_count: usize = 0;
        var child_count: usize = 0;

        if (self.rewatch and now_ms >= self.rewatch_at_ms) self.rewatchRuns(now_ms);

        // What's due and can't be taken this pass stays for the next.
        while (self.timers.peek()) |top| {
            if (top.at_ms > now_ms or due_count == due_keys.len) break;
            _ = self.timers.popIndex(0);
            const run = self.runs.getPtr(top.ns_key) orelse continue;
            if (timerDue(run, now_ms) != .none) {
                due_keys[due_count] = top.ns_key;
                due_count += 1;
            }
        }

        var it = self.parked.keyIterator();
        while (it.next()) |key_ptr| {
            const ns_key = key_ptr.*;
            const run = self.runs.getPtr(ns_key) orelse {
                self.parked.removeByPtr(key_ptr);
                continue;
            };
            if (run.status != .waiting or (run.pending_action_run_id_owned == null and run.pending_child_run_id_owned == null)) {
                self.parked.removeByPtr(key_ptr);
                continue;
            }
            if (endingInFlight(shard, run)) continue;

            // Check async action completion
            if (run.pending_action_run_id_owned) |action_rid| {
                if (shard.actions_handler.takeHeldNote(action_rid)) |action| {
                    defer self.allocator.free(action);
                    self.addHistoryEvent(run, "action_not_registered", action, now_ms);
                }
                const done = if (getActionRunResult(shard, ns_key, run.pending_action_shard_id, action_rid)) |result|
                    result.status == .completed or result.status == .failed or result.status == .timed_out or result.status == .cancelled
                else
                    pendingLost(shard, run, run.pending_action_shard_id);
                if (done and resume_count < resume_keys.len) {
                    resume_keys[resume_count] = ns_key;
                    resume_count += 1;
                }
            }

            // Check child workflow completion
            if (run.pending_child_run_id_owned) |child_rid| {
                if (self.childRunTerminal(shard, ns_key, child_rid, run)) {
                    if (child_count < child_keys.len) {
                        child_keys[child_count] = ns_key;
                        child_count += 1;
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

        // A run can have two entries due at once, and what ran before this
        // may have moved it on: each is asked again as it fires.
        for (due_keys[0..due_count]) |ns_key| {
            const r = self.runs.getPtr(ns_key) orelse continue;
            if (endingInFlight(shard, r)) {
                // Its timer stands if the cancel doesn't.
                if (timerDue(r, now_ms) != .none) self.armTimer(r, now_ms +| ENDING_RECHECK_MS);
                continue;
            }
            switch (timerDue(r, now_ms)) {
                .none => {},
                // Re-invoke the current step's action.
                .poll => {
                    r.poll_next_at_ms = 0;
                    r.status = .running;
                    if (r.wait_signal_type_owned) |sig| self.allocator.free(sig);
                    r.wait_signal_type_owned = null;
                    self.advanceWorkflow(shard, ns_key);
                },
                .wait => self.handleWaitTimeout(shard, ns_key, now_ms),
            }
        }

        // Its capacity is what the walk above costs.
        if (self.parked.count() == 0 and self.parked.capacity() > 64) self.parked.clearAndFree(self.allocator);
    }

    const ENDING_RECHECK_MS: i64 = 100;

    /// Which of a run's timers has come due, if any.
    fn timerDue(run: *const RunRecord, now_ms: i64) enum { none, poll, wait } {
        if (run.status != .waiting) return .none;
        if (run.poll_next_at_ms > 0 and now_ms >= run.poll_next_at_ms) return .poll;
        if (run.wait_timeout_at_ms > 0 and now_ms >= run.wait_timeout_at_ms and run.pending_action_run_id_owned == null) return .wait;
        return .none;
    }

    /// A wait is over: its deadline and target aren't the next wait's.
    fn clearWaitTimeout(self: *WorkflowHandler, run: *RunRecord) void {
        run.wait_timeout_at_ms = 0;
        if (run.wait_timeout_target_owned) |t| self.allocator.free(t);
        run.wait_timeout_target_owned = null;
    }

    /// An insert into `parked` or `timers` failed: what they should hold is
    /// read from the runs again, at most once a second until it fits.
    fn rewatchRuns(self: *WorkflowHandler, now_ms: i64) void {
        self.rewatch = false;
        var it = self.runs.valueIterator();
        while (it.next()) |run| {
            if (run.status != .waiting) continue;
            if (run.pending_action_run_id_owned != null or run.pending_child_run_id_owned != null) self.watchParked(run);
            if (run.poll_next_at_ms > 0) self.armTimer(run, run.poll_next_at_ms);
            if (run.wait_timeout_at_ms > 0) self.armTimer(run, run.wait_timeout_at_ms);
        }
        self.rewatch_at_ms = now_ms +| 1000;
    }

    /// Whether the child run referenced by a parent is in a terminal state.
    /// The child is in its parent's namespace.
    fn childRunTerminal(self: *WorkflowHandler, shard: *Shard, parent_ns_key: []const u8, child_run_id: []const u8, parent: *const RunRecord) bool {
        const child_shard_id = parent.pending_child_shard_id;
        const child_ns_key = self.nsKey(keyNamespace(parent_ns_key), child_run_id) orelse return false;
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

        const ns = keyNamespace(run_ns_key);
        const child_ns_key = self.nsKey(ns, child_rid) orelse return;
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
                log.warn("workflow run {f}: the child run {s} its step waits for was never created (its start was dropped); the step fails", .{ shown(run_ns_key), child_rid });
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
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return;
        var def = self.parseStored(def_record.yaml_owned, def_ns_key) catch return;
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
                self.advanceWorkflow(shard, key_copy);
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
        // What another shard told this one is taken here, before the run's
        // pending state (its id among it) is freed, and freed only once
        // the step's outcome, which may point into it, is spent.
        const remote = (run.pending_action_shard_id orelse shard.id) != shard.id;
        const taken: ?ActionsHandler.Finished = shard.actions_handler.takeFinished(action_rid);
        defer if (taken) |f| shard.actions_handler.freeFinished(f);
        const found: ?ActionsHandler.InternalRunResult = if (taken) |f| finishedResult(f) else if (remote) null else getActionRunResult(shard, run_ns_key, run.pending_action_shard_id, action_rid);
        if (found == null) {
            if (!pendingLost(shard, run, run.pending_action_shard_id)) return;
            log.warn("workflow run {f}: the action run {s} its step waits for was never created (its invoke was dropped); the step fails", .{ shown(run_ns_key), action_rid });
        }

        const outcome: []const u8 = if (found) |result| switch (result.status) {
            .completed => blk: {
                // The outcome the worker named, "success" when it named none.
                const action_outcome = if (result.outcome) |o| (if (o.len == 0) definition.StepOutcome.success else o) else definition.StepOutcome.success;
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
            else => if (result.outcome != null) definition.StepOutcome.target_not_found else definition.StepOutcome.execution_failure,
        } else definition.StepOutcome.execution_failure;

        // Record completion before clearing state (step_label points into pending_step_name_owned)
        run.status = .running;
        if (found) |result| {
            if (std.mem.eql(u8, outcome, definition.StepOutcome.target_not_found)) {
                self.addHistoryEvent(run, "action_not_found", result.output orelse "", now_ms);
            } else self.addHistoryEvent(run, "action_completed", outcome, now_ms);
        } else self.addHistoryEvent(run, "action_lost", outcome, now_ms);
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
        const ns = keyNamespace(run_ns_key);
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return;

        var def = self.parseStored(def_record.yaml_owned, def_ns_key) catch return;
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
                            self.recordPlanHealth(ns, run.workflow_name_owned, run_step.targetName(), attempted_executor, is_success, now_ms);
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
                            self.advanceWorkflow(shard, key_copy);
                            return;
                        }
                    }
                }

                // Plan executor fallback: if an async action failed and this is a plan step,
                // try the next executor before following the failure transition.
                if (run_step.isPlan() and (std.mem.eql(u8, outcome, definition.StepOutcome.failure) or
                    std.mem.eql(u8, outcome, definition.StepOutcome.execution_failure) or
                    std.mem.eql(u8, outcome, definition.StepOutcome.target_not_found)))
                {
                    if (def.getPlan(run_step.targetName())) |plan| {
                        // Use natural index — in health-weighted mode plan_executor_idx
                        // is the position in sorted order, not the index into plan.executors[].
                        std.debug.assert(plan.executors.len <= plan_types.MAX_PLAN_EXECUTORS);
                        const exec_count: u8 = @intCast(plan.executors.len);

                        // Check per-executor retry first
                        if (run.plan_executor_idx < exec_count and
                            run.plan_executor_natural_idx < plan.executors.len)
                        {
                            const executor = plan.executors[run.plan_executor_natural_idx];
                            // Retrying can't help an action that isn't registered.
                            const missing = std.mem.eql(u8, outcome, definition.StepOutcome.target_not_found);
                            if (if (missing) null else executor.retry) |retry| {
                                if (run.plan_executor_retry_count < retry.max_attempts) {
                                    run.plan_executor_retry_count += 1;
                                    self.addHistoryEvent(run, "plan_executor_retry", executor.name, now_ms);
                                    const key_copy = self.allocator.dupe(u8, run_ns_key) catch return;
                                    defer self.allocator.free(key_copy);
                                    self.advanceWorkflow(shard, key_copy);
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
                                self.advanceWorkflow(shard, key_copy);
                                return;
                            }
                        }
                        // All executors exhausted — reset and fall through to failure transition
                        resetPlanPass(run);
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
                self.advanceWorkflow(shard, key_copy);
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
            const def_ns_key = self.nsKey(keyNamespace(run_ns_key), run.workflow_name_owned) orelse return;
            defer self.allocator.free(def_ns_key);
            const def_record = self.definitions.get(def_ns_key) orelse return;

            var def = self.parseStored(def_record.yaml_owned, def_ns_key) catch return;
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
            self.advanceWorkflow(shard, key_copy);
        } else {
            // No timeout target configured — just time out the run
            if (run.wait_signal_type_owned) |s| self.allocator.free(s);
            run.wait_signal_type_owned = null;
            run.wait_timeout_at_ms = 0;
            self.completeRun(shard, run_ns_key, run, .timed_out, "signal timeout", now_ms);
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

    /// Ends a run once its completion record, built from the run's state
    /// now, applies. A record too large to log is trimmed to
    /// fit (see encodeTrimmed). One the log refuses is kept and proposed
    /// again from the run's state on a later tick, on a node that has
    /// stopped leading once it leads again (`completionRetryable`).
    fn completeRun(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) void {
        self.tryComplete(shard, run_ns_key, run, status, detail, now_ms) catch |err| {
            if (completionRetryable(err)) self.retryCompletionLater(run_ns_key, status, detail, now_ms, 0);
        };
    }

    /// Whether a completion the log refused may be proposed again: all but
    /// one that can never fit. Steps aren't logged, so a node that stopped
    /// leading keeps it for when it leads again; nothing else can end the run.
    fn completionRetryable(err: anyerror) bool {
        return switch (err) {
            error.PayloadTooLarge, error.KeyTooLarge, error.BadKey => false,
            else => true,
        };
    }

    /// Proposes the run's completion. The run ends when it applies, and is
    /// held meanwhile. The completion stays on the retry list until the run
    /// ends: steps aren't logged, so if the entry is cut, only this node
    /// can end the run, and it proposes it again once it leads.
    fn tryComplete(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) !void {
        const proposed = try self.proposeComplete(shard, run_ns_key, run, status, detail, now_ms);
        self.allocator.free(proposed.value);
        run.ending_index = proposed.result.index;
        run.ending_term = proposed.result.term;
        run.ending_cancel = false;
        self.keepCompletion(run_ns_key, status, detail, now_ms, 0);
    }

    /// Logs a run's completion record, without touching the run. The caller
    /// frees `value`.
    fn proposeComplete(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) !struct { value: []u8, result: persistence_mod.ProposeResult } {
        const ns = keyNamespace(run_ns_key);
        const output: ?[]const u8 = if (status == .completed) self.workflowOutput(run, ns) else null;
        defer if (output) |o| self.allocator.free(o);
        // Search tags re-computed now that $.steps.* and $.flo.* are available.
        const tags: ?[]const u8 = blk: {
            const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse break :blk null;
            defer self.allocator.free(def_ns_key);
            break :blk self.buildSearchTags(def_ns_key, run);
        };
        defer if (tags) |t| self.allocator.free(t);

        const limit = completionLimit(run_ns_key);
        const parts: Completion = .{
            .status = status,
            .output = output,
            .steps = if (run.step_outputs) |so| so.entries else &.{},
            .history = run.history.items,
            // History events record steps and signals taken on the leader,
            // off the log, so they keep the leader's clock until steps are
            // logged. The run's completion time is the entry's stamp.
            .last = .{ .event_type = terminalEventType(status), .detail = detail, .timestamp_ms = now_ms },
            .tags = tags,
        };
        const value = encodeCompletion(self.allocator, parts, limit) catch |err| switch (err) {
            error.OutOfMemory => return err,
            error.TooLarge => try encodeTrimmed(self.allocator, parts, limit),
        };
        errdefer self.allocator.free(value);

        const result = self.propose(shard, run_ns_key, value) catch |err| {
            if (!shard.quietHalted(err)) log.err("workflow run {f}: completion not persisted: {s}", .{ shown(run_ns_key), @errorName(err) });
            return err;
        };
        return .{ .value = value, .result = result };
    }

    fn proposeCompletion(shard: *Shard, run_ns_key: []const u8, value: []const u8) anyerror!persistence_mod.ProposeResult {
        return persistence_mod.proposeEntryIn(shard, .workflow_complete, entry_mod.Flags.NONE, keyNamespace(run_ns_key), run_ns_key, value);
    }

    /// A completion the log was too busy to take, retried from the run's
    /// state once `due_ms` passes.
    const PendingCompletion = struct {
        run_ns_key: []const u8,
        status: RunStatus,
        detail: []const u8,
        attempt: u32,
        due_ms: i64,
    };

    fn retryCompletionLater(self: *WorkflowHandler, run_ns_key: []const u8, status: RunStatus, detail: []const u8, now_ms: i64, attempt: u32) void {
        self.keepCompletion(run_ns_key, status, detail, now_ms + completionBackoff(attempt), attempt);
    }

    /// Keeps a run's completion to propose once `due_ms` passes, unless
    /// the run has ended or one is in flight by then.
    fn keepCompletion(self: *WorkflowHandler, run_ns_key: []const u8, status: RunStatus, detail: []const u8, due_ms: i64, attempt: u32) void {
        for (self.completion_retries.items) |*p| if (std.mem.eql(u8, p.run_ns_key, run_ns_key)) {
            // The latest completion is the one to propose again.
            if (p.status != status or !std.mem.eql(u8, p.detail, detail)) {
                const owned = self.allocator.dupe(u8, detail) catch return log.err("workflow run {f}: completion dropped: out of memory", .{shown(run_ns_key)});
                self.allocator.free(p.detail);
                p.detail = owned;
                p.status = status;
            }
            p.attempt = attempt;
            p.due_ms = due_ms;
            return;
        };
        const key = self.allocator.dupe(u8, run_ns_key) catch return log.err("workflow run {f}: completion dropped: out of memory", .{shown(run_ns_key)});
        const owned_detail = self.allocator.dupe(u8, detail) catch {
            self.allocator.free(key);
            return log.err("workflow run {f}: completion dropped: out of memory", .{shown(run_ns_key)});
        };
        self.completion_retries.append(self.allocator, .{ .run_ns_key = key, .status = status, .detail = owned_detail, .attempt = attempt, .due_ms = due_ms }) catch {
            self.allocator.free(key);
            self.allocator.free(owned_detail);
            log.err("workflow run {f}: completion dropped: out of memory", .{shown(run_ns_key)});
        };
    }

    fn dropCompletionRetry(self: *WorkflowHandler, run_ns_key: []const u8) void {
        for (self.completion_retries.items, 0..) |p, i| if (std.mem.eql(u8, p.run_ns_key, run_ns_key)) {
            _ = self.completion_retries.swapRemove(i);
            self.allocator.free(p.run_ns_key);
            self.allocator.free(p.detail);
            return;
        };
    }

    /// 100 ms doubling to 5 s.
    fn completionBackoff(attempt: u32) i64 {
        return @min(@as(i64, 100) << @intCast(@min(attempt, 6)), 5000);
    }

    /// Completes the runs whose retry is due, encoding each from its state now.
    fn retryCompletions(self: *WorkflowHandler, shard: *Shard, now_ms: i64) void {
        var i: usize = 0;
        while (i < self.completion_retries.items.len) {
            const p = self.completion_retries.items[i];
            const run_now = self.runs.getPtr(p.run_ns_key);
            if (run_now == null or run_now.?.status.isTerminal()) {
                _ = self.completion_retries.swapRemove(i);
                self.allocator.free(p.run_ns_key);
                self.allocator.free(p.detail);
                continue;
            }
            if (p.due_ms > now_ms or endingInFlight(shard, run_now.?)) {
                i += 1;
                continue;
            }
            _ = self.completion_retries.swapRemove(i);
            defer self.allocator.free(p.run_ns_key);
            defer self.allocator.free(p.detail);
            const run = self.runs.getPtr(p.run_ns_key) orelse continue;
            if (run.status.isTerminal()) continue;
            self.tryComplete(shard, p.run_ns_key, run, p.status, p.detail, now_ms) catch |err| {
                if (completionRetryable(err)) self.retryCompletionLater(p.run_ns_key, p.status, p.detail, now_ms, p.attempt +| 1);
            };
        }
    }

    fn terminalEventType(status: RunStatus) []const u8 {
        return switch (status) {
            .completed => "workflow_completed",
            .failed => "workflow_failed",
            .cancelled => "workflow_cancelled",
            .timed_out => "workflow_timed_out",
            else => "workflow_ended",
        };
    }

    /// Resolve the workflow's `output` mapping (same format as a step's input_mapping).
    /// Looks up the definition, parses the YAML, and if `output:` is declared,
    /// resolves the JSON mapping via PathResolver and stores it on the run.
    ///
    /// Two modes:
    ///  1. `output: "$.steps.ship.output"` → direct path passthrough (raw bytes).
    ///  2. `output: '{"key": "$.path"}'` → JSON mapping with interpolation.
    /// If `output` is not declared, workflow output remains null.
    fn workflowOutput(self: *WorkflowHandler, run: *RunRecord, ns: NamespaceId) ?[]const u8 {
        // Look up the workflow definition
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return null;
        defer self.allocator.free(def_ns_key);
        const def_record = self.definitions.get(def_ns_key) orelse return null;

        // Parse definition to access the output mapping
        var def = self.parseStored(def_record.yaml_owned, def_ns_key) catch return null;
        defer def.deinit(self.allocator);

        const output_expr = def.output orelse return null;

        const trimmed = std.mem.trim(u8, output_expr, " \t");
        if (trimmed.len < 2) return null;

        // Mode 1: Direct path passthrough ("$.steps.ship.output" or "$.input")
        if (trimmed[0] == '$' and trimmed[1] == '.') {
            var resolver = jsonpath.PathResolver.init(
                self.allocator,
                run.input_owned,
                if (run.step_outputs) |*so| so else null,
                run.run_id_owned,
            );
            return resolver.resolve(trimmed) catch |err| {
                log.warn("Workflow output path resolution failed for '{s}': {}", .{ trimmed, err });
                return null;
            };
        }

        // Mode 2: JSON mapping — resolve all $.path references within the object
        return jsonpath.resolveInput(
            self.allocator,
            output_expr,
            run.input_owned,
            if (run.step_outputs) |*so| so else null,
            run.run_id_owned,
        ) catch |err| {
            log.warn("Workflow output mapping resolution failed: {}", .{err});
            return null;
        };
    }

    /// Build pre-computed search attribute JSON from definition + run data.
    /// Returns an owned JSON string like `{"customer_id":"C-789","order_amount":149.99}`
    /// or null if the definition has no search attributes.
    fn buildSearchTags(self: *WorkflowHandler, def_ns_key: []const u8, run: *RunRecord) ?[]const u8 {
        const def_rec = self.definitions.get(def_ns_key) orelse return null;
        var def = self.parseStored(def_rec.yaml_owned, def_ns_key) catch return null;
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

    /// Start the next pass through a plan from its first executor.
    fn resetPlanPass(run: *RunRecord) void {
        run.plan_executor_idx = 0;
        run.plan_executor_natural_idx = 0;
        run.plan_executor_retry_count = 0;
        run.plan_order_len = 0;
    }

    /// Update the run's current step pointer.
    fn setCurrentStep(self: *WorkflowHandler, run: *RunRecord, step_name: []const u8) void {
        if (run.current_step_name_owned) |old| self.allocator.free(old);
        run.current_step_name_owned = self.allocator.dupe(u8, step_name) catch null;
        // Reset plan state on step transition
        resetPlanPass(run);
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
        ns: NamespaceId,
        workflow_name: []const u8,
        trigger: definition.StreamTriggerDef,
    ) void {
        const trigger_key = self.nsKey(ns, workflow_name) orelse return;

        // A re-created definition keeps its place in the stream: starting
        // over at zero would run every consumed event again. A trigger moved
        // to another stream starts that stream from its beginning.
        var cursor_ts: u64 = 0;
        var cursor_seq: u64 = 0;
        const stream_ns = self.otherStreamNamespace(ns, trigger.namespace);
        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            const same_ns = if (state.stream_namespace_owned) |a| (if (stream_ns) |b| std.mem.eql(u8, a, b) else false) else stream_ns == null;
            if (std.mem.eql(u8, state.stream_name_owned, trigger.stream) and same_ns) {
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
        const owned_stream = self.allocator.dupe(u8, trigger.stream) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            return;
        };
        const owned_stream_ns: ?[]const u8 = if (stream_ns) |n| self.allocator.dupe(u8, n) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_stream);
            return;
        } else null;

        self.stream_triggers.put(trigger_key, .{
            .workflow_name_owned = owned_wf,
            .ns = ns,
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
            self.allocator.free(owned_stream);
            if (owned_stream_ns) |n| self.allocator.free(n);
            return;
        };
        _ = global_stream_trigger_count.fetchAdd(1, .monotonic);
    }

    /// The namespace a trigger names for its stream, or null when that is
    /// the workflow's own: named, empty or left out, all three read the same
    /// stream, so a redefinition moving between them keeps its cursor.
    fn otherStreamNamespace(self: *const WorkflowHandler, ns: NamespaceId, named: ?[]const u8) ?[]const u8 {
        const name = named orelse return null;
        if (name.len == 0) return null;
        const own = if (self.namespaces) |h| h.nameOf(ns) orelse h.table.nameOf(ns) else null;
        if (own) |o| if (std.mem.eql(u8, o, name)) return null;
        return name;
    }

    /// Remove the stream trigger for a workflow definition.
    fn unregisterStreamTrigger(self: *WorkflowHandler, ns: NamespaceId, workflow_name: []const u8) void {
        const trigger_key = self.nsKey(ns, workflow_name) orelse return;
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
        ns: NamespaceId,
        workflow_name: []const u8,
        schedule: definition.ScheduleDef,
        anchor_ms: i64,
    ) void {
        if (schedule.paused) return;
        // The validator lets through exactly one of a valid cron or an
        // interval; a stored definition holds the same.
        var parsed: ?cron_mod.Cron = null;
        const interval_ms: i64 = if (schedule.cron_expr) |expr| blk: {
            parsed = cron_mod.Cron.parse(expr) orelse return;
            break :blk 0;
        } else schedule.interval_ms orelse return;
        if (parsed == null and interval_ms <= 0) return;

        const sched_key = self.nsKey(ns, workflow_name) orelse return;

        // A redefinition replaces the schedule; the slots it fired stay
        // fired.
        var fired_ms: i64 = std.math.minInt(i64);
        if (self.schedules.fetchRemove(sched_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            fired_ms = state.fired_ms;
            self.freeScheduleState(&state);
        }

        const owned_wf = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(sched_key);
            return;
        };
        const owned_input: ?[]const u8 = if (schedule.input) |inp|
            self.allocator.dupe(u8, inp) catch null
        else
            null;

        self.schedules.put(sched_key, .{
            .workflow_name_owned = owned_wf,
            .ns = ns,
            .interval_ms = interval_ms,
            .cron = parsed,
            .max_concurrent = schedule.max_concurrent,
            .input_owned = owned_input,
            .anchor_ms = anchor_ms,
            .fired_ms = fired_ms,
        }) catch {
            self.allocator.free(sched_key);
            self.allocator.free(owned_wf);
            if (owned_input) |inp| self.allocator.free(inp);
        };
    }

    fn unregisterSchedule(self: *WorkflowHandler, ns: NamespaceId, workflow_name: []const u8) void {
        const sched_key = self.nsKey(ns, workflow_name) orelse return;
        defer self.allocator.free(sched_key);

        if (self.schedules.fetchRemove(sched_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeScheduleState(&state);
        }
    }

    /// The latest slot due at `now_ms`, or null before the first. Slot k
    /// is at anchor + k·interval; slot 0 is the anchor itself. A cron's
    /// slots are its fire times at or after the anchor.
    fn dueSlot(schedule: *ScheduleState, now_ms: i64) ?i64 {
        if (now_ms < schedule.anchor_ms) return null;
        if (schedule.cron) |c| {
            if (now_ms >= schedule.cron_until_ms) {
                schedule.cron_latest_ms = c.latest(now_ms);
                schedule.cron_until_ms = now_ms - @mod(now_ms, 60_000) + 60_000;
            }
            const latest = schedule.cron_latest_ms orelse return null;
            return if (latest >= schedule.anchor_ms) latest else null;
        }
        const k = @divFloor(now_ms - schedule.anchor_ms, schedule.interval_ms);
        return schedule.anchor_ms + k * schedule.interval_ms;
    }

    /// Start a run for each schedule whose latest slot hasn't fired. Slots
    /// missed while no leader was ready fire once, as the latest one: a
    /// failover or restart neither bursts nor skips. A leader whose clock
    /// runs ahead fires early by as much.
    pub fn tickSchedules(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        const term = shard.raft_node.current_term;

        var it = self.schedules.iterator();
        while (it.next()) |entry| {
            const schedule = entry.value_ptr;
            const slot = dueSlot(schedule, now_ms) orelse continue;
            if (slot <= schedule.fired_ms) continue;
            if (schedule.proposed_term == term and slot <= schedule.proposed_ms) continue;

            // Skip disabled workflows
            if (self.disabled.contains(entry.key_ptr.*)) continue;

            // Check max_concurrent
            if (schedule.max_concurrent > 0) {
                if ((self.active_runs.get(entry.key_ptr.*) orelse 0) >= schedule.max_concurrent) continue;
            }

            if (self.startRunFromSchedule(shard, schedule, slot)) {
                schedule.proposed_ms = slot;
                schedule.proposed_term = term;
            }
        }
    }

    /// Start a workflow run for a schedule's slot. Its idempotency key
    /// names the schedule and the slot's time, so a slot starts one run
    /// however many leaders propose it. The time alone names the slot: a
    /// re-anchor moves later slots off the old ones' times, and `fired_ms`
    /// outlives it. Returns whether it was proposed.
    fn startRunFromSchedule(
        self: *WorkflowHandler,
        shard: *Shard,
        schedule: *const ScheduleState,
        slot_ms: i64,
    ) bool {
        const input = schedule.input_owned orelse "{}";

        var key_buf: [512]u8 = undefined;
        const idem = std.fmt.bufPrint(&key_buf, "schedule:{d}:{s}:{d}", .{ schedule.ns.int(), schedule.workflow_name_owned, slot_ms }) catch return false;

        // Generate run ID with the definition's slot in its bits
        var run_id_buf: [32]u8 = undefined;
        const slot = router.nsKeySlot(schedule.ns, schedule.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, &run_id_buf);

        return self.proposeRun(shard, schedule.ns, run_id_str, schedule.workflow_name_owned, "latest", input, idem, "schedule_started") != null;
    }

    /// A schedule's start applied: its slot, the key's last part, has
    /// fired.
    fn restoreScheduleSlot(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, idem: []const u8) void {
        const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
        const slot = std.fmt.parseInt(i64, idem[colon + 1 ..], 10) catch return;
        const key = self.nsKey(ns, wf_name) orelse return;
        defer self.allocator.free(key);
        const schedule = self.schedules.getPtr(key) orelse return;
        schedule.fired_ms = @max(schedule.fired_ms, slot);
    }

    /// Poll all active stream triggers and start workflow runs for new events.
    /// Called from shard.tick() on every reactor cycle.
    pub fn tickStreamTriggers(self: *WorkflowHandler, shard: *Shard) void {
        const now_ms = @import("stdx").time.milliTimestamp();

        // A `stream_appended` wake flag (push-wake) forces an immediate poll of
        // every trigger this tick, bypassing each trigger's poll-interval timer.
        // The timer remains as a fallback for events that arrive without a
        // notification (e.g. appended before the trigger registered).
        const forced = self.triggers_dirty;
        self.triggers_dirty = false;

        var it = self.stream_triggers.iterator();
        while (it.next()) |entry| tick: {
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
            // A namespace the trigger names that isn't known here yet
            // reads as empty.
            const stream_ns = if (trigger.stream_namespace_owned) |n| shard.namespaceId(n) else trigger.ns;
            const stream_handler = resolveStreamHandler(shard, stream_ns, trigger.stream_name_owned);
            // As many events as this shard can start runs for now; with room
            // for none the trigger waits, its cursor where it is.
            const raft = shard.raft_node;
            const room: usize = @intCast(raft_node_mod.MAX_OUTSTANDING -| (raft.log.lastIndex() -| raft.last_applied));
            if (room == 0) continue;
            const result = stream_handler.readPayloadsForStreamIn(
                trigger.stream_name_owned,
                stream_ns,
                cursor,
                @min(@max(batch_limit, 1) * 10, room * @max(batch_limit, 1)), // read ahead for batching
            );
            defer result.deinit(stream_handler.allocator);
            if (result.payloads.len == 0) {
                // Appends that couldn't be read still move the cursor past
                // them (the stream handler logged them), so the trigger
                // doesn't stall.
                trigger.stream_cursor_ts = result.last_id.timestamp_ms;
                trigger.stream_cursor_seq = result.last_id.sequence;
                continue;
            }

            // Start one run per event (or per batch if batch_size > 1). A
            // start the log refuses (full for now) leaves the cursor where the
            // read began: the next tick reads these events again, and those
            // already started are the same runs, by their idempotency keys.
            if (trigger.batch_size <= 1) {
                // One run per event
                for (result.payloads, result.ids) |payload, event_id| {
                    if (!self.startRunFromTrigger(shard, trigger, payload, event_id)) break :tick;
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
                    if (!self.startRunFromTrigger(shard, trigger, batch_json, result.ids[end - 1])) break :tick;
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
    /// unconsumed ones. False when the start was refused for now: the
    /// trigger reads the event again next tick.
    fn startRunFromTrigger(
        self: *WorkflowHandler,
        shard: *Shard,
        trigger: *const StreamTriggerState,
        input: []const u8,
        event_id: StreamID,
    ) bool {
        // Generate run ID with the definition's slot in its bits
        var run_id_buf: [32]u8 = undefined;
        const slot = router.nsKeySlot(trigger.ns, trigger.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, &run_id_buf);

        // Room for the longest stream name a trigger may have, and both
        // numbers at their widest.
        var idem_buf: [parser.MAX_TRIGGER_STREAM + "trigger:@:".len + 2 * 20]u8 = undefined;
        const idem = std.fmt.bufPrint(&idem_buf, "trigger:{s}@{d}:{d}", .{ trigger.stream_name_owned, event_id.timestamp_ms, event_id.sequence }) catch {
            log.err("workflow {s}: trigger on stream of {d} bytes not started: its key doesn't fit", .{ trigger.workflow_name_owned, trigger.stream_name_owned.len });
            return true;
        };

        // The first-step queue is shared with a parent run on another
        // shard starting a child here, which holds this handler's lock.
        self.mu.lock();
        defer self.mu.unlock();
        _ = self.proposeStart(shard, trigger.ns, run_id_str, trigger.workflow_name_owned, "latest", input, idem, "trigger_started") catch |err| {
            if (!shard.quietHalted(err)) log.err("workflow {s}: run {s} not started: {s}", .{ trigger.workflow_name_owned, run_id_str, @errorName(err) });
            // One that can pass (the log full for now, this node not leading
            // or not writing) is read again; one that can't is passed over,
            // or it would hold the trigger for good.
            return switch (err) {
                error.Overloaded, error.NotLeader, error.WritesStopped, error.Halted => false,
                else => true,
            };
        };
        return true;
    }

    /// Advance a stream trigger's cursor to the event a restored run came
    /// from. Runs apply in log order after their definition, so by the end
    /// of replay the cursor sits at the last event that started a run.
    fn restoreTriggerCursor(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, idem: []const u8) void {
        const at = std.mem.lastIndexOfScalar(u8, idem, '@') orelse return;
        const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
        if (colon <= at) return;
        const ts = std.fmt.parseInt(u64, idem[at + 1 .. colon], 10) catch return;
        const seq = std.fmt.parseInt(u64, idem[colon + 1 ..], 10) catch return;
        const key = self.nsKey(ns, wf_name) orelse return;
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
        ns: NamespaceId,
        workflow: []const u8,
        plan_name: []const u8,
        executor_name: []const u8,
    ) ?*plan_types.ExecutorHealth {
        const key = healthKey(self.allocator, ns, workflow, plan_name, executor_name) catch return null;

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
        ns: NamespaceId,
        workflow: []const u8,
        plan_name: []const u8,
        executor: definition.ExecutorConfig,
        success: bool,
        now_ms: i64,
    ) void {
        const health = self.getOrCreateHealth(ns, workflow, plan_name, executor.name) orelse return;
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
        ns: NamespaceId,
        workflow: []const u8,
    ) void {
        // Insertion sort — max 32 executors, O(n²) is fine
        var i: usize = 1;
        while (i < order.len) : (i += 1) {
            var j = i;
            while (j > 0) {
                const a_score = self.getExecutorHealthScore(ns, workflow, plan.name, plan.executors[order[j]].name);
                const b_score = self.getExecutorHealthScore(ns, workflow, plan.name, plan.executors[order[j - 1]].name);
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
        ns: NamespaceId,
        workflow: []const u8,
        plan_name: []const u8,
        executor_name: []const u8,
    ) f64 {
        const key = healthKey(self.allocator, ns, workflow, plan_name, executor_name) catch return 1.0;
        defer self.allocator.free(key);
        if (self.plan_health.get(key)) |health| {
            return health.healthScore();
        }
        return 1.0;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// The run id a start's value chooses, if it chooses one and reads that
    /// far: `[ver_len:u16][ver][has_idem:u8][key_len:u16]?[key]?[has_rid:u8][rid_len:u16]?[rid]?`.
    /// `handleStart` reads the whole value and refuses a malformed one.
    pub fn startChosenRunId(value: []const u8) ?[]const u8 {
        var off: usize = 0;
        if (value.len < 2) return null;
        off += 2 + std.mem.readInt(u16, value[0..2], .little);
        if (off >= value.len) return null;
        const has_idem = value[off];
        off += 1;
        if (has_idem == 1) {
            if (off + 2 > value.len) return null;
            off += 2 + std.mem.readInt(u16, value[off..][0..2], .little);
        }
        if (off >= value.len or value[off] != 1) return null;
        off += 1;
        if (off + 2 > value.len) return null;
        const len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + len > value.len or len == 0) return null;
        return value[off .. off + len];
    }

    /// Whether this shard holds run `run_id` in namespace `ns`.
    pub fn holdsRun(self: *const WorkflowHandler, ns: NamespaceId, run_id: []const u8) bool {
        // A run's start entry holds a key of at most `MAX_START_KEY` bytes,
        // so a longer id is no run's.
        var buf: [MAX_START_KEY]u8 = undefined;
        if (KEY_PREFIX + run_id.len > buf.len) return false;
        return self.runs.contains(writeKey(&buf, ns, run_id));
    }

    /// A key for the maps (`writeKey`), allocated; null when out of memory.
    pub fn nsKey(self: *WorkflowHandler, ns: NamespaceId, name: []const u8) ?[]const u8 {
        const buf = self.allocator.alloc(u8, KEY_PREFIX + name.len) catch return null;
        return writeKey(buf, ns, name);
    }

    /// The key a plan executor's health is kept under: each name
    /// length-prefixed, so no byte in one can make two executors' keys meet.
    fn healthKey(allocator: Allocator, ns: NamespaceId, workflow: []const u8, plan_name: []const u8, executor_name: []const u8) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}{d}:{s}{d}:{s}{s}", .{ &ns.bytes(), workflow.len, workflow, plan_name.len, plan_name, executor_name });
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

    /// Persist a workflow_create entry to the UAL so the definition survives
    /// restart, keyed as the definitions map keys it.
    fn proposeCreate(self: *WorkflowHandler, shard: *Shard, ns: NamespaceId, name: []const u8, yaml: []const u8) !persistence_mod.ProposeResult {
        _ = self;
        var key_buf: [MAX_START_KEY]u8 = undefined;
        if (KEY_PREFIX + name.len > key_buf.len) return error.KeyTooLarge;
        return persistence_mod.proposeEntryIn(shard, .workflow_create, entry_mod.Flags.NONE, ns, writeKey(&key_buf, ns, name), yaml);
    }

    /// Start a run for a producer (a schedule, a trigger, a parent run):
    /// the start is proposed and the tick takes its first step once it
    /// applies, as for a client's. Returns the start's log index. The caller holds `mu`.
    fn proposeRun(
        self: *WorkflowHandler,
        shard: *Shard,
        ns: NamespaceId,
        run_id: []const u8,
        wf_name: []const u8,
        version: []const u8,
        input: []const u8,
        idempotency_key: ?[]const u8,
        event_type: []const u8,
    ) ?u64 {
        const proposed = self.proposeStart(shard, ns, run_id, wf_name, version, input, idempotency_key, event_type) catch |err| {
            if (!shard.quietHalted(err)) log.err("workflow {s}: run {s} not started: {s}", .{ wf_name, run_id, @errorName(err) });
            return null;
        };
        return proposed.index;
    }

    /// Start entry, keyed as the runs map keys the run.
    /// Value: [wf_name_len:u16][wf_name][ver_len:u16][ver][status:u8][created_at_ms:i64]
    ///   [event_len:u16][event_type][idem_len:u16][idempotency_key][input...]
    /// The event type is the run's first history event (manual, schedule,
    /// trigger or child start); search tags are derived by the applier.
    fn proposeStart(
        self: *WorkflowHandler,
        shard: *Shard,
        ns: NamespaceId,
        run_id: []const u8,
        wf_name: []const u8,
        version: []const u8,
        input: []const u8,
        idempotency_key: ?[]const u8,
        event_type: []const u8,
    ) !persistence_mod.ProposeResult {
        _ = self;
        var ns_key_buf: [MAX_START_KEY]u8 = undefined;
        if (KEY_PREFIX + run_id.len > ns_key_buf.len) return error.KeyTooLarge;
        const ns_key = writeKey(&ns_key_buf, ns, run_id);
        const idem = idempotency_key orelse "";
        const value_len = 2 + wf_name.len + 2 + version.len + 1 + 2 + event_type.len + 2 + idem.len + input.len;
        // Checked whole first, so each length below fits u16.
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
        // The run is created at the entry's stamp, by the applier.
        value_buf[off] = @backingInt(RunStatus.running);
        off += 1;
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
        return persistence_mod.proposeEntryIn(shard, .workflow_start, entry_mod.Flags.NONE, ns, ns_key, value_buf[0..off]);
    }

    /// A run's completion record, gathered without touching the run.
    /// The run completes at its entry's stamp, which the applier sets.
    const Completion = struct {
        status: RunStatus,
        output: ?[]const u8,
        steps: []const wf_types.StepOutput,
        /// Events before `last`, oldest first.
        history: []const HistoryEvent,
        /// Earlier events left out to fit; recorded as one marker event.
        dropped: usize = 0,
        last: Event,
        tags: ?[]const u8,

        const Event = struct { event_type: []const u8, detail: []const u8, timestamp_ms: i64 };

        fn eventSize(event_type: []const u8, detail: []const u8) usize {
            return 2 + event_type.len + 2 + detail.len + 8;
        }

        /// Encoded size. Every length field fits when this is at most a log
        /// entry, which is under 64 KiB.
        fn size(c: Completion, marker_detail: []const u8) usize {
            var n: usize = 1 + 1 + 2 + 2 + 2;
            if (c.output) |o| n += 4 + o.len;
            for (c.steps) |e| n += 2 + e.step_name.len + 2 + e.outcome.len + 4 + e.output.len;
            if (c.dropped > 0) n += eventSize(MARKER_EVENT, marker_detail);
            for (c.history) |e| n += eventSize(e.event_type_owned, e.detail_owned);
            n += eventSize(c.last.event_type, c.last.detail);
            if (c.tags) |t| n += t.len;
            return n;
        }
    };

    const MARKER_EVENT = "history_trimmed";

    /// A cancel reason goes into the run's history and its completion record.
    const MAX_CANCEL_REASON: usize = 1024;

    /// The most a workflow_complete value for `run_ns_key` may hold: a log
    /// entry less its command header and key.
    fn completionLimit(run_ns_key: []const u8) usize {
        return persistence_mod.MAX_PERSIST_PAYLOAD - entry_mod.COMMAND_PREFIX_SIZE - run_ns_key.len;
    }

    /// The bytes a run's completion record takes for what it holds now:
    /// its step outputs and history, before the final event, output and tags.
    fn completionBase(run: *const RunRecord) usize {
        var n: usize = 1 + 1 + 2 + 2 + 2;
        if (run.step_outputs) |so| for (so.entries) |e| {
            n += 2 + e.step_name.len + 2 + e.outcome.len + 4 + e.output.len;
        };
        for (run.history.items) |e| n += Completion.eventSize(e.event_type_owned, e.detail_owned);
        return n;
    }

    /// Room kept for what a completion adds to a run's history: its final
    /// event, output and tags. A signal that would leave less is refused.
    const COMPLETION_HEADROOM: usize = 8 * 1024;

    fn encodeCompletion(allocator: Allocator, c: Completion, limit: usize) error{ TooLarge, OutOfMemory }![]u8 {
        var marker_buf: [64]u8 = undefined;
        const marker = std.fmt.bufPrint(&marker_buf, "{d} earlier events dropped", .{c.dropped}) catch unreachable;
        const total = c.size(marker);
        if (total > limit) return error.TooLarge;
        // total <= limit < 64 KiB, so every length and count below fits its field.
        const buf = try allocator.alloc(u8, total);
        var w: std.Io.Writer = .fixed(buf);
        w.writeByte(@backingInt(c.status)) catch unreachable;
        if (c.output) |o| {
            w.writeByte(1) catch unreachable;
            w.writeInt(u32, @intCast(o.len), .little) catch unreachable;
            w.writeAll(o) catch unreachable;
        } else w.writeByte(0) catch unreachable;
        w.writeInt(u16, @intCast(c.steps.len), .little) catch unreachable;
        for (c.steps) |e| {
            w.writeInt(u16, @intCast(e.step_name.len), .little) catch unreachable;
            w.writeAll(e.step_name) catch unreachable;
            w.writeInt(u16, @intCast(e.outcome.len), .little) catch unreachable;
            w.writeAll(e.outcome) catch unreachable;
            w.writeInt(u32, @intCast(e.output.len), .little) catch unreachable;
            w.writeAll(e.output) catch unreachable;
        }
        const events = c.history.len + 1 + @intFromBool(c.dropped > 0);
        w.writeInt(u16, @intCast(events), .little) catch unreachable;
        const write = struct {
            fn event(wr: *std.Io.Writer, t: []const u8, d: []const u8, ts: i64) void {
                wr.writeInt(u16, @intCast(t.len), .little) catch unreachable;
                wr.writeAll(t) catch unreachable;
                wr.writeInt(u16, @intCast(d.len), .little) catch unreachable;
                wr.writeAll(d) catch unreachable;
                wr.writeInt(i64, ts, .little) catch unreachable;
            }
        };
        if (c.dropped > 0) {
            const ts = if (c.history.len > 0) c.history[0].timestamp_ms else c.last.timestamp_ms;
            write.event(&w, MARKER_EVENT, marker, ts);
        }
        for (c.history) |e| write.event(&w, e.event_type_owned, e.detail_owned, e.timestamp_ms);
        write.event(&w, c.last.event_type, c.last.detail, c.last.timestamp_ms);
        const t = c.tags orelse "";
        w.writeInt(u16, @intCast(t.len), .little) catch unreachable;
        w.writeAll(t) catch unreachable;
        std.debug.assert(w.end == total);
        return buf;
    }

    /// The record for a completion too large to log, without the run's
    /// output, step outputs or tags, keeping the most recent history events
    /// that fit and a marker for the rest. A cancel stays a cancel, its
    /// reason kept; any other completion fails the run, saying why. Built
    /// from the run alone, and replay applies these bytes, so every node
    /// agrees.
    fn encodeTrimmed(allocator: Allocator, full: Completion, limit: usize) error{OutOfMemory}![]u8 {
        var marker_buf: [64]u8 = undefined;
        const full_size = full.size(std.fmt.bufPrint(&marker_buf, "{d} earlier events dropped", .{full.dropped}) catch unreachable);
        // A client's cancel reason is capped at MAX_CANCEL_REASON, a terminal
        // name lower still; cut anything longer so this always fits.
        var detail_buf: [MAX_CANCEL_REASON + 96]u8 = undefined;
        const cancelled = full.status == .cancelled;
        const reason = full.last.detail[0..@min(full.last.detail.len, MAX_CANCEL_REASON)];
        const detail = if (cancelled)
            std.fmt.bufPrint(&detail_buf, "{s} (completion record too large: {d} bytes > {d})", .{ reason, full_size, limit }) catch unreachable
        else
            std.fmt.bufPrint(&detail_buf, "completion record too large: {d} bytes > {d}", .{ full_size, limit }) catch unreachable;
        var c: Completion = .{
            .status = if (cancelled) .cancelled else .failed,
            .output = null,
            .steps = &.{},
            .history = full.history,
            .last = .{ .event_type = terminalEventType(if (cancelled) .cancelled else .failed), .detail = detail, .timestamp_ms = full.last.timestamp_ms },
            .tags = null,
        };
        // Drop the oldest events until the rest fit.
        while (true) {
            return encodeCompletion(allocator, c, limit) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.TooLarge => {
                    // A run's key and the fixed parts always fit; only history can be too long.
                    std.debug.assert(c.history.len > 0);
                    c.history = c.history[1..];
                    c.dropped += 1;
                    continue;
                },
            };
        }
    }

    fn noteRunFinished(self: *WorkflowHandler, run: *const RunRecord) void {
        const key = self.nsKey(keyNamespace(run.ns_key), run.workflow_name_owned) orelse return;
        defer self.allocator.free(key);
        if (self.active_runs.getPtr(key)) |n| n.* -|= 1;
    }

    /// Sets the run's terminal state from a workflow_complete value stamped
    /// `completed_at_ms`; the producer and replay both apply it here. A
    /// value cut short, or an allocation that fails, leaves the run as it
    /// was.
    fn applyComplete(self: *WorkflowHandler, run: *RunRecord, value: []const u8, completed_at_ms: i64) persistence_mod.ApplyError!void {
        const entry = CompleteEntry.parse(value) orelse return error.Halt;
        const built = try self.buildComplete(&entry);

        if (!run.status.isTerminal()) self.noteRunFinished(run);
        _ = self.to_step.remove(run.ns_key);
        // The entry carries the whole terminal state: the run's steps,
        // outputs and history are replaced with what was logged.
        run.status = entry.status;
        run.completed_at_ms = completed_at_ms;
        if (run.output_owned) |o| self.allocator.free(o);
        run.output_owned = built.output;
        if (run.step_outputs) |*so| {
            var mutable = so.*;
            mutable.deinit(self.allocator);
        }
        run.step_outputs = built.steps;
        for (run.history.items) |evt| {
            self.allocator.free(evt.event_type_owned);
            self.allocator.free(evt.detail_owned);
        }
        run.history.deinit(self.allocator);
        run.history = built.history;
        if (built.tags) |t| {
            if (run.search_tags_owned) |old| self.allocator.free(old);
            run.search_tags_owned = t;
        }
    }

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *WorkflowHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.workflow_create, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_start, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_complete, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_enabled, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_step_started, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const self: *WorkflowHandler = @ptrCast(@alignCast(ctx));
        return self.replayEntry(entry, out);
    }

    /// Apply a workflow entry: live commit, replicated entry or boot replay
    /// alike. A create is answered with the definition's name; a start with
    /// whether it created its run (u8) and the run's id.
    pub fn replayEntry(self: *WorkflowHandler, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const etype: entry_mod.EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        // Other shard threads walk these maps under this lock. Nothing
        // holds it across an apply, so taking it here cannot nest.
        self.mu.lock();
        defer self.mu.unlock();
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return error.Halt;
        // Every workflow key leads with the namespace its entry is stamped with.
        if (cmd.key.len < KEY_PREFIX or keyNamespace(cmd.key) != cmd.namespace_id) return error.Halt;

        return switch (etype) {
            .workflow_create => self.replayCreate(cmd.key, cmd.value, @intCast(entry.header.timestamp_ns / 1_000_000), out),
            .workflow_start => self.replayStart(cmd.key, cmd.value, @intCast(entry.header.timestamp_ns / std.time.ns_per_ms), out),
            .workflow_enabled => self.replayEnabled(cmd.key, cmd.value, @intCast(entry.header.timestamp_ns / std.time.ns_per_ms), out),
            .workflow_step_started => self.replayStepStarted(cmd.key, cmd.value, out),
            .workflow_complete => self.replayComplete(cmd.key, cmd.value, @intCast(entry.header.timestamp_ns / std.time.ns_per_ms), out),
            else => error.Halt,
        };
    }

    /// Apply a workflow_create entry, keyed by the definition's key.
    fn replayCreate(self: *WorkflowHandler, ns_key_raw: []const u8, yaml: []const u8, created_at_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        const ns = keyNamespace(ns_key_raw);
        const raw_name = keyName(ns_key_raw);
        out.putBytes(raw_name);

        // Parsed before anything changes: a definition that won't parse
        // halts here and leaves the one it would replace in place.
        var def = self.parseStored(yaml, ns_key_raw) catch |err| {
            return if (err == error.OutOfMemory) error.OutOfMemory else error.Halt;
        };
        defer def.deinit(self.allocator);

        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return error.OutOfMemory;
        if (self.definitions.fetchRemove(ns_key)) |old| {
            self.allocator.free(old.key); // old ns-qualified key
            self.allocator.free(old.value.name_owned);
            self.allocator.free(old.value.version_owned);
            self.allocator.free(old.value.yaml_owned);
        }

        const version = self.allocator.dupe(u8, def.version) catch {
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        const owned_name = self.allocator.dupe(u8, raw_name) catch {
            self.allocator.free(ns_key);
            self.allocator.free(version);
            return error.OutOfMemory;
        };
        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(version);
            return error.OutOfMemory;
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
            return error.OutOfMemory;
        };

        // A definition's trigger and schedule are part of it: (re)register
        // them here so a restart keeps them and a redefinition without one
        // drops it.
        if (!self.install_producers) return;
        if (def.trigger) |trigger| {
            self.registerStreamTrigger(ns, raw_name, trigger);
        } else {
            self.unregisterStreamTrigger(ns, raw_name);
        }
        if (def.schedule) |schedule| {
            self.registerSchedule(ns, raw_name, schedule, created_at_ms);
        } else {
            self.unregisterSchedule(ns, raw_name);
        }
    }

    /// Apply a workflow_start entry, keyed by the run's key.
    fn replayStart(self: *WorkflowHandler, ns_key_raw: []const u8, value: []const u8, created_at_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        const ns = keyNamespace(ns_key_raw);
        const raw_run_id = keyName(ns_key_raw);

        var off: usize = 0;
        if (off + 2 > value.len) return error.Halt;
        const wf_name_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + wf_name_len > value.len) return error.Halt;
        const wf_name = value[off .. off + wf_name_len];
        off += wf_name_len;

        if (off + 2 > value.len) return error.Halt;
        const ver_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + ver_len > value.len) return error.Halt;
        const version = value[off .. off + ver_len];
        off += ver_len;

        if (off + 1 > value.len) return error.Halt;
        const status = std.enums.fromInt(RunStatus, value[off]) orelse return error.Halt;
        off += 1;

        if (off + 2 > value.len) return error.Halt;
        const evt_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + evt_len > value.len) return error.Halt;
        const event_type = value[off .. off + evt_len];
        off += evt_len;

        if (off + 2 > value.len) return error.Halt;
        const idem_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + idem_len > value.len) return error.Halt;
        const idem: ?[]const u8 = if (idem_len > 0) value[off .. off + idem_len] else null;
        off += idem_len;

        const input = if (off < value.len) value[off..] else "{}";

        // Two starts with one idempotency key can both pass the handler's
        // check before either applies; the log's order decides. The later
        // one creates nothing, and its client is answered with the earlier
        // run. Checked before the run id, as the handler does: a retry
        // carrying both its key and its own run id is the same start.
        if (idem) |k| {
            if (self.runKeyForIdempotencyKey(ns, wf_name, k)) |run_key| {
                const earlier = self.runs.get(run_key) orelse return error.Halt;
                out.putInt(u8, 0);
                out.putBytes(earlier.run_id_owned);
                return;
            }
        }

        // Already present: a re-apply, or a second start with a run id
        // the client chose, which the handler let through while the first
        // was uncommitted. The log's order decides; the later is refused.
        if (self.runs.contains(ns_key_raw)) return out.refuse(.conflict, .unclassified, "conflict: a run with this id already exists");

        // A disable or a redefinition that commits ahead of a start decides
        // it, as the handler's checks would have. Only client and schedule
        // starts. Not a child's: a parent reads a child on another shard
        // that its start never created as not finished yet, so refusing it
        // here would leave the parent waiting for good. Not a trigger's:
        // its leader has moved the trigger's cursor past the record
        // already, so a trigger start proposed before a disable commits
        // still runs. A refused schedule start leaves its slot unfired;
        // enabling re-anchors past it.
        const client_start = std.mem.eql(u8, event_type, "workflow_started");
        if (client_start or std.mem.eql(u8, event_type, "schedule_started")) {
            const def_key = self.nsKey(ns, wf_name) orelse return error.OutOfMemory;
            defer self.allocator.free(def_key);
            if (self.disabled.contains(def_key)) return out.refuse(.bad_request, .unclassified, "workflow is disabled");
            if (client_start and idem == null) {
                if (self.definitions.get(def_key)) |def| {
                    if (def.idempotency == .required) return out.refuse(.bad_request, .unclassified, "idempotency key is required for this workflow");
                }
            }
        }

        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return error.OutOfMemory;
        var run = RunRecord{
            .run_id_owned = self.allocator.dupe(u8, raw_run_id) catch {
                self.allocator.free(ns_key);
                return error.OutOfMemory;
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
            return error.OutOfMemory;
        };
        run.workflow_version_owned = self.allocator.dupe(u8, version) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(run.workflow_name_owned);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        run.input_owned = self.allocator.dupe(u8, input) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(run.workflow_name_owned);
            self.allocator.free(run.workflow_version_owned);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        run.idempotency_key_owned = if (idem) |k| self.allocator.dupe(u8, k) catch {
            self.allocator.free(run.run_id_owned);
            self.allocator.free(run.workflow_name_owned);
            self.allocator.free(run.workflow_version_owned);
            self.allocator.free(run.input_owned);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        } else null;
        // The run's first history event, as the start path recorded it.
        if (event_type.len > 0) self.addHistoryEvent(&run, event_type, input, created_at_ms);
        if (idem) |k| {
            if (std.mem.eql(u8, event_type, "trigger_started") and std.mem.startsWith(u8, k, "trigger:")) {
                self.restoreTriggerCursor(ns, wf_name, k);
            }
            if (std.mem.eql(u8, event_type, "schedule_started") and std.mem.startsWith(u8, k, "schedule:")) {
                self.restoreScheduleSlot(ns, wf_name, k);
            }
        }

        run.ns_key = ns_key;
        const active = self.activeRuns(ns, wf_name) orelse {
            self.freeRunRecord(&run);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        self.to_step.ensureUnusedCapacity(self.allocator, 1) catch {
            self.freeRunRecord(&run);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        self.runs.put(ns_key, run) catch {
            self.freeRunRecord(&run);
            self.allocator.free(ns_key);
            return error.OutOfMemory;
        };
        active.* += 1;
        if (status == .running) self.to_step.putAssumeCapacity(ns_key, {});
        out.putInt(u8, 1);
        out.putBytes(raw_run_id);
        if (idem) |k| self.indexIdempotencyKey(ns, wf_name, k, ns_key);
        // Search tags come from the definition and the input; derived here so
        // every node computes the same ones.
        if (self.runs.getPtr(ns_key)) |stored| {
            if (self.nsKey(ns, wf_name)) |def_ns_key| {
                defer self.allocator.free(def_ns_key);
                stored.search_tags_owned = self.buildSearchTags(def_ns_key, stored);
            }
        }
    }

    /// The count of namespace `ns`'s unfinished runs of a workflow, for the
    /// caller to change at once.
    fn activeRuns(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8) ?*u32 {
        const key = self.nsKey(ns, wf_name) orelse return null;
        const got = self.active_runs.getOrPut(self.allocator, key) catch {
            self.allocator.free(key);
            return null;
        };
        if (got.found_existing) self.allocator.free(key) else got.value_ptr.* = 0;
        return got.value_ptr;
    }

    /// Apply a workflow_complete entry. Updates the run's terminal status,
    /// output, step_outputs, and history events.
    /// Value format: [status:u8]
    ///   [has_output:u8][output_len:u32][output]?
    ///   [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
    ///   [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
    fn replayComplete(self: *WorkflowHandler, ns_key_raw: []const u8, value: []const u8, completed_at_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        // Runs are never removed: a completion names one that started.
        const run = self.runs.getPtr(ns_key_raw) orelse return error.Halt;
        // The first completion in the log ends the run; a cancel and a
        // completion in flight together leave it as the first left it. A
        // second with the same outcome (two cancels) succeeds, changing
        // nothing.
        if (run.status.isTerminal()) {
            const entry = CompleteEntry.parse(value) orelse return error.Halt;
            if (entry.status == run.status) return;
            return out.refuse(.bad_request, .unclassified, "run already finished");
        }
        try self.applyComplete(run, value, completed_at_ms);
    }

    /// A workflow_complete value, checked against its length:
    ///   [status:u8]
    ///   [has_output:u8]([output_len:u32][output])?
    ///   [step_count:u16]([name_len:u16][name][outcome_len:u16][outcome][output_len:u32][output])*
    ///   [history_count:u16]([type_len:u16][type][detail_len:u16][detail][timestamp:i64])*
    ///   [tags_len:u16][search_tags]
    const CompleteEntry = struct {
        status: RunStatus,
        output: ?[]const u8,
        step_count: u16,
        steps: Cursor,
        history_count: u16,
        history: Cursor,
        tags: ?[]const u8,

        fn parse(value: []const u8) ?CompleteEntry {
            var c = Cursor{ .bytes = value };
            const status = std.enums.fromInt(RunStatus, (c.take(1) orelse return null)[0]) orelse return null;
            const has_output = (c.take(1) orelse return null)[0];
            const output: ?[]const u8 = if (has_output == 1) c.take(c.int(u32) orelse return null) orelse return null else null;
            const step_count = c.int(u16) orelse return null;
            const steps = c;
            for (0..step_count) |_| {
                _ = c.take(c.int(u16) orelse return null) orelse return null;
                _ = c.take(c.int(u16) orelse return null) orelse return null;
                _ = c.take(c.int(u32) orelse return null) orelse return null;
            }
            const history_count = c.int(u16) orelse return null;
            const history = c;
            for (0..history_count) |_| {
                _ = c.take(c.int(u16) orelse return null) orelse return null;
                _ = c.take(c.int(u16) orelse return null) orelse return null;
                _ = c.int(i64) orelse return null;
            }
            const tags_len = c.int(u16) orelse return null;
            const tags: ?[]const u8 = if (tags_len > 0) c.take(tags_len) orelse return null else null;
            return .{ .status = status, .output = output, .step_count = step_count, .steps = steps, .history_count = history_count, .history = history, .tags = tags };
        }
    };

    const BuiltComplete = struct {
        output: ?[]const u8,
        steps: ?StepOutputMap,
        history: std.ArrayList(HistoryEvent),
        tags: ?[]const u8,
    };

    /// The run's new terminal state, owned. A failed allocation frees what
    /// was built and leaves the run as it was.
    fn buildComplete(self: *WorkflowHandler, entry: *const CompleteEntry) !BuiltComplete {
        const output: ?[]const u8 = if (entry.output) |o| try self.allocator.dupe(u8, o) else null;
        errdefer if (output) |o| self.allocator.free(o);

        var steps: ?StepOutputMap = null;
        errdefer if (steps) |*so| so.deinit(self.allocator);
        if (entry.step_count > 0) {
            steps = StepOutputMap.init();
            var c = entry.steps;
            for (0..entry.step_count) |_| {
                const name = c.take(c.int(u16).?).?;
                const outcome = c.take(c.int(u16).?).?;
                const out = c.take(c.int(u32).?).?;
                try steps.?.put(self.allocator, name, out, outcome);
            }
        }

        var history: std.ArrayList(HistoryEvent) = .empty;
        errdefer {
            for (history.items) |evt| {
                self.allocator.free(evt.event_type_owned);
                self.allocator.free(evt.detail_owned);
            }
            history.deinit(self.allocator);
        }
        try history.ensureTotalCapacity(self.allocator, entry.history_count);
        var c = entry.history;
        for (0..entry.history_count) |_| {
            const event_type = try self.allocator.dupe(u8, c.take(c.int(u16).?).?);
            errdefer self.allocator.free(event_type);
            const detail = try self.allocator.dupe(u8, c.take(c.int(u16).?).?);
            history.appendAssumeCapacity(.{ .event_type_owned = event_type, .detail_owned = detail, .timestamp_ms = c.int(i64).? });
        }

        const tags: ?[]const u8 = if (entry.tags) |t| try self.allocator.dupe(u8, t) else null;
        return .{ .output = output, .steps = steps, .history = history, .tags = tags };
    }

    pub fn definitionCount(self: *const WorkflowHandler) usize {
        return self.definitions.count();
    }

    pub fn runCount(self: *const WorkflowHandler) usize {
        return self.runs.count();
    }
};

/// Reads a value front to back; every read is null rather than past the end.
const Cursor = struct {
    bytes: []const u8,
    off: usize = 0,

    fn take(self: *Cursor, n: usize) ?[]const u8 {
        if (self.bytes.len - self.off < n) return null;
        defer self.off += n;
        return self.bytes[self.off..][0..n];
    }

    fn int(self: *Cursor, comptime T: type) ?T {
        const b = self.take(@sizeOf(T)) orelse return null;
        return std.mem.readInt(T, b[0..@sizeOf(T)], .little);
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
/// A result for the tests that apply a completion directly and don't read it.
var replay_out_for_tests: Result = .{};

/// Namespaces the tests apply entries in, as ids the table would allocate.
const TEST_NS_A: NamespaceId = .of(16);
const TEST_NS_B: NamespaceId = .of(17);
const TEST_NS: NamespaceId = .of(18);

/// `name`'s key in namespace `ns`, built as the handler builds one.
pub fn testKey(comptime ns: NamespaceId, comptime name: []const u8) []const u8 {
    return comptime blk: {
        var buf: [KEY_PREFIX + name.len]u8 = undefined;
        _ = writeKey(&buf, ns, name);
        const out = buf;
        break :blk &out;
    };
}

fn makeRequest(op: OpCode, key: []const u8, value: []const u8) Request {
    return .{
        .header = proto.RequestHeader.init(op, 1),
        .namespace = "default",
        .ns_id = .default,
        .key = key,
        .value = value,
        .options = "",
    };
}

test "workflow handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    WorkflowHandler.register(&dispatcher);

    try testing.expect(dispatcher.hasHandler(OpCode.workflow_create));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_start));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_signal));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_cancel));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_status));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_history));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_list_runs));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_get_definition));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_disable));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_enable));
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_list_definitions));

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

    handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events" });
    const key = try allocator.dupe(u8, testKey(TEST_NS, "wf"));
    defer allocator.free(key);
    handler.stream_triggers.getPtr(key).?.stream_cursor_ts = 1700;
    handler.stream_triggers.getPtr(key).?.stream_cursor_seq = 3;

    handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events", .batch_size = 2 });
    const same = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(u64, 1700), same.stream_cursor_ts);
    try testing.expectEqual(@as(u64, 3), same.stream_cursor_seq);
    try testing.expectEqual(@as(u32, 2), same.batch_size);

    handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "other" });
    const moved = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(u64, 0), moved.stream_cursor_ts);
    try testing.expectEqual(@as(u64, 0), moved.stream_cursor_seq);
}

test "workflow handler: a re-created trigger naming its own namespace, or none, keeps its cursor; another namespace resets it" {
    const allocator = testing.allocator;
    var namespaces = try NamespaceHandler.init(allocator);
    defer namespaces.deinit();
    try testing.expectEqual(.created, namespaces.applyCreate("team-a"));
    try testing.expectEqual(.created, namespaces.applyCreate("team-b"));
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    handler.namespaces = &namespaces;
    const ns = namespaces.idOf("team-a").?;

    handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .batch_size = 4 });
    const key = handler.nsKey(ns, "wf").?;
    defer allocator.free(key);
    for ([_]?[]const u8{ "team-a", null, "", "team-a" }) |named| {
        handler.stream_triggers.getPtr(key).?.stream_cursor_ts = 1700;
        handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .namespace = named, .batch_size = 4 });
        try testing.expectEqual(@as(u64, 1700), handler.stream_triggers.getPtr(key).?.stream_cursor_ts);
    }

    handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .namespace = "team-b", .batch_size = 4 });
    try testing.expectEqual(@as(u64, 0), handler.stream_triggers.getPtr(key).?.stream_cursor_ts);
    try testing.expectEqualStrings("team-b", handler.stream_triggers.getPtr(key).?.stream_namespace_owned.?);
}

// ── Step Executor Tests ─────────────────────────────────────────────────

/// Minimal 2-step workflow: start → step_b → flo.Completed
const test_workflow_json =
    \\{"kind":"Workflow","name":"test-wf","version":"1.0.0",
    \\"start":{"run":"@actions/step-a","transitions":{"success":"step_b"}},
    \\"steps":{"step_b":{"run":"@actions/step-b","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}}
;

/// Two waits: the first's timeout goes to the second, which has none.
const test_two_waits_json =
    \\{"kind":"Workflow","name":"two-waits","version":"1.0.0",
    \\"start":{"wait_for_signal":{"type":"first","timeout_ms":3600000,"on_timeout":"second"},"transitions":{"success":"second"}},
    \\"steps":{"second":{"wait_for_signal":{"type":"second"},"transitions":{"success":"flo.Completed"}}}}
;

/// Workflow with a wait_for_signal step
const test_wait_workflow_json =
    \\{"kind":"Workflow","name":"wait-wf","version":"1.0.0",
    \\"start":{"run":"@actions/init","transitions":{"success":"wait_approval"}},
    \\"steps":{"wait_approval":{"wait_for_signal":{"type":"approval"},"transitions":{"success":"flo.Completed"}}}}
;

/// Workflow with input mapping
const test_input_mapping_json =
    \\{"kind":"Workflow","name":"map-wf","version":"1.0.0",
    \\"start":{"run":"@actions/step-a","input_mapping":"{\"user\":\"$.input.name\"}","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

/// Workflow with retry policy
const test_retry_workflow_json =
    \\{"kind":"Workflow","name":"retry-wf","version":"1.0.0",
    \\"start":{"run":"@actions/flaky","retry":{"max_attempts":3},"transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

// Poll workflow: action returns `pending`; the engine should re-arm a backoff
// poll (delays 0 for test speed) and follow `timeout` after max_attempts=2.
const test_poll_workflow_json =
    \\{"kind":"Workflow","name":"poll-wf","version":"1.0.0",
    \\"start":{"run":"@actions/poller",
    \\"poll":{"max_attempts":2,"initial_delay_ms":0,"base_delay_ms":0,"max_delay_ms":0,"backoff":"constant"},
    \\"transitions":{"success":"flo.Completed","failure":"flo.Failed","timeout":"flo.TimedOut"}}}
;

const test_action_entries = @import("../protocol/action_entries.zig");
const test_action_machine = @import("../actions/machine.zig");
var test_action_index: u64 = 1_000_000;

/// An action entry applied to the test shard's store, as its log would.
fn applyTestActionEntry(shard: *Shard, comptime etype: @import("../storage/ual/entry.zig").EntryType, body: anytype, stamp_ns: u64) void {
    var buf: [8192]u8 = undefined;
    test_action_index += 1;
    const e = ActionsHandler.for_tests.entry(etype, body, 1, test_action_index, stamp_ns, &buf) catch unreachable;
    var out = persistence_mod.Result.start(&e);
    const part = shard.defaultPartition();
    @import("../contract/machine.zig").applyEntry(&test_action_machine.machine, &part.store, &part.apply_scratch, &e, &out) catch unreachable;
}

/// Register a test action, with one attempt: a run whose lease runs out
/// times out.
fn registerTestAction(shard: *Shard, name: []const u8) void {
    applyTestActionEntry(shard, .action_register, test_action_entries.Register.Value{ .ns = NamespaceId.default.int(), .name = name, .owner = "", .lease_ms = 300_000, .max_attempts = 1 }, 1);
}

/// A run a worker took: its deliver applied.
fn deliverTestAction(shard: *Shard, run_id: []const u8) ?test_action_machine.Run.Value {
    const run = ActionsHandler.for_tests.run(shard, .default, run_id) orelse return null;
    applyTestActionEntry(shard, .action_deliver, test_action_entries.Deliver.Value{ .ns = NamespaceId.default.int(), .action = run.action, .run = run_id, .due_ns = run.due_ns, .holder = "w", .request_key = null, .lease_ms = 300_000 }, run.due_ns);
    return ActionsHandler.for_tests.run(shard, .default, run_id);
}

/// Complete a test action run with outcome "success".
/// Call this after advanceWorkflow parks, then call checkPendingActions to resume.
fn completeTestAction(shard: *Shard, run_id: []const u8) void {
    completeTestActionWith(shard, run_id, "success");
}

/// Complete a test action run with a specific outcome: taken by a worker,
/// then settled by it.
fn completeTestActionWith(shard: *Shard, run_id: []const u8, outcome: []const u8) void {
    const run = deliverTestAction(shard, run_id) orelse return;
    applyTestActionEntry(shard, .action_settle, test_action_entries.Settle.Value{ .ns = NamespaceId.default.int(), .action = run.action, .run = run_id, .holder = "w", .request_key = null, .deliveries = run.deliveries, .token = run.token, .expiry_ns = run.expiry_ns, .outcome = outcome, .result = "" }, run.delivered_ns + 1);
}

/// A test action run whose only attempt's lease ran out.
fn timeOutTestAction(shard: *Shard, run_id: []const u8) void {
    const run = deliverTestAction(shard, run_id) orelse return;
    var ib: [test_action_entries.ExpireItem.max_size]u8 = undefined;
    const item = test_action_entries.ExpireItem.encodeInto(&ib, .{ .ns = NamespaceId.default.int(), .action = run.action, .run = run_id, .expiry_ns = run.expiry_ns, .holder = "w", .request_key = null }) catch unreachable;
    applyTestActionEntry(shard, .action_expire, test_action_entries.Expire.Value{ .items = .{ .bytes = item, .count = 1 } }, run.expiry_ns);
}

/// Create a minimal test shard with an actions handler for unit tests.
/// Only `actions_handler` is usable — all other fields are undefined.
fn createTestShard(actions: *ActionsHandler) !Shard {
    const waiter_pool_mod = @import("../node/waiter_pool.zig");
    const RaftNode = @import("../raft/node.zig").RaftNode;
    var shard: Shard = undefined;
    shard.id = 0;
    shard.allocator = std.testing.allocator;
    shard.actions_handler = actions;
    shard.peer_shards = null;
    shard.peer_mailboxes = null;
    // `Shard = undefined` means field defaults do not apply — anything the code
    // under test reads must be assigned here or it holds garbage.
    shard.metrics_registry = null;
    shard.shard_metrics = null;
    shard.offline = null;
    shard.halted = null;
    shard.duties = .{};
    shard.node_health = &.{};
    shard.marker = .{};
    shard.skips = .{};
    shard.skip_scan_from = 0;
    shard.diverged_damage = null;
    shard.offline_published = .init(false);
    shard.snapshot_window = null;
    shard.hold_sync_flush = false;
    shard.test_bar = 0;
    shard.snapshot_skips_leader_not_ready = 0;
    shard.kv_sink_retries = .empty;
    shard.kv_sink_retry_overflow = 0;
    shard.replica.diverged = false;
    shard.losses = .{};
    const raft_node = try std.testing.allocator.create(RaftNode);
    // Room for an entry at the payload limit, which a 64 KiB ring can't hold.
    raft_node.* = try RaftNode.init(std.testing.allocator, 1, 0, 256 * 1024, .{});
    try raft_node.bootstrap();
    shard.raft_node = raft_node;
    shard.raft_network = null;
    shard.router = router.Router.init(1, 0);
    shard.run_id_gen = .{};
    shard.waiter_pool = try waiter_pool_mod.WaiterPool.init(std.testing.allocator);
    // The one applier needs a partition to append to (whose store holds
    // the actions), and the copy buffer it reads committed entries through.
    const partition = try std.testing.allocator.create(Partition);
    partition.* = try Partition.init(std.testing.allocator, 0, 256 * 1024, 0);
    const partitions = try std.testing.allocator.alloc(*Partition, 1);
    partitions[0] = partition;
    shard.partitions = partitions;
    shard.replay_registry = .{};
    shard.apply_buf = try std.testing.allocator.alloc(u8, @import("../protocol/limits.zig").MAX_LOG_ENTRY_PAYLOAD);
    shard.entry_buf = try std.testing.allocator.alloc(u8, persistence_mod.MAX_PERSIST_PAYLOAD);
    shard.scratch = try @import("../node/scratch.zig").Scratch.init(std.testing.allocator, shard_mod.SCRATCH_SIZE);
    shard.durability = .async_flush;
    shard.pending = try std.testing.allocator.alloc(shard_mod.Pending, shard_mod.PENDING_SLOTS);
    @memset(shard.pending, .{});
    shard.pending_count = 0;
    shard.applying = false;
    shard.last_entry_applied = true;
    shard.wake_workers = false;
    shard.replies_held = 0;
    shard.forward_count = 0;
    // A write's entry carries its namespace's id, from the node's table.
    const namespaces = try std.testing.allocator.create(NamespaceHandler);
    namespaces.* = try NamespaceHandler.init(std.testing.allocator);
    shard.namespace_handler = namespaces;
    return shard;
}

fn destroyTestShard(shard: *Shard) void {
    shard.waiter_pool.deinit(std.testing.allocator);
    shard.raft_node.deinit();
    std.testing.allocator.destroy(shard.raft_node);
    shard.partitions[0].deinit();
    std.testing.allocator.destroy(shard.partitions[0]);
    std.testing.allocator.free(shard.partitions);
    std.testing.allocator.free(shard.apply_buf);
    std.testing.allocator.free(shard.entry_buf);
    shard.scratch.deinit(std.testing.allocator);
    std.testing.allocator.free(shard.pending);
    shard.namespace_handler.deinit();
    std.testing.allocator.destroy(shard.namespace_handler);
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
        .ns_key = owned_ns,
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
        .ns_key = owned_ns,
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

test "a producer's start leaves its run to step, its logged first step joins the queue, then it takes its step; one whose index applied without creating a run is dropped" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // Proposed, not yet applied: there is no run to step, and nothing waits.
    try testing.expect(handler.proposeRun(&shard, .default, "run-p", "test-wf", "latest", "{}", null, "schedule_started") != null);
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 0), handler.started_to_advance.items.len);
    try testing.expect(handler.runs.get(testKey(.default, "run-p")) == null);

    // Applied: the run it created is one to step.
    _ = shard.applyCommitted();
    try testing.expect(handler.to_step.contains(testKey(.default, "run-p")));
    // Its first step logged and applied on the leader: queued.
    handler.advanceStartedRuns(&shard);
    _ = shard.applyCommitted();
    try testing.expect(!handler.to_step.contains(testKey(.default, "run-p")));
    try testing.expectEqual(@as(usize, 1), handler.started_to_advance.items.len);
    // A start whose index applied without creating a run (dropped with the
    // log's tail) is dropped from the queue, not kept for ever.
    handler.queueFirstStep(testKey(.default, "ghost"), shard.raft_node.last_applied);
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 0), handler.started_to_advance.items.len);
    const run = handler.runs.get(testKey(.default, "run-p")).?;
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
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-c"), "run-c", "test-wf");

    // Parked on a child whose start is at an index this shard has applied
    // but which created no run, as when a lost leadership truncated it.
    const proposed = try @import("../kv/handler.zig").KVHandler.testProposePut(&shard, "k", "v");
    _ = shard.applyCommitted();
    const run = handler.runs.getPtr(testKey(.default, "run-c")).?;
    handler.parkForChild(run, "gone", "start", "child-wf", shard.id, 0, proposed.index);
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    const after = handler.runs.get(testKey(.default, "run-c")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, after.status);
    var lost = false;
    for (after.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "child_lost")) lost = true;
    }
    try testing.expect(lost);
}

test "a parent waiting on a child resumes once the child's completion commits, not when it is proposed" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "test-wf");
    createTestRun(&handler, testKey(.default, "kid"), "kid", "test-wf");
    const parent = handler.runs.getPtr(testKey(.default, "par")).?;
    handler.parkForChild(parent, "kid", "start", "test-wf", shard.id, 0, 0);

    const kid = handler.runs.getPtr(testKey(.default, "kid")).?;
    handler.completeRun(&shard, testKey(.default, "kid"), kid, .completed, "flo.Completed", 3);
    // Proposed, not applied: the parent still waits on it.
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.running, handler.runs.get(testKey(.default, "kid")).?.status);
    const waiting = handler.runs.get(testKey(.default, "par")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, waiting.status);
    try testing.expectEqualStrings("kid", waiting.pending_child_run_id_owned.?);

    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, handler.runs.get(testKey(.default, "kid")).?.status);
    handler.checkPendingActions(&shard);
    const resumed = handler.runs.get(testKey(.default, "par")).?;
    try testing.expect(resumed.pending_child_run_id_owned == null);
    var completed = false;
    for (resumed.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "child_completed")) completed = true;
    }
    try testing.expect(completed);
}

test "a step whose action run the log dropped fails once the invoke's index has applied" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-l"), "run-l", "test-wf");

    handler.advanceWorkflow(&shard, testKey(.default, "run-l"));
    const run = handler.runs.getPtr(testKey(.default, "run-l")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    // Not applied yet: the run waits.
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "run-l")).?.status);

    // Point the run at an action run no invoke created, then apply the
    // invoke's index: the run it names does not exist, as when a lost
    // leadership truncated the invoke from the log.
    allocator.free(run.pending_action_run_id_owned.?);
    run.pending_action_run_id_owned = try allocator.dupe(u8, "gone");
    _ = shard.applyCommitted();
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.runs.get(testKey(.default, "run-l")).?.status);
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
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-1"), "run-1", "test-wf");

    try testing.expectEqual(@as(usize, 1), handler.runCount());

    // Phase 1: advance invokes step-a asynchronously, workflow parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-1"));
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-a with "success"
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes step-a, advances to step-b (parks again)
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "success"
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 3: checkPendingActions resumes step-b, workflow completes
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-1")).?;
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
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "init");

    createTestDef(&handler, testKey(.default, "wait-wf"), "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, testKey(.default, "run-2"), "run-2", "wait-wf");

    // Phase 1: advance invokes init action, parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-2"));
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-2")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete init with success so workflow transitions to wait_approval
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes init, workflow transitions to wait_approval (parks for signal)
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-2")).?;
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
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "init");

    createTestDef(&handler, testKey(.default, "wait-wf"), "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, testKey(.default, "run-3"), "run-3", "wait-wf");

    // Advance until init parks, then complete it
    handler.advanceWorkflow(&shard, testKey(.default, "run-3"));
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-3")).?;
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }
    // Resume init → transitions to wait_approval → parks for signal
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.get(testKey(.default, "run-3")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    }

    // Simulate signal delivery: set up matching signal, clear wait, resume
    {
        const run = handler.runs.getPtr(testKey(.default, "run-3")).?;
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
    handler.advanceWorkflow(&shard, testKey(.default, "run-3"));
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-3")).?;
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
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    // No definition registered
    createTestRun(&handler, testKey(.default, "run-4"), "run-4", "nonexistent-wf");

    // Should gracefully no-op (no definition found)
    handler.advanceWorkflow(&shard, testKey(.default, "run-4"));
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-4")).?;
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
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-5"), "run-5", "test-wf");

    // With no wait set, the step fails at once.
    actions.missing_action_wait_ms = 0;
    handler.advanceWorkflow(&shard, testKey(.default, "run-5"));
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-5")).?;
    // Should fail because action "step-a" is not registered
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
}

test "step executor: a step waits for its action, and starts when it registers" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-w"), "run-w", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-w"));
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "run-w")).?.status);
    try testing.expectEqual(@as(u32, 1), actions.held.count());

    registerTestAction(&shard, "step-a");
    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    _ = shard.applyCommitted();
    handler.checkPendingActions(&shard);
    // The run exists, so the step waits on it, not on a lost invoke.
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "run-w")).?.status);
    try testing.expectEqual(@as(usize, 1), ActionsHandler.runCount(&shard));
}

test "step executor: past MAX_HELD waiting steps, a step fails at once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    var id_buf: [32]u8 = undefined;
    for (0..ActionsHandler.MAX_HELD + 1) |i| {
        const run_id = try std.fmt.bufPrint(&id_buf, "h{d}", .{i});
        const message = ActionsHandler.encodeStartRunMessage(allocator, .default, run_id, "nope", "{}", "wf-run", "wf", true).?;
        defer allocator.free(message);
        actions.startRunFromInbox(&shard, shard.id, message);
    }
    try testing.expectEqual(@as(u32, ActionsHandler.MAX_HELD), actions.held.count());
    const told = actions.takeFinished(id_buf[0.."h1024".len]).?;
    defer actions.freeFinished(told);
    try testing.expectEqualStrings(ActionsHandler.NOT_REGISTERED, told.outcome);
    try testing.expectEqualStrings("action nope is not registered in namespace default; too many steps are already waiting for an action on this shard", told.output);
}

test "step executor: a step whose hold a step-down dropped fails, rather than wait for ever" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-d"), "run-d", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-d"));
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "run-d")).?.status);

    actions.stoppedLeading(&shard);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.runs.get(testKey(.default, "run-d")).?.status);
}

test "step executor: a start let go of isn't lost before its invoke applies" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-r"), "run-r", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-r"));
    handler.checkPendingActions(&shard);
    registerTestAction(&shard, "step-a");
    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    // Proposed, not applied.
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "run-r")).?.status);
}

test "step executor: an action deleted after its invoke was proposed fails the step at once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-x"), "run-x", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-x"));
    // The delete comes first in the log: the invoke applies to no action.
    applyTestActionEntry(&shard, .action_delete, test_action_entries.Delete.Value{ .ns = NamespaceId.default.int(), .name = "step-a" }, 1);
    _ = shard.applyCommitted();
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-x")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    var found = false;
    for (run.history.items) |e| {
        if (std.mem.eql(u8, e.event_type_owned, "action_not_found")) {
            try testing.expectEqualStrings("action step-a is not registered in namespace default", e.detail_owned);
            found = true;
        }
    }
    try testing.expect(found);
}

test "step executor: a start too long to log fails its step, and isn't held" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    const input = try allocator.alloc(u8, 70 * 1024);
    defer allocator.free(input);
    @memset(input, 'x');
    const message = ActionsHandler.encodeStartRunMessage(allocator, .default, "big", "nope", input, "wf-run", "wf", true).?;
    defer allocator.free(message);
    actions.startRunFromInbox(&shard, shard.id, message);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    const told = actions.takeFinished("big").?;
    defer actions.freeFinished(told);
    try testing.expectEqualStrings("", told.outcome);
}

test "step executor: a held start that can't be proposed is tried until the wait ends, then fails" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    const message = ActionsHandler.encodeStartRunMessage(allocator, .default, "stuck", "late", "{}", "wf-run", "wf", true).?;
    defer allocator.free(message);
    actions.startRunFromInbox(&shard, shard.id, message);
    registerTestAction(&shard, "late");
    shard.raft_node.halt_at = .{ .index = 1, .term = 1, .crc = 0 };
    defer shard.raft_node.halt_at = null;

    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 1), actions.held.count());

    actions.held.getPtr("stuck").?.until_ms = 0;
    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    const told = actions.takeFinished("stuck").?;
    defer actions.freeFinished(told);
    try testing.expectEqualStrings("", told.outcome);
}

test "step executor: a plan's executor waits when the only other's breaker is open, and not when it could run" {
    const allocator = testing.allocator;
    for ([_]bool{ true, false }) |open| {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        var actions = ActionsHandler.init(allocator);
        defer actions.deinit();
        var shard = try createTestShard(&actions);
        defer destroyTestShard(&shard);
        handler.registerReplay(&shard.replay_registry);
        shard.workflow_handler = &handler;

        createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", test_plan_exhaust_workflow_json);
        createTestRun(&handler, testKey(.default, "run-b"), "run-b", "plan-wf");
        const paypal = handler.getOrCreateHealth(.default, "plan-wf", "charge", "paypal").?;
        if (open) {
            paypal.breaker_state = .open;
            paypal.breaker_opened_at_ms = @import("stdx").time.milliTimestamp();
        }
        // stripe is tried first either way: as listed first, and as the
        // healthier when paypal's breaker is open.
        handler.advanceWorkflow(&shard, testKey(.default, "run-b"));
        try testing.expectEqual(@as(u32, if (open) 1 else 0), actions.held.count());
    }
}

test "workflow duty: looks only at runs parked on an action or a child" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // A run that hasn't parked, and one parked and then done with: neither
    // is walked.
    createTestRun(&handler, testKey(.default, "idle"), "idle", "test-wf");
    try testing.expectEqual(@as(u32, 0), handler.parked.count());
    createTestRun(&handler, testKey(.default, "p"), "p", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "p"));
    _ = shard.applyCommitted();
    try testing.expectEqual(@as(u32, 1), handler.parked.count());
    const run = handler.runs.getPtr(testKey(.default, "p")).?;
    run.status = .running;
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(u32, 0), handler.parked.count());
}

test "workflow duty: a timer fires when due, and one the run moved past is dropped" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    createTestRun(&handler, testKey(.default, "moved"), "moved", "test-wf");
    createTestRun(&handler, testKey(.default, "later"), "later", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    for ([_][]const u8{ testKey(.default, "due"), testKey(.default, "moved"), testKey(.default, "later") }, [_]i64{ now - 1, now - 1, now + 3_600_000 }) |key, at| {
        const run = handler.runs.getPtr(key).?;
        run.status = .waiting;
        run.wait_timeout_at_ms = at;
        handler.armTimer(run, at);
    }
    handler.runs.getPtr(testKey(.default, "moved")).?.status = .running;
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.runs.get(testKey(.default, "due")).?.status);
    try testing.expectEqual(WorkflowHandler.RunStatus.running, handler.runs.get(testKey(.default, "moved")).?.status);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "later")).?.status);
    // The later one's timer is still held.
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
}

test "workflow duty: a run with two timers due fires once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));

    // Parked on its first wait, whose timeout goes to a second with none.
    // Two entries come due at once, as when an earlier wait's stays behind.
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    run.wait_timeout_at_ms = 1;
    handler.armTimer(run, 1);
    handler.armTimer(run, 1);
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const after = handler.runs.get(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, after.status);
    try testing.expectEqualStrings("second", after.current_step_name_owned.?);
    try testing.expectEqual(@as(i64, 0), after.wait_timeout_at_ms);
}

test "workflow duty: a wait with a timeout arms its timer, which holds until due" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    try testing.expectEqual(@as(usize, 1), handler.timers.count());

    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    // Not due: it stays.
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expect(run.current_step_name_owned == null);
}

test "workflow duty: an entry from before doesn't fire a run's later timer" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const later = @import("stdx").time.milliTimestamp() + 3_600_000;
    createTestRun(&handler, testKey(.default, "w"), "w", "test-wf");
    createTestRun(&handler, testKey(.default, "p"), "p", "test-wf");
    const waits = handler.runs.getPtr(testKey(.default, "w")).?;
    waits.status = .waiting;
    waits.wait_timeout_at_ms = later;
    handler.armTimer(waits, 1);
    const polls = handler.runs.getPtr(testKey(.default, "p")).?;
    polls.status = .waiting;
    polls.poll_next_at_ms = later;
    handler.armTimer(polls, 1);

    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.runs.get(testKey(.default, "w")).?.status);
    try testing.expectEqual(later, handler.runs.get(testKey(.default, "p")).?.poll_next_at_ms);
    try testing.expectEqual(@as(usize, 0), handler.timers.count());
}

test "workflow duty: a signal already there ends a wait, and leaves no deadline or target for the next" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    // What an earlier wait left, and the signal the first wait wants.
    run.wait_timeout_at_ms = 1;
    run.wait_timeout_target_owned = try allocator.dupe(u8, "elsewhere");
    try run.signals.append(allocator, .{ .signal_type_owned = try allocator.dupe(u8, "first"), .payload_owned = null, .received_at_ms = 0 });

    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
    try testing.expectEqual(@as(i64, 0), run.wait_timeout_at_ms);
    try testing.expect(run.wait_timeout_target_owned == null);
}

test "workflow duty: a failed insert is made good from the runs" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    createTestRun(&handler, testKey(.default, "p"), "p", "two-waits");
    const timed = handler.runs.getPtr(testKey(.default, "r")).?;
    timed.status = .waiting;
    timed.wait_timeout_at_ms = @import("stdx").time.milliTimestamp() + 3_600_000;
    const parked = handler.runs.getPtr(testKey(.default, "p")).?;
    parked.status = .waiting;
    parked.pending_action_run_id_owned = try allocator.dupe(u8, "gone");

    // The inserts failed: nothing is held for either run.
    handler.rewatch = true;
    handler.checkPendingActions(&shard);
    try testing.expect(!handler.rewatch);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    try testing.expect(handler.parked.contains(testKey(.default, "p")));
}

test "workflow schedule: max_concurrent follows runs starting and finishing, each counted once, per namespace" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    var out: Result = .{};
    var value: [64]u8 = undefined;
    var off: usize = 0;
    for ([_][]const u8{ "wf", "1" }) |p| {
        std.mem.writeInt(u16, value[off..][0..2], @intCast(p.len), .little);
        @memcpy(value[off + 2 ..][0..p.len], p);
        off += 2 + p.len;
    }
    value[off] = @backingInt(WorkflowHandler.RunStatus.running);
    off += 1;
    @memset(value[off..][0..4], 0); // no event type, no idempotency key
    off += 4;
    try handler.replayStart(testKey(TEST_NS_A, "r1"), value[0..off], 1, &out);
    try handler.replayStart(testKey(TEST_NS_A, "r2"), value[0..off], 1, &out);
    try handler.replayStart(testKey(TEST_NS_B, "r1"), value[0..off], 1, &out);
    try testing.expectEqual(@as(?u32, 2), handler.active_runs.get(testKey(TEST_NS_A, "wf")));
    try testing.expectEqual(@as(?u32, 1), handler.active_runs.get(testKey(TEST_NS_B, "wf")));
    // A start applied again creates nothing.
    try handler.replayStart(testKey(TEST_NS_A, "r1"), value[0..off], 1, &out);
    try testing.expectEqual(@as(?u32, 2), handler.active_runs.get(testKey(TEST_NS_A, "wf")));

    // A start's entry carries its namespace's id: "a" is created first.
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = 1, .max_concurrent = 2 }, 0);
    const runsOf = struct {
        fn count(h: *WorkflowHandler, shard_: *Shard) usize {
            const sched = h.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
            sched.fired_ms = std.math.minInt(i64);
            sched.proposed_ms = std.math.minInt(i64);
            const before = shard_.raft_node.log.lastIndex();
            h.tickSchedules(shard_);
            return shard_.raft_node.log.lastIndex() - before;
        }
    }.count;
    // Two unfinished here: it holds, though another namespace's run makes three.
    try testing.expectEqual(@as(usize, 0), runsOf(&handler, &shard));

    const run = handler.runs.getPtr(testKey(TEST_NS_A, "r1")).?;
    const c: WorkflowHandler.Completion = .{ .status = .cancelled, .output = null, .steps = &.{}, .history = &.{}, .last = .{ .event_type = "workflow_cancelled", .detail = "stop", .timestamp_ms = 7 }, .tags = null };
    const record = try WorkflowHandler.encodeCompletion(allocator, c, WorkflowHandler.completionLimit(testKey(TEST_NS_A, "r1")));
    defer allocator.free(record);
    // The producer's copy and the commit's: one run finished.
    try handler.applyComplete(run, record, 7);
    try handler.applyComplete(run, record, 7);
    try testing.expectEqual(@as(?u32, 1), handler.active_runs.get(testKey(TEST_NS_A, "wf")));
    // Room now: it fires once.
    try testing.expectEqual(@as(usize, 1), runsOf(&handler, &shard));
}

/// A workflow_start's value, as `proposeStart` encodes it.
fn startValue(buf: []u8, wf: []const u8, event_type: []const u8, idem: []const u8) []const u8 {
    var off: usize = 0;
    for ([_][]const u8{ wf, "1" }) |p| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(p.len), .little);
        @memcpy(buf[off + 2 ..][0..p.len], p);
        off += 2 + p.len;
    }
    buf[off] = @backingInt(WorkflowHandler.RunStatus.running);
    off += 1;
    for ([_][]const u8{ event_type, idem }) |p| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(p.len), .little);
        @memcpy(buf[off + 2 ..][0..p.len], p);
        off += 2 + p.len;
    }
    return buf[0..off];
}

test "workflow first steps: a burst bigger than a pass is logged across passes, each run once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    // An entry carries its namespace's id: "a" is created first.
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    const a = shard.namespace_handler.idOf("a").?;
    var keys: [200][16]u8 = undefined;
    for (&keys, 0..) |*k, i| {
        var id_buf: [8]u8 = undefined;
        const key = writeKey(k, a, try std.fmt.bufPrint(&id_buf, "r{d:0>5}", .{i}));
        try handler.to_step.put(allocator, key, {});
    }
    var counts: [5]u64 = undefined;
    for (&counts) |*c| {
        const before = shard.raft_node.log.lastIndex();
        handler.mu.lock();
        handler.proposeFirstSteps(&shard);
        handler.mu.unlock();
        c.* = shard.raft_node.log.lastIndex() - before;
    }
    try testing.expectEqualSlices(u64, &.{ 64, 64, 64, 8, 0 }, &counts);
    try testing.expectEqual(@as(u32, 200), handler.stepping.count());
}

test "workflow schedule: slots count from the anchor, and the latest one due is the one that fires" {
    var s: WorkflowHandler.ScheduleState = .{ .workflow_name_owned = "", .ns = .default, .interval_ms = 1000, .max_concurrent = 1, .input_owned = null, .anchor_ms = 10_500 };
    try testing.expectEqual(@as(?i64, null), WorkflowHandler.dueSlot(&s, 10_499));
    try testing.expectEqual(@as(?i64, 10_500), WorkflowHandler.dueSlot(&s, 10_500));
    try testing.expectEqual(@as(?i64, 10_500), WorkflowHandler.dueSlot(&s, 11_499));
    try testing.expectEqual(@as(?i64, 11_500), WorkflowHandler.dueSlot(&s, 11_500));
    // Hours missed: the latest slot, not each of them.
    try testing.expectEqual(@as(?i64, 10_500 + 3600 * 1000), WorkflowHandler.dueSlot(&s, 10_500 + 3600 * 1000 + 999));
    s.anchor_ms = 0;
    try testing.expectEqual(@as(?i64, 7000), WorkflowHandler.dueSlot(&s, 7999));
}

test "workflow schedule: a cron's slots are its fire times from the anchor on" {
    const minute = cron_mod.calendarToEpochMs(2024, 1, 15, 10, 0);
    var s: WorkflowHandler.ScheduleState = .{ .workflow_name_owned = "", .ns = .default, .interval_ms = 0, .cron = cron_mod.Cron.parse("*/5 * * * *").?, .max_concurrent = 1, .input_owned = null, .anchor_ms = minute + 30_000 };
    // Created after 10:00 struck: nothing until 10:05.
    try testing.expectEqual(@as(?i64, null), WorkflowHandler.dueSlot(&s, minute + 50_000));
    try testing.expectEqual(@as(?i64, null), WorkflowHandler.dueSlot(&s, minute + 4 * 60_000 + 59_999));
    try testing.expectEqual(@as(?i64, minute + 5 * 60_000), WorkflowHandler.dueSlot(&s, minute + 5 * 60_000));
    // A day missed: the latest fire, once.
    try testing.expectEqual(@as(?i64, minute + 86_400_000), WorkflowHandler.dueSlot(&s, minute + 86_400_000 + 4 * 60_000));
}

test "workflow schedule: enabled again, slots count from the enable, and those missed while disabled don't fire" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var out: Result = .{};
    try handler.replayCreate(testKey(TEST_NS_A, "wf"),
        \\kind: Workflow
        \\name: wf
        \\version: 1.0.0
        \\schedule:
        \\  interval: 1000
        \\start:
        \\  wait_for_signal:
        \\    type: never
        \\  transitions:
        \\    success: flo.Completed
    , 1000, &out);
    const s = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    try testing.expectEqual(@as(?i64, 4000), WorkflowHandler.dueSlot(s, 4999));

    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{0}, 5000, &out);
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{1}, 9500, &out);
    // Not 9000, the old anchor's slot: the enable is slot 0.
    try testing.expectEqual(@as(?i64, 9500), WorkflowHandler.dueSlot(s, 9600));
    try testing.expectEqual(@as(?i64, 10_500), WorkflowHandler.dueSlot(s, 10_600));
    // An enable of an enabled workflow moves nothing.
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{1}, 12_250, &out);
    try testing.expectEqual(@as(?i64, 12_500), WorkflowHandler.dueSlot(s, 12_600));
}

test "workflow start: a disable committed ahead refuses a client or schedule start at apply, leaving the slot unfired; trigger and child starts still run" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var out: Result = .{};
    try handler.replayCreate(testKey(TEST_NS_A, "wf"),
        \\kind: Workflow
        \\name: wf
        \\version: 1.0.0
        \\schedule:
        \\  interval: 1000
        \\start:
        \\  wait_for_signal:
        \\    type: never
        \\  transitions:
        \\    success: flo.Completed
    , 1000, &out);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    const fired_before = sched.fired_ms;

    // The leader proposed slot 4000's start; a disable commits first.
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{0}, 4500, &out);
    var buf: [256]u8 = undefined;
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-sched"), startValue(&buf, "wf", "schedule_started", "schedule:16:wf:4000"), 4600, &out);
    try testing.expectEqualStrings("workflow is disabled", out.refused().?.refusal.message);
    try testing.expect(!handler.runs.contains(testKey(TEST_NS_A, "r-sched")));
    try testing.expectEqual(fired_before, sched.fired_ms);
    try testing.expectEqual(@as(u32, 0), handler.active_runs.get(testKey(TEST_NS_A, "wf")) orelse 0);

    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-client"), startValue(&buf, "wf", "workflow_started", ""), 4600, &out);
    try testing.expectEqualStrings("workflow is disabled", out.refused().?.refusal.message);
    try testing.expect(!handler.runs.contains(testKey(TEST_NS_A, "r-client")));

    // A trigger's start is not held to it: its leader moved the cursor.
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-trig"), startValue(&buf, "wf", "trigger_started", ""), 4600, &out);
    try testing.expect(out.refused() == null);
    try testing.expect(handler.runs.contains(testKey(TEST_NS_A, "r-trig")));
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-child"), startValue(&buf, "wf", "child_started", ""), 4600, &out);
    try testing.expect(out.refused() == null);

    // Enabled again, the refused slot doesn't fire: slots count from the enable.
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{1}, 7250, &out);
    try testing.expectEqual(@as(?i64, 7250), WorkflowHandler.dueSlot(sched, 7300));
}

test "workflow start: a redefinition that requires a key, committed ahead, refuses a client start without one at apply" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var out: Result = .{};
    try handler.replayCreate(testKey(TEST_NS_A, "wf"),
        \\kind: Workflow
        \\name: wf
        \\version: 1.0.0
        \\idempotency: required
        \\start:
        \\  wait_for_signal:
        \\    type: never
        \\  transitions:
        \\    success: flo.Completed
    , 1000, &out);
    var buf: [256]u8 = undefined;
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r1"), startValue(&buf, "wf", "workflow_started", ""), 2000, &out);
    try testing.expectEqualStrings("idempotency key is required for this workflow", out.refused().?.refusal.message);
    try testing.expect(!handler.runs.contains(testKey(TEST_NS_A, "r1")));
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r2"), startValue(&buf, "wf", "workflow_started", "k1"), 2000, &out);
    try testing.expect(out.refused() == null);
    // A child's start carries no key and was never held to one.
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r3"), startValue(&buf, "wf", "child_started", ""), 2000, &out);
    try testing.expect(out.refused() == null);
}

test "workflow schedule: a slot starts one run however many leaders propose it, and a missed one fires once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    // Created ten intervals ago; slot 0 fired then, nothing since.
    const interval: i64 = 3_600_000;
    const now = @import("stdx").time.milliTimestamp();
    const anchor = now - 10 * interval;
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = interval, .max_concurrent = 0 }, anchor);
    var buf: [256]u8 = undefined;
    var key_buf: [128]u8 = undefined;
    var out: Result = .{};
    const slot0 = try std.fmt.bufPrint(&key_buf, "schedule:16:wf:{d}", .{anchor});
    try handler.replayStart(testKey(TEST_NS_A, "r0"), startValue(&buf, "wf", "schedule_started", slot0), 1, &out);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    try testing.expectEqual(anchor, sched.fired_ms);

    // The nine missed slots and the current one fire as one run.
    var before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    const latest = anchor + 10 * interval;
    try testing.expectEqual(latest, sched.proposed_ms);
    // Proposed: this term doesn't propose it again.
    before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    // The next leader, its start unapplied, proposes the same slot.
    shard.raft_node.current_term += 1;
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());

    // Both commit: the second creates nothing, and the slot has fired.
    const key = try std.fmt.bufPrint(&key_buf, "schedule:16:wf:{d}", .{latest});
    try handler.replayStart(testKey(TEST_NS_A, "r1"), startValue(&buf, "wf", "schedule_started", key), 2, &out);
    try handler.replayStart(testKey(TEST_NS_A, "r2"), startValue(&buf, "wf", "schedule_started", key), 3, &out);
    try testing.expect(handler.runs.contains(testKey(TEST_NS_A, "r1")));
    try testing.expect(!handler.runs.contains(testKey(TEST_NS_A, "r2")));
    try testing.expectEqual(latest, sched.fired_ms);
    // A later leader proposes nothing until the next slot.
    shard.raft_node.current_term += 1;
    before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
}

/// An entry proposed and not yet applied, as a cancel's is: its index and term.
fn unappliedEntry(shard: *Shard) !persistence_mod.ProposeResult {
    return shard.raft_node.propose(.raft_noop, entry_mod.Flags.NONE, "");
}

test "workflow cancel: while one is in flight nothing moves the run, and once it can't apply the run carries on" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    run.wait_timeout_at_ms = 1;
    handler.armTimer(run, 1);

    const raft = shard.raft_node;
    const cancel = try unappliedEntry(&shard);
    run.ending_index = cancel.index;
    run.ending_term = cancel.term;
    try testing.expect(WorkflowHandler.endingInFlight(&shard, run));
    // A timer due while it's in flight neither fires nor is lost.
    handler.checkPendingActions(&shard);
    try testing.expect(run.current_step_name_owned == null);
    // The wait's own far-off timer, and the due one again, a little later.
    try testing.expectEqual(@as(usize, 2), handler.timers.count());

    // Elected again, its entry still in the log: it may yet apply.
    raft.current_term += 1;
    try testing.expect(WorkflowHandler.endingInFlight(&shard, run));
    raft.current_term -= 1;
    // Its entry cut and replaced by another term's.
    run.ending_term = cancel.term + 1;
    try testing.expect(!WorkflowHandler.endingInFlight(&shard, run));
    run.ending_term = cancel.term;
    // No longer leading.
    raft.role = .follower;
    try testing.expect(!WorkflowHandler.endingInFlight(&shard, run));
    raft.role = .leader;
    // Its index applied without ending the run.
    _ = shard.applyCommitted();
    try testing.expect(!WorkflowHandler.endingInFlight(&shard, run));

    // The timer it kept fires now.
    @import("stdx").time.sleep(@as(u64, WorkflowHandler.ENDING_RECHECK_MS + 20) * std.time.ns_per_ms);
    handler.checkPendingActions(&shard);
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
}

test "workflow cancel: in flight, a finished action, a first step and a completion retry all wait for it" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const raft = shard.raft_node;

    createTestRun(&handler, testKey(.default, "parked"), "parked", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "parked"));
    _ = shard.applyCommitted();
    const parked = handler.runs.getPtr(testKey(.default, "parked")).?;
    const action_run = parked.pending_action_run_id_owned.?;
    completeTestAction(&shard, action_run);

    createTestRun(&handler, testKey(.default, "started"), "started", "test-wf");
    handler.queueFirstStep(testKey(.default, "started"), raft.last_applied);
    createTestRun(&handler, testKey(.default, "retry"), "retry", "test-wf");
    handler.retryCompletionLater(testKey(.default, "retry"), .failed, "stop", 1, 0);
    handler.completion_retries.items[0].due_ms = 0;

    const cancel = try unappliedEntry(&shard);
    for ([_][]const u8{ testKey(.default, "parked"), testKey(.default, "started"), testKey(.default, "retry") }) |key| {
        const run = handler.runs.getPtr(key).?;
        run.ending_index = cancel.index;
        run.ending_term = cancel.term;
    }
    handler.advanceStartedRuns(&shard);
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, parked.status);
    // Not resumed: still at its first step, on the same action run.
    try testing.expect(parked.current_step_name_owned == null);
    try testing.expectEqualStrings(action_run, parked.pending_action_run_id_owned.?);
    try testing.expectEqual(@as(usize, 1), handler.started_to_advance.items.len);
    try testing.expectEqual(@as(usize, 1), handler.completion_retries.items.len);
    try testing.expectEqual(WorkflowHandler.RunStatus.running, handler.runs.get(testKey(.default, "retry")).?.status);

    // Its entry cut, each goes on.
    parked.ending_term = cancel.term + 1;
    try testing.expect(!WorkflowHandler.endingInFlight(&shard, parked));
}

test "a halted shard's expire duty holds back the starts it holds, and lets them go once it serves" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    const message = ActionsHandler.encodeStartRunMessage(allocator, .default, "held-run", "late", "{}", "wf-run", "wf", true).?;
    defer allocator.free(message);
    actions.startRunFromInbox(&shard, shard.id, message);
    registerTestAction(&shard, "late");

    shard.halted = .{ .index = 1, .term = 1, .crc = 0 };
    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 1), actions.held.count());
    try testing.expectEqual(@as(u32, 1), shard.duties.held());

    shard.halted = null;
    _ = actions.expireDue(&shard);
    try testing.expectEqual(@as(u32, 0), actions.held.count());
    try testing.expectEqual(@as(u32, 0), shard.duties.held());
}

test "workflow_enabled entries disable and enable a workflow on every replica, and refuse one gone" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    var index: u64 = 0;
    const apply = struct {
        fn one(h: *WorkflowHandler, i: *u64, key: []const u8, enabled: bool) !Result {
            var pbuf: [128]u8 = undefined;
            i.* += 1;
            const e = entry_mod.buildCommandEntry(.workflow_enabled, entry_mod.Flags.NONE, 1, i.*, 0, .default, key, &.{@intFromBool(enabled)}, &pbuf).?;
            var out = Result.start(&e);
            try h.replayEntry(&e, &out);
            return out;
        }
    }.one;
    _ = try apply(&handler, &index, testKey(.default, "test-wf"), false);
    try testing.expect(handler.disabled.contains(testKey(.default, "test-wf")));
    _ = try apply(&handler, &index, testKey(.default, "test-wf"), false);
    try testing.expectEqual(@as(u32, 1), handler.disabled.count());
    _ = try apply(&handler, &index, testKey(.default, "test-wf"), true);
    try testing.expect(!handler.disabled.contains(testKey(.default, "test-wf")));
    const gone = try apply(&handler, &index, testKey(.default, "no-such"), false);
    try testing.expectEqual(proto.StatusCode.not_found, gone.refused().?.status);
    try testing.expectEqual(@as(u32, 0), handler.disabled.count());
}

test "step executor: failing action follows failure transition" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b"); // step-b will fail

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-6"), "run-6", "test-wf");

    // Phase 1: advance invokes step-a, parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-6"));
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-6")).?;
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }
    // Phase 2: step-a completes (success) → transitions to step_b → step-b parks
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-6")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "failure"
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "failure");
    }
    // Phase 3: step-b fails → follows "failure" transition → flo.Failed
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-6")).?;
    // step-a succeeds → step_b → step-b fails → "failure" → flo.Failed
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
}

test "step executor: an action completed naming no outcome takes the success transition" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-e"), "run-e", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-e"));
    _ = shard.applyCommitted();
    // Both steps' workers complete with an empty outcome, as a plain
    // complete sends.
    for (0..2) |_| {
        const run = handler.runs.getPtr(testKey(.default, "run-e")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "");
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, handler.runs.get(testKey(.default, "run-e")).?.status);
}

test "step executor: a step whose action run another shard says was lost resumes, and fails" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-l"), "run-l", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-l"));
    _ = shard.applyCommitted();
    const run = handler.runs.getPtr(testKey(.default, "run-l")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    // As if its action ran on shard 1, which says it never started it.
    run.pending_action_shard_id = 1;
    const rid = run.pending_action_run_id_owned.?;
    var msg: [128]u8 = undefined;
    std.mem.writeInt(u16, msg[0..2], @intCast(rid.len), .little);
    @memcpy(msg[2..][0..rid.len], rid);
    msg[2 + rid.len] = @backingInt(@import("../protocol/result.zig").CommandResult.ActionRunStatus.cancelled);
    std.mem.writeInt(u16, msg[3 + rid.len ..][0..2], 0, .little);
    actions.finishedFromInbox(msg[0 .. 5 + rid.len]);
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    try testing.expect(handler.runs.get(testKey(.default, "run-l")).?.status != .waiting);
}

test "step executor: retry on failure" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "flaky"); // always fails

    createTestDef(&handler, testKey(.default, "retry-wf"), "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, testKey(.default, "run-7"), "run-7", "retry-wf");

    // Initial attempt: flaky action parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-7"));
    _ = shard.applyCommitted();

    // Drive attempts: 1 original + 3 retries (max_attempts=3) = 4 total
    // Each iteration: complete current action with "failure", checkPendingActions
    // (which either retries→re-parks, or exhausts retries→flo.Failed)
    var attempt: u32 = 0;
    while (attempt < 5) : (attempt += 1) {
        const run_ptr = handler.runs.getPtr(testKey(.default, "run-7")).?;
        if (run_ptr.status.isTerminal()) break;
        const rid = run_ptr.pending_action_run_id_owned orelse break;
        completeTestActionWith(&shard, rid, "failure");
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }

    const run = handler.runs.get(testKey(.default, "run-7")).?;
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

// Two executors that always fail, each with one retry, under
// health-weighted selection: a failure lowers the failing executor's health,
// which used to sort it under the pass's next position, so one executor ran
// past its retries and the other was never tried.
const test_plan_exhaust_workflow_json =
    \\{"kind":"Workflow","name":"plan-wf","version":"1.0.0",
    \\"plans":{"charge":{"selection":"health-weighted","executors":[
    \\{"name":"stripe","run":"@actions/stripe","retry":{"max_attempts":1},"breaker":{"failure_threshold":50}},
    \\{"name":"paypal","run":"@actions/paypal","retry":{"max_attempts":1},"breaker":{"failure_threshold":50}}]}},
    \\"start":{"run":"@plan/charge","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

test "step executor: a plan whose executors all fail fails the step after one pass" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "stripe");
    registerTestAction(&shard, "paypal");

    createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", test_plan_exhaust_workflow_json);
    createTestRun(&handler, testKey(.default, "run-plan"), "run-plan", "plan-wf");

    handler.advanceWorkflow(&shard, testKey(.default, "run-plan"));
    _ = shard.applyCommitted();

    // Each executor runs once and retries once: four tasks in all.
    var tasks: u32 = 0;
    while (tasks < 40) : (tasks += 1) {
        const run_ptr = handler.runs.getPtr(testKey(.default, "run-plan")).?;
        if (run_ptr.status.isTerminal()) break;
        const rid = run_ptr.pending_action_run_id_owned orelse break;
        completeTestActionWith(&shard, rid, "failure");
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }

    const run = handler.runs.get(testKey(.default, "run-plan")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    try testing.expectEqual(@as(u32, 4), tasks);
    var exhausted: [2]u32 = .{ 0, 0 };
    for (run.history.items) |evt| {
        if (!std.mem.eql(u8, evt.event_type_owned, "plan_executor_exhausted")) continue;
        const i: usize = if (std.mem.eql(u8, evt.detail_owned, "stripe")) 0 else 1;
        exhausted[i] += 1;
    }
    try testing.expectEqual([2]u32{ 1, 1 }, exhausted);
}

test "step executor: an action run that timed out takes the step's failure transition" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "flaky");

    createTestDef(&handler, testKey(.default, "retry-wf"), "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, testKey(.default, "run-to"), "run-to", "retry-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-to"));
    _ = shard.applyCommitted();

    // Each attempt's action run times out; the step's retries then fail it.
    var attempts: u32 = 0;
    while (attempts < 6) : (attempts += 1) {
        const run = handler.runs.getPtr(testKey(.default, "run-to")).?;
        if (run.status.isTerminal()) break;
        const rid = run.pending_action_run_id_owned orelse break;
        timeOutTestAction(&shard, rid);
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.runs.get(testKey(.default, "run-to")).?.status);
}

// An action that returns the `pending` business outcome on a `poll:` step must
// re-arm a backoff poll (not fail with "no transition"), and after max_attempts
// is exceeded must follow the `timeout` transition. Driven at the handler layer
// because the worker CLI cannot emit a `pending` outcome.
test "step executor: poll re-arms on pending and times out after max_attempts" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "poller");

    createTestDef(&handler, testKey(.default, "poll-wf"), "poll-wf", test_poll_workflow_json);
    createTestRun(&handler, testKey(.default, "run-poll"), "run-poll", "poll-wf");

    // Initial: poller invoked, parks for async completion.
    handler.advanceWorkflow(&shard, testKey(.default, "run-poll"));
    _ = shard.applyCommitted();

    // First `pending` must ARM a poll (not fail): status waiting, attempt incremented.
    {
        const run = handler.runs.getPtr(testKey(.default, "run-poll")).?;
        try testing.expect(run.pending_action_run_id_owned != null);
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "pending");
    }
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();
    {
        const run = handler.runs.getPtr(testKey(.default, "run-poll")).?;
        try testing.expect(!run.status.isTerminal()); // pre-fix bug: would be .failed
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        try testing.expectEqual(@as(u32, 1), run.poll_attempt);
    }

    // Drive remaining poll cycles: each tick either fires the due poll timer
    // (re-invoking the action) or processes the re-invoked action's `pending`
    // completion (re-arming). max_attempts=2 → exhausted → `timeout` → flo.TimedOut.
    var i: u32 = 0;
    while (i < 12) : (i += 1) {
        const run = handler.runs.getPtr(testKey(.default, "run-poll")).?;
        if (run.status.isTerminal()) break;
        if (run.pending_action_run_id_owned) |rid| {
            completeTestActionWith(&shard, rid, "pending");
        }
        handler.checkPendingActions(&shard);
        _ = shard.applyCommitted();
    }

    const run = handler.runs.get(testKey(.default, "run-poll")).?;
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
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // Manually create a run that's parked waiting for an action
    {
        const alloc = handler.allocator;
        const ns_key = alloc.dupe(u8, testKey(.default, "run-8")) catch return;
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

        try handler.parked.put(alloc, ns_key, {});
        handler.runs.put(ns_key, .{
            .ns_key = ns_key,
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

    // A completed action run, of an action the transitions resolve.
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");
    applyTestActionEntry(&shard, .action_invoke, test_action_entries.Invoke.Value{ .ns = NamespaceId.default.int(), .action = "step-a", .run = "action-42", .input = "", .labels = null, .source = .workflow, .caller_run = "run-8", .caller_workflow = null, .caller_shard = null }, 1000);
    completeTestAction(&shard, "action-42");

    // checkPendingActions should detect the completed action and resume step-a
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    // After step-a resumes, workflow advances to step_b (async) — parks again
    {
        const run = handler.runs.getPtr(testKey(.default, "run-8")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b so the workflow can finish
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Second checkPendingActions: step-b done → flo.Completed
    handler.checkPendingActions(&shard);
    _ = shard.applyCommitted();

    const run = handler.runs.get(testKey(.default, "run-8")).?;
    // After resuming start → step_b (success) → flo.Completed
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
}

test "list-runs query: every truncation of a valid value is refused, not read past" {
    const valid = [_]u8{ 7, 0, 0, 0, 7, 0 } ++ "running".* ++ [_]u8{ 2, 0 } ++ "c1".* ++ [_]u8{ 3, 0 } ++ "abc".*;
    for (0..valid.len) |n| {
        try testing.expect(WorkflowHandler.parseListRunsQuery(valid[0..n]) == null);
    }
    const q = WorkflowHandler.parseListRunsQuery(&valid).?;
    try testing.expectEqual(@as(u32, 7), q.limit);
    try testing.expectEqualStrings("running", q.status.?);
    try testing.expectEqualStrings("c1", q.cursor.?);
    try testing.expectEqualStrings("abc", q.search.?);

    const empty = [_]u8{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const e = WorkflowHandler.parseListRunsQuery(&empty).?;
    try testing.expect(e.status == null and e.cursor == null and e.search == null);
}

test "list-runs query: a length that overruns the value is refused" {
    // Each value's last length prefix, or the bytes it claims, runs past the end.
    try testing.expect(WorkflowHandler.parseListRunsQuery(&[_]u8{ 0, 0, 0, 0, 1, 0, 'x', 0, 0, 0 }) == null);
    try testing.expect(WorkflowHandler.parseListRunsQuery(&[_]u8{ 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 0 }) == null);
    try testing.expect(WorkflowHandler.parseListRunsQuery(&[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 'a', 'b' }) == null);
}

test "workflow replay: a start that runs out of memory fails and creates nothing, and one that applies keeps its idempotency key" {
    // [wf_len][wf][ver_len][ver][status][evt_len][idem_len][idem]
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1, 0, 0, 3, 0, 'k', 'e', 'y' };
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var fa = std.testing.FailingAllocator.init(testing.allocator, .{});
        var handler = WorkflowHandler.init(fa.allocator());
        defer handler.deinit();
        // Room for the run up front: a failed copy must not be masked by
        // the map's insert failing after it.
        try handler.runs.ensureTotalCapacity(1);
        fa.fail_index = fa.alloc_index + fail_at;
        var out: Result = .{};
        handler.replayStart(testKey(.default, "r1"), &start, 1, &out) catch {
            try testing.expect(fa.has_induced_failure);
            try testing.expectEqual(@as(usize, 0), handler.runCount());
            continue;
        };
        const run = handler.runs.get(testKey(.default, "r1")) orelse return error.RunMissing;
        try testing.expectEqualStrings("key", run.idempotency_key_owned orelse return error.IdempotencyKeyLost);
        if (!fa.has_induced_failure) break;
    }
}

test "workflow replay: a start answers with its run id whole, at the longest key an entry carries" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
    var key: [persistence_mod.MAX_ENTRY_KEY]u8 = undefined;
    _ = writeKey(&key, .default, &@as([key.len - KEY_PREFIX]u8, @splat('r')));
    var out: Result = .{};
    try handler.replayStart(&key, &start, 1, &out);
    try testing.expect(!out.overflowed);
    var r = out.reader();
    try testing.expectEqual(@as(?u8, 1), r.int(u8));
    try testing.expectEqualSlices(u8, keyName(&key), r.slice().?);
}

test "workflow replay: an unknown status, or a complete entry without its output flag, halts and changes nothing" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    // [wf_len][wf][ver_len][ver][status][evt_len][idem_len]
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
    var bad_status = start;
    bad_status[7] = 0xee;
    var out: Result = .{};
    try testing.expectError(error.Halt, handler.replayStart(testKey(.default, "r0"), &bad_status, 1, &out));
    try testing.expectEqual(@as(usize, 0), handler.runCount());

    try handler.replayStart(testKey(.default, "r1"), &start, 1, &out);
    try testing.expectEqual(@as(usize, 1), handler.runCount());

    // Missing has_output byte; then an unknown status. Neither may change the run.
    try testing.expectError(error.Halt, handler.replayComplete(testKey(.default, "r1"), &[_]u8{3}, 2, &replay_out_for_tests));
    try testing.expectError(error.Halt, handler.replayComplete(testKey(.default, "r1"), &[_]u8{ 0xee, 0 }, 2, &replay_out_for_tests));
    try testing.expectEqual(WorkflowHandler.RunStatus.running, handler.runs.get(testKey(.default, "r1")).?.status);
}

test "workflow replay: a redefinition that won't parse halts and keeps the definition it would replace" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    const yaml =
        \\kind: Workflow
        \\name: wf
        \\version: "1"
        \\
        \\start:
        \\  run: "@actions/a"
        \\  transitions:
        \\    success: flo.Completed
        \\    failure: flo.Failed
    ;
    var out: Result = .{};
    try handler.replayCreate(testKey(.default, "wf"), yaml, 1, &out);
    try testing.expectError(error.Halt, handler.replayCreate(testKey(.default, "wf"), "kind: [", 2, &out));
    const def = handler.definitions.get(testKey(.default, "wf")) orelse return error.DefinitionDropped;
    try testing.expectEqualStrings("1", def.version_owned);
    try testing.expectEqual(@as(usize, 1), handler.definitions.count());
}

test "workflow replay: a run starts and completes at its entries' stamps, not the host's clock" {
    const host_ms = @import("stdx").time.milliTimestamp();
    // One stamp far behind the host's clock and one a day ahead: an applier
    // reading the clock misses one or the other.
    for ([_]i64{ 1_000, host_ms + std.time.ms_per_day }) |stamp_ms| {
        var handler = WorkflowHandler.init(testing.allocator);
        defer handler.deinit();
        const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
        const stamp_ns: u64 = @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms;
        var pbuf: [128]u8 = undefined;
        // Through `replayEntry`, as the log applies them.
        var out: Result = .{};
        try handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_start, 0, 1, 1, stamp_ns, .default, testKey(.default, "r1"), &start, &pbuf).?, &out);
        try testing.expectEqual(stamp_ms, handler.runs.get(testKey(.default, "r1")).?.created_at_ms);
        // completed, no output, no steps, no history, no tags
        try handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_complete, 0, 1, 2, stamp_ns + 9 * std.time.ns_per_ms, .default, testKey(.default, "r1"), &[_]u8{ 3, 0, 0, 0, 0, 0, 0, 0 }, &pbuf).?, &out);
        const run = handler.runs.get(testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
        try testing.expectEqual(@as(?i64, stamp_ms + 9), run.completed_at_ms);
    }
}

test "workflow replay: a complete entry cut short leaves the run as it was, and a whole one replaces it" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
    var start_out: Result = .{};
    try handler.replayStart(testKey(.default, "r1"), &start, 1, &start_out);

    // completed, output "ok", one step "s"/"done"/"x", one history event "e"/"d"@7, tags "t"
    const whole = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++
        [_]u8{ 1, 0, 1, 0, 's', 4, 0, 'd', 'o', 'n', 'e', 1, 0, 0, 0, 'x' } ++
        [_]u8{ 1, 0, 1, 0, 'e', 1, 0, 'd', 7, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{ 1, 0, 't' };

    // The run already holds an event, which a skipped entry must keep.
    try handler.runs.getPtr(testKey(.default, "r1")).?.history.append(allocator, .{
        .event_type_owned = try allocator.dupe(u8, "started"),
        .detail_owned = try allocator.dupe(u8, ""),
        .timestamp_ms = 1,
    });

    // Every cut through the history or the steps is skipped whole.
    const history_before = handler.runs.get(testKey(.default, "r1")).?.history.items.len;
    try testing.expectEqual(@as(usize, 1), history_before);
    for (1..whole.len - 3) |n| {
        try testing.expectError(error.Halt, handler.replayComplete(testKey(.default, "r1"), whole[0..n], 2, &replay_out_for_tests));
        const run = handler.runs.get(testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.running, run.status);
        try testing.expect(run.output_owned == null);
        try testing.expectEqual(history_before, run.history.items.len);
    }

    try handler.replayComplete(testKey(.default, "r1"), &whole, 2, &replay_out_for_tests);
    const run = handler.runs.get(testKey(.default, "r1")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqualStrings("ok", run.output_owned.?);
    try testing.expectEqual(@as(usize, 1), run.history.items.len);
    try testing.expectEqualStrings("t", run.search_tags_owned.?);
}

test "workflow replay: a cancel logged after a completion is refused and changes nothing, on every replica; a second cancel after a cancel succeeds" {
    // A completion a deposed leader logged commits ahead of a cancel the
    // next leader took while that completion hadn't applied there.
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
    const completed = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 };
    const cancelled = [_]u8{5} ++ [_]u8{0} ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 };

    // Two replicas apply the same log.
    for (0..2) |_| {
        var handler = WorkflowHandler.init(testing.allocator);
        defer handler.deinit();
        var out: Result = .{};
        try handler.replayStart(testKey(.default, "r1"), &start, 1, &out);
        out = .{};
        try handler.replayComplete(testKey(.default, "r1"), &completed, 2, &out);
        try testing.expect(out.refused() == null);
        out = .{};
        try handler.replayComplete(testKey(.default, "r1"), &cancelled, 3, &out);
        const refused = out.refused().?;
        try testing.expectEqualStrings("run already finished", refused.refusal.message);
        try testing.expectEqual(proto.Ran.no, refused.refusal.ran);
        const run = handler.runs.get(testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
        try testing.expectEqualStrings("ok", run.output_owned.?);
        try testing.expectEqual(@as(i64, 2), run.completed_at_ms);
    }

    // Two cancels: the first ends the run, the second is told it did.
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    var out: Result = .{};
    try handler.replayStart(testKey(.default, "r1"), &start, 1, &out);
    try handler.replayComplete(testKey(.default, "r1"), &cancelled, 2, &out);
    out = .{};
    try handler.replayComplete(testKey(.default, "r1"), &cancelled, 3, &out);
    try testing.expect(out.refused() == null);
    try testing.expectEqual(@as(i64, 2), handler.runs.get(testKey(.default, "r1")).?.completed_at_ms);
}

/// Fills a run's history past what a completion record holds.
fn overfillHistory(handler: *WorkflowHandler, run: *WorkflowHandler.RunRecord) !void {
    const detail = @as([2000]u8, @splat('d'));
    for (0..40) |i| handler.addHistoryEvent(run, "signal_received", &detail, @intCast(i));
}

test "a completion too large to log fails the run with a trimmed record that replay applies the same way" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "big")).?;
    try overfillHistory(&handler, run);
    const before = run.history.items.len;

    // What the producer would encode, kept to apply on a second handler as replay would.
    const limit = WorkflowHandler.completionLimit(testKey(.default, "big"));
    const full: WorkflowHandler.Completion = .{
        .status = .completed,
        .output = null,
        .steps = &.{},
        .history = run.history.items,
        .last = .{ .event_type = "workflow_completed", .detail = "flo.Completed", .timestamp_ms = 99 },
        .tags = null,
    };
    try testing.expectError(error.TooLarge, WorkflowHandler.encodeCompletion(allocator, full, limit));
    const record = try WorkflowHandler.encodeTrimmed(allocator, full, limit);
    defer allocator.free(record);
    try testing.expect(record.len <= limit);

    handler.completeRun(&shard, testKey(.default, "big"), run, .completed, "flo.Completed", 99);
    _ = shard.applyCommitted();

    const done = handler.runs.getPtr(testKey(.default, "big")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, done.status);
    try testing.expectEqual(@as(?[]const u8, null), done.output_owned);
    const events = done.history.items;
    try testing.expectEqualStrings("history_trimmed", events[0].event_type_owned);
    const kept = events.len - 2; // less the marker and the final event
    var want_marker: [64]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want_marker, "{d} earlier events dropped", .{before - kept}), events[0].detail_owned);
    try testing.expectEqualStrings("workflow_failed", events[events.len - 1].event_type_owned);
    try testing.expect(std.mem.startsWith(u8, events[events.len - 1].detail_owned, "completion record too large: "));

    // A follower applies the same bytes and ends in the same state.
    var follower = WorkflowHandler.init(allocator);
    defer follower.deinit();
    createTestRun(&follower, testKey(.default, "big"), "big", "test-wf");
    try follower.replayComplete(testKey(.default, "big"), record, 2, &replay_out_for_tests);
    const f = follower.runs.get(testKey(.default, "big")).?;
    try testing.expectEqual(done.status, f.status);
    try testing.expectEqual(done.history.items.len, f.history.items.len);
    for (done.history.items, f.history.items) |a, b| {
        try testing.expectEqualStrings(a.event_type_owned, b.event_type_owned);
        try testing.expectEqualStrings(a.detail_owned, b.detail_owned);
    }
}

test "a cancel whose completion record is too large stays a cancel, trimmed to fit" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "big")).?;
    try overfillHistory(&handler, run);

    try handler.tryComplete(&shard, testKey(.default, "big"), run, .cancelled, "stop", 5);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);
    const events = run.history.items;
    try testing.expectEqualStrings("history_trimmed", events[0].event_type_owned);
    try testing.expectEqualStrings("workflow_cancelled", events[events.len - 1].event_type_owned);
    try testing.expect(std.mem.startsWith(u8, events[events.len - 1].detail_owned, "stop (completion record too large: "));
}

test "a completion record that fits round-trips through replay" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    handler.addHistoryEvent(run, "signal_received", "go", 1);

    const c: WorkflowHandler.Completion = .{
        .status = .cancelled,
        .output = "out",
        .steps = &.{},
        .history = run.history.items,
        .last = .{ .event_type = "workflow_cancelled", .detail = "stop", .timestamp_ms = 7 },
        .tags = "{\"a\":1}",
    };
    const record = try WorkflowHandler.encodeCompletion(allocator, c, WorkflowHandler.completionLimit(testKey(.default, "r")));
    defer allocator.free(record);
    try handler.applyComplete(run, record, 7);
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);
    try testing.expectEqualStrings("out", run.output_owned.?);
    try testing.expectEqualStrings("{\"a\":1}", run.search_tags_owned.?);
    try testing.expectEqual(@as(usize, 2), run.history.items.len);
    try testing.expectEqualStrings("stop", run.history.items[1].detail_owned);
}

/// The answer a handler queued on `conn`.
fn queuedAnswer(conn: *Connection) !proto.Response {
    return proto.Response.parse(conn.write_buf.peek());
}

test "a signal that would leave the completion no headroom is refused, holding nothing" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();

    createTestRun(&handler, testKey(.default, "s"), "s", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "s")).?;
    // Leave less than the headroom but more than two signal events: refused
    // only while the headroom is kept.
    const limit = WorkflowHandler.completionLimit(testKey(.default, "s"));
    // 4 KiB short of the limit: far more than two signal events, less than the headroom.
    const want = limit - 4096;
    const base = WorkflowHandler.completionBase(run);
    const pad = want - base - WorkflowHandler.Completion.eventSize("pad", "");
    const detail = try allocator.alloc(u8, pad);
    defer allocator.free(detail);
    @memset(detail, 'p');
    handler.addHistoryEvent(run, "pad", detail, 0);
    const events = run.history.items.len;

    const value = [_]u8{ 2, 0 } ++ "go".* ++ "payload".*;
    handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "s", &value));
    const answer = try queuedAnswer(&conn);
    try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
    try testing.expectEqualStrings("run history is full", (try answer.refused()).message);
    try testing.expectEqual(@as(usize, 0), run.signals.items.len);
    try testing.expectEqual(events, run.history.items.len);
}

var fake_propose_refusals: u32 = 0;
fn refuseThenPropose(shard: *Shard, run_ns_key: []const u8, value: []const u8) anyerror!persistence_mod.ProposeResult {
    if (fake_propose_refusals > 0) {
        fake_propose_refusals -= 1;
        return error.Overloaded;
    }
    return WorkflowHandler.proposeCompletion(shard, run_ns_key, value);
}
fn notLeading(_: *Shard, _: []const u8, _: []const u8) anyerror!persistence_mod.ProposeResult {
    return error.NotLeader;
}

test "a completion the log is too busy for is retried from the run's state, and applies only once logged" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    const status = run.status;

    handler.propose = refuseThenPropose;
    fake_propose_refusals = 1;
    handler.completeRun(&shard, testKey(.default, "r"), run, .completed, "flo.Completed", 10);
    try testing.expectEqual(status, run.status); // not applied: not logged
    try testing.expectEqual(@as(usize, 1), handler.completion_retries.items.len);

    handler.retryCompletions(&shard, 10); // not yet due
    try testing.expectEqual(status, run.status);
    handler.retryCompletions(&shard, 10 + 60_000);
    try testing.expectEqual(status, run.status); // logged, not yet applied
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqual(@as(usize, 0), handler.completion_retries.items.len);
}

test "a completion on a node that isn't leading changes nothing, and is kept for when it leads" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    const status = run.status;
    handler.addHistoryEvent(run, "step_started", "a", 1);

    handler.propose = notLeading;
    handler.completeRun(&shard, testKey(.default, "r"), run, .failed, "x", 10);
    try testing.expectEqual(status, run.status);
    try testing.expectEqual(@as(usize, 1), run.history.items.len);
    try testing.expectEqual(@as(usize, 1), handler.completion_retries.items.len);

    // Leading again, it's proposed and applies.
    handler.propose = WorkflowHandler.proposeCompletion;
    handler.retryCompletions(&shard, 10 + 60_000);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    try testing.expectEqual(@as(usize, 0), handler.completion_retries.items.len);
}

test "an ending is counted only for the entry this leader proposed: its index and its term" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    // Proposed at index 5 in term 1.
    run.ending_index = 5;
    run.ending_term = 1;
    var pbuf: [128]u8 = undefined;
    const value = [_]u8{ 5, 0, 0, 0, 0, 0, 0, 0 };
    const result: Result = .{};
    // Another leader's entry at that index, in term 2, isn't it.
    const theirs = entry_mod.buildCommandEntry(.workflow_complete, 0, 2, 5, 0, .default, testKey(.default, "r"), &value, &pbuf).?;
    handler.startApplied(&shard, &theirs, &result);
    try testing.expectEqual(@as(u64, 5), run.ending_index);
    const ours = entry_mod.buildCommandEntry(.workflow_complete, 0, 1, 5, 0, .default, testKey(.default, "r"), &value, &pbuf).?;
    handler.startApplied(&shard, &ours, &result);
    try testing.expectEqual(@as(u64, 0), run.ending_index);
}

test "a trimmed record leaves out the output, step outputs and tags" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "big")).?;
    try overfillHistory(&handler, run);
    const steps = [_]wf_types.StepOutput{.{ .step_name = "a", .output = "{\"x\":1}", .outcome = "success" }};
    const full: WorkflowHandler.Completion = .{
        .status = .completed,
        .output = "{\"done\":true}",
        .steps = &steps,
        .history = run.history.items,
        .last = .{ .event_type = "workflow_completed", .detail = "flo.Completed", .timestamp_ms = 1 },
        .tags = "{\"t\":1}",
    };
    const record = try WorkflowHandler.encodeTrimmed(allocator, full, WorkflowHandler.completionLimit(testKey(.default, "big")));
    defer allocator.free(record);
    const entry = WorkflowHandler.CompleteEntry.parse(record).?;
    try testing.expectEqual(@as(?[]const u8, null), entry.output);
    try testing.expectEqual(@as(u16, 0), entry.step_count);
    try testing.expectEqual(@as(?[]const u8, null), entry.tags);
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, entry.status);
}

test "the completion limit counts the run's key: one byte over it is trimmed, exactly at it completes" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    // A long key, so a limit that forgot it would let through a record the log refuses.
    const run_id = @as([3000]u8, @splat('k'));
    const key = comptime testKey(.default, &run_id);
    for ([_]usize{ 0, 1 }) |over| {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        handler.registerReplay(&shard.replay_registry);
        shard.workflow_handler = &handler;
        createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
        createTestRun(&handler, key, &run_id, "test-wf");
        const run = handler.runs.getPtr(key).?;
        const limit = WorkflowHandler.completionLimit(key);
        const fixed = 1 + 1 + 2 + 2 + 2 + WorkflowHandler.Completion.eventSize("workflow_completed", "flo.Completed");
        const detail = try allocator.alloc(u8, limit + over - fixed - WorkflowHandler.Completion.eventSize("pad", ""));
        defer allocator.free(detail);
        @memset(detail, 'p');
        handler.addHistoryEvent(run, "pad", detail, 0);

        handler.completeRun(&shard, key, run, .completed, "flo.Completed", 3);
        _ = shard.applyCommitted();
        try testing.expectEqual(if (over == 0) WorkflowHandler.RunStatus.completed else .failed, run.status);
        // Completed at its entry's stamp, not at the leader's `now_ms`.
        const stamp_ms: i64 = @intCast(shard.raft_node.log.last_stamp / std.time.ns_per_ms);
        try testing.expectEqual(@as(?i64, stamp_ms), run.completed_at_ms);
    }
}

test "a completion record cut inside its tags is refused, not read without them" {
    const allocator = testing.allocator;
    const c: WorkflowHandler.Completion = .{
        .status = .completed,
        .output = null,
        .steps = &.{},
        .history = &.{},
        .last = .{ .event_type = "workflow_completed", .detail = "", .timestamp_ms = 1 },
        .tags = "{\"t\":1}",
    };
    const record = try WorkflowHandler.encodeCompletion(allocator, c, 1 << 15);
    defer allocator.free(record);
    try testing.expect(WorkflowHandler.CompleteEntry.parse(record) != null);
    try testing.expectEqual(@as(?WorkflowHandler.CompleteEntry, null), WorkflowHandler.CompleteEntry.parse(record[0 .. record.len - 1]));
    // Cut before the tags' length too: the field is required.
    try testing.expectEqual(@as(?WorkflowHandler.CompleteEntry, null), WorkflowHandler.CompleteEntry.parse(record[0 .. record.len - 2 - 7]));
}

test "cancelling a run that already finished is refused and keeps its result" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    handler.completeRun(&shard, testKey(.default, "r"), run, .completed, "flo.Completed", 3);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    const events = run.history.items.len;

    try testing.expect(handler.handleCancel(&shard, &conn, makeRequest(.workflow_cancel, "r", "")) == null);
    const answer = try queuedAnswer(&conn);
    try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
    try testing.expectEqualStrings("run already finished", (try answer.refused()).message);
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqual(events, run.history.items.len);
}

test "an engine-reached cancel with a detail past the cancel-reason cap is trimmed, not a crash" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "big")).?;
    try overfillHistory(&handler, run);

    const detail = @as([2000]u8, @splat('t'));
    handler.completeRun(&shard, testKey(.default, "big"), run, .cancelled, &detail, 5);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);
    const last = run.history.items[run.history.items.len - 1].detail_owned;
    try testing.expect(std.mem.startsWith(u8, last, detail[0..WorkflowHandler.MAX_CANCEL_REASON] ++ " (completion record too large: "));
}

test "a retry pending when the run is cancelled doesn't undo the cancel" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;

    handler.propose = refuseThenPropose;
    fake_propose_refusals = 1;
    handler.completeRun(&shard, testKey(.default, "r"), run, .completed, "flo.Completed", 10);
    try testing.expectEqual(@as(usize, 1), handler.completion_retries.items.len);

    try handler.tryComplete(&shard, testKey(.default, "r"), run, .cancelled, "stop", 11);
    _ = shard.applyCommitted();
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);

    handler.retryCompletions(&shard, 10 + 60_000);
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);
    try testing.expectEqual(@as(usize, 0), handler.completion_retries.items.len);
}

test "a wait that times out with no target ends the run through the log" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);

    for ([_]bool{ false, true }) |leading| {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        handler.registerReplay(&shard.replay_registry);
        shard.workflow_handler = &handler;
        createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
        createTestRun(&handler, testKey(.default, "w"), "w", "test-wf");
        const run = handler.runs.getPtr(testKey(.default, "w")).?;
        run.status = .waiting;
        run.wait_signal_type_owned = try allocator.dupe(u8, "go");
        run.wait_timeout_at_ms = 1;
        if (!leading) handler.propose = notLeading;

        handler.handleWaitTimeout(&shard, testKey(.default, "w"), 2);
        _ = shard.applyCommitted();
        if (leading) {
            try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, run.status);
        } else {
            // Not logged, so not applied.
            try testing.expect(!run.status.isTerminal());
        }
    }
}

test "a signal to a finished run is refused and leaves its history alone" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    const run = handler.runs.getPtr(testKey(.default, "r")).?;
    handler.completeRun(&shard, testKey(.default, "r"), run, .completed, "flo.Completed", 3);
    _ = shard.applyCommitted();
    const events = run.history.items.len;

    const value = [_]u8{ 2, 0 } ++ "go".*;
    handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", &value));
    const answer = try queuedAnswer(&conn);
    try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
    try testing.expectEqualStrings("run already finished", (try answer.refused()).message);
    try testing.expectEqual(events, run.history.items.len);
    try testing.expectEqual(@as(usize, 0), run.signals.items.len);
}

test "workflow handler: a trigger cursor restored inside an append resumes with its next record" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events" });

    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    const StreamHandler = @import("../stream/handler.zig").StreamHandler;
    var streams = StreamHandler.init(allocator, &partition);
    defer streams.deinit();

    // One append of five records: [count:u32]([len:u32][payload][hdr:u16])*
    var blob: [4 + 5 * 7]u8 = undefined;
    std.mem.writeInt(u32, blob[0..4], 5, .little);
    for (0..5) |i| {
        std.mem.writeInt(u32, blob[4 + i * 7 ..][0..4], 1, .little);
        blob[4 + i * 7 + 4] = @intCast('a' + i);
        std.mem.writeInt(u16, blob[4 + i * 7 + 5 ..][0..2], 0, .little);
    }
    _ = streams.appendForTest("ns", "events", &blob);

    const all = streams.readPayloadsForStream("events", "ns", StreamID.MIN, 100);
    defer allocator.free(all.payloads);
    defer allocator.free(all.ids);
    try testing.expectEqual(@as(usize, 5), all.payloads.len);

    // Replay restored the run record "b" started: its id is the cursor.
    var idem_buf: [96]u8 = undefined;
    const idem = try std.fmt.bufPrint(&idem_buf, "trigger:events@{d}:{d}", .{ all.ids[1].timestamp_ms, all.ids[1].sequence });
    handler.restoreTriggerCursor(TEST_NS, "wf", idem);
    const t = handler.stream_triggers.getPtr(testKey(TEST_NS, "wf")).?;
    const cursor = StreamID{ .timestamp_ms = t.stream_cursor_ts, .sequence = t.stream_cursor_seq };
    try testing.expect(cursor.eql(all.ids[1]));

    // The next poll reads c, d, e: the rest of that append, nothing skipped.
    const rest = streams.readPayloadsForStream("events", "ns", cursor, 100);
    defer allocator.free(rest.payloads);
    defer allocator.free(rest.ids);
    try testing.expectEqual(@as(usize, 3), rest.payloads.len);
    for (rest.payloads, "cde") |p, want| try testing.expectEqual(want, p[0]);
}

test "a step whose mapped input would nest past 128 levels fails its run" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    registerTestAction(&shard, "step-a");
    // Its completions apply through the log, as on a shard.
    handler.registerReplay(&shard.replay_registry);
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "map-wf"), "map-wf",
        \\{"kind":"Workflow","name":"map-wf","version":"1.0.0",
        \\"start":{"run":"@actions/step-a","input_mapping":"\"$.input\"","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
    );

    for ([_]usize{ 128, 129, 300 }) |n| {
        const input = try json_depth.nestedArrays(allocator, n);
        defer allocator.free(input);
        var id_buf: [16]u8 = undefined;
        var key_buf: [32]u8 = undefined;
        const run_id = try std.fmt.bufPrint(&id_buf, "run-{d}", .{n});
        const ns_key = writeKey(&key_buf, .default, run_id);
        createTestRunWithInput(&handler, ns_key, run_id, "map-wf", input);

        handler.advanceWorkflow(&shard, ns_key);
        _ = shard.applyCommitted();
        const want: WorkflowHandler.RunStatus = if (n <= json_depth.MAX_DEPTH) .waiting else .failed;
        try testing.expectEqual(want, handler.runs.get(ns_key).?.status);
    }
}

test "workflow replay: a start whose input nests past 128 levels applies; a search attribute read from it is null" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    createTestDef(&handler, testKey(.default, "wf"), "wf",
        \\{"kind":"Workflow","name":"wf","version":"1.0.0","search_attributes":[{"name":"a","from":"$.input.a"}],
        \\"start":{"run":"@actions/step-a","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
    );

    for ([_]usize{ 128, 129, 300 }) |n| {
        // {"a": <n-1 arrays>} nests n levels.
        const arrays = try json_depth.nestedArrays(allocator, n - 1);
        defer allocator.free(arrays);
        // [wf_len][wf][ver_len][ver][status][evt_len][idem_len][input]
        const value = try std.fmt.allocPrint(allocator, "\x02\x00wf\x01\x001\x01\x00\x00\x00\x00{{\"a\":{s}}}", .{arrays});
        defer allocator.free(value);
        var id_buf: [16]u8 = undefined;
        var key_buf: [32]u8 = undefined;
        const ns_key = writeKey(&key_buf, .default, try std.fmt.bufPrint(&id_buf, "r{d}", .{n}));

        var out: Result = .{};
        try handler.replayStart(ns_key, value, 1, &out);
        const tags = handler.runs.get(ns_key).?.search_tags_owned.?;
        if (n <= json_depth.MAX_DEPTH) {
            try testing.expect(std.mem.indexOf(u8, tags, arrays) != null);
        } else {
            try testing.expectEqualStrings("{\"a\":null}", tags);
        }
    }
}

fn createTestRunAt(handler: *WorkflowHandler, run_id: []const u8, wf_name: []const u8, started_at: i64, status: WorkflowHandler.RunStatus) !void {
    var key_buf: [64]u8 = undefined;
    const ns_key = writeKey(&key_buf, .default, run_id);
    createTestRun(handler, ns_key, run_id, wf_name);
    const run = handler.runs.getPtr(ns_key) orelse return error.OutOfMemory;
    run.started_at_ms = started_at;
    run.status = status;
}

/// The run ids of a shard's page of runs, in order.
fn pageRunIds(page: list_runs.PageResponse.Value, out: [][]const u8) ![]const []const u8 {
    var it = page.runs.iterator();
    var n: usize = 0;
    while (try it.next()) |run| : (n += 1) out[n] = run.at.run_id;
    return out[0..n];
}

fn expectRunIds(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "list runs: a shard's page is newest first, by id among runs started together, and resumes after the run given" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    try createTestRunAt(&handler, "r-old", "wf", 10, .running);
    try createTestRunAt(&handler, "r-b", "wf", 20, .running);
    try createTestRunAt(&handler, "r-a", "wf", 20, .running);
    try createTestRunAt(&handler, "r-new", "wf", 30, .running);
    const buf = try testing.allocator.alloc(u8, system_ops.Runs.max_size);
    defer testing.allocator.free(buf);
    var ids: [8][]const u8 = undefined;

    const first = try list_runs.ownPage(&handler, .default, "", .{ .limit = 2, .status = "", .search = "", .after = null }, 1 << 20, buf);
    try expectRunIds(&.{ "r-new", "r-a" }, try pageRunIds(first, &ids));
    try testing.expect(first.more);

    const rest = try list_runs.ownPage(&handler, .default, "", .{ .limit = 10, .status = "", .search = "", .after = .{ .created_at = 20, .run_id = "r-a" } }, 1 << 20, buf);
    try expectRunIds(&.{ "r-b", "r-old" }, try pageRunIds(rest, &ids));
    try testing.expect(!rest.more);
}

test "list runs: a shard's page holds only runs with the status asked" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    try createTestRunAt(&handler, "r-1", "wf", 10, .running);
    try createTestRunAt(&handler, "r-2", "wf", 20, .completed);
    try createTestRunAt(&handler, "r-3", "wf", 30, .running);
    const buf = try testing.allocator.alloc(u8, system_ops.Runs.max_size);
    defer testing.allocator.free(buf);
    var ids: [8][]const u8 = undefined;
    const page = try list_runs.ownPage(&handler, .default, "", .{ .limit = 10, .status = WorkflowHandler.RunStatus.completed.toString(), .search = "", .after = null }, 1 << 20, buf);
    try expectRunIds(&.{"r-2"}, try pageRunIds(page, &ids));
}

test "list runs: a shard's page holds only runs the search matches" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    try createTestRunAt(&handler, "r-1", "orders", 10, .running);
    try createTestRunAt(&handler, "r-2", "billing", 20, .running);
    const buf = try testing.allocator.alloc(u8, system_ops.Runs.max_size);
    defer testing.allocator.free(buf);
    var ids: [8][]const u8 = undefined;
    const page = try list_runs.ownPage(&handler, .default, "", .{ .limit = 10, .status = "", .search = "bill", .after = null }, 1 << 20, buf);
    try expectRunIds(&.{"r-2"}, try pageRunIds(page, &ids));
}

test "list runs: a shard's page stops before its entries pass the room it was given" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    try createTestRunAt(&handler, "r-1", "wf", 10, .running);
    try createTestRunAt(&handler, "r-2", "wf", 20, .running);
    const buf = try testing.allocator.alloc(u8, system_ops.Runs.max_size);
    defer testing.allocator.free(buf);
    var ids: [8][]const u8 = undefined;
    // One entry: [3]["r-2"][2]["wf"][7]["running"][i64] = 2+3+2+2+2+7+8 bytes.
    const one: u32 = 26;
    const page = try list_runs.ownPage(&handler, .default, "", .{ .limit = 10, .status = "", .search = "", .after = null }, one + 1, buf);
    try expectRunIds(&.{"r-2"}, try pageRunIds(page, &ids));
    try testing.expect(page.more);
}

test "workflow keys: two namespaces keep their own workflows and runs, whatever their names hold" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    // Each namespace holds a workflow and a run of the same name, and one
    // pair whose name and key once ran together: "a" + ":bx" and "a:b" + "x"
    // were both "a:b:x".
    const Case = struct { ns: NamespaceId, name: []const u8 };
    const cases = [_]Case{
        .{ .ns = TEST_NS_A, .name = "wf" },
        .{ .ns = TEST_NS_B, .name = "wf" },
        .{ .ns = TEST_NS_A, .name = ":bx" },
        .{ .ns = TEST_NS_B, .name = "x" },
    };
    var index: u64 = 1;
    for (cases) |c| {
        var kbuf: [16]u8 = undefined;
        var pbuf: [2048]u8 = undefined;
        var vbuf: [64]u8 = undefined;
        var out: Result = .{};
        const create = entry_mod.buildCommandEntry(.workflow_create, 0, 1, index, 0, c.ns, writeKey(&kbuf, c.ns, c.name), test_workflow_json, &pbuf).?;
        try handler.replayEntry(&create, &out);
        index += 1;
        out = .{};
        const start = entry_mod.buildCommandEntry(.workflow_start, 0, 1, index, 0, c.ns, writeKey(&kbuf, c.ns, c.name), startValue(&vbuf, c.name, "workflow_started", "k"), &pbuf).?;
        try handler.replayEntry(&start, &out);
        index += 1;
        var r = out.reader();
        try testing.expectEqual(@as(?u8, 1), r.int(u8));
    }
    try testing.expectEqual(@as(usize, 4), handler.definitionCount());
    try testing.expectEqual(@as(usize, 4), handler.runCount());

    for ([_]NamespaceId{ TEST_NS_A, TEST_NS_B }) |ns| {
        var runs: std.ArrayListUnmanaged(WorkflowHandler.RunInfo) = .empty;
        defer runs.deinit(allocator);
        try handler.collectMatchingRuns(&runs, ns, "", null, null);
        try testing.expectEqual(@as(usize, 2), runs.items.len);
        // Each run listed is one this namespace started.
        for (runs.items) |listed| {
            var own = false;
            for (cases) |c| own = own or (c.ns == ns and std.mem.eql(u8, c.name, listed.run_id));
            try testing.expect(own);
        }
        // And each lists only its own definitions.
        const defs = WorkflowHandler.localScanWorkflowDefs(@ptrCast(&handler), .{ .name = "", .id = ns }, "", null, 0);
        try testing.expectEqual(@as(usize, 2), defs.items.len);
        // One idempotency key, used in both, names each namespace's own run.
        const run = handler.runForIdempotencyKey(ns, "wf", "k").?;
        try testing.expectEqual(ns, keyNamespace(run.ns_key));
    }
    // A key reads back as the namespace and name it was built from.
    var kbuf: [16]u8 = undefined;
    const k = writeKey(&kbuf, TEST_NS_A, ":bx");
    try testing.expectEqual(TEST_NS_A, keyNamespace(k));
    try testing.expectEqualStrings(":bx", keyName(k));
    try testing.expect(!handler.holdsRun(TEST_NS_B, ":bx"));
    try testing.expect(!handler.holdsRun(TEST_NS_A, "x"));

    // An entry whose key names another namespace than its stamp halts.
    var pbuf: [2048]u8 = undefined;
    var out: Result = .{};
    const crossed = entry_mod.buildCommandEntry(.workflow_create, 0, 1, index, 0, TEST_NS_A, writeKey(&kbuf, TEST_NS_B, "wf2"), test_workflow_json, &pbuf).?;
    try testing.expectError(error.Halt, handler.replayEntry(&crossed, &out));
    try testing.expectEqual(@as(usize, 4), handler.definitionCount());
}
