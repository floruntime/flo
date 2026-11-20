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
const source_page = @import("../processing/source_page.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
const Partition = @import("../storage/partition.zig").Partition;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const router = @import("../node/router.zig");
const ae = @import("../protocol/action_entries.zig");
const calls_mod = @import("../actions/calls_machine.zig");
const wire_schema = @import("../protocol/schema.zig");
const runs_machine = @import("runs_machine.zig");
const progress_mod = @import("progress.zig");
const children_mod = runs_machine;
const contract_view = @import("../contract/view.zig");
const run_id_mod = @import("../node/run_id.zig");
const NamespaceId = @import("../namespace/table.zig").Id;
const NamespaceHandler = @import("../namespace/handler.zig").NamespaceHandler;
const defs_machine = @import("defs_machine.zig");
const contract_machine = @import("../contract/machine.zig");
const Store = @import("../contract/store.zig").Store;

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
    allocator: Allocator,

    /// Runs not ended that wait on nothing (`workflow.run`'s wait), by key,
    /// owned: an index of applied state, kept as each run entry applies on
    /// every replica (`entryApplied`), so whichever node leads steps them.
    /// They go in this map's order, not their starts'.
    to_step: std.StringHashMapUnmanaged(void) = .empty,
    /// Where the last pass over `to_step` stopped, so a burst of starts is
    /// walked once rather than from the top each pass.
    to_step_cursor: u32 = 0,
    /// The rev each run's step this leader logged in `stepping_term` was
    /// taken at, by key, owned: not decided again until it applies. Leader
    /// memory: a new term starts it empty.
    stepping: std.StringHashMapUnmanaged(u64) = .empty,
    stepping_term: u64 = 0,
    /// Runs not ended that wait on a call, by key, owned: indexed like
    /// `to_step`, so the duty's cost follows the work in flight.
    parked: std.StringHashMapUnmanaged(void) = .empty,
    /// When a run's timer or signal deadline is due, earliest first, keys
    /// owned. An entry is checked against the run's wait as applied when it
    /// comes due, so one the run moved past is dropped then.
    timers: std.PriorityQueue(Timer, void, timerCompare) = .empty,
    /// An index couldn't keep an entry: all are rebuilt from the log.
    rewatch: bool = false,
    /// The cancel this leader proposed last for a run, by key, owned: its
    /// end is counted when that entry applies. Leader memory.
    ending: std.StringHashMapUnmanaged(Ending) = .empty,
    /// Logs a cancel's completion. A seam so a test can make the log refuse.
    propose: *const fn (shard: *Shard, run_ns_key: []const u8, value: []const u8) anyerror!persistence_mod.ProposeResult = proposeCompletion,
    /// When this leader next looks for calls whose workflow run has ended,
    /// and the last call key it looked at.
    calls_sweep_at_ms: i64 = 0,
    calls_sweep_after: std.ArrayListUnmanaged(u8) = .empty,
    /// Children this leader has proposed a start or a refusal for in
    /// `child_starting_term`, by their parent record's key, at that entry's
    /// index: not proposed again while it may yet apply. Leader memory; a
    /// new term starts it empty.
    child_starting: std.StringHashMapUnmanaged(u64) = .empty,
    child_starting_term: u64 = 0,
    /// Where the next pass over parent records resumes.
    child_scan_after: std.ArrayListUnmanaged(u8) = .empty,

    /// The shard's contract state, where the definitions machine keeps each
    /// workflow (`defs_machine.zig`), keyed `writeKey`: namespace id ‖ name.
    store: ?*Store = null,
    /// A store of its own, for a handler built without a shard (tests).
    own_store: Store,
    /// What a definition applied here directly (`replayEntry`) stages in.
    def_scratch: std.heap.ArenaAllocator,
    /// The create index each definition's schedule and trigger were last
    /// built from (`syncProducers`), by definition key. Node memory.
    producers_built: std.StringHashMapUnmanaged(u64) = .empty,

    /// The runs this node has stepped while leading, by key (namespace id ‖
    /// run id): a working copy built from `workflow.run` when it takes one
    /// (`liveRun`), with the step it is at and what it waits for. Node
    /// memory, kept past a step-down: the run itself is the runs machine's.
    runs: std.StringHashMap(RunRecord),
    /// For tests: the copies `inspect` built, each owning its key.
    inspected: std.ArrayListUnmanaged(*RunRecord) = .empty,

    /// Active stream triggers, by their definitions' keys.
    /// Registered when a workflow with a `trigger:` block is created.
    stream_triggers: std.StringHashMap(StreamTriggerState),

    /// Set when a stream this shard watches was just appended to (via a
    /// peer's `stream_appended` wake flag or a local append). The next
    /// `tickStreamTriggers` force-polls all triggers, bypassing their poll
    /// timer — turning a polled (≤ batch_timeout_ms) latency into ~one tick.
    /// Single-threaded per shard (set/read only on the shard's reactor thread).
    triggers_dirty: bool = false,
    /// The index the next definition applied here directly takes (tests).
    direct_index: u64 = 0,
    /// This shard's namespace handler, whose table an applier reads a
    /// namespace's name from.
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

    pub const RunStatus = runs_machine.RunStatus;

    /// A definition as the store holds it: its slices point into the
    /// store, so they hold until the next apply.
    pub const DefinitionRecord = struct {
        name: []const u8,
        version: []const u8,
        yaml: []const u8,
        created_at_ms: i64,
        created_index: u64,
        idempotency: definition.IdempotencyMode,
        own_namespace: bool,
        enabled: bool,
        anchor_ms: i64,
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

        /// The callee's run id while the step waits on its call.
        pending_action_run_id_owned: ?[]const u8 = null,

        /// Step name that triggered the pending action.
        pending_step_name_owned: ?[]const u8 = null,

        /// The pending action's slot (its name's), resolved to a shard
        /// through the router at each use, so it follows a moved slot.
        pending_action_slot: ?u32 = null,
        /// What the pending call is of: an action's run or a child's.
        pending_call_kind: ae.CallKind = .action,

        /// Number of poll attempts taken for the current step (a `poll:` step that
        /// keeps returning `pending`). Reset on step transition.
        poll_attempt: u32 = 0,

        /// Absolute time the next poll re-invocation is due (ms; 0 = not polling).
        poll_next_at_ms: i64 = 0,

        /// The current step's start was refused for room and is retried on
        /// the poll timer; the retries add nothing more to history.
        waiting_for_room: bool = false,
        /// Its step's call found its action not registered: said once in
        /// its history. Leader memory, like the rest of a step's wait.
        call_unregistered_noted: bool = false,
        /// When this leader may next ask again for that call, and the index
        /// of the last time it did: until that applies, the step waits on
        /// it, so it can't give up as the start goes out again.
        call_retry_at_ms: i64 = 0,
        call_retry_index: u64 = 0,

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

        /// The run's `rev` when this copy was taken: the step it decides is
        /// logged at it (`commitRun`).
        rev: u64 = 0,
        /// How many history events came from the log: the rest this
        /// decision added.
        hist_logged: usize = 0,
        /// The callee the run waited on when taken, if it waited on a call,
        /// and whether this decision took that call's end.
        took_call: ?[]const u8 = null,
        took_call_end: bool = false,
        /// The call this decision makes (an encoded `ae.Call`), logged with
        /// the step that waits on it.
        call_payload: ?[]const u8 = null,
        /// The run's end this decision reached, and what its last event says.
        ending: ?RunStatus = null,
        ending_detail_owned: ?[]const u8 = null,

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
        /// The last event a run applied here started from (`followTrigger`):
        /// what every replica holds alike.
        stream_cursor_ts: u64,
        stream_cursor_seq: u64,
        /// How far this leader has read in `proposed_term`, past starts it
        /// proposed that may not apply: a step-down can drop them from the
        /// log, so another term reads from the applied cursor again, and
        /// the starts made twice are one run each by their idempotency keys.
        /// Leader memory.
        proposed: ?struct { at: StreamID, term: u64 } = null,
        last_poll_ms: i64,
        /// A read of a stream another shard owns, in flight (0 for none):
        /// its number and the term it was asked in. At most one at a time.
        read_seq: u64 = 0,
        read_term: u64 = 0,
        read_sent_ms: i64 = 0,
        next_read_seq: u64 = 0,
        /// Which stream the cursor is in (the stamp that made it), once a
        /// read has said; a read of one made since starts the cursor over.
        stream_made_ns: ?u64 = null,
    };

    /// Where a trigger reads from in `term`: past what this leader proposed
    /// in that term, else from the applied cursor.
    fn readFrom(trigger: *const StreamTriggerState, term: u64) StreamID {
        const applied = StreamID{ .timestamp_ms = trigger.stream_cursor_ts, .sequence = trigger.stream_cursor_seq };
        const p = trigger.proposed orelse return applied;
        if (p.term != term or applied.greaterThan(p.at)) return applied;
        return p.at;
    }

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
    const Ending = struct { index: u64, term: u64 };

    fn timerCompare(_: void, a: Timer, b: Timer) std.math.Order {
        return std.math.order(a.at_ms, b.at_ms);
    }

    pub fn init(allocator: Allocator) WorkflowHandler {
        return .{
            .allocator = allocator,
            .own_store = Store.init(allocator),
            .def_scratch = .init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
            .stream_triggers = std.StringHashMap(StreamTriggerState).init(allocator),
            .schedules = std.StringHashMap(ScheduleState).init(allocator),
            .plan_health = std.StringHashMap(plan_types.ExecutorHealth).init(allocator),
        };
    }

    pub fn deinit(self: *WorkflowHandler) void {
        self.calls_sweep_after.deinit(self.allocator);
        self.freeChildStarting();
        self.child_starting.deinit(self.allocator);
        self.child_scan_after.deinit(self.allocator);
        self.own_store.deinit();
        self.def_scratch.deinit();
        var pk = self.producers_built.keyIterator();
        while (pk.next()) |k| self.allocator.free(k.*);
        self.producers_built.deinit(self.allocator);

        // Free all run records (ns-qualified key + record fields)
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // ns-qualified key
            self.freeRunRecord(entry.value_ptr);
        }
        self.runs.deinit();
        for (self.inspected.items) |copy| {
            self.allocator.free(copy.ns_key);
            self.freeRunRecord(copy);
            self.allocator.destroy(copy);
        }
        self.inspected.deinit(self.allocator);
        inline for (.{ &self.parked, &self.to_step, &self.ending }) |map| {
            var k = map.keyIterator();
            while (k.next()) |key| self.allocator.free(key.*);
            map.deinit(self.allocator);
        }
        self.clearStepping();
        self.stepping.deinit(self.allocator);
        while (self.timers.pop()) |t| self.allocator.free(t.ns_key);
        self.timers.deinit(self.allocator);

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
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        if (run.wait_timeout_target_owned) |t| self.allocator.free(t);
        if (run.output_owned) |o| self.allocator.free(o);
        if (run.search_tags_owned) |t| self.allocator.free(t);
        if (run.took_call) |c| self.allocator.free(c);
        if (run.call_payload) |c| self.allocator.free(c);
        if (run.ending_detail_owned) |d| self.allocator.free(d);
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
        const id = ns.id.bytes();
        var n: usize = 0;
        var counter = handler.defsRead().range(defs_machine.DefFam, &id);
        while (counter.next() catch unreachable) |_| n += 1; // written by the machine alone
        const name_buf = dispatcher_mod.scanScratch(n);

        var count: usize = 0;
        var it = handler.defsRead().range(defs_machine.DefFam, &id);
        while (it.next() catch unreachable) |rec| {
            if (count >= name_buf.len) break;
            name_buf[count] = rec.value.v.name;
            count += 1;
        }

        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    fn dispatchWorkflow(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
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

    /// After a run's entry applies on the leader that proposed it: counted.
    /// Never on a follower.
    pub fn startApplied(self: *WorkflowHandler, shard: *Shard, entry: *const entry_mod.Entry, result: *const Result) void {
        const etype = std.enums.fromInt(entry_mod.EntryType, entry.header.entry_type) orelse return;
        if (etype != .workflow_start and etype != .workflow_step and etype != .workflow_complete) return;
        if (shard.raft_node.role != .leader) return;
        // A cancel is counted against the one this leader recorded, of any
        // term it led (it may be elected again with it in its log); the
        // rest only in the term that proposed them.
        if (etype != .workflow_complete and entry.header.term != shard.raft_node.current_term) return;
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        switch (etype) {
            .workflow_start => {
                if (result.refused() != null) return;
                var r = result.reader();
                if ((r.int(u8) orelse 0) == 1) if (shard.metrics_registry) |m| m.workflow.recordStarted();
                return;
            },
            .workflow_complete => {
                // Counted once, for the cancel this leader proposed last.
                const e = self.ending.get(cmd.key) orelse return;
                if (e.index != entry.header.index or e.term != entry.header.term) return;
                const kv = self.ending.fetchRemove(cmd.key).?;
                self.allocator.free(kv.key);
            },
            else => {
                // Its step applied, or was refused: the run is decided again
                // from what applied.
                if (self.stepping.fetchRemove(cmd.key)) |kv| self.allocator.free(kv.key);
                if (result.refused() != null) return;
                if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();
            },
        }
        if (result.refused() != null) return;
        const run = self.appliedRun(cmd.key) orelse return;
        if (shard.metrics_registry) |m| switch (appliedStatus(run)) {
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
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        switch (op) {
            .workflow_create => return self.handleCreate(shard, conn, req),
            .workflow_start => return self.handleStart(shard, conn, req),
            .workflow_signal => return self.handleSignal(shard, conn, req),
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
        // Whether the trigger names the workflow's own namespace is decided
        // here, where the name is the request's, and logged with the entry:
        // a replica applying it may not have the name in its table yet (a
        // replay at boot, before shard 0 publishes), and must read the same
        // stream as the leader.
        const own_ns = if (def.trigger) |t| if (t.namespace) |n| std.mem.eql(u8, n, if (req.namespace.len == 0) "default" else req.namespace) else false else false;
        const proposed = self.proposeCreate(shard, req.ns_id, name, yaml, if (own_ns) entry_mod.Flags.OWN_NAMESPACE else entry_mod.Flags.NONE) catch |err| {
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
        const def_now = self.defOf(def_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return null;
        };

        // Check workflow is not disabled
        if (!def_now.enabled) {
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
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: run ids of the form wfr-<hex>-<hex>-<hex> are the server's; choose another");
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
            if (self.defOf(def_ns_key)) |def_rec| {
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
                shard.sendOkResponse(conn, req.header.request_id, existing);
                return null;
            }
        }

        // Generate or use explicit run ID.
        // Server-generated IDs have embedded partition bits
        // for O(1) cross-shard routing on status/signal/cancel/history.
        var run_id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const run_id_str = if (explicit_run_id) |rid|
            rid
        else blk: {
            const slot = router.nsKeySlot(req.ns_id, workflow_name);
            break :blk shard.run_id_gen.next(.workflow, slot, shard.raft_node.current_term, &run_id_buf);
        };

        // Build namespace-qualified key for the runs map
        const run_ns_key = self.nsKey(req.ns_id, run_id_str) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };

        // Guard against ID collision (e.g. replayed entry with same key)
        if (self.appliedRun(run_ns_key) != null) {
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

    pub fn hasDutyWork(self: *const WorkflowHandler) bool {
        return self.to_step.count() > 0 or self.parked.count() > 0 or self.timers.count() > 0 or self.rewatch;
    }

    /// Most step_started entries one pass proposes, and most runs it looks
    /// at doing so.
    const STEPS_PER_PASS: usize = 64;
    const VISITS_PER_PASS: usize = 4 * STEPS_PER_PASS;

    /// Steps each run that waits on nothing, up to `STEPS_PER_PASS`,
    /// picking up where the last pass stopped. Leader only: a step
    /// proposes.
    pub fn advanceStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        if (shard.raft_node.role != .leader) return;
        if (self.rewatch) self.rewatchRuns();
        if (self.to_step.count() == 0) return;
        var keys: [STEPS_PER_PASS][]const u8 = undefined;
        var n: usize = 0;
        var visited: usize = 0;
        var it = self.to_step.iterator();
        // A cursor past the end (the map grew or shrank) starts over.
        const start: u32 = if (self.to_step_cursor <= self.to_step.capacity()) self.to_step_cursor else 0;
        it.index = start;
        var wrapped = start == 0;
        defer self.to_step_cursor = it.index;
        while (n < STEPS_PER_PASS and visited < VISITS_PER_PASS) {
            if (wrapped and start != 0 and it.index >= start) break;
            const entry = it.next() orelse {
                // At the end: once more from the top, up to where it began.
                if (wrapped) break;
                wrapped = true;
                it = self.to_step.iterator();
                continue;
            };
            visited += 1;
            if (self.steppingAt(shard, entry.key_ptr.*) != null) continue;
            if (self.endingInFlight(shard, entry.key_ptr.*)) continue;
            keys[n] = self.allocator.dupe(u8, entry.key_ptr.*) catch break;
            n += 1;
        }
        // Stepping one changes the index: keys are taken first.
        for (keys[0..n]) |k| {
            defer self.allocator.free(k);
            self.advanceWorkflow(shard, k);
        }
    }

    /// This node stopped leading: what it proposed and is yet to apply is
    /// decided again by whichever node leads, from what applied.
    pub fn dropStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        _ = shard;
        self.clearStepping();
        var it = self.ending.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.ending.clearRetainingCapacity();
    }

    fn clearStepping(self: *WorkflowHandler) void {
        var it = self.stepping.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.stepping.clearRetainingCapacity();
    }

    // ── SIGNAL ──────────────────────────────────────────────────────────

    /// A client's signal, logged (`workflow_signal`): the client is
    /// answered once it applies, and a run waiting on its type wakes there.
    fn handleSignal(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) ?persistence_mod.ProposeResult {
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
        // What apply refuses, answered at once where this node can tell.
        const applied = self.appliedRun(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return null;
        };
        if (appliedStatus(applied).isTerminal()) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run already finished");
            return null;
        }

        // [signal_len:u16][signal_type][payload...]
        if (req.value.len < 2) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "signal type is required");
            return null;
        }
        const sig_len: usize = std.mem.readInt(u16, req.value[0..2], .little);
        if (req.value.len - 2 < sig_len) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed signal");
            return null;
        }
        const signal_type = req.value[2 .. 2 + sig_len];
        // A leading '_' is reserved for types the server makes.
        if (signal_type.len > 0 and signal_type[0] == '_') {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "reserved signal type: a leading '_' is the system's");
            return null;
        }
        const payload = req.value[2 + sig_len ..];
        const buf = self.allocator.alloc(u8, runs_machine.Signal.max_size) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };
        defer self.allocator.free(buf);
        const value = runs_machine.Signal.encodeInto(buf, .{ .type = signal_type, .payload = payload }) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "signal too large");
            return null;
        };
        const proposed = persistence_mod.proposeEntryIn(shard, .workflow_signal, entry_mod.Flags.NONE, req.ns_id, run_ns_key, value) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "signal not persisted"));
            return null;
        };
        if (shard.metrics_registry) |m| m.workflow.recordSignalDelivered();
        return proposed;
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

        const run = self.appliedRun(run_ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return null;
        };
        if (appliedStatus(run).isTerminal()) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run already finished");
            return null;
        }
        if (req.value.len > MAX_CANCEL_REASON) {
            var msg: [96]u8 = undefined;
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, std.fmt.bufPrint(&msg, "a cancel reason is at most {d} bytes, not {d}", .{ MAX_CANCEL_REASON, req.value.len }) catch "cancel reason too long");
            return null;
        }
        const reason = if (req.value.len > 0) req.value else "cancelled by user";
        const c: runs_machine.Complete = .{ .status = .cancelled, .output = null, .event = terminalEventType(.cancelled), .detail = reason, .tags = "", .workflow = run.workflow };
        var vbuf: [MAX_CANCEL_REASON + MAX_WORKFLOW_NAME_LEN + 64]u8 = undefined;
        const value = c.encode(&vbuf) orelse {
            shard.sendRefusal(conn, req.header.request_id, .internal_error, proto.notRun("internal error: a cancel that doesn't encode"));
            return null;
        };
        const proposed = self.propose(shard, run_ns_key, value) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureCode(err).toStatus(), proto.notRun(persistence_mod.failureMessage(err, "the cancel was not persisted")));
            return null;
        };
        // The run ends when the entry applies, and the client is answered
        // then. If it never does, the client is told the leader changed, and
        // the run carries on.
        self.noteEnding(run_ns_key, proposed);
        // Its step's call goes, so the run's end, if it comes, finds none.
        if (run.wait == .call) {
            const ns = keyNamespace(run_ns_key);
            proposeClear(shard, ns, calls_mod.callerSlot(ns, run.workflow), run.wait_on) catch {};
        }
        return proposed;
    }

    fn noteEnding(self: *WorkflowHandler, run_key: []const u8, proposed: persistence_mod.ProposeResult) void {
        const gop = self.ending.getOrPut(self.allocator, run_key) catch return;
        if (!gop.found_existing) gop.key_ptr.* = self.allocator.dupe(u8, run_key) catch {
            self.ending.removeByPtr(gop.key_ptr);
            return;
        };
        gop.value_ptr.* = .{ .index = proposed.index, .term = proposed.term };
    }

    /// Whether a cancel this leader proposed for `run_key` may yet apply:
    /// nothing else moves the run meanwhile.
    pub fn endingInFlight(self: *const WorkflowHandler, shard: *const Shard, run_key: []const u8) bool {
        const e = self.ending.get(run_key) orelse return false;
        const raft = shard.raft_node;
        if (raft.role != .leader or raft.last_applied >= e.index) return false;
        return (raft.log.entryTerm(e.index) orelse return false) == e.term;
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

        const run = self.shownRun(run_ns_key, keyName(run_ns_key)) orelse {
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

    /// A run as a read shows it: as applied, so every node answers alike.
    const ShownRun = struct {
        run_id_owned: []const u8,
        workflow_name_owned: []const u8,
        workflow_version_owned: []const u8,
        status: RunStatus,
        current_step_name_owned: ?[]const u8,
        input_owned: []const u8,
        created_at_ms: i64,
        started_at_ms: ?i64,
        completed_at_ms: ?i64,
        wait_signal_type_owned: ?[]const u8,
        output_owned: ?[]const u8,
        search_tags_owned: ?[]const u8,
    };

    fn shownRun(self: *WorkflowHandler, run_key: []const u8, run_id: []const u8) ?ShownRun {
        const rec = self.appliedRun(run_key) orelse return null;
        const status = appliedStatus(rec);
        return .{
            .run_id_owned = run_id,
            .workflow_name_owned = rec.workflow,
            .workflow_version_owned = rec.version,
            .status = status,
            .current_step_name_owned = if (rec.step.len > 0) rec.step else null,
            .input_owned = rec.input,
            .created_at_ms = rec.created_at_ms,
            .started_at_ms = rec.created_at_ms,
            .completed_at_ms = if (rec.completed_at_ms != 0) rec.completed_at_ms else null,
            .wait_signal_type_owned = if (!status.isTerminal() and rec.wait == .signal) rec.wait_on else null,
            .output_owned = if (rec.has_output != 0) rec.output else null,
            .search_tags_owned = if (rec.search_tags.len > 0) rec.search_tags else null,
        };
    }

    /// Run `run_key`'s history as applied (`workflow.hist`), oldest first;
    /// the events point into the store, so they hold until the next apply.
    fn appliedHistory(self: *WorkflowHandler, run_key: []const u8, out: *std.ArrayListUnmanaged(HistoryEvent)) !void {
        const prefix = try runs_machine.runRange(self.allocator, keyNamespace(run_key), keyName(run_key));
        defer self.allocator.free(prefix);
        var it = self.defsRead().range(runs_machine.HistFam, prefix);
        while (try it.next()) |item| try out.append(self.allocator, .{ .event_type_owned = item.value.v.event, .detail_owned = item.value.v.detail, .timestamp_ms = item.value.v.at_ms });
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

        const shown_run = self.shownRun(run_ns_key, keyName(run_ns_key)) orelse {
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
        // record's, itself under 64 KiB. So each length fits u16. The run's,
        // as applied: every node answers alike.
        _ = shown_run;
        var applied_events: std.ArrayListUnmanaged(HistoryEvent) = .empty;
        defer applied_events.deinit(self.allocator);
        self.appliedHistory(run_ns_key, &applied_events) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        const events = applied_events.items;
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

    /// Collect the runs of namespace `ns` matching filters, as applied, on
    /// this handler's own thread.
    pub fn collectMatchingRuns(
        self: *WorkflowHandler,
        runs_list: *std.ArrayListUnmanaged(RunInfo),
        ns: NamespaceId,
        workflow_name: []const u8,
        status_filter: ?[]const u8,
        search_lower: ?[]const u8,
    ) !void {
        const id = ns.bytes();
        var rit = self.defsRead().range(runs_machine.RunFam, &id);
        while (rit.next() catch unreachable) |item| { // written by the machine alone
            const run = self.shownRun(item.key, keyName(item.key)).?;
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

        const def = self.defOf(ns_key) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // If a specific version is requested, check it matches
        if (req.value.len > 0) {
            if (!std.mem.eql(u8, def.version, req.value)) {
                shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
                return;
            }
        }

        shard.sendOkResponse(conn, req.header.request_id, def.yaml);
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
        if (self.defOf(ns_key) == null) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return null;
        }
        return persistence_mod.proposeEntryIn(shard, .workflow_enabled, entry_mod.Flags.NONE, req.ns_id, ns_key, &.{@intFromBool(enabled)}) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureCode(err).toStatus(), proto.notRun(persistence_mod.failureMessage(err, "not persisted")));
            return null;
        };
    }

    // ── LIST DEFINITIONS ────────────────────────────────────────────────

    fn handleListDefinitions(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const DefInfo = struct { name: []const u8, version: []const u8, created_at: i64, enabled: bool };
        var defs: std.ArrayListUnmanaged(DefInfo) = .empty;
        defer defs.deinit(self.allocator);

        const id = req.ns_id.bytes();
        var dit = self.defsRead().range(defs_machine.DefFam, &id);
        while (dit.next() catch unreachable) |rec| { // written by the machine alone
            const def = rec.value.v;
            defs.append(self.allocator, .{
                .name = def.name,
                .version = def.version,
                .created_at = def.created_at_ms,
                .enabled = def.enabled != 0,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }

        // The walk's answer, for a shard with no walk context (`walk.zig`):
        // [count:u32]([name_len:u16][name][version_len:u16][version][created_at:i64][enabled:u8])*
        // [has_more:u8][cursor_len:u16]
        var total: usize = 4; // count
        for (defs.items) |d| {
            total += 2 + d.name.len + 2 + d.version.len + 8 + 1;
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
            buf[pos] = @intFromBool(d.enabled);
            pos += 1;
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
    /// Takes run `run_ns_key`'s next steps, until it waits or ends, and
    /// logs them as one step. Within a decision already taken (a call's
    /// end resumes it, say), they join that decision.
    fn advanceWorkflow(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8) void {
        if (self.runs.getPtr(run_ns_key)) |run| return self.advanceCopy(shard, run_ns_key, run);
        const run = self.takeRun(shard, run_ns_key) orelse return;
        if (run.ending == null and !self.endingInFlight(shard, run_ns_key)) self.advanceCopy(shard, run_ns_key, run);
        self.commitRun(shard, run_ns_key);
    }

    fn advanceCopy(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord) void {
        if (run.ending != null) return;
        const ns = keyNamespace(run_ns_key);

        // Look up the definition
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);

        // A run with no definition to step by ends, saying so, rather than
        // waiting at its step for good.
        const def_record = self.defOf(def_ns_key) orelse
            return self.completeRun(shard, run_ns_key, run, .failed, "workflow definition not found", 0);

        // Parse the definition to access the step graph
        var def = self.parseStored(def_record.yaml, def_ns_key) catch
            return self.completeRun(shard, run_ns_key, run, .failed, "workflow definition doesn't parse", 0);
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
                    const signal_found = runs_machine.hasSignal(self.defsRead(), run_ns_key, wait_step.signal_type);

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
        // `@workflow/<name>` or `@workflow/<name>:<version>`: a pinned version
        // starts only if it is the one the child's shard holds.
        const raw = run_step.targetName();
        const colon = std.mem.indexOfScalar(u8, raw, ':');
        const child_name = if (colon) |i| raw[0..i] else raw;
        const version = if (colon) |i| raw[i + 1 ..] else "";
        return self.callStep(shard, run, .workflow, child_name, version, step_input, step_label, ns, false, now_ms);
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
        return null;
    }

    /// The shard a pending action or child run is on, from its slot; null
    /// when nothing is pending.
    fn slotShard(shard: *const Shard, slot: ?u32) ?u16 {
        return shard.router.slotToShard(slot orelse return null);
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
        return self.callStep(shard, run, .action, action_name, "", input, step_label, ns, can_wait, now_ms);
    }

    /// A step's call of an action's run or a child workflow's run: logged
    /// here, with the callee's start sent to its slot from that entry. The
    /// callee's shard answers it, and tells this one when the callee ends.
    /// Its run id is chosen here, from this shard's own generator, in the
    /// callee's slot: the generator's ids carry the shard that drew them, so
    /// no other shard's draw names the same run. Asking again names it too.
    fn callStep(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, kind: ae.CallKind, target: []const u8, version: []const u8, input: []const u8, step_label: []const u8, ns: NamespaceId, can_wait: bool, now_ms: i64) ?[]const u8 {
        const slot = router.nsKeySlot(ns, target);
        if (input.len > ae.MAX_INPUT or version.len > ae.MAX_VERSION) return definition.StepOutcome.execution_failure;
        // Its start goes to the callee's shard: held, as for a full log,
        // while that outbox, this shard's, or the namespace's share of
        // either has no room, so one tenant's calls can't fill them.
        if (shard.sendWait(shard.router.slotToShard(slot), ns) != null) return self.holdStepForRoom(run, step_label, now_ms);
        var run_id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const run_id = shard.run_id_gen.next(if (kind == .action) .action else .workflow, slot, shard.raft_node.current_term, &run_id_buf);
        const buf = self.allocator.alloc(u8, ae.Call.max_size) catch return definition.StepOutcome.execution_failure;
        const payload = ae.Call.encodeInto(buf, .{
            .ns = ns.int(),
            .kind = kind,
            .caller_run = run.run_id_owned,
            .caller_workflow = run.workflow_name_owned,
            .step = step_label,
            .target = target,
            .version = version,
            .run = run_id,
            .input = input,
            .can_wait = can_wait,
        }) catch {
            self.allocator.free(buf);
            return definition.StepOutcome.execution_failure;
        };
        run.waiting_for_room = false;
        // The call goes in the step that waits on it, with that step's
        // other fields: one that can't fit an entry fails the step.
        if (payload.len > MAX_LOGGED_CALL) {
            self.allocator.free(buf);
            self.addHistoryEvent(run, "step_call_too_large", "input too large to log", now_ms);
            return definition.StepOutcome.execution_failure;
        }
        if (run.call_payload) |old| self.allocator.free(old);
        run.call_payload = (self.allocator.realloc(buf, payload.len) catch buf)[0..payload.len];
        self.parkForCall(run, kind, run_id, step_label, target, slot, now_ms);
        return null;
    }

    /// The most a step's call may take: an entry, less room for the rest of
    /// the step that makes it (its key, its fields, a few events).
    pub const MAX_LOGGED_CALL: usize = persistence_mod.MAX_PERSIST_PAYLOAD - entry_mod.COMMAND_PREFIX_SIZE - MAX_START_KEY - 8 * 1024;

    /// Proposes a step's call entry, charged to its namespace's share of
    /// the log's room as a client's write is.
    fn proposeCharged(shard: *Shard, ns: NamespaceId, etype: entry_mod.EntryType, payload: []const u8) !@import("../raft/node.zig").ProposeResult {
        shard.raft_node.charge_ns = ns.int();
        defer shard.raft_node.charge_ns = 0;
        return shard.raft_node.propose(etype, entry_mod.Flags.NONE, payload);
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

    /// Why a call was refused: what its answer said (a repair's skip), or
    /// the one other refusal, a run id another caller holds.
    fn refusedWhy(call: ?calls_mod.Call.Value) []const u8 {
        if (call) |c| if (c.outcome.len > 0) return c.outcome;
        return "its run id names another caller's run";
    }

    /// A step's call as this shard has applied it.
    fn callOf(shard: *Shard, run: *const RunRecord, action_rid: []const u8) ?calls_mod.Call.Value {
        const ns = keyNamespace(run.ns_key);
        const read: contract_view.ReadView = .{ .store = &shard.defaultPartition().store };
        return calls_mod.callOf(read, ns, calls_mod.callerSlot(ns, run.workflow_name_owned), action_rid);
    }

    /// The first wait before asking again for a call whose action isn't
    /// registered, and the longest: each is twice the last.
    pub const CALL_RETRY_FIRST_MS: u64 = 5_000;
    pub const CALL_RETRY_MAX_MS: u64 = 60_000;

    /// The wait after a call's `attempts`th start before the next.
    pub fn callRetryMs(attempts: u32) u64 {
        const doublings: u6 = @intCast(@min(@max(attempts, 1) - 1, 16));
        return @min(CALL_RETRY_FIRST_MS << doublings, CALL_RETRY_MAX_MS);
    }

    /// Whether a step's call is settled, so the step resumes: its run
    /// ended; its action wasn't registered and the step can't wait, or has
    /// waited `missing_action_wait_ms`; or its entry applied without making
    /// a call. Meanwhile one whose action isn't registered is asked for
    /// again, waiting longer each time.
    fn callSettled(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, action_rid: []const u8, now_ms: i64) bool {
        // Recorded with the step that waits on it: none is a call cleared
        // already, which the step takes as lost.
        const c = callOf(shard, run, action_rid) orelse return true;
        switch (c.state) {
            .sent, .started => return false,
            .done, .refused => return true,
            .no_action => {
                // With no wait set it fails at once, saying nothing of a wait.
                if (!c.can_wait or shard.actions_handler.missing_action_wait_ms == 0) return true;
                if (!run.call_unregistered_noted) {
                    run.call_unregistered_noted = true;
                    self.addHistoryEvent(run, "action_not_registered", c.target, now_ms);
                }
                if (run.call_retry_index > shard.raft_node.last_applied) return false;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                if ((now_ns -| c.first_ns) / std.time.ns_per_ms >= shard.actions_handler.missing_action_wait_ms) return true;
                const wait_ms = callRetryMs(c.attempts);
                if (now_ns < c.last_ns +| wait_ms * std.time.ns_per_ms or now_ms < run.call_retry_at_ms) return false;
                self.retryCall(shard, run, action_rid, c, now_ms);
                return false;
            },
        }
    }

    /// Asks again for a call whose action wasn't registered. Not proposed
    /// (a busy log, a step-down): asked again a second later.
    fn retryCall(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, action_rid: []const u8, c: calls_mod.Call.Value, now_ms: i64) void {
        run.call_retry_at_ms = now_ms +| 1_000;
        const ns = keyNamespace(run.ns_key);
        // Its start is sent again: not while its outbox has no room.
        if (shard.sendWait(shard.router.slotToShard(router.nsKeySlot(ns, c.target)), ns) != null) return;
        var buf: [ae.CallRetry.max_size]u8 = undefined;
        const payload = ae.CallRetry.encodeInto(&buf, .{ .ns = ns.int(), .slot = calls_mod.callerSlot(ns, run.workflow_name_owned), .run = action_rid }) catch return;
        run.call_retry_index = (proposeCharged(shard, ns, .step_call_retry, payload) catch return).index;
        if (shard.metrics_registry) |m| m.workflow.recordActionCallRetry();
        var nb: [ae.MAX_ACTION_NAME + 96]u8 = undefined;
        const next_s = callRetryMs(c.attempts +| 1) / 1000;
        self.addHistoryEvent(run, "action_call_retry", std.fmt.bufPrint(&nb, "waiting for action {s} to be registered, retry in {d} s", .{ c.target, next_s }) catch c.target, now_ms);
    }

    fn proposeClear(shard: *Shard, ns: NamespaceId, slot: u16, action_rid: []const u8) !void {
        var buf: [ae.CallClear.max_size]u8 = undefined;
        const payload = try ae.CallClear.encodeInto(&buf, .{ .ns = ns.int(), .slot = slot, .run = action_rid });
        _ = try proposeCharged(shard, ns, .step_call_clear, payload);
    }

    /// Whether run `run_id`, a child, must wait to end: its parent's outbox
    /// has no room for its end.
    fn parentWait(shard: *Shard, ns: NamespaceId, run_id: []const u8) bool {
        const read: contract_view.ReadView = .{ .store = &shard.defaultPartition().store };
        const p = children_mod.parentOf(read, ns, run_id) orelse return false;
        return shard.sendWait(shard.router.slotToShard(p.parent_slot), ns) != null;
    }

    /// Parent records one pass looks at, at most.
    pub const CHILD_SCAN_MAX: usize = 256;

    fn freeChildStarting(self: *WorkflowHandler) void {
        var kit = self.child_starting.keyIterator();
        while (kit.next()) |k| self.allocator.free(k.*);
        self.child_starting.clearRetainingCapacity();
    }

    /// This shard's leader starts each child a parent asked it for
    /// (`workflow.parents`, pending) from its own thread, with the run id
    /// and version the parent's call chose; one with no such workflow or
    /// version is refused. A start proposed isn't proposed again while it
    /// may yet apply. A pass looks at `CHILD_SCAN_MAX` records past where
    /// the last stopped, so many running children don't slow every tick.
    fn startChildren(self: *WorkflowHandler, shard: *Shard) void {
        const raft = shard.raft_node;
        if (!raft.leaderReady()) return;
        if (self.child_starting_term != raft.current_term) {
            self.freeChildStarting();
            self.child_starting_term = raft.current_term;
        }
        const store = &shard.defaultPartition().store;
        var it = if (self.child_scan_after.items.len > 0) store.rangeAfter(children_mod.Parents.ID, "", self.child_scan_after.items) else store.range(children_mod.Parents.ID, "");
        var looked: usize = 0;
        var at_end = true;
        while (it.next()) |rec| {
            if (looked == CHILD_SCAN_MAX) {
                at_end = false;
                break;
            }
            looked += 1;
            const k = rec.key();
            self.child_scan_after.clearRetainingCapacity();
            self.child_scan_after.appendSlice(self.allocator, k) catch {};
            const p = (children_mod.Parents.Value.decode(rec.value) catch continue).v;
            if (p.state != .pending) {
                if (self.child_starting.fetchRemove(k)) |kv| self.allocator.free(kv.key);
                continue;
            }
            if (self.child_starting.get(k)) |index| if (raft.last_applied < index) continue;
            if (k.len <= 4) continue;
            const ns: NamespaceId = .of(std.mem.readInt(u32, k[0..4], .big));
            const run_id = k[4..];
            const proposed = self.startChild(shard, ns, run_id, p) catch continue;
            const gop = self.child_starting.getOrPut(self.allocator, k) catch continue;
            if (!gop.found_existing) gop.key_ptr.* = self.allocator.dupe(u8, k) catch {
                self.child_starting.removeByPtr(gop.key_ptr);
                continue;
            };
            gop.value_ptr.* = proposed.index;
        }
        if (at_end) self.child_scan_after.clearRetainingCapacity();
    }

    /// A child's start, as its parent asked: the workflow and version this
    /// shard holds; or a refusal when it holds no such workflow, or not that
    /// version, or a run already holds the child's id.
    fn startChild(self: *WorkflowHandler, shard: *Shard, ns: NamespaceId, run_id: []const u8, p: children_mod.Parent.Value) !persistence_mod.ProposeResult {
        const def_key = self.nsKey(ns, p.workflow) orelse return error.OutOfMemory;
        defer self.allocator.free(def_key);
        const def = self.defOf(def_key);
        const held = if (def) |d| p.version.len == 0 or std.mem.eql(u8, d.version, p.version) else false;
        // A run already holding its id is another's: a start of ours, once
        // applied, would have marked the record started. Proposing it would
        // be refused, unobserved, for ever; the parent is told instead.
        const run_key = self.nsKey(ns, run_id) orelse return error.OutOfMemory;
        defer self.allocator.free(run_key);
        const taken = self.appliedRun(run_key) != null;
        if (!held or taken) {
            // A refusal is told to the parent: not past its outbox's room.
            if (shard.sendWait(shard.router.slotToShard(p.parent_slot), ns) != null) return error.Overloaded;
            var buf: [ae.ChildRefuse.max_size]u8 = undefined;
            const payload = try ae.ChildRefuse.encodeInto(&buf, .{ .ns = ns.int(), .run = run_id, .answer = if (taken) .conflict else .no_action });
            return shard.raft_node.propose(.child_refuse, entry_mod.Flags.NONE, payload);
        }
        return self.proposeStart(shard, ns, run_id, p.workflow, if (p.version.len > 0) p.version else "latest", p.input, null, "child_started");
    }

    /// How often a leader looks for calls whose workflow run has ended, and
    /// how many it looks at a pass.
    pub const CALLS_SWEEP_EVERY_MS: i64 = 60_000;
    pub const CALLS_SWEEP_MAX: usize = 256;

    /// Drops the calls whose workflow run has ended or is gone: a clear the
    /// log didn't take, at a cancel or a resolve. A pass looks at
    /// `CALLS_SWEEP_MAX` calls past where the last stopped.
    fn sweepCalls(self: *WorkflowHandler, shard: *Shard, now_ms: i64) void {
        if (!shard.raft_node.leaderReady() or now_ms < self.calls_sweep_at_ms) return;
        self.calls_sweep_at_ms = now_ms +| CALLS_SWEEP_EVERY_MS;
        const store = &shard.defaultPartition().store;
        var it = if (self.calls_sweep_after.items.len > 0) store.rangeAfter(calls_mod.Calls.ID, "", self.calls_sweep_after.items) else store.range(calls_mod.Calls.ID, "");
        var looked: usize = 0;
        var at_end = true;
        while (it.next()) |rec| {
            if (looked == CALLS_SWEEP_MAX) {
                at_end = false;
                break;
            }
            looked += 1;
            const k = rec.key();
            self.calls_sweep_after.clearRetainingCapacity();
            self.calls_sweep_after.appendSlice(self.allocator, k) catch {};
            // (ns, slot, run): `calls_mod.callKey`.
            if (k.len < 6) continue;
            const ns: NamespaceId = .of(std.mem.readInt(u32, k[0..4], .big));
            const slot = std.mem.readInt(u16, k[4..6], .big);
            const call = (calls_mod.Calls.Value.decode(rec.value) catch continue).v;
            const run_key = self.nsKey(ns, call.caller_run) orelse continue;
            defer self.allocator.free(run_key);
            const ended = if (self.appliedRun(run_key)) |r| appliedStatus(r).isTerminal() else true;
            if (ended) proposeClear(shard, ns, slot, k[6..]) catch {};
        }
        if (at_end) self.calls_sweep_after.clearRetainingCapacity();
    }

    /// Park a workflow run waiting for a step's call: an action's run or a
    /// child's.
    fn parkForCall(self: *WorkflowHandler, run: *RunRecord, kind: ae.CallKind, action_run_id: []const u8, step_label: []const u8, action_name: []const u8, slot: u32, now_ms: i64) void {
        run.pending_call_kind = kind;
        run.status = .waiting;

        // Store tracking info for checkPendingActions
        if (run.pending_action_run_id_owned) |old| self.allocator.free(old);
        run.pending_action_run_id_owned = self.allocator.dupe(u8, action_run_id) catch null;

        if (run.pending_step_name_owned) |old| self.allocator.free(old);
        run.pending_step_name_owned = self.allocator.dupe(u8, step_label) catch null;

        // Store which slot the action run lives on
        run.pending_action_slot = slot;

        // A call settles only from its record in `step.calls`.
        if (run.wait_signal_type_owned) |old| self.allocator.free(old);
        run.wait_signal_type_owned = null;

        self.addHistoryEvent(run, if (kind == .action) "awaiting_action" else "awaiting_child", action_name, now_ms);
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

    /// Runs whose call settled or whose timer is due, each decided and its
    /// step logged. Called periodically by the shard's task scheduler;
    /// leader only.
    pub fn checkPendingActions(self: *WorkflowHandler, shard: *Shard) void {
        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        self.sweepCalls(shard, now_ms);
        self.startChildren(shard);
        if (shard.raft_node.role != .leader) return;
        if (self.rewatch) self.rewatchRuns();

        // What's due and can't be taken this pass stays for the next.
        var due_keys: [64][]const u8 = undefined;
        var due_count: usize = 0;
        while (self.timers.peek()) |top| {
            if (top.at_ms > now_ms or due_count == due_keys.len) break;
            const t = self.timers.popIndex(0);
            due_keys[due_count] = t.ns_key;
            due_count += 1;
        }
        defer for (due_keys[0..due_count]) |k| self.allocator.free(k);

        var resume_keys: [64][]const u8 = undefined;
        var resume_count: usize = 0;
        defer for (resume_keys[0..resume_count]) |k| self.allocator.free(k);
        var it = self.parked.keyIterator();
        while (it.next()) |key_ptr| {
            if (resume_count == resume_keys.len) break;
            const ns_key = key_ptr.*;
            if (self.steppingAt(shard, ns_key) != null or self.endingInFlight(shard, ns_key)) continue;
            const rec = self.appliedRun(ns_key) orelse continue;
            const ns = keyNamespace(ns_key);
            // Only a call that settled, or one waiting on its action to be
            // registered (it may be asked again), is a decision to take.
            if (calls_mod.callOf(self.defsRead(), ns, calls_mod.callerSlot(ns, rec.workflow), rec.wait_on)) |c| switch (c.state) {
                .sent, .started => continue,
                .done, .refused, .no_action => {},
            };
            resume_keys[resume_count] = self.allocator.dupe(u8, ns_key) catch break;
            resume_count += 1;
        }

        for (resume_keys[0..resume_count]) |ns_key| {
            const run = self.takeRun(shard, ns_key) orelse continue;
            const rid = run.pending_action_run_id_owned orelse {
                self.dropRun(ns_key);
                continue;
            };
            const retry_index = run.call_retry_index;
            const noted = run.call_unregistered_noted;
            if (self.callSettled(shard, run, rid, now_ms)) {
                self.resumeFromAction(shard, ns_key, now_ms);
            } else if (run.call_retry_index == retry_index and run.call_unregistered_noted == noted) {
                // Still waiting, nothing changed: nothing to log.
                self.dropRun(ns_key);
                continue;
            }
            self.commitRun(shard, ns_key);
        }

        // A run can have two entries due at once, and what ran before this
        // may have moved it on: each is asked again as it fires.
        for (due_keys[0..due_count]) |ns_key| {
            const rec = self.appliedRun(ns_key) orelse continue;
            if (rec.wait_due_ms == 0 or rec.wait_due_ms > now_ms) continue;
            if (rec.wait != .timer and rec.wait != .signal) continue;
            if (self.steppingAt(shard, ns_key) != null) continue;
            if (self.endingInFlight(shard, ns_key)) {
                // Its timer stands if the cancel doesn't.
                self.armTimer(ns_key, now_ms +| ENDING_RECHECK_MS);
                continue;
            }
            const r = self.takeRun(shard, ns_key) orelse continue;
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
            self.commitRun(shard, ns_key);
        }
    }

    /// Drops a decision's copy without logging anything: nothing changed.
    fn dropRun(self: *WorkflowHandler, run_key: []const u8) void {
        var kv = self.runs.fetchRemove(run_key) orelse return;
        self.freeRunRecord(&kv.value);
        self.allocator.free(kv.key);
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

    /// Resume a workflow run once its step's call has settled.
    fn resumeFromAction(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, now_ms: i64) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        const action_rid = run.pending_action_run_id_owned orelse return;
        const step_label = run.pending_step_name_owned orelse "unknown";

        // Its call, as applied, recorded with the step that waits on it:
        // none is one cleared already, and the step fails. The outcome may
        // point into the call's record, a copy of applied state this shard
        // keeps until the clear below applies.
        const call = callOf(shard, run, action_rid);
        if (call == null) log.warn("workflow run {f}: the call of action run {s} its step waits for is gone; the step fails", .{ shown(run_ns_key), action_rid });
        const Settled = enum { completed, failed, timed_out, no_action, refused, lost };
        const settled: Settled = if (call) |c| switch (c.state) {
            .done => switch (std.enums.fromInt(ae.DoneStatus, c.status) orelse .failed) {
                .completed => .completed,
                .failed => .failed,
                .timed_out => .timed_out,
                .cancelled => .failed,
            },
            .no_action => .no_action,
            .refused => .refused,
            // Not settled: `callSettled` resumes only a settled one.
            .sent, .started => return,
        } else .lost;

        // A child that failed, was cancelled or timed out takes the step's
        // failure transition, as a failed action does; an action whose lease
        // ran out is an execution failure. Only a completed run has an output
        // (its definition's mapping is for completion), so a failed one
        // gives "{}".
        const child = run.pending_call_kind == .workflow;
        const outcome: []const u8 = switch (settled) {
            .completed => blk: {
                // The outcome the worker named, "success" when it named none.
                const c = call.?;
                const action_outcome = if (c.outcome.len == 0) definition.StepOutcome.success else c.outcome;
                if (run.step_outputs) |*so| {
                    so.put(self.allocator, step_label, if (c.result.len == 0) "{}" else c.result, action_outcome) catch {};
                }
                break :blk action_outcome;
            },
            .failed => blk: {
                if (run.step_outputs) |*so| so.put(self.allocator, step_label, "{}", definition.StepOutcome.failure) catch {};
                break :blk definition.StepOutcome.failure;
            },
            .timed_out => if (child) blk: {
                if (run.step_outputs) |*so| so.put(self.allocator, step_label, "{}", definition.StepOutcome.failure) catch {};
                break :blk definition.StepOutcome.failure;
            } else definition.StepOutcome.execution_failure,
            .no_action => definition.StepOutcome.target_not_found,
            .refused, .lost => definition.StepOutcome.execution_failure,
        };

        // Record completion before clearing state (step_label points into pending_step_name_owned)
        run.status = .running;
        if (child) switch (settled) {
            .no_action => {
                const c = call.?;
                var nb: [ae.TargetName.max_size + ae.MAX_VERSION + 2]u8 = undefined;
                const named = if (c.version.len > 0) std.fmt.bufPrint(&nb, "{s}:{s}", .{ c.target, c.version }) catch c.target else c.target;
                self.addHistoryEvent(run, "child_not_found", named, now_ms);
            },
            .lost => self.addHistoryEvent(run, "child_lost", outcome, now_ms),
            .refused => self.addHistoryEvent(run, "child_refused", refusedWhy(call), now_ms),
            else => self.addHistoryEvent(run, "child_completed", outcome, now_ms),
        } else switch (settled) {
            .no_action => {
                // Said as the action's shard would: which namespace, and how
                // long it waited, when it did.
                const c = call.?;
                const ns = keyNamespace(run_ns_key);
                const nh = shard.namespace_handler;
                const name = nh.table.nameOf(ns) orelse nh.createdName(ns) orelse "";
                const waited = c.can_wait and shard.actions_handler.missing_action_wait_ms > 0;
                var db: [ae.MAX_ACTION_NAME + 192]u8 = undefined;
                var wb: [48]u8 = undefined;
                const why = if (waited) std.fmt.bufPrint(&wb, "; waited {d} ms", .{shard.actions_handler.missing_action_wait_ms}) catch "" else "";
                const detail = std.fmt.bufPrint(&db, "action {s} is not registered in namespace {s}{s}", .{ c.target, name, why }) catch c.target;
                self.addHistoryEvent(run, "action_not_found", detail, now_ms);
            },
            .lost => self.addHistoryEvent(run, "action_lost", outcome, now_ms),
            .refused => self.addHistoryEvent(run, "action_refused", refusedWhy(call), now_ms),
            else => self.addHistoryEvent(run, "action_completed", outcome, now_ms),
        }
        self.addHistoryEvent(run, "step_completed", step_label, now_ms);
        if (shard.metrics_registry) |m| m.workflow.recordStepExecuted();
        // The call's record goes with the step that takes its end.
        run.took_call_end = true;
        run.call_unregistered_noted = false;
        run.call_retry_at_ms = 0;
        run.call_retry_index = 0;

        // Clear pending action state
        if (run.pending_action_run_id_owned) |a| self.allocator.free(a);
        run.pending_action_run_id_owned = null;
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        run.pending_step_name_owned = null;
        run.pending_action_slot = null;
        if (run.wait_signal_type_owned) |s| self.allocator.free(s);
        run.wait_signal_type_owned = null;

        // Look up definition and resolve transition for the completed step
        const ns = keyNamespace(run_ns_key);
        const def_ns_key = self.nsKey(ns, run.workflow_name_owned) orelse return;
        defer self.allocator.free(def_ns_key);
        const def_record = self.defOf(def_ns_key) orelse return;

        var def = self.parseStored(def_record.yaml, def_ns_key) catch return;
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
            const def_record = self.defOf(def_ns_key) orelse return;

            var def = self.parseStored(def_record.yaml, def_ns_key) catch return;
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

    /// The decision on `run`'s copy ends the run `status`, its last event
    /// saying `detail`: logged with its step (`commitRun`), so the run ends
    /// when that applies, on every replica alike.
    pub fn completeRun(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) void {
        _ = shard;
        _ = run_ns_key;
        _ = now_ms;
        if (run.ending != null) return;
        run.ending = status;
        run.ending_detail_owned = self.allocator.dupe(u8, detail[0..@min(detail.len, MAX_CANCEL_REASON)]) catch null;
    }

    /// Takes run `run_key` for one decision: a copy built from what is
    /// applied, in `runs` until `commitRun` logs what it decided and drops
    /// it. Null for a run that isn't there or has ended, or one whose step
    /// this leader logged and is yet to apply.
    fn takeRun(self: *WorkflowHandler, shard: *Shard, run_key: []const u8) ?*RunRecord {
        std.debug.assert(!self.runs.contains(run_key));
        const rec = runs_machine.runOf(self.defsRead(), run_key) orelse return null;
        if (appliedStatus(rec).isTerminal()) return null;
        if (self.steppingAt(shard, run_key)) |rev| if (rev == rec.rev) return null;
        self.copyRun(run_key, rec) catch {
            log.err("workflow run {f}: not taken on (out of memory)", .{shown(run_key)});
            return null;
        };
        return self.runs.getPtr(run_key);
    }

    /// The rev this leader logged a step of `run_key` at, in this term.
    pub fn steppingAt(self: *WorkflowHandler, shard: *const Shard, run_key: []const u8) ?u64 {
        if (self.stepping_term != shard.raft_node.current_term) {
            self.clearStepping();
            self.stepping_term = shard.raft_node.current_term;
        }
        return self.stepping.get(run_key);
    }

    /// Logs the step the decision on `run_key`'s copy took, at the rev it
    /// was taken at, and drops the copy. A step the log can't take now is
    /// decided again on a later tick: nothing changed.
    fn commitRun(self: *WorkflowHandler, shard: *Shard, run_key: []const u8) void {
        var kv = self.runs.fetchRemove(run_key) orelse return;
        defer {
            self.freeRunRecord(&kv.value);
            self.allocator.free(kv.key);
        }
        const run = &kv.value;
        const index = self.proposeStep(shard, run) catch |err| {
            if (err == error.Unchanged or err == error.HeldForParent) return;
            if (!shard.quietHalted(err)) log.warn("shard {d}: workflow run {f}: step not logged: {s}; taken again later", .{ shard.id, shown(run_key), @errorName(err) });
            return;
        };
        _ = index;
        const owned = self.allocator.dupe(u8, run_key) catch return;
        const gop = self.stepping.getOrPut(self.allocator, owned) catch {
            self.allocator.free(owned);
            return;
        };
        if (gop.found_existing) self.allocator.free(owned);
        gop.value_ptr.* = run.rev;
    }

    /// The `workflow_step` value for `run`'s copy, proposed.
    fn proposeStep(self: *WorkflowHandler, shard: *Shard, run: *RunRecord) !u64 {
        const ns = keyNamespace(run.ns_key);
        // A child's end goes to its parent's slot: held, and decided again,
        // while that outbox has no room. Each child is held for its own
        // parent, so one full parent holds no other run's end.
        if (run.ending != null and parentWait(shard, ns, run.run_id_owned)) return error.HeldForParent;
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        const limit = completionLimit(run.ns_key);
        const Step = runs_machine.Step;

        var events: wire_schema.Writer = .{ .buf = try a.alloc(u8, limit) };
        var event_count: u32 = 0;
        const new_events = run.history.items[@min(run.hist_logged, run.history.items.len)..];
        const final: ?runs_machine.Event.Value = if (run.ending) |status| .{ .event = terminalEventType(status), .detail = run.ending_detail_owned orelse "", .at_ms = now_ms } else null;
        // The newest that fit: a step that adds more than one entry holds
        // keeps its last ones, and says how many it left out.
        const room = runs_machine.MAX_STEP_EVENTS - 1 - @intFromBool(final != null);
        const skip = new_events.len -| room;
        if (skip > 0) {
            const detail = try std.fmt.allocPrint(a, "{d} events left out of the log", .{skip});
            try runs_machine.Event.encode(&events, .{ .event = MARKER_EVENT, .detail = detail, .at_ms = now_ms });
            event_count += 1;
        }
        for (new_events[skip..]) |e| {
            try runs_machine.Event.encode(&events, .{ .event = e.event_type_owned, .detail = e.detail_owned, .at_ms = e.timestamp_ms });
            event_count += 1;
        }
        if (final) |f| {
            try runs_machine.Event.encode(&events, f);
            event_count += 1;
        }

        var outputs: wire_schema.Writer = .{ .buf = try a.alloc(u8, limit) };
        var output_count: u32 = 0;
        if (run.step_outputs) |so| {
            const prefix = try runs_machine.runRange(a, ns, keyName(run.ns_key));
            for (so.entries) |e| {
                const k = try std.mem.concat(a, u8, &.{ prefix, e.step_name });
                if (self.defsRead().get(runs_machine.OutFam, k) catch null) |logged| {
                    if (std.mem.eql(u8, logged.v.outcome, e.outcome) and std.mem.eql(u8, logged.v.output, e.output)) continue;
                }
                if (output_count == runs_machine.MAX_STEP_OUTPUTS) break;
                try runs_machine.StepOutput.encode(&outputs, .{ .step = e.step_name, .outcome = e.outcome, .output = e.output });
                output_count += 1;
            }
        }

        var wait: runs_machine.Wait = .none;
        var wait_on: []const u8 = "";
        var wait_due: i64 = 0;
        var timeout_to: []const u8 = "";
        if (run.ending == null) {
            if (run.pending_action_run_id_owned) |callee| {
                wait = .call;
                wait_on = callee;
            } else if (run.wait_signal_type_owned) |sig| {
                wait = .signal;
                wait_on = sig;
                wait_due = run.wait_timeout_at_ms;
                timeout_to = run.wait_timeout_target_owned orelse "";
            } else if (run.poll_next_at_ms > 0) {
                wait = .timer;
                wait_due = run.poll_next_at_ms;
            }
        }
        // The call whose end it took.
        const settles: []const u8 = if (run.took_call_end) run.took_call orelse "" else "";

        var pbuf: [progress_mod.Progress.max_size]u8 = undefined;
        const prog = progressOf(run);
        // A decision that changed nothing logs nothing.
        if (run.ending == null and event_count == 0 and output_count == 0 and run.call_payload == null and settles.len == 0) {
            const rec = self.appliedRun(run.ns_key) orelse return error.Unchanged;
            var lb: [progress_mod.Progress.max_size]u8 = undefined;
            const now_progress = prog.encode(&lb);
            if (std.mem.eql(u8, rec.step, run.current_step_name_owned orelse "") and rec.wait == wait and
                std.mem.eql(u8, rec.wait_on, wait_on) and rec.wait_due_ms == wait_due and
                std.mem.eql(u8, rec.wait_timeout_to, timeout_to) and
                (std.mem.eql(u8, rec.progress, now_progress) or (rec.progress.len == 0 and std.mem.eql(u8, now_progress, (progress_mod.Progress{}).encode(&pbuf)))))
                return error.Unchanged;
        }
        var output: ?[]const u8 = null;
        var tags: []const u8 = "";
        if (run.ending) |status| {
            if (status == .completed) output = self.workflowOutput(run, ns);
            if (self.nsKey(ns, run.workflow_name_owned)) |def_key| {
                defer self.allocator.free(def_key);
                if (self.buildSearchTags(def_key, run)) |t| {
                    defer self.allocator.free(t);
                    tags = try a.dupe(u8, t);
                }
            }
        }
        defer if (output) |o| self.allocator.free(o);

        const value: Step.Value = .{
            .rev = run.rev,
            .step = run.current_step_name_owned orelse "",
            .events = .{ .bytes = events.written(), .count = event_count },
            .outputs = .{ .bytes = outputs.written(), .count = output_count },
            .wait = wait,
            .wait_kind = if (wait == .call) run.pending_call_kind else .action,
            .wait_slot = if (wait == .call) @intCast(run.pending_action_slot orelse 0) else 0,
            .wait_on = wait_on,
            .wait_due_ms = wait_due,
            .wait_timeout_to = timeout_to,
            .progress = if (run.ending == null) prog.encode(&pbuf) else "",
            .settles = settles,
            .caller_slot = calls_mod.callerSlot(ns, run.workflow_name_owned),
            .workflow = run.workflow_name_owned,
            .ends = if (run.ending) |status| @backingInt(status) else 0,
            .has_output = output != null,
            .output = output orelse "",
            .tags = tags,
            .call = if (wait == .call) run.call_payload orelse "" else "",
        };
        const bytes = Step.encodeInto(try a.alloc(u8, limit), value) catch |err| switch (err) {
            error.NoSpaceLeft, error.TooLong => return self.proposeTooLarge(shard, run, a, limit, now_ms),
            else => return err,
        };
        return (try persistence_mod.proposeEntryIn(shard, .workflow_step, entry_mod.Flags.NONE, ns, run.ns_key, bytes)).index;
    }

    /// A step too large to log ends its run failed, saying so: that always
    /// fits.
    fn proposeTooLarge(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, a: Allocator, limit: usize, now_ms: i64) !u64 {
        _ = self;
        const ns = keyNamespace(run.ns_key);
        var events: wire_schema.Writer = .{ .buf = try a.alloc(u8, 512) };
        try runs_machine.Event.encode(&events, .{ .event = terminalEventType(.failed), .detail = "step record too large to log", .at_ms = now_ms });
        const value: runs_machine.Step.Value = .{
            .rev = run.rev,
            .step = run.current_step_name_owned orelse "",
            .events = .{ .bytes = events.written(), .count = 1 },
            .outputs = .{ .bytes = "", .count = 0 },
            .wait = .none,
            .wait_kind = .action,
            .wait_slot = 0,
            .wait_on = "",
            .wait_due_ms = 0,
            .wait_timeout_to = "",
            .progress = "",
            .settles = "",
            .caller_slot = calls_mod.callerSlot(ns, run.workflow_name_owned),
            .workflow = run.workflow_name_owned,
            .ends = @backingInt(RunStatus.failed),
            .has_output = false,
            .output = "",
            .tags = "",
            .call = "",
        };
        const bytes = try runs_machine.Step.encodeInto(try a.alloc(u8, limit), value);
        return (try persistence_mod.proposeEntryIn(shard, .workflow_step, entry_mod.Flags.NONE, ns, run.ns_key, bytes)).index;
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
        const def_record = self.defOf(def_ns_key) orelse return null;

        // Parse definition to access the output mapping
        var def = self.parseStored(def_record.yaml, def_ns_key) catch return null;
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
        const def_rec = self.defOf(def_ns_key) orelse return null;
        var def = self.parseStored(def_rec.yaml, def_ns_key) catch return null;
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
        var proposed: @FieldType(StreamTriggerState, "proposed") = null;
        const stream_ns = self.otherStreamNamespace(ns, trigger.namespace);
        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            const same_ns = if (state.stream_namespace_owned) |a| (if (stream_ns) |b| std.mem.eql(u8, a, b) else false) else stream_ns == null;
            if (std.mem.eql(u8, state.stream_name_owned, trigger.stream) and same_ns) {
                cursor_ts = state.stream_cursor_ts;
                cursor_seq = state.stream_cursor_seq;
                proposed = state.proposed;
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
            .proposed = proposed,
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
    /// stream, so a redefinition moving between them keeps its cursor. The
    /// own name is read from the node's table by id; a create entry whose
    /// leader found its trigger naming its own namespace says so itself
    /// (`Flags.OWN_NAMESPACE`), so a replica whose table lacks the name
    /// yet resolves it alike.
    fn otherStreamNamespace(self: *const WorkflowHandler, ns: NamespaceId, named: ?[]const u8) ?[]const u8 {
        const name = named orelse return null;
        if (name.len == 0) return null;
        const own = if (self.namespaces) |h| h.table.nameOf(ns) else null;
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
        const now_ms: i64 = @import("stdx").time.milliTimestamp();
        const term = shard.raft_node.current_term;

        var it = self.schedules.iterator();
        while (it.next()) |entry| {
            const schedule = entry.value_ptr;
            const slot = dueSlot(schedule, now_ms) orelse continue;
            if (slot <= schedule.fired_ms) continue;
            if (schedule.proposed_term == term and slot <= schedule.proposed_ms) continue;

            // Skip disabled workflows
            if (self.isDisabled(entry.key_ptr.*)) continue;

            // Check max_concurrent
            if (schedule.max_concurrent > 0) {
                const active = if (runs_machine.perDefOf(self.defsRead(), entry.key_ptr.*)) |per| per.active else 0;
                if (active >= schedule.max_concurrent) continue;
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
        var run_id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const slot = router.nsKeySlot(schedule.ns, schedule.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, shard.raft_node.current_term, &run_id_buf);

        return self.proposeRun(shard, schedule.ns, run_id_str, schedule.workflow_name_owned, "latest", input, idem, "schedule_started") != null;
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
        while (it.next()) |entry| {
            const trigger = entry.value_ptr;

            // Enforce poll interval unless a push-wake forced this poll.
            if (!forced) {
                const poll_ms: i64 = @intCast(trigger.poll_interval_ms);
                if (now_ms - trigger.last_poll_ms < poll_ms) continue;
            }

            // Skip disabled workflows
            if (self.isDisabled(entry.key_ptr.*)) continue;

            // As many events as this shard can start runs for now; with room
            // for none the trigger waits, its cursor where it is.
            const limit = triggerReadLimit(shard, trigger) orelse continue;
            // A namespace the trigger names that isn't known here yet
            // reads as empty.
            const stream_ns = if (trigger.stream_namespace_owned) |n| shard.namespaceId(n) else trigger.ns;
            const owner = streamOwner(shard, stream_ns, trigger.stream_name_owned);
            if (owner != shard.id) {
                // The shard that owns the stream reads it, on its thread.
                self.askTrigger(shard, entry.key_ptr.*, trigger, stream_ns, owner, limit, now_ms);
                continue;
            }
            trigger.last_poll_ms = now_ms;
            const sh = shard.stream_handler;
            const made = sh.madeAt(stream_ns, trigger.stream_name_owned) orelse continue;
            if (!sameStream(trigger, made)) continue;
            const cursor = readFrom(trigger, shard.raft_node.current_term);
            const result = sh.readPayloadsForStreamIn(trigger.stream_name_owned, stream_ns, cursor, limit);
            defer result.deinit(sh.allocator);
            self.takeRecords(shard, trigger, result.payloads, result.ids, result.last_id);
        }
    }

    /// How many events a trigger reads now: ahead for batching, but no more
    /// than this shard's log has room to start runs for. Null for none.
    fn triggerReadLimit(shard: *Shard, trigger: *const StreamTriggerState) ?usize {
        const raft = shard.raft_node;
        const room: usize = @intCast(raft_node_mod.MAX_OUTSTANDING -| (raft.log.lastIndex() -| raft.last_applied));
        if (room == 0) return null;
        const batch: usize = @max(@as(usize, @intCast(trigger.batch_size)), 1);
        return @min(batch * 10, room * batch);
    }

    /// The shard that owns stream `name` in `ns`, as the dispatcher routes
    /// its appends.
    fn streamOwner(shard: *Shard, ns: NamespaceId, name: []const u8) u16 {
        return switch (shard.router.route(router.routeNsKey(ns, name))) {
            .local => shard.id,
            .shard => |t| t.shard_id,
        };
    }

    /// Whether a read of the stream made at `made` goes on from the
    /// trigger's cursor. The first read says which stream the cursor is in.
    /// A read of a stream made later (deleted and made again, which today
    /// numbers its appends afresh) starts the cursor over, and reads again
    /// from there. Stamps only rise, so a read of one made earlier comes
    /// from a replica that hasn't applied the remake yet: it is dropped,
    /// and the cursor stays.
    fn sameStream(trigger: *StreamTriggerState, made: u64) bool {
        const was = trigger.stream_made_ns orelse {
            trigger.stream_made_ns = made;
            return true;
        };
        if (made == was) return true;
        if (made < was) return false;
        trigger.stream_made_ns = made;
        trigger.stream_cursor_ts = 0;
        trigger.stream_cursor_seq = 0;
        trigger.proposed = null;
        trigger.last_poll_ms = 0;
        return false;
    }

    /// Most a trigger's read of another shard's stream waits for its page
    /// before it is asked again: the answer may have found an inbox full.
    pub const TRIGGER_READ_DEADLINE_MS: i64 = 5_000;

    /// A stream another shard owns: ask that shard for a page from the
    /// trigger's cursor, at most one at a time. The page starts runs when it
    /// comes (`takeTriggerPage`).
    fn askTrigger(self: *WorkflowHandler, shard: *Shard, key: []const u8, trigger: *StreamTriggerState, ns: NamespaceId, owner: u16, limit: usize, now_ms: i64) void {
        _ = self;
        const raft = shard.raft_node;
        if (raft.role != .leader) return;
        if (trigger.read_seq != 0 and trigger.read_term == raft.current_term and now_ms - trigger.read_sent_ms < TRIGGER_READ_DEADLINE_MS) return;
        const mailboxes = shard.peer_mailboxes orelse return;
        if (owner >= mailboxes.len) return;
        const ns_name = trigger.stream_namespace_owned orelse (shard.namespace_handler.table.nameOf(ns) orelse return);
        const seq = trigger.next_read_seq + 1;
        const from = readFrom(trigger, raft.current_term);
        const bytes = (source_page.Read{
            .term = raft.current_term,
            .seq = seq,
            .kind = .stream,
            .asker = .trigger,
            .key = key,
            .namespace = ns_name,
            .source = trigger.stream_name_owned,
            .from_a = from.timestamp_ms,
            .from_b = from.sequence,
            .limit = @intCast(limit),
        }).encode(shard.allocator) orelse return;
        if (!mailboxes[owner].inbox.send(.{ .tag = .source_read, .src_shard = @intCast(shard.id), .payload_len = @intCast(bytes.len), .payload_ptr = bytes.ptr })) {
            // A full inbox: asked again next tick.
            shard.allocator.free(bytes);
            return;
        }
        trigger.last_poll_ms = now_ms;
        trigger.next_read_seq = seq;
        trigger.read_seq = seq;
        trigger.read_term = raft.current_term;
        trigger.read_sent_ms = now_ms;
    }

    /// A page the owner of a trigger's stream sent. Taken only if it
    /// answers that trigger's read in flight, this node still leads in the
    /// term it asked in, the workflow isn't disabled, the trigger's cursor
    /// is still where the read began, and the owner read the stream (an
    /// owner that couldn't says so, and the cursor stays); dropped
    /// otherwise, and the next tick asks again.
    pub fn takeTriggerPage(self: *WorkflowHandler, shard: *Shard, page: source_page.Page) void {
        const entry = self.stream_triggers.getEntry(page.key) orelse return;
        const trigger = entry.value_ptr;
        if (trigger.read_seq != page.seq or trigger.read_term != page.term) return;
        trigger.read_seq = 0;
        const raft = shard.raft_node;
        if (raft.role != .leader or raft.current_term != page.term) return;
        if (self.isDisabled(entry.key_ptr.*)) return;
        if (page.kind != .stream or page.status != .ok) return;
        const from = readFrom(trigger, page.term);
        if (from.timestamp_ms != page.from_a or from.sequence != page.from_b) return;
        if (!sameStream(trigger, page.made_ns)) return;
        _ = triggerReadLimit(shard, trigger) orelse return;
        const records = self.allocator.alloc(source_page.Page.Record, page.count) catch return;
        defer self.allocator.free(records);
        const got = page.records(records) orelse return;
        const payloads = self.allocator.alloc([]const u8, got.len) catch return;
        defer self.allocator.free(payloads);
        const ids = self.allocator.alloc(StreamID, got.len) catch return;
        defer self.allocator.free(ids);
        for (got, payloads, ids) |r, *p, *id| {
            p.* = r.payload;
            id.* = .{ .timestamp_ms = r.ms, .sequence = r.seq };
        }
        self.takeRecords(shard, trigger, payloads, ids, .{ .timestamp_ms = page.last_a, .sequence = page.last_b });
    }

    /// Starts the runs for events read from the trigger's cursor through
    /// `last`, and moves this leader's read position there (`proposed`). A read that found no events but
    /// reached past appends it couldn't read moves the cursor past them, so
    /// the trigger doesn't stall. A start the log refuses (full for now)
    /// leaves the cursor where the read began: the next read takes these
    /// events again, and those already started are the same runs, by their
    /// idempotency keys.
    fn takeRecords(self: *WorkflowHandler, shard: *Shard, trigger: *StreamTriggerState, payloads: []const []const u8, ids: []const StreamID, last: StreamID) void {
        if (payloads.len > 0) {
            if (trigger.batch_size <= 1) {
                // One run per event
                for (payloads, ids) |payload, event_id| {
                    if (!self.startRunFromTrigger(shard, trigger, payload, event_id)) return;
                }
            } else {
                // Batch mode: collect batch_size events into a JSON array per run
                const batch_limit: usize = @intCast(trigger.batch_size);
                var i: usize = 0;
                while (i < payloads.len) {
                    const end = @min(i + batch_limit, payloads.len);
                    const batch_json = self.buildBatchJson(payloads[i..end]) orelse {
                        i = end;
                        continue;
                    };
                    defer self.allocator.free(batch_json);
                    if (!self.startRunFromTrigger(shard, trigger, batch_json, ids[end - 1])) return;
                    i = end;
                }
            }
        }
        trigger.proposed = .{ .at = last, .term = shard.raft_node.current_term };
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
        var run_id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const slot = router.nsKeySlot(trigger.ns, trigger.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, shard.raft_node.current_term, &run_id_buf);

        // Room for the longest stream name a trigger may have, and both
        // numbers at their widest.
        var idem_buf: [parser.MAX_TRIGGER_STREAM + "trigger:@:".len + 2 * 20]u8 = undefined;
        const idem = std.fmt.bufPrint(&idem_buf, "trigger:{s}@{d}:{d}", .{ trigger.stream_name_owned, event_id.timestamp_ms, event_id.sequence }) catch {
            log.err("workflow {s}: trigger on stream of {d} bytes not started: its key doesn't fit", .{ trigger.workflow_name_owned, trigger.stream_name_owned.len });
            return true;
        };

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
        return self.appliedRun(writeKey(&buf, ns, run_id)) != null;
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
    fn proposeCreate(self: *WorkflowHandler, shard: *Shard, ns: NamespaceId, name: []const u8, yaml: []const u8, flags: u16) !persistence_mod.ProposeResult {
        _ = self;
        var key_buf: [MAX_START_KEY]u8 = undefined;
        if (KEY_PREFIX + name.len > key_buf.len) return error.KeyTooLarge;
        return persistence_mod.proposeEntryIn(shard, .workflow_create, flags, ns, writeKey(&key_buf, ns, name), yaml);
    }

    /// Start a run for a producer (a schedule, a trigger, a parent run):
    /// the start is proposed and the tick takes its first step once it
    /// applies, as for a client's. Returns the start's log index.
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

    const MARKER_EVENT = "history_trimmed";

    /// A cancel reason goes into the run's history and its completion record.
    const MAX_CANCEL_REASON: usize = 1024;

    /// The most a workflow_complete value for `run_ns_key` may hold: a log
    /// entry less its command header and key.
    fn completionLimit(run_ns_key: []const u8) usize {
        return persistence_mod.MAX_PERSIST_PAYLOAD - entry_mod.COMMAND_PREFIX_SIZE - run_ns_key.len;
    }

    /// A workflow entry applied on this handler's store, through the
    /// machine that owns it, then what follows it, as a shard's applier
    /// runs them: for tests, and a handler built without a shard. A create
    /// is answered with the definition's name; a start with whether it
    /// created its run (u8) and the run's id.
    pub fn replayEntry(self: *WorkflowHandler, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const etype = std.enums.fromInt(entry_mod.EntryType, entry.header.entry_type) orelse return error.Halt;
        const m: *const contract_machine.Machine = switch (etype) {
            .workflow_create, .workflow_enabled => &defs_machine.machine,
            .workflow_start, .workflow_step, .workflow_signal, .workflow_complete, .child_start, .child_refuse, .step_call_retry, .step_call_clear, .call_reply, .call_done => &runs_machine.machine,
            else => return error.Halt,
        };
        try contract_machine.applyEntry(m, self.defsRead().store, &self.def_scratch, entry, out);
        self.entryApplied(entry, out);
    }

    // ── Definitions ─────────────────────────────────────────────────────

    fn defsRead(self: *const WorkflowHandler) contract_view.ReadView {
        return .{ .store = self.store orelse @constCast(&self.own_store) };
    }

    /// The definition `def_key` names, as the store holds it, or null.
    pub fn defOf(self: *const WorkflowHandler, def_key: []const u8) ?DefinitionRecord {
        const d = defs_machine.get(self.defsRead(), def_key) orelse return null;
        return .{
            .name = d.name,
            .version = d.version,
            .yaml = d.yaml,
            .created_at_ms = d.created_at_ms,
            .created_index = d.created_index,
            .idempotency = std.enums.fromInt(definition.IdempotencyMode, d.idempotency) orelse .none,
            .own_namespace = d.own_namespace != 0,
            .enabled = d.enabled != 0,
            .anchor_ms = d.anchor_ms,
        };
    }

    pub fn isDisabled(self: *const WorkflowHandler, def_key: []const u8) bool {
        const d = self.defOf(def_key) orelse return false;
        return !d.enabled;
    }

    /// For tests: a committed create of `ns_key`'s workflow, stamped
    /// `created_at_ms`, applied as a shard applies one.
    pub fn replayCreate(self: *WorkflowHandler, ns_key: []const u8, yaml: []const u8, created_at_ms: i64, flags: u16, out: *Result) persistence_mod.ApplyError!void {
        return self.applyDirect(.workflow_create, flags, ns_key, yaml, created_at_ms, out);
    }

    /// For tests: a committed enable (1) or disable (0), as `replayCreate`.
    pub fn replayEnabled(self: *WorkflowHandler, ns_key: []const u8, value: []const u8, stamp_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        return self.applyDirect(.workflow_enabled, 0, ns_key, value, stamp_ms, out);
    }

    fn applyDirect(self: *WorkflowHandler, t: entry_mod.EntryType, flags: u16, ns_key: []const u8, value: []const u8, stamp_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        if (ns_key.len < KEY_PREFIX) return error.Halt;
        const cmd: entry_mod.CommandPayload = .{ .namespace_id = keyNamespace(ns_key), .key_length = @intCast(ns_key.len), .value_length = @intCast(value.len), .key = ns_key, .value = value };
        const buf = try self.allocator.alloc(u8, cmd.serializedSize());
        defer self.allocator.free(buf);
        _ = cmd.serialize(buf).?;
        self.direct_index += 1;
        const e = entry_mod.buildEntry(t, flags, 1, self.direct_index, @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms, buf);
        out.reset(&e);
        return self.replayEntry(&e, out);
    }

    /// For tests: a committed start of run `ns_key`, as `replayCreate`.
    pub fn replayStart(self: *WorkflowHandler, ns_key: []const u8, value: []const u8, stamp_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        return self.applyDirect(.workflow_start, 0, ns_key, value, stamp_ms, out);
    }

    /// For tests: a committed completion of run `ns_key`.
    pub fn replayComplete(self: *WorkflowHandler, ns_key: []const u8, value: []const u8, stamp_ms: i64, out: *Result) persistence_mod.ApplyError!void {
        return self.applyDirect(.workflow_complete, 0, ns_key, value, stamp_ms, out);
    }

    /// After a workflow entry applied, on every replica, live and at boot
    /// replay alike (`PartitionReplay`). A definition's schedule and trigger
    /// follow its record, in log order. A run's entry keeps `to_step`, the
    /// producers' fired slot and cursor, and this node's working copy of the
    /// run, if it holds one, as `workflow.run` now says.
    pub fn entryApplied(self: *WorkflowHandler, e: *const entry_mod.Entry, out: *const Result) void {
        if (out.refused() != null) return;
        const t = std.enums.fromInt(entry_mod.EntryType, e.header.entry_type) orelse return;
        switch (t) {
            .workflow_create, .workflow_enabled, .workflow_start, .workflow_step, .workflow_signal, .workflow_complete => {},
            else => return,
        }
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
        switch (t) {
            .workflow_create, .workflow_enabled => self.syncProducers(cmd.key),
            else => self.runApplied(t, cmd.key),
        }
    }

    fn runApplied(self: *WorkflowHandler, t: entry_mod.EntryType, run_key: []const u8) void {
        const run = runs_machine.runOf(self.defsRead(), run_key) orelse return;
        const live = !appliedStatus(run).isTerminal();
        if (live and run.wait == .none) self.indexToStep(run_key) else self.unindexToStep(run_key);
        if (live and run.wait == .call) self.indexKey(&self.parked, run_key) else unindexKey(self, &self.parked, run_key);
        if (live and (run.wait == .timer or (run.wait == .signal and run.wait_due_ms > 0))) self.armTimer(run_key, run.wait_due_ms);
        if (t == .workflow_start) {
            self.followFired(keyNamespace(run_key), run.workflow);
            self.followTrigger(keyNamespace(run_key), run.workflow, run.idempotency_key);
        }
    }

    fn indexKey(self: *WorkflowHandler, map: *std.StringHashMapUnmanaged(void), run_key: []const u8) void {
        if (map.contains(run_key)) return;
        const owned = self.allocator.dupe(u8, run_key) catch return self.lostIndex(run_key);
        map.put(self.allocator, owned, {}) catch {
            self.allocator.free(owned);
            self.lostIndex(run_key);
        };
    }

    fn unindexKey(self: *WorkflowHandler, map: *std.StringHashMapUnmanaged(void), run_key: []const u8) void {
        if (map.fetchRemove(run_key)) |kv| self.allocator.free(kv.key);
    }

    fn lostIndex(self: *WorkflowHandler, run_key: []const u8) void {
        log.err("workflow run {f}: not indexed for its next step (out of memory); indexed again from the log", .{shown(run_key)});
        self.rewatch = true;
    }

    /// A timer for run `run_key` at `at_ms`: when it comes due, the run's
    /// wait as applied says whether it still stands.
    fn armTimer(self: *WorkflowHandler, run_key: []const u8, at_ms: i64) void {
        const owned = self.allocator.dupe(u8, run_key) catch return self.lostIndex(run_key);
        self.timers.push(self.allocator, .{ .at_ms = at_ms, .ns_key = owned }) catch {
            self.allocator.free(owned);
            self.lostIndex(run_key);
        };
    }

    /// Every index rebuilt from the runs as applied, after one couldn't
    /// keep an entry.
    fn rewatchRuns(self: *WorkflowHandler) void {
        self.rewatch = false;
        var it = self.defsRead().range(runs_machine.RunFam, "");
        while (it.next() catch null) |item| self.runApplied(.workflow_step, item.key);
    }

    fn indexToStep(self: *WorkflowHandler, run_key: []const u8) void {
        if (self.to_step.contains(run_key)) return;
        const owned = self.allocator.dupe(u8, run_key) catch return self.lostToStep(run_key);
        self.to_step.put(self.allocator, owned, {}) catch {
            self.allocator.free(owned);
            self.lostToStep(run_key);
        };
    }

    fn lostToStep(_: *WorkflowHandler, run_key: []const u8) void {
        log.err("workflow run {f} started but cannot be indexed for its first step (out of memory); it stays at its start", .{shown(run_key)});
    }

    fn unindexToStep(self: *WorkflowHandler, run_key: []const u8) void {
        if (self.to_step.fetchRemove(run_key)) |kv| self.allocator.free(kv.key);
    }

    /// A schedule's slots fired, as the starts applied so far moved them
    /// (`workflow.per_def`), so a new leader fires only later slots.
    fn followFired(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8) void {
        const def_key = self.nsKey(ns, wf_name) orelse return;
        defer self.allocator.free(def_key);
        const per = runs_machine.perDefOf(self.defsRead(), def_key) orelse return;
        if (self.schedules.getPtr(def_key)) |schedule| {
            if (per.fired_ms != 0) schedule.fired_ms = @max(schedule.fired_ms, per.fired_ms);
        }
    }

    /// A trigger's cursor moved to the event a start applied here came from
    /// (its key, "trigger:<stream>@<ts>:<seq>"), when that is the stream the
    /// trigger reads now. Runs apply in log order after their definition,
    /// so the cursor sits at the last event that started a run; a
    /// redefinition onto another stream reads it from the start.
    fn followTrigger(self: *WorkflowHandler, ns: NamespaceId, wf_name: []const u8, idem: []const u8) void {
        if (!std.mem.startsWith(u8, idem, "trigger:")) return;
        const at = std.mem.lastIndexOfScalar(u8, idem, '@') orelse return;
        const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
        if (colon <= at) return;
        const ts = std.fmt.parseInt(u64, idem[at + 1 .. colon], 10) catch return;
        const seq = std.fmt.parseInt(u64, idem[colon + 1 ..], 10) catch return;
        const def_key = self.nsKey(ns, wf_name) orelse return;
        defer self.allocator.free(def_key);
        const trigger = self.stream_triggers.getPtr(def_key) orelse return;
        if (!std.mem.eql(u8, idem["trigger:".len..at], trigger.stream_name_owned)) return;
        const event = StreamID{ .timestamp_ms = ts, .sequence = seq };
        const cursor = StreamID{ .timestamp_ms = trigger.stream_cursor_ts, .sequence = trigger.stream_cursor_seq };
        if (event.greaterThan(cursor)) {
            trigger.stream_cursor_ts = ts;
            trigger.stream_cursor_seq = seq;
        }
    }

    fn appliedStatus(r: runs_machine.Run.Value) RunStatus {
        return std.enums.fromInt(RunStatus, r.status) orelse .failed;
    }

    /// Run `run_key` as `workflow.run` holds it, or null.
    pub fn appliedRun(self: *const WorkflowHandler, run_key: []const u8) ?runs_machine.Run.Value {
        return runs_machine.runOf(self.defsRead(), run_key);
    }

    /// The id of the run an idempotency key started for this workflow in
    /// this namespace, if one has applied (`workflow.idem`).
    fn runForIdempotencyKey(self: *const WorkflowHandler, ns: NamespaceId, wf_name: []const u8, key: []const u8) ?[]const u8 {
        const k = runs_machine.idemKey(self.allocator, ns, wf_name, key) catch return null;
        defer self.allocator.free(k);
        const got = (self.defsRead().get(runs_machine.IdemFam, k) catch return null) orelse return null;
        return got.v.run;
    }

    fn freeHistory(a: Allocator, history: *std.ArrayList(HistoryEvent)) void {
        for (history.items) |evt| {
            a.free(evt.event_type_owned);
            a.free(evt.detail_owned);
        }
        history.deinit(a);
    }

    fn copyRun(self: *WorkflowHandler, run_key: []const u8, rec: runs_machine.Run.Value) !void {
        return self.copyRunInto(&self.runs, run_key, rec);
    }

    /// For tests: run `run_key` ended `status` by a step this leader logs.
    pub fn endRunForTests(self: *WorkflowHandler, shard: *Shard, run_key: []const u8, status: RunStatus, detail: []const u8) void {
        const run = self.takeRun(shard, run_key) orelse return;
        self.completeRun(shard, run_key, run, status, detail, 0);
        self.commitRun(shard, run_key);
    }

    /// For tests: run `run_key` as a decision would take it, built from
    /// what is applied. Read only; good until the handler's deinit.
    pub fn inspect(self: *WorkflowHandler, run_key: []const u8) ?*const RunRecord {
        const rec = runs_machine.runOf(self.defsRead(), run_key) orelse return null;
        var one = std.StringHashMap(RunRecord).init(self.allocator);
        defer one.deinit();
        self.copyRunInto(&one, run_key, rec) catch return null;
        var it = one.iterator();
        const kv = it.next().?;
        const copy = self.allocator.create(RunRecord) catch {
            self.allocator.free(kv.key_ptr.*);
            self.freeRunRecord(kv.value_ptr);
            return null;
        };
        copy.* = kv.value_ptr.*;
        self.inspected.append(self.allocator, copy) catch {
            self.allocator.free(kv.key_ptr.*);
            self.freeRunRecord(copy);
            self.allocator.destroy(copy);
            return null;
        };
        return copy;
    }

    fn copyRunInto(self: *WorkflowHandler, into: *std.StringHashMap(RunRecord), run_key: []const u8, rec: runs_machine.Run.Value) !void {
        const a = self.allocator;
        var run = RunRecord{
            .run_id_owned = "",
            .workflow_name_owned = "",
            .workflow_version_owned = "",
            .status = appliedStatus(rec),
            .input_owned = "",
            .created_at_ms = rec.created_at_ms,
            .started_at_ms = rec.created_at_ms,
            .completed_at_ms = if (rec.completed_at_ms != 0) rec.completed_at_ms else null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
        };
        errdefer self.freeRunRecord(&run);
        run.run_id_owned = try a.dupe(u8, keyName(run_key));
        run.workflow_name_owned = try a.dupe(u8, rec.workflow);
        run.workflow_version_owned = try a.dupe(u8, rec.version);
        run.input_owned = try a.dupe(u8, rec.input);
        if (rec.idempotency_key.len > 0) run.idempotency_key_owned = try a.dupe(u8, rec.idempotency_key);
        if (rec.has_output != 0) run.output_owned = try a.dupe(u8, rec.output);
        if (rec.search_tags.len > 0) run.search_tags_owned = try a.dupe(u8, rec.search_tags);

        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const read = self.defsRead();
        const prefix = try runs_machine.runRange(scratch.allocator(), keyNamespace(run_key), keyName(run_key));
        var hit = read.range(runs_machine.HistFam, prefix);
        while (try hit.next()) |item| {
            const ev = item.value.v;
            const event_type = try a.dupe(u8, ev.event);
            errdefer a.free(event_type);
            const detail = try a.dupe(u8, ev.detail);
            errdefer a.free(detail);
            try run.history.append(a, .{ .event_type_owned = event_type, .detail_owned = detail, .timestamp_ms = ev.at_ms });
        }
        var oit = read.range(runs_machine.OutFam, prefix);
        while (try oit.next()) |item| {
            if (run.step_outputs == null) run.step_outputs = StepOutputMap.init();
            try run.step_outputs.?.put(a, item.key[prefix.len..], item.value.v.output, item.value.v.outcome);
        }

        run.hist_logged = run.history.items.len;
        run.rev = rec.rev;
        if (run.step_outputs == null) run.step_outputs = StepOutputMap.init();
        if (rec.step.len > 0) run.current_step_name_owned = try a.dupe(u8, rec.step);
        switch (rec.wait) {
            .none => {},
            .call => {
                run.pending_action_run_id_owned = try a.dupe(u8, rec.wait_on);
                run.took_call = try a.dupe(u8, rec.wait_on);
                run.pending_step_name_owned = try a.dupe(u8, if (rec.step.len > 0) rec.step else "start");
                run.pending_call_kind = rec.wait_kind;
                run.pending_action_slot = rec.wait_slot;
            },
            .signal => {
                run.wait_signal_type_owned = try a.dupe(u8, rec.wait_on);
                run.wait_timeout_at_ms = rec.wait_due_ms;
                if (rec.wait_timeout_to.len > 0) run.wait_timeout_target_owned = try a.dupe(u8, rec.wait_timeout_to);
            },
            .timer => run.poll_next_at_ms = rec.wait_due_ms,
        }
        const p = progress_mod.Progress.decode(rec.progress) catch |err| {
            // Ended at once by its leader, saying why: never read.
            run.ending = .failed;
            run.ending_detail_owned = try a.dupe(u8, progress_mod.Progress.why(err));
            return keepCopy(self, into, run_key, &run);
        };
        run.retry_count = p.retry_count;
        run.poll_attempt = p.poll_attempt;
        run.waiting_for_room = p.waiting_for_room;
        run.call_unregistered_noted = p.call_unregistered_noted;
        run.call_retry_at_ms = p.call_retry_at_ms;
        run.call_retry_index = p.call_retry_index;
        run.plan_executor_idx = p.plan_executor_idx;
        run.plan_executor_natural_idx = p.plan_executor_natural_idx;
        run.plan_executor_retry_count = p.plan_executor_retry_count;
        run.plan_order = p.plan_order;
        run.plan_order_len = p.plan_order_len;
        return keepCopy(self, into, run_key, &run);
    }

    fn keepCopy(self: *WorkflowHandler, into: *std.StringHashMap(RunRecord), run_key: []const u8, run: *RunRecord) !void {
        const owned_key = try self.allocator.dupe(u8, run_key);
        errdefer self.allocator.free(owned_key);
        run.ns_key = owned_key;
        try into.put(owned_key, run.*);
    }

    /// The step counters `run` holds, as a step logs them.
    fn progressOf(run: *const RunRecord) progress_mod.Progress {
        return .{
            .retry_count = run.retry_count,
            .poll_attempt = run.poll_attempt,
            .waiting_for_room = run.waiting_for_room,
            .call_unregistered_noted = run.call_unregistered_noted,
            .call_retry_at_ms = run.call_retry_at_ms,
            .call_retry_index = run.call_retry_index,
            .plan_executor_idx = run.plan_executor_idx,
            .plan_executor_natural_idx = run.plan_executor_natural_idx,
            .plan_executor_retry_count = run.plan_executor_retry_count,
            .plan_order = run.plan_order,
            .plan_order_len = run.plan_order_len,
        };
    }

    /// `def_key`'s schedule and stream trigger as its record says: built,
    /// rebuilt after a redefinition (keeping the slots fired and the
    /// cursor, `registerSchedule`/`registerStreamTrigger`), or dropped
    /// when the definition has none. Node memory on every replica; only a
    /// ready leader fires them (`Shard.tick` runs producers only when it `leads`).
    fn syncProducers(self: *WorkflowHandler, def_key: []const u8) void {
        const d = self.defOf(def_key) orelse return;
        const ns = keyNamespace(def_key);
        const name = keyName(def_key);
        const built = self.producers_built.get(def_key);
        if (built == null or built.? != d.created_index) {
            var def = self.parseStored(d.yaml, def_key) catch return;
            defer def.deinit(self.allocator);
            if (def.trigger) |named| {
                var trigger = named;
                if (d.own_namespace) trigger.namespace = null;
                self.registerStreamTrigger(ns, name, trigger);
            } else self.unregisterStreamTrigger(ns, name);
            if (def.schedule) |schedule| self.registerSchedule(ns, name, schedule, d.anchor_ms) else self.unregisterSchedule(ns, name);
            self.noteBuilt(def_key, d.created_index);
        }
        // An enable after a disable moves the anchor without a create.
        if (self.schedules.getPtr(def_key)) |schedule| schedule.anchor_ms = d.anchor_ms;
        // The slots the starts applied so far fired.
        self.followFired(ns, name);
    }

    fn noteBuilt(self: *WorkflowHandler, def_key: []const u8, index: u64) void {
        if (self.producers_built.getPtr(def_key)) |v| {
            v.* = index;
            return;
        }
        const owned = self.allocator.dupe(u8, def_key) catch return;
        self.producers_built.put(self.allocator, owned, index) catch self.allocator.free(owned);
    }

    pub fn definitionCount(self: *const WorkflowHandler) usize {
        return self.defsRead().store.families[defs_machine.DefFam.ID].count;
    }

    pub fn runCount(self: *const WorkflowHandler) usize {
        return self.defsRead().store.families[runs_machine.RunFam.ID].count;
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

test "workflow handler: a create entry saying its trigger names its own namespace reads the own stream where the table lacks the name" {
    const allocator = testing.allocator;
    // Another shard's handler, before shard 0 publishes anything: as at
    // replay during boot.
    var namespaces = try NamespaceHandler.initFor(allocator, 1, null, null);
    defer namespaces.deinit();
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    handler.namespaces = &namespaces;
    const head = "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\",\"batch_size\":4";
    const tail = "},\"start\":{\"run\":\"@actions/a\",\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}";
    var out: Result = .{};
    const key = handler.nsKey(TEST_NS_A, "wf").?;
    defer allocator.free(key);

    try handler.replayCreate(testKey(TEST_NS_A, "wf"), head ++ tail, 1, 0, &out);
    handler.stream_triggers.getPtr(key).?.stream_cursor_ts = 1700;
    // Named, as the workflow's own: the entry says so, and the cursor stays.
    try handler.replayCreate(testKey(TEST_NS_A, "wf"), head ++ ",\"namespace\":\"team-a\"" ++ tail, 2, entry_mod.Flags.OWN_NAMESPACE, &out);
    try testing.expect(handler.stream_triggers.getPtr(key).?.stream_namespace_owned == null);
    try testing.expectEqual(@as(u64, 1700), handler.stream_triggers.getPtr(key).?.stream_cursor_ts);
    // Named, and not its own: another stream, read from its start.
    try handler.replayCreate(testKey(TEST_NS_A, "wf"), head ++ ",\"namespace\":\"team-b\"" ++ tail, 3, 0, &out);
    try testing.expectEqualStrings("team-b", handler.stream_triggers.getPtr(key).?.stream_namespace_owned.?);
    try testing.expectEqual(@as(u64, 0), handler.stream_triggers.getPtr(key).?.stream_cursor_ts);
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

/// Applies what's committed, then takes in this shard's messages to
/// itself, as its self-channel does, until none are left: a step's call,
/// its run's start, the start's answer and the run's end.
fn settleTest(shard: *Shard) void {
    while (true) {
        _ = shard.applyCommitted();
        if (!deliverToSelf(shard)) return;
    }
}

var test_inbound_index: u64 = 1 << 40;

fn deliverToSelf(shard: *Shard) bool {
    const channels = @import("../contract/channels.zig");
    const ce = @import("../protocol/channel_entries.zig");
    const part = shard.defaultPartition();
    const read: contract_view.ReadView = .{ .store = &part.store };
    const next = channels.metaOf(read, 0).next_seq;
    const first = (if (channels.inOf(read, 0)) |i| i.applied else 0) + 1;
    if (first >= next) return false;
    var mb: [128 * 1024]u8 = undefined;
    var w: wire_schema.Writer = .{ .buf = &mb };
    var seq = first;
    while (seq < next) : (seq += 1) {
        var kb: [channels.OUT_KEY]u8 = undefined;
        const o = (read.get(channels.OutFam, channels.outKey(&kb, 0, seq)) catch unreachable).?.v;
        ce.Message.encode(&w, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload }) catch unreachable;
    }
    var ib: [130 * 1024]u8 = undefined;
    const inbound = ce.Inbound.encodeInto(&ib, .{ .from = 0, .first_seq = first, .messages = .{ .bytes = w.written(), .count = @intCast(next - first) } }) catch unreachable;
    test_inbound_index += 1;
    var h = std.mem.zeroes(@import("../storage/ual/entry.zig").Header);
    h.entry_type = @backingInt(@import("../storage/ual/entry.zig").EntryType.channel_inbound);
    h.index = test_inbound_index;
    h.timestamp_ns = @intCast(@max(@import("stdx").time.milliTimestamp(), 0) *| std.time.ns_per_ms);
    h.payload_len = @intCast(inbound.len);
    const e: @import("../storage/ual/entry.zig").Entry = .{ .header = h, .payload = inbound };
    var out = persistence_mod.Result.start(&e);
    @import("../contract/machine.zig").applyEntry(&@import("../contract/machines.zig").Channels.machine, &part.store, &part.apply_scratch, &e, &out) catch unreachable;
    return true;
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
    settleTest(shard);
}

/// A test action run whose only attempt's lease ran out.
fn timeOutTestAction(shard: *Shard, run_id: []const u8) void {
    const run = deliverTestAction(shard, run_id) orelse return;
    var ib: [test_action_entries.ExpireItem.max_size]u8 = undefined;
    const item = test_action_entries.ExpireItem.encodeInto(&ib, .{ .ns = NamespaceId.default.int(), .action = run.action, .run = run_id, .expiry_ns = run.expiry_ns, .holder = "w", .request_key = null }) catch unreachable;
    applyTestActionEntry(shard, .action_expire, test_action_entries.Expire.Value{ .items = .{ .bytes = item, .count = 1 } }, run.expiry_ns);
    settleTest(shard);
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
    // No disk in this harness: the apply loop's async-flush gap check reads
    // durable_log, so it must be null, not the `undefined` garbage above.
    shard.durable_log = null;
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
    // Its sink bookkeeping sees every KV and stream entry that applies.
    const processing = try std.testing.allocator.create(@import("../processing/handler.zig").ProcessingHandler);
    processing.* = @import("../processing/handler.zig").ProcessingHandler.init(std.testing.allocator);
    shard.processing_handler = processing;
    // One group of one: a step's call sends to this shard itself, held to
    // its outbox caps like any send.
    partition.store.place = .{ .group = 0, .count = 1 };
    shard.channel = try @import("../contract/channel_driver.zig").Driver.init(std.testing.allocator, 0, 1);
    shard.outbox_caps = .{};
    return shard;
}

fn destroyTestShard(shard: *Shard) void {
    shard.channel.deinit();
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
    shard.processing_handler.deinit();
    std.testing.allocator.destroy(shard.processing_handler);
}

fn createTestRun(handler: *WorkflowHandler, ns_key: []const u8, run_id: []const u8, wf_name: []const u8) void {
    createTestRunWithInput(handler, ns_key, run_id, wf_name, "{}");
}

/// A running run of `wf_name`, at its start: applied as a shard applies
/// its start.
fn createTestRunWithInput(handler: *WorkflowHandler, ns_key: []const u8, run_id: []const u8, wf_name: []const u8, input: []const u8) void {
    _ = run_id; // the key's
    const buf = handler.allocator.alloc(u8, 64 + wf_name.len + input.len) catch @panic("out of memory");
    defer handler.allocator.free(buf);
    var off: usize = 0;
    for ([_][]const u8{ wf_name, "1.0.0" }) |p| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(p.len), .little);
        @memcpy(buf[off + 2 ..][0..p.len], p);
        off += 2 + p.len;
    }
    buf[off] = @backingInt(WorkflowHandler.RunStatus.running);
    off += 1;
    @memset(buf[off..][0..4], 0); // no event, no idempotency key
    off += 4;
    @memcpy(buf[off..][0..input.len], input);
    off += input.len;
    var out: Result = .{};
    handler.replayStart(ns_key, buf[0..off], 0, &out) catch |err| std.debug.panic("test run not started: {s}", .{@errorName(err)});
    _ = handler.inspect(ns_key) orelse std.debug.panic("test run not started", .{});
}

fn createTestDef(handler: *WorkflowHandler, ns_key: []const u8, name: []const u8, yaml: []const u8) void {
    _ = name; // the key's
    var out: Result = .{};
    handler.replayCreate(ns_key, yaml, 0, 0, &out) catch |err| std.debug.panic("test definition not created: {s}", .{@errorName(err)});
}

test "a producer's start leaves its run to step; its first step is logged, and the run waits on its call once that applies" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // Proposed, not yet applied: there is no run to step.
    try testing.expect(handler.proposeRun(&shard, .default, "run-p", "test-wf", "latest", "{}", null, "schedule_started") != null);
    handler.advanceStartedRuns(&shard);
    try testing.expect(!handler.to_step.contains(testKey(.default, "run-p")));
    try testing.expectEqual(@as(u32, 0), handler.runs.count());

    // Applied: the run it created is one to step.
    settleTest(&shard);
    try testing.expect(handler.to_step.contains(testKey(.default, "run-p")));
    // Its step logged: not taken again while that may apply.
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(?u64, 0), handler.stepping.get(testKey(.default, "run-p")));
    try testing.expectEqual(@as(u32, 0), handler.runs.count());
    // Applied: it waits on its call, on every replica alike.
    settleTest(&shard);
    try testing.expect(!handler.to_step.contains(testKey(.default, "run-p")));
    try testing.expect(handler.parked.contains(testKey(.default, "run-p")));
    const run = handler.inspect(testKey(.default, "run-p")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expectEqual(@as(u64, 1), run.rev);
    try testing.expect(run.pending_action_run_id_owned != null);
}

const test_parent_json =
    \\{"kind":"Workflow","name":"par-wf","version":"1.0.0",
    \\"start":{"run":"@workflow/kid-wf","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

const test_kid_json =
    \\{"kind":"Workflow","name":"kid-wf","version":"1.0.0",
    \\"start":{"run":"@actions/step-a","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
;

/// Takes every pending child's start: the duty proposes it, and its call
/// hears.
fn startTestChildren(handler: *WorkflowHandler, shard: *Shard) void {
    for (0..3) |_| {
        handler.checkPendingActions(shard);
        settleTest(shard);
    }
}

test "a child pinned to a version starts that version, and one pinned to a version its shard doesn't hold is refused, as not found" {
    const allocator = testing.allocator;
    // The child definition is at 1.0.0.
    for ([_][]const u8{ "1.0.0", "9.9.9" }) |pin| {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        var actions = ActionsHandler.init(allocator);
        defer actions.deinit();
        var shard = try createTestShard(&actions);
        defer destroyTestShard(&shard);
        handler.store = &shard.defaultPartition().store;
        shard.workflow_handler = &handler;
        registerTestAction(&shard, "step-a");
        var jb: [512]u8 = undefined;
        const parent_json = try std.fmt.bufPrint(&jb,
            \\{{"kind":"Workflow","name":"par-wf","version":"1.0.0",
            \\"start":{{"run":"@workflow/kid-wf:{s}","transitions":{{"success":"flo.Completed","failure":"flo.Failed","target_not_found":"flo.Failed"}}}}}}
        , .{pin});
        createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", parent_json);
        createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
        createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
        handler.advanceWorkflow(&shard, testKey(.default, "par"));
        settleTest(&shard);
        startTestChildren(&handler, &shard);
        const parent = handler.inspect(testKey(.default, "par")).?.*;
        if (std.mem.eql(u8, pin, "1.0.0")) {
            // The child runs, at its pinned version.
            try testing.expectEqual(WorkflowHandler.RunStatus.waiting, parent.status);
            const kid_key = handler.nsKey(.default, parent.pending_action_run_id_owned.?).?;
            defer allocator.free(kid_key);
            try testing.expectEqualStrings("1.0.0", handler.inspect(kid_key).?.*.workflow_version_owned);
        } else {
            // No run, and the parent's step says which.
            try testing.expectEqual(@as(usize, 1), handler.runCount());
            var said = false;
            for (parent.history.items) |evt| {
                if (std.mem.eql(u8, evt.event_type_owned, "child_not_found") and std.mem.eql(u8, evt.detail_owned, "kid-wf:9.9.9")) said = true;
            }
            try testing.expect(said);
        }
    }
}

test "a child whose run id another run already holds is refused, not proposed for ever, and its parent fails" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    // Before its start, a run already holds the child's id.
    const kid_id = try allocator.dupe(u8, handler.inspect(testKey(.default, "par")).?.*.pending_action_run_id_owned.?);
    defer allocator.free(kid_id);
    const kid_key = handler.nsKey(.default, kid_id).?;
    defer allocator.free(kid_key);
    createTestRun(&handler, kid_key, kid_id, "kid-wf");
    const raft = shard.raft_node;
    const before = raft.log.lastIndex();
    startTestChildren(&handler, &shard);
    var i = before + 1;
    while (i <= raft.log.lastIndex()) : (i += 1) try testing.expect(raft.log.getEntry(i).?.header.entry_type != @backingInt(entry_mod.EntryType.workflow_start));
    try testing.expect(children_mod.parentOf(.{ .store = &shard.defaultPartition().store }, .default, kid_id) == null);
    const parent = handler.inspect(testKey(.default, "par")).?.*;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, parent.status);
    var refused = false;
    for (parent.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "child_refused")) refused = true;
    }
    try testing.expect(refused);
}

test "a pending child's start is proposed once while it may apply, and a second start of its run applies nothing" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    // The child's start is pending; two passes propose it once.
    const raft = shard.raft_node;
    const before = raft.log.lastIndex();
    handler.checkPendingActions(&shard);
    handler.checkPendingActions(&shard);
    var starts: usize = 0;
    var i = before + 1;
    while (i <= raft.log.lastIndex()) : (i += 1) {
        if (raft.log.getEntry(i).?.header.entry_type == @backingInt(entry_mod.EntryType.workflow_start)) starts += 1;
    }
    try testing.expectEqual(@as(usize, 1), starts);
    // A new term's leader proposes it again before the first applied: the
    // second start of the same run applies nothing.
    handler.child_starting_term = 0;
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqual(@as(usize, 2), handler.runCount());
    const parent = handler.inspect(testKey(.default, "par")).?.*;
    try testing.expectEqual(calls_mod.State.started, WorkflowHandler.callOf(&shard, &parent, parent.pending_action_run_id_owned.?).?.state);
}

test "a parent's child is called, started by the child's shard, and its end told back: the parent resumes once the end commits, not when it is proposed" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    startTestChildren(&handler, &shard);
    const parent = handler.inspect(testKey(.default, "par")).?.*;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, parent.status);
    try testing.expectEqual(ae.CallKind.workflow, parent.pending_call_kind);
    const kid_id = parent.pending_action_run_id_owned.?;
    try testing.expectEqual(calls_mod.State.started, WorkflowHandler.callOf(&shard, &parent, kid_id).?.state);
    // The child ran its step; its end is proposed, not applied.
    const kid_key = handler.nsKey(.default, kid_id).?;
    defer allocator.free(kid_key);
    testEndProposed(&handler, &shard, kid_key, .completed, "flo.Completed");
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "par")).?.*.status);
    // Applied: its end reaches the call, and the parent resumes and ends.
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const resumed = handler.inspect(testKey(.default, "par")).?.*;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, resumed.status);
    var completed = false;
    for (resumed.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "child_completed")) completed = true;
    }
    try testing.expect(completed);
    // Its parent record is gone.
    try testing.expect(children_mod.parentOf(.{ .store = &shard.defaultPartition().store }, .default, kid_id) == null);
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-1"), "run-1", "test-wf");

    try testing.expectEqual(@as(usize, 1), handler.runCount());

    // Phase 1: advance invokes step-a asynchronously, workflow parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-1"));
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-a with "success"
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes step-a, advances to step-b (parks again)
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "success"
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 3: checkPendingActions resumes step-b, workflow completes
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-1")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "init");

    createTestDef(&handler, testKey(.default, "wait-wf"), "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, testKey(.default, "run-2"), "run-2", "wait-wf");

    // Phase 1: advance invokes init action, parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-2"));
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-2")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete init with success so workflow transitions to wait_approval
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Phase 2: checkPendingActions resumes init, workflow transitions to wait_approval (parks for signal)
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-2")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "init");

    createTestDef(&handler, testKey(.default, "wait-wf"), "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, testKey(.default, "run-3"), "run-3", "wait-wf");

    // Advance until init parks, then complete it
    handler.advanceWorkflow(&shard, testKey(.default, "run-3"));
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-3")).?;
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }
    // Resume init → transitions to wait_approval → parks for signal
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-3")).?.*;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    }

    // A client's signal of the type it waits for: logged with the step it
    // resumes, following the success transition to flo.Completed.
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    const value = [_]u8{ 8, 0 } ++ "approval".*;
    // Logged: the run wakes when it applies, and takes its next step.
    try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "run-3", &value)) != null);
    settleTest(&shard);
    handler.advanceStartedRuns(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-3")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;

    // No definition registered
    createTestRun(&handler, testKey(.default, "run-4"), "run-4", "nonexistent-wf");

    // No definition to step by: it fails, saying so, rather than waiting
    // for good.
    handler.advanceWorkflow(&shard, testKey(.default, "run-4"));
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-4")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    try testing.expectEqualStrings("workflow definition not found", run.history.items[run.history.items.len - 1].detail_owned);
}

test "a call whose action isn't registered is asked for again after 5 s, doubling to a minute" {
    try testing.expectEqual(@as(u64, 5_000), WorkflowHandler.callRetryMs(1));
    try testing.expectEqual(@as(u64, 10_000), WorkflowHandler.callRetryMs(2));
    try testing.expectEqual(@as(u64, 40_000), WorkflowHandler.callRetryMs(4));
    try testing.expectEqual(@as(u64, 60_000), WorkflowHandler.callRetryMs(5));
    try testing.expectEqual(@as(u64, 60_000), WorkflowHandler.callRetryMs(400));
}

test "a leader drops the calls whose workflow run has ended, and keeps a waiting run's" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    actions.missing_action_wait_ms = 3_600_000;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    // Two runs waiting on calls of an action not registered; one then ends,
    // its call left behind.
    var callees: [2][]const u8 = undefined;
    for ([_][]const u8{ "run-live", "run-ended" }, &callees) |id, *callee| {
        var kb: [32]u8 = undefined;
        const key = writeKey(&kb, .default, id);
        createTestRun(&handler, key, id, "test-wf");
        handler.advanceWorkflow(&shard, key);
        settleTest(&shard);
        callee.* = try allocator.dupe(u8, handler.inspect(key).?.pending_action_run_id_owned.?);
    }
    defer for (callees) |c| allocator.free(c);
    testEnd(&handler, &shard, testKey(.default, "run-ended"), .cancelled, "stop");
    const read: contract_view.ReadView = .{ .store = &shard.defaultPartition().store };
    const slot = calls_mod.callerSlot(.default, "test-wf");
    try testing.expect(calls_mod.callOf(read, .default, slot, callees[1]) != null);

    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expect(calls_mod.callOf(read, .default, slot, callees[0]) != null);
    try testing.expect(calls_mod.callOf(read, .default, slot, callees[1]) == null);
}

test "step executor: a step whose action's outbox is at its cap waits for room, and calls once there is" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-1"), "run-1", "test-wf");
    createTestRun(&handler, testKey(.default, "run-2"), "run-2", "test-wf");
    // The first run's call leaves its start in the outbox, untrimmed.
    handler.advanceWorkflow(&shard, testKey(.default, "run-1"));
    settleTest(&shard);
    try testing.expect(handler.inspect(testKey(.default, "run-1")).?.pending_action_run_id_owned != null);
    // One byte a channel: the second's step waits on its timer, its step
    // logged with no call.
    shard.outbox_caps = .{ .shard = 1 << 30, .channel = 1 };
    const before = shard.raft_node.log.lastIndex();
    handler.advanceWorkflow(&shard, testKey(.default, "run-2"));
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    settleTest(&shard);
    const held = handler.inspect(testKey(.default, "run-2")).?;
    try testing.expect(held.waiting_for_room);
    try testing.expect(held.pending_action_run_id_owned == null);
    try testing.expect(held.poll_next_at_ms > 0);
    // Room again: its next try calls.
    shard.outbox_caps = .{};
    handler.advanceWorkflow(&shard, testKey(.default, "run-2"));
    settleTest(&shard);
    try testing.expect(handler.inspect(testKey(.default, "run-2")).?.pending_action_run_id_owned != null);
}

test "step executor: a step whose action isn't registered doesn't give up while it asks again" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-r"), "run-r", "test-wf");
    // A wait already over the moment it's answered.
    actions.missing_action_wait_ms = 1;
    handler.advanceWorkflow(&shard, testKey(.default, "run-r"));
    settleTest(&shard);
    @import("stdx").time.sleep(2 * std.time.ns_per_ms);
    // It asks again, and the retry isn't applied yet: the start may still
    // go out, so the step waits for it rather than give up under it.
    const run = handler.takeRun(&shard, testKey(.default, "run-r")).?;
    const rid = run.pending_action_run_id_owned.?;
    handler.retryCall(&shard, run, rid, WorkflowHandler.callOf(&shard, run, rid).?, @import("stdx").time.milliTimestamp());
    try testing.expect(run.call_retry_index > shard.raft_node.last_applied);
    // Logged with the decision's step, as the duty would.
    handler.commitRun(&shard, testKey(.default, "run-r"));
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "run-r")).?.status);
    // Applied, and answered again (no action): now it gives up.
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.inspect(testKey(.default, "run-r")).?.status);
}

test "step executor: a call asked for again waits while its action's outbox has no room" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-q"), "run-q", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-q"));
    settleTest(&shard);
    const run = handler.takeRun(&shard, testKey(.default, "run-q")).?;
    defer handler.dropRun(testKey(.default, "run-q"));
    const rid = run.pending_action_run_id_owned.?;
    try testing.expectEqual(calls_mod.State.no_action, WorkflowHandler.callOf(&shard, run, rid).?.state);
    // No room: nothing is proposed.
    shard.outbox_caps = .{ .shard = 0, .channel = 1 << 30 };
    const before = shard.raft_node.log.lastIndex();
    handler.retryCall(&shard, run, rid, WorkflowHandler.callOf(&shard, run, rid).?, @import("stdx").time.milliTimestamp());
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(@as(u64, 0), run.call_retry_index);
    // Room: it is.
    shard.outbox_caps = .{};
    handler.retryCall(&shard, run, rid, WorkflowHandler.callOf(&shard, run, rid).?, @import("stdx").time.milliTimestamp());
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-5"), "run-5", "test-wf");

    // With no wait set, the step fails at once.
    actions.missing_action_wait_ms = 0;
    handler.advanceWorkflow(&shard, testKey(.default, "run-5"));
    // Its call applies, and is answered: no such action.
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-5")).?.*;
    // Should fail because action "step-a" is not registered
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
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
        handler.store = &shard.defaultPartition().store;
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
        // Its call waits for an action not registered only when the other
        // executor can't run.
        const raft = shard.raft_node;
        // The call rides the step that makes it.
        const e = raft.log.getEntry(raft.log.lastIndex()).?;
        try testing.expectEqual(@backingInt(entry_mod.EntryType.workflow_step), e.header.entry_type);
        var diag: wire_schema.Diagnostic = .{};
        const step = try runs_machine.Step.decodeAll(entry_mod.CommandPayload.deserialize(e.payload).?.value, &diag);
        try testing.expectEqual(open, (try ae.Call.decodeAll(step.call, &diag)).can_wait);
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // A run that hasn't parked, and one parked and then done with: neither
    // is walked.
    createTestRun(&handler, testKey(.default, "idle"), "idle", "test-wf");
    try testing.expectEqual(@as(u32, 0), handler.parked.count());
    createTestRun(&handler, testKey(.default, "p"), "p", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "p"));
    settleTest(&shard);
    try testing.expectEqual(@as(u32, 1), handler.parked.count());
    testEnd(&handler, &shard, testKey(.default, "p"), .cancelled, "stop");
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    createTestRun(&handler, testKey(.default, "moved"), "moved", "test-wf");
    createTestRun(&handler, testKey(.default, "later"), "later", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    for ([_][]const u8{ testKey(.default, "due"), testKey(.default, "moved"), testKey(.default, "later") }, [_]i64{ now - 1, now - 1, now + 3_600_000 }) |key, at| {
        testSetWait(&handler, &shard, key, .{ .signal = "go", .due = at });
    }
    // Moved on before its timer fired: the timer finds nothing to do.
    testSetWait(&handler, &shard, testKey(.default, "moved"), .{});
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.inspect(testKey(.default, "due")).?.status);
    try testing.expectEqual(WorkflowHandler.RunStatus.running, handler.inspect(testKey(.default, "moved")).?.status);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "later")).?.status);
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);

    // Parked on its first wait, whose timeout goes to a second with none.
    // Two entries come due at once, as when an earlier wait's stays behind.
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "r")).?.status);
    testSetWait(&handler, &shard, testKey(.default, "r"), .{ .signal = "first", .due = 1, .to = "second" });
    handler.armTimer(testKey(.default, "r"), 1);
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const after = handler.inspect(testKey(.default, "r")).?;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());

    // Not due: it stays.
    handler.checkPendingActions(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const later = @import("stdx").time.milliTimestamp() + 3_600_000;
    createTestRun(&handler, testKey(.default, "w"), "w", "test-wf");
    createTestRun(&handler, testKey(.default, "p"), "p", "test-wf");
    testSetWait(&handler, &shard, testKey(.default, "w"), .{ .signal = "go", .due = later });
    testSetWait(&handler, &shard, testKey(.default, "p"), .{ .poll = later });
    // Entries left from earlier waits, due now.
    handler.armTimer(testKey(.default, "w"), 1);
    handler.armTimer(testKey(.default, "p"), 1);

    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "w")).?.status);
    try testing.expectEqual(later, handler.inspect(testKey(.default, "p")).?.poll_next_at_ms);
    // Only the two the runs' waits armed are held.
    try testing.expectEqual(@as(usize, 2), handler.timers.count());
}

test "workflow duty: a signal already there ends a wait, and leaves no deadline or target for the next" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    // The signal the first wait wants, sent before it waits; then what an
    // earlier wait left.
    try testSignal(&handler, testKey(.default, "r"), "first");
    testSetWait(&handler, &shard, testKey(.default, "r"), .{ .signal = "other", .due = 1, .to = "elsewhere" });

    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
    try testing.expectEqual(@as(i64, 0), run.wait_timeout_at_ms);
    try testing.expect(run.wait_timeout_target_owned == null);
}

test "workflow schedule: max_concurrent follows runs starting and finishing, each counted once, per namespace" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
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
    try testing.expectEqual(@as(?u32, 2), activeOf(&handler, testKey(TEST_NS_A, "wf")));
    try testing.expectEqual(@as(?u32, 1), activeOf(&handler, testKey(TEST_NS_B, "wf")));
    // A start applied again creates nothing.
    try handler.replayStart(testKey(TEST_NS_A, "r1"), value[0..off], 1, &out);
    try testing.expectEqual(@as(?u32, 2), activeOf(&handler, testKey(TEST_NS_A, "wf")));

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

    const c: runs_machine.Complete = .{ .status = .cancelled, .output = null, .event = "workflow_cancelled", .detail = "stop", .tags = "", .workflow = "wf" };
    var record_buf: [128]u8 = undefined;
    const record = c.encode(&record_buf).?;
    // Applied twice (a second cancel): one run finished.
    try handler.replayComplete(testKey(TEST_NS_A, "r1"), record, 7, &out);
    try handler.replayComplete(testKey(TEST_NS_A, "r1"), record, 7, &out);
    try testing.expectEqual(@as(?u32, 1), activeOf(&handler, testKey(TEST_NS_A, "wf")));
    // Room now: it fires once.
    try testing.expectEqual(@as(usize, 1), runsOf(&handler, &shard));
}

/// The runs of workflow `def_key` not ended, as applied; null before any started.
fn activeOf(handler: *WorkflowHandler, def_key: []const u8) ?u32 {
    const per = runs_machine.perDefOf(handler.defsRead(), def_key) orelse return null;
    return per.active;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;

    // An entry carries its namespace's id: "a" is created first.
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    const a = shard.namespace_handler.idOf("a").?;
    var def_kb: [16]u8 = undefined;
    createTestDef(&handler, writeKey(&def_kb, a, "wf"), "wf", test_wait_workflow_json);
    var keys: [200][16]u8 = undefined;
    for (&keys, 0..) |*k, i| {
        var id_buf: [8]u8 = undefined;
        const key = writeKey(k, a, try std.fmt.bufPrint(&id_buf, "r{d:0>5}", .{i}));
        var vb: [64]u8 = undefined;
        var out: Result = .{};
        try handler.replayStart(key, startValue(&vb, "wf", "", ""), 1, &out);
    }
    var counts: [5]u64 = undefined;
    for (&counts) |*c| {
        const before = shard.raft_node.log.lastIndex();
        handler.advanceStartedRuns(&shard);
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
    , 1000, 0, &out);
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
    , 1000, 0, &out);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    const fired_before = sched.fired_ms;

    // The leader proposed slot 4000's start; a disable commits first.
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{0}, 4500, &out);
    var buf: [256]u8 = undefined;
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-sched"), startValue(&buf, "wf", "schedule_started", "schedule:16:wf:4000"), 4600, &out);
    try testing.expectEqualStrings("workflow is disabled", out.refused().?.refusal.message);
    try testing.expect(!(handler.appliedRun(testKey(TEST_NS_A, "r-sched")) != null));
    try testing.expectEqual(fired_before, sched.fired_ms);
    try testing.expectEqual(@as(?u32, null), activeOf(&handler, testKey(TEST_NS_A, "wf")));

    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-client"), startValue(&buf, "wf", "workflow_started", ""), 4600, &out);
    try testing.expectEqualStrings("workflow is disabled", out.refused().?.refusal.message);
    try testing.expect(!(handler.appliedRun(testKey(TEST_NS_A, "r-client")) != null));

    // A trigger's start is not held to it: its leader moved the cursor.
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r-trig"), startValue(&buf, "wf", "trigger_started", ""), 4600, &out);
    try testing.expect(out.refused() == null);
    try testing.expect((handler.appliedRun(testKey(TEST_NS_A, "r-trig")) != null));
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
    , 1000, 0, &out);
    var buf: [256]u8 = undefined;
    out = .{};
    try handler.replayStart(testKey(TEST_NS_A, "r1"), startValue(&buf, "wf", "workflow_started", ""), 2000, &out);
    try testing.expectEqualStrings("idempotency key is required for this workflow", out.refused().?.refusal.message);
    try testing.expect(!(handler.appliedRun(testKey(TEST_NS_A, "r1")) != null));
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
    handler.store = &shard.defaultPartition().store;
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
    try testing.expect((handler.appliedRun(testKey(TEST_NS_A, "r1")) != null));
    try testing.expect(!(handler.appliedRun(testKey(TEST_NS_A, "r2")) != null));
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    const key = testKey(.default, "r");
    handler.advanceWorkflow(&shard, key);
    settleTest(&shard);
    testSetWait(&handler, &shard, key, .{ .signal = "first", .due = 1, .to = "second" });

    const raft = shard.raft_node;
    const cancel = try unappliedEntry(&shard);
    handler.noteEnding(key, cancel);
    try testing.expect(handler.endingInFlight(&shard, key));
    // A timer due while it's in flight neither fires nor is lost.
    handler.checkPendingActions(&shard);
    try testing.expect(handler.inspect(key).?.current_step_name_owned == null);
    // The wait's first, far-off timer, and the due one again, a little later.
    try testing.expectEqual(@as(usize, 2), handler.timers.count());

    // Elected again, its entry still in the log: it may yet apply.
    raft.current_term += 1;
    try testing.expect(handler.endingInFlight(&shard, key));
    raft.current_term -= 1;
    // Its entry cut and replaced by another term's.
    var replaced = cancel;
    replaced.term += 1;
    handler.noteEnding(key, replaced);
    try testing.expect(!handler.endingInFlight(&shard, key));
    handler.noteEnding(key, cancel);
    // No longer leading.
    raft.role = .follower;
    try testing.expect(!handler.endingInFlight(&shard, key));
    raft.role = .leader;
    // Its index applied without ending the run.
    settleTest(&shard);
    try testing.expect(!handler.endingInFlight(&shard, key));

    // The timer it kept fires now.
    @import("stdx").time.sleep(@as(u64, WorkflowHandler.ENDING_RECHECK_MS + 20) * std.time.ns_per_ms);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqualStrings("second", handler.inspect(key).?.current_step_name_owned.?);
}

test "workflow cancel: in flight, a finished action and a first step both wait for it" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    createTestRun(&handler, testKey(.default, "parked"), "parked", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "parked"));
    settleTest(&shard);
    const action_run = handler.inspect(testKey(.default, "parked")).?.pending_action_run_id_owned.?;
    completeTestAction(&shard, action_run);
    createTestRun(&handler, testKey(.default, "started"), "started", "test-wf");

    const cancel = try unappliedEntry(&shard);
    for ([_][]const u8{ testKey(.default, "parked"), testKey(.default, "started") }) |key| handler.noteEnding(key, cancel);
    handler.advanceStartedRuns(&shard);
    handler.checkPendingActions(&shard);
    // Not resumed: still at its first step, on the same action run.
    const parked = handler.inspect(testKey(.default, "parked")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, parked.status);
    try testing.expect(parked.current_step_name_owned == null);
    try testing.expectEqualStrings(action_run, parked.pending_action_run_id_owned.?);
    // Not stepped: at its start.
    try testing.expectEqual(@as(u64, 0), handler.inspect(testKey(.default, "started")).?.rev);
    try testing.expectEqual(@as(u32, 0), handler.stepping.count());
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
    try testing.expect(handler.isDisabled(testKey(.default, "test-wf")));
    _ = try apply(&handler, &index, testKey(.default, "test-wf"), false);
    try testing.expect(handler.isDisabled(testKey(.default, "test-wf")));
    _ = try apply(&handler, &index, testKey(.default, "test-wf"), true);
    try testing.expect(!handler.isDisabled(testKey(.default, "test-wf")));
    const gone = try apply(&handler, &index, testKey(.default, "no-such"), false);
    try testing.expectEqual(proto.StatusCode.not_found, gone.refused().?.status);
    try testing.expect(handler.defOf(testKey(.default, "no-such")) == null);
}

test "step executor: failing action follows failure transition" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b"); // step-b will fail

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-6"), "run-6", "test-wf");

    // Phase 1: advance invokes step-a, parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-6"));
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-6")).?;
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }
    // Phase 2: step-a completes (success) → transitions to step_b → step-b parks
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-6")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b with "failure"
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "failure");
    }
    // Phase 3: step-b fails → follows "failure" transition → flo.Failed
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-6")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-e"), "run-e", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-e"));
    settleTest(&shard);
    // Both steps' workers complete with an empty outcome, as a plain
    // complete sends.
    for (0..2) |_| {
        const run = handler.inspect(testKey(.default, "run-e")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "");
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, handler.inspect(testKey(.default, "run-e")).?.*.status);
}

test "step executor: retry on failure" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "flaky"); // always fails

    createTestDef(&handler, testKey(.default, "retry-wf"), "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, testKey(.default, "run-7"), "run-7", "retry-wf");

    // Initial attempt: flaky action parks
    handler.advanceWorkflow(&shard, testKey(.default, "run-7"));
    settleTest(&shard);

    // Drive attempts: 1 original + 3 retries (max_attempts=3) = 4 total
    // Each iteration: complete current action with "failure", checkPendingActions
    // (which either retries→re-parks, or exhausts retries→flo.Failed)
    var attempt: u32 = 0;
    while (attempt < 5) : (attempt += 1) {
        const run_ptr = handler.inspect(testKey(.default, "run-7")).?;
        if (run_ptr.status.isTerminal()) break;
        const rid = run_ptr.pending_action_run_id_owned orelse break;
        completeTestActionWith(&shard, rid, "failure");
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }

    const run = handler.inspect(testKey(.default, "run-7")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "stripe");
    registerTestAction(&shard, "paypal");

    createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", test_plan_exhaust_workflow_json);
    createTestRun(&handler, testKey(.default, "run-plan"), "run-plan", "plan-wf");

    handler.advanceWorkflow(&shard, testKey(.default, "run-plan"));
    settleTest(&shard);

    // Each executor runs once and retries once: four tasks in all.
    var tasks: u32 = 0;
    while (tasks < 40) : (tasks += 1) {
        const run_ptr = handler.inspect(testKey(.default, "run-plan")).?;
        if (run_ptr.status.isTerminal()) break;
        const rid = run_ptr.pending_action_run_id_owned orelse break;
        completeTestActionWith(&shard, rid, "failure");
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }

    const run = handler.inspect(testKey(.default, "run-plan")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "flaky");

    createTestDef(&handler, testKey(.default, "retry-wf"), "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, testKey(.default, "run-to"), "run-to", "retry-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-to"));
    settleTest(&shard);

    // Each attempt's action run times out; the step's retries then fail it.
    var attempts: u32 = 0;
    while (attempts < 6) : (attempts += 1) {
        const run = handler.inspect(testKey(.default, "run-to")).?;
        if (run.status.isTerminal()) break;
        const rid = run.pending_action_run_id_owned orelse break;
        timeOutTestAction(&shard, rid);
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.inspect(testKey(.default, "run-to")).?.*.status);
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "poller");

    createTestDef(&handler, testKey(.default, "poll-wf"), "poll-wf", test_poll_workflow_json);
    createTestRun(&handler, testKey(.default, "run-poll"), "run-poll", "poll-wf");

    // Initial: poller invoked, parks for async completion.
    handler.advanceWorkflow(&shard, testKey(.default, "run-poll"));
    settleTest(&shard);

    // First `pending` must ARM a poll (not fail): status waiting, attempt incremented.
    {
        const run = handler.inspect(testKey(.default, "run-poll")).?;
        try testing.expect(run.pending_action_run_id_owned != null);
        completeTestActionWith(&shard, run.pending_action_run_id_owned.?, "pending");
    }
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    {
        const run = handler.inspect(testKey(.default, "run-poll")).?;
        try testing.expect(!run.status.isTerminal()); // pre-fix bug: would be .failed
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        try testing.expectEqual(@as(u32, 1), run.poll_attempt);
    }

    // Drive remaining poll cycles: each tick either fires the due poll timer
    // (re-invoking the action) or processes the re-invoked action's `pending`
    // completion (re-arming). max_attempts=2 → exhausted → `timeout` → flo.TimedOut.
    var i: u32 = 0;
    while (i < 12) : (i += 1) {
        const run = handler.inspect(testKey(.default, "run-poll")).?;
        if (run.status.isTerminal()) break;
        if (run.pending_action_run_id_owned) |rid| {
            completeTestActionWith(&shard, rid, "pending");
        }
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }

    const run = handler.inspect(testKey(.default, "run-poll")).?.*;
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
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;

    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // A run parked on its first step's action, whose run then completes.
    registerTestAction(&shard, "step-a");
    registerTestAction(&shard, "step-b");
    createTestRun(&handler, testKey(.default, "run-8"), "run-8", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "run-8"));
    settleTest(&shard);
    completeTestAction(&shard, handler.inspect(testKey(.default, "run-8")).?.pending_action_run_id_owned.?);

    // checkPendingActions should detect the completed action and resume step-a
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    // After step-a resumes, workflow advances to step_b (async) — parks again
    {
        const run = handler.inspect(testKey(.default, "run-8")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
        // Complete step-b so the workflow can finish
        completeTestAction(&shard, run.pending_action_run_id_owned.?);
    }

    // Second checkPendingActions: step-b done → flo.Completed
    handler.checkPendingActions(&shard);
    settleTest(&shard);

    const run = handler.inspect(testKey(.default, "run-8")).?.*;
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
        fa.fail_index = fa.alloc_index + fail_at;
        var out: Result = .{};
        handler.replayStart(testKey(.default, "r1"), &start, 1, &out) catch {
            try testing.expect(fa.has_induced_failure);
            try testing.expectEqual(@as(usize, 0), handler.runCount());
            continue;
        };
        const run = handler.appliedRun(testKey(.default, "r1")) orelse return error.RunMissing;
        try testing.expectEqualStrings("key", run.idempotency_key);
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
    try testing.expectEqual(WorkflowHandler.RunStatus.running, shownOf(&handler, testKey(.default, "r1")).?.status);
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
    try handler.replayCreate(testKey(.default, "wf"), yaml, 1, 0, &out);
    try testing.expectError(error.Halt, handler.replayCreate(testKey(.default, "wf"), "kind: [", 2, 0, &out));
    const def = handler.defOf(testKey(.default, "wf")) orelse return error.DefinitionDropped;
    try testing.expectEqualStrings("1", def.version);
    try testing.expectEqual(@as(usize, 1), handler.definitionCount());
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
        try testing.expectEqual(stamp_ms, shownOf(&handler, testKey(.default, "r1")).?.created_at_ms);
        // completed, no output, no steps, no history, no tags, workflow "wf"
        try handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_complete, 0, 1, 2, stamp_ns + 9 * std.time.ns_per_ms, .default, testKey(.default, "r1"), &[_]u8{ 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 'w', 'f' }, &pbuf).?, &out);
        const run = shownOf(&handler, testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
        try testing.expectEqual(@as(?i64, stamp_ms + 9), run.completed_at_ms);
    }
}

test "workflow replay: a complete entry cut short leaves the run as it was, and a whole one ends it" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    // Its start records one event.
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 7, 0, 's', 't', 'a', 'r', 't', 'e', 'd', 0, 0 };
    var start_out: Result = .{};
    try handler.replayStart(testKey(.default, "r1"), &start, 1, &start_out);

    // completed, output "ok", last event "e"/"d", tags "t", workflow "wf"
    const whole = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++ [_]u8{ 1, 0, 'e', 1, 0, 'd' } ++ [_]u8{ 1, 0, 't' } ++ [_]u8{ 2, 0, 'w', 'f' };

    var events: std.ArrayListUnmanaged(WorkflowHandler.HistoryEvent) = .empty;
    defer events.deinit(allocator);
    try handler.appliedHistory(testKey(.default, "r1"), &events);
    try testing.expectEqual(@as(usize, 1), events.items.len);
    // Every cut is skipped whole.
    for (1..whole.len) |n| {
        try testing.expectError(error.Halt, handler.replayComplete(testKey(.default, "r1"), whole[0..n], 2, &replay_out_for_tests));
        const run = shownOf(&handler, testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.running, run.status);
        try testing.expect(run.output_owned == null);
        events.clearRetainingCapacity();
        try handler.appliedHistory(testKey(.default, "r1"), &events);
        try testing.expectEqualStrings("started", events.items[0].event_type_owned);
    }

    try handler.replayComplete(testKey(.default, "r1"), &whole, 2, &replay_out_for_tests);
    const run = shownOf(&handler, testKey(.default, "r1")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqualStrings("ok", run.output_owned.?);
    try testing.expectEqualStrings("t", run.search_tags_owned.?);
    // Its last event follows the history it had.
    events.clearRetainingCapacity();
    try handler.appliedHistory(testKey(.default, "r1"), &events);
    try testing.expectEqual(@as(usize, 2), events.items.len);
    try testing.expectEqualStrings("e", events.items[1].event_type_owned);
}

test "workflow replay: a cancel logged after a completion is refused and changes nothing, on every replica; a second cancel after a cancel succeeds" {
    // A completion a deposed leader logged commits ahead of a cancel the
    // next leader took while that completion hadn't applied there.
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0 };
    const wf = [_]u8{ 2, 0, 'w', 'f' };
    const completed = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ wf;
    const cancelled = [_]u8{5} ++ [_]u8{0} ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ wf;

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
        const run = shownOf(&handler, testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
        try testing.expectEqualStrings("ok", run.output_owned.?);
        try testing.expectEqual(@as(?i64, 2), run.completed_at_ms);
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
    try testing.expectEqual(@as(?i64, 2), shownOf(&handler, testKey(.default, "r1")).?.completed_at_ms);
}

/// The answer a handler queued on `conn`.
fn queuedAnswer(conn: *Connection) !proto.Response {
    return proto.Response.parse(conn.write_buf.peek());
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

test "cancelling a run that already finished is refused and keeps its result" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    testEnd(&handler, &shard, testKey(.default, "r"), .completed, "flo.Completed");
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, handler.inspect(testKey(.default, "r")).?.status);
    const events = handler.inspect(testKey(.default, "r")).?.history.items.len;

    try testing.expect(handler.handleCancel(&shard, &conn, makeRequest(.workflow_cancel, "r", "")) == null);
    const answer = try queuedAnswer(&conn);
    try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
    try testing.expectEqualStrings("run already finished", (try answer.refused()).message);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqual(events, run.history.items.len);
}

test "an engine-reached end with a detail past the cancel-reason cap is trimmed, not a crash" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");

    const detail = @as([2000]u8, @splat('t'));
    testEnd(&handler, &shard, testKey(.default, "big"), .cancelled, &detail);
    const run = handler.inspect(testKey(.default, "big")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.cancelled, run.status);
    const last = run.history.items[run.history.items.len - 1].detail_owned;
    try testing.expectEqualStrings(detail[0..WorkflowHandler.MAX_CANCEL_REASON], last);
}

test "a wait that times out with no target ends the run through the log, and only a leader logs it" {
    const allocator = testing.allocator;
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    for ([_]bool{ false, true }, [_][]const u8{ testKey(.default, "w0"), testKey(.default, "w1") }) |leading, key| {
        createTestRun(&handler, key, keyName(key), "test-wf");
        testSetWait(&handler, &shard, key, .{ .signal = "go", .due = 1 });
        if (!leading) shard.raft_node.role = .follower;
        handler.checkPendingActions(&shard);
        shard.raft_node.role = .leader;
        settleTest(&shard);
        const status = handler.inspect(key).?.status;
        if (leading) {
            try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, status);
        } else {
            // Not logged, so not applied.
            try testing.expect(!status.isTerminal());
        }
    }
}

test "a client signal with a reserved type is refused and doesn't move a run parked on its call; another is taken" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "init");
    createTestDef(&handler, testKey(.default, "wait-wf"), "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "wait-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    const callee = run.pending_action_run_id_owned.?;
    const events = run.history.items.len;

    var sig_buf: [128]u8 = undefined;
    const forged = try std.fmt.bufPrint(&sig_buf, "_action_done:{s}", .{callee});
    for ([_][]const u8{ forged, "_x" }) |sig| {
        var conn = try Connection.init(allocator, -1, 0, 0);
        defer conn.deinit();
        var value: [130]u8 = undefined;
        std.mem.writeInt(u16, value[0..2], @intCast(sig.len), .little);
        @memcpy(value[2..][0..sig.len], sig);
        try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", value[0 .. 2 + sig.len])) == null);
        const answer = try queuedAnswer(&conn);
        try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
        try testing.expectEqualStrings("reserved signal type: a leading '_' is the system's", (try answer.refused()).message);
    }
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const after = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, after.status);
    try testing.expectEqualStrings(callee, after.pending_action_run_id_owned.?);
    try testing.expect(!runs_machine.hasSignal(handler.defsRead(), testKey(.default, "r"), forged));
    try testing.expectEqual(events, after.history.items.len);

    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    const value = [_]u8{ 8, 0 } ++ "approval".*;
    try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", &value)) != null);
    settleTest(&shard);
    try testing.expect(runs_machine.hasSignal(handler.defsRead(), testKey(.default, "r"), "approval"));
    // A signal to a run waiting on a call wakes nothing: it waits on its call.
    try testing.expectEqualStrings(callee, handler.inspect(testKey(.default, "r")).?.pending_action_run_id_owned.?);
}

test "a step's call at the most an entry holds is logged with it, and one a byte over fails the step, saying so" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);

    // What a call takes besides its input, from a small one.
    const small = "{}";
    createTestRunWithInput(&handler, testKey(.default, "r0"), "r0", "test-wf", small);
    handler.advanceWorkflow(&shard, testKey(.default, "r0"));
    const e = shard.raft_node.log.getEntry(shard.raft_node.log.lastIndex()).?;
    var diag: wire_schema.Diagnostic = .{};
    const step = try runs_machine.Step.decodeAll(entry_mod.CommandPayload.deserialize(e.payload).?.value, &diag);
    const overhead = step.call.len - small.len;
    settleTest(&shard);

    const input = try allocator.alloc(u8, WorkflowHandler.MAX_LOGGED_CALL - overhead + 1);
    defer allocator.free(input);
    @memset(input, 'x');
    // At the bound: logged, and the run waits on its call.
    createTestRunWithInput(&handler, testKey(.default, "r1"), "r1", "test-wf", input[0 .. input.len - 1]);
    handler.advanceWorkflow(&shard, testKey(.default, "r1"));
    settleTest(&shard);
    try testing.expect(handler.inspect(testKey(.default, "r1")).?.pending_action_run_id_owned != null);
    // A byte over: the step fails as an execution failure, saying why.
    createTestRunWithInput(&handler, testKey(.default, "r2"), "r2", "test-wf", input);
    handler.advanceWorkflow(&shard, testKey(.default, "r2"));
    settleTest(&shard);
    const over = handler.inspect(testKey(.default, "r2")).?;
    try testing.expect(over.pending_action_run_id_owned == null);
    var said = false;
    for (over.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "step_call_too_large") and std.mem.eql(u8, evt.detail_owned, "input too large to log")) said = true;
    }
    try testing.expect(said);
}

test "a step's logged counters that won't decode end the run failed, saying why, once" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    // A step whose counters are of a version this build doesn't read.
    const step: runs_machine.Step.Value = .{
        .rev = 0,
        .step = "",
        .events = .{ .bytes = "", .count = 0 },
        .outputs = .{ .bytes = "", .count = 0 },
        .wait = .none,
        .wait_kind = .action,
        .wait_slot = 0,
        .wait_on = "",
        .wait_due_ms = 0,
        .wait_timeout_to = "",
        .progress = &.{ progress_mod.VERSION + 1, 0 },
        .settles = "",
        .caller_slot = calls_mod.callerSlot(.default, "test-wf"),
        .workflow = "test-wf",
        .ends = 0,
        .has_output = false,
        .output = "",
        .tags = "",
        .call = "",
    };
    var sb: [512]u8 = undefined;
    var out: Result = .{};
    try handler.applyDirect(.workflow_step, 0, testKey(.default, "r"), try runs_machine.Step.encodeInto(&sb, step), 1, &out);

    handler.advanceStartedRuns(&shard);
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    try testing.expectEqualStrings(progress_mod.Progress.why(error.WrongVersion), run.history.items[run.history.items.len - 1].detail_owned);
    // Ended: nothing more is taken.
    const last = shard.raft_node.log.lastIndex();
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(last, shard.raft_node.log.lastIndex());
}

test "a signal that wakes a run is the log's: a leader with none of the old one's memory takes the run on" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);
    try testing.expectEqualStrings("first", handler.inspect(testKey(.default, "r")).?.wait_signal_type_owned.?);
    try testSignal(&handler, testKey(.default, "r"), "first");

    var fresh = WorkflowHandler.init(allocator);
    defer fresh.deinit();
    fresh.store = handler.store;
    shard.workflow_handler = &fresh;
    defer shard.workflow_handler = &handler;
    fresh.rewatch = true;
    fresh.advanceStartedRuns(&shard);
    settleTest(&shard);
    const run = fresh.inspect(testKey(.default, "r")).?;
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
    try testing.expectEqualStrings("second", run.wait_signal_type_owned.?);
}

test "a signal to a finished run is refused and leaves its history alone" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    testEnd(&handler, &shard, testKey(.default, "r"), .completed, "flo.Completed");
    const events = handler.inspect(testKey(.default, "r")).?.history.items.len;

    const value = [_]u8{ 2, 0 } ++ "go".*;
    try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", &value)) == null);
    const answer = try queuedAnswer(&conn);
    try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
    try testing.expectEqualStrings("run already finished", (try answer.refused()).message);
    try testing.expectEqual(events, handler.inspect(testKey(.default, "r")).?.history.items.len);
    try testing.expect(!runs_machine.hasSignal(handler.defsRead(), testKey(.default, "r"), "go"));
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
    _ = streams.appendForTest("default", "events", &blob);

    const all = streams.readPayloadsForStream("events", "default", StreamID.MIN, 100);
    defer allocator.free(all.payloads);
    defer allocator.free(all.ids);
    try testing.expectEqual(@as(usize, 5), all.payloads.len);

    // Replay restored the run record "b" started: its id is the cursor.
    var idem_buf: [96]u8 = undefined;
    const idem = try std.fmt.bufPrint(&idem_buf, "trigger:events@{d}:{d}", .{ all.ids[1].timestamp_ms, all.ids[1].sequence });
    var start_buf: [160]u8 = undefined;
    var out: Result = .{};
    try handler.replayStart(testKey(TEST_NS, "b"), startValue(&start_buf, "wf", "trigger_started", idem), 1, &out);
    const t = handler.stream_triggers.getPtr(testKey(TEST_NS, "wf")).?;
    const cursor = StreamID{ .timestamp_ms = t.stream_cursor_ts, .sequence = t.stream_cursor_seq };
    try testing.expect(cursor.eql(all.ids[1]));

    // The next poll reads c, d, e: the rest of that append, nothing skipped.
    const rest = streams.readPayloadsForStream("events", "default", cursor, 100);
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
    handler.store = &shard.defaultPartition().store;
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
        settleTest(&shard);
        const want: WorkflowHandler.RunStatus = if (n <= json_depth.MAX_DEPTH) .waiting else .failed;
        try testing.expectEqual(want, handler.inspect(ns_key).?.*.status);
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
        const tags = shownOf(&handler, ns_key).?.search_tags_owned.?;
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
    var value: [128]u8 = undefined;
    var out: Result = .{};
    try handler.replayStart(ns_key, startValue(&value, wf_name, "", ""), started_at, &out);
    switch (status) {
        .running, .pending => {},
        // A step that waits on a signal.
        .waiting => {
            const step: runs_machine.Step.Value = .{
                .rev = 0,
                .step = "",
                .events = .{ .bytes = "", .count = 0 },
                .outputs = .{ .bytes = "", .count = 0 },
                .wait = .signal,
                .wait_kind = .action,
                .wait_slot = 0,
                .wait_on = "go",
                .wait_due_ms = 0,
                .wait_timeout_to = "",
                .progress = "",
                .settles = "",
                .caller_slot = calls_mod.callerSlot(.default, wf_name),
                .workflow = wf_name,
                .ends = 0,
                .has_output = false,
                .output = "",
                .tags = "",
                .call = "",
            };
            var sb: [512]u8 = undefined;
            try handler.applyDirect(.workflow_step, 0, ns_key, try runs_machine.Step.encodeInto(&sb, step), started_at, &out);
        },
        else => {
            const c: runs_machine.Complete = .{ .status = status, .output = null, .event = "ended", .detail = "", .tags = "", .workflow = wf_name };
            var cb: [256]u8 = undefined;
            try handler.replayComplete(ns_key, c.encode(&cb).?, started_at, &out);
        },
    }
}

const TestWait = struct {
    signal: ?[]const u8 = null,
    due: i64 = 0,
    to: ?[]const u8 = null,
    poll: i64 = 0,
};

/// Run `key` made to wait as `w` says (nothing, for none), by a step its
/// leader logs, applied.
fn testSetWait(handler: *WorkflowHandler, shard: *Shard, key: []const u8, w: TestWait) void {
    const a = handler.allocator;
    const run = handler.takeRun(shard, key).?;
    if (run.wait_signal_type_owned) |old| a.free(old);
    run.wait_signal_type_owned = if (w.signal) |sig| a.dupe(u8, sig) catch unreachable else null;
    if (run.wait_timeout_target_owned) |old| a.free(old);
    run.wait_timeout_target_owned = if (w.to) |t| a.dupe(u8, t) catch unreachable else null;
    run.wait_timeout_at_ms = w.due;
    run.poll_next_at_ms = w.poll;
    handler.commitRun(shard, key);
    settleTest(shard);
}

/// A client's signal of `signal_type` to run `key`, applied.
fn testSignal(handler: *WorkflowHandler, key: []const u8, signal_type: []const u8) !void {
    var buf: [256]u8 = undefined;
    var out: Result = .{};
    try handler.applyDirect(.workflow_signal, 0, key, try runs_machine.Signal.encodeInto(&buf, .{ .type = signal_type, .payload = "" }), 1, &out);
    try testing.expect(out.refused() == null);
}

/// Run `key` ended `status` by a step its leader logs, applied.
fn testEnd(handler: *WorkflowHandler, shard: *Shard, key: []const u8, status: WorkflowHandler.RunStatus, detail: []const u8) void {
    testEndProposed(handler, shard, key, status, detail);
    settleTest(shard);
}

/// `testEnd`'s step, logged and not yet applied.
fn testEndProposed(handler: *WorkflowHandler, shard: *Shard, key: []const u8, status: WorkflowHandler.RunStatus, detail: []const u8) void {
    const run = handler.takeRun(shard, key) orelse return;
    handler.completeRun(shard, key, run, status, detail, 0);
    handler.commitRun(shard, key);
}

/// Run `key` as a read shows it.
fn shownOf(handler: *WorkflowHandler, key: []const u8) ?WorkflowHandler.ShownRun {
    return handler.shownRun(key, keyName(key));
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
        var kb: [64]u8 = undefined;
        const run_id = handler.runForIdempotencyKey(ns, "wf", "k").?;
        try testing.expect(handler.appliedRun(writeKey(&kb, ns, run_id)) != null);
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

test "workflow handler: a shard's thread never takes another shard's workflow handler" {
    // Split so this test's own text doesn't match.
    const src = @embedFile("handler.zig");
    try testing.expect(std.mem.indexOf(u8, src, "]." ++ "workflow_handler") == null);
    try testing.expect(std.mem.indexOf(u8, src, "target_" ++ "handler") == null);
    try testing.expect(std.mem.indexOf(u8, src, "peer_shards) |peers| {\n                    return peers[t.shard_id]." ++ "workflow") == null);
}

test "workflow producers: a redefinition keeps a schedule's fired slot and a trigger's cursor that starts restored" {
    const allocator = testing.allocator;
    const sched_v1 = "kind: Workflow\nname: sched\nversion: 1.0.0\nschedule:\n  interval: 1000\nstart:\n  wait_for_signal:\n    type: never\n  transitions:\n    success: flo.Completed\n";
    const sched_v2 = "kind: Workflow\nname: sched\nversion: 2.0.0\nschedule:\n  interval: 1000\nstart:\n  wait_for_signal:\n    type: never\n  transitions:\n    success: flo.Completed\n";
    const trig_v1 = "{\"kind\":\"Workflow\",\"name\":\"trig\",\"version\":\"1.0.0\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"run\":\"@actions/a\",\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}";
    const trig_v2 = "{\"kind\":\"Workflow\",\"name\":\"trig\",\"version\":\"2.0.0\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"run\":\"@actions/a\",\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}";
    const Step = union(enum) { def: struct { name: []const u8, yaml: []const u8, at: i64 }, start: struct { run: []const u8, wf: []const u8, event: []const u8, idem: []const u8 } };
    const steps = [_]Step{
        .{ .def = .{ .name = "sched", .yaml = sched_v1, .at = 1000 } },
        .{ .def = .{ .name = "trig", .yaml = trig_v1, .at = 1000 } },
        .{ .start = .{ .run = "s1", .wf = "sched", .event = "schedule_started", .idem = "schedule:16:sched:3000" } },
        .{ .start = .{ .run = "t1", .wf = "trig", .event = "trigger_started", .idem = "trigger:events@1700:0" } },
        .{ .def = .{ .name = "sched", .yaml = sched_v2, .at = 5000 } },
        .{ .def = .{ .name = "trig", .yaml = trig_v2, .at = 5000 } },
        .{ .start = .{ .run = "s2", .wf = "sched", .event = "schedule_started", .idem = "schedule:16:sched:6000" } },
        .{ .start = .{ .run = "t2", .wf = "trig", .event = "trigger_started", .idem = "trigger:events@1800:2" } },
    };
    {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        var buf: [256]u8 = undefined;
        inline for (steps) |step| switch (step) {
            .def => |d| {
                var out: Result = .{};
                try handler.replayCreate(testKey(TEST_NS_A, d.name), d.yaml, d.at, 0, &out);
            },
            .start => |st| {
                var out: Result = .{};
                try handler.replayStart(testKey(TEST_NS_A, st.run), startValue(&buf, st.wf, st.event, st.idem), 7000, &out);
                try testing.expect(out.refused() == null);
            },
        };
        const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "sched")).?;
        try testing.expectEqual(@as(i64, 6000), sched.fired_ms);
        try testing.expectEqual(@as(i64, 5000), sched.anchor_ms);
        const trig = handler.stream_triggers.getPtr(testKey(TEST_NS_A, "trig")).?;
        try testing.expectEqual(@as(u64, 1800), trig.stream_cursor_ts);
        try testing.expectEqual(@as(u64, 2), trig.stream_cursor_seq);
    }
}

test "a shard with no walk context lists definitions in the walk's entry form, whether each is enabled included" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    var out: Result = .{};
    try handler.replayEnabled(testKey(.default, "test-wf"), &.{0}, 2, &out);

    handler.handleListDefinitions(&shard, &conn, makeRequest(.workflow_list_definitions, "", ""));
    const answer = try queuedAnswer(&conn);
    const body = answer.data;
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, body[0..4], .little));
    var pos: usize = 4;
    const name_len = std.mem.readInt(u16, body[pos..][0..2], .little);
    try testing.expectEqualStrings("test-wf", body[pos + 2 ..][0..name_len]);
    pos += 2 + name_len;
    pos += 2 + std.mem.readInt(u16, body[pos..][0..2], .little) + 8;
    // Disabled, then the trailer: no more, no cursor.
    try testing.expectEqual(@as(u8, 0), body[pos]);
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0 }, body[pos + 1 ..]);
}
