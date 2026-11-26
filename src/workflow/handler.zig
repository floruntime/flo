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
//! | workflow_history     | run_id            | on the op table (`workflow.history`) |
//! | workflow_list_runs   | workflow_name     | [limit:u32][status_len:u16][status][cursor_len:u16][cursor][search_len:u16][search] |
//! | workflow_get_def     | workflow_name     | version (optional)                  |
//! | workflow_disable     | workflow_name     | version (optional)                  |
//! | workflow_enable      | workflow_name     | version (optional)                  |
//! | workflow_list_defs   | (unused)          | (empty)                             |

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const gate = @import("../budget/gate.zig");
const hq = @import("../budget/history_quota.zig");
const raft_node_mod = @import("../raft/node.zig");
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const system_ops = @import("../protocol/system_ops.zig");
const list_runs = @import("list_runs.zig");
const parser = @import("parser.zig");
const stream_names = @import("../stream/names.zig");

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
const StreamHandler = @import("../stream/handler.zig").StreamHandler;
const history_read = @import("../stream/history_read.zig");
const history_ops = @import("../node/history_ops.zig");
const table_run = @import("../node/table_run.zig");
const ops = @import("../protocol/ops.zig");
const history_machine = @import("../stream/history_machine.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
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
    /// owned: a leader's index of applied state (`leadCaches`), kept as
    /// each run entry applies on it (`entryApplied`). They go in this map's
    /// order, not their starts'.
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
    /// Parked runs whose call may need a step: one whose call record just
    /// changed, whose retry is due, or whose action was registered. Only
    /// these are looked at, so runs whose calls can't move cost nothing
    /// per tick. Keys owned.
    call_dirty: std.StringHashMapUnmanaged(void) = .empty,
    /// Where the last pass over `call_dirty` stopped.
    call_dirty_cursor: u32 = 0,
    /// The earliest timer armed for each parked run's call, by key, owned:
    /// a run looked at again arms none it already has.
    call_timer: std.StringHashMapUnmanaged(i64) = .empty,
    /// Decisions taken (`takeRun`), each a copy built from what applied:
    /// what an idle pass must not grow.
    taken: u64 = 0,
    /// When a run's timer or signal deadline is due, earliest first, keys
    /// owned. An entry is checked against the run's wait as applied when it
    /// comes due, so one the run moved past is dropped then.
    timers: std.PriorityQueue(Timer, void, timerCompare) = .empty,
    /// Runs whose timer came due while their step was held, each with its
    /// hold, keys owned; and how many runs each hold holds. Each tick asks
    /// every hold once, not every run, and a hold that clears puts its runs
    /// back on `timers`, due then: so many runs held in one namespace take
    /// no other run's turn at the 64 due timers a pass handles. A run
    /// leaves when any entry of its own applies (its wait as applied arms
    /// what it needs) or the hold clears, so only live runs are held, once
    /// each. Leader memory (`clearCaches`).
    held: std.StringHashMapUnmanaged(Held) = .empty,
    /// Each hold's runs, keys borrowed from `held`: a hold that clears walks
    /// its own list alone, so no pass scans the whole held set, and one
    /// namespace's flapping hold costs another's nothing.
    held_by: std.AutoHashMapUnmanaged(Hold, std.ArrayListUnmanaged([]const u8)) = .empty,
    /// The namespaces whose quota may have eased since the last release
    /// pass (`Store.Eased`): only their quota holds are asked, so a pass
    /// with nothing eased asks none, and ordinary history traffic, which
    /// eases nothing, wakes no hold. Leader memory (`clearHeld`).
    quota_eased: @import("../contract/store.zig").Eased = .{},
    /// How many times in a row each run's timer step was refused at apply,
    /// keys owned: its recheck backs off, so a step refused the same way
    /// every time costs a log entry a minute, not a second. Cleared when an
    /// entry of the run's own applies. Leader memory (`clearCaches`).
    refusals: std.StringHashMapUnmanaged(u8) = .empty,
    /// An index couldn't keep an entry: the runs are walked again
    /// (`leadCaches`).
    rewatch: bool = false,
    /// Index rows a run list passed over (a Flo bug): its run gone, or its
    /// entry unlistable. Each kind is logged once, not every page.
    missing_runs: u64 = 0,
    missing_runs_logged: u32 = 0,
    unlistable_said: bool = false,
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
    /// The schedules and triggers this shard last counted held.
    memory_held: u64 = 0,
    /// The same for the namespace's run-history quota.
    quota_held: u64 = 0,
    /// Holds said in the log, one per episode.
    holds_said: u64 = 0,

    /// The shard's contract state, where the definitions machine keeps each
    /// workflow (`defs_machine.zig`), keyed `writeKey`: namespace id ‖ name.
    store: ?*Store = null,
    /// The shard's stream handler, which reads a run's history from the
    /// rows the stream machine keeps and the log they point into.
    history: ?*StreamHandler = null,
    /// A store of its own, for a handler built without a shard (tests).
    own_store: Store,
    /// What a definition applied here directly (`replayEntry`) stages in.
    def_scratch: std.heap.ArenaAllocator,
    /// The create index each definition's schedule and trigger were last
    /// built from (`syncProducers`), by definition key. A leader's cache.
    producers_built: std.StringHashMapUnmanaged(u64) = .empty,
    /// The term this node built its leader caches in (`leadCaches`); 0
    /// while it holds none.
    caches_term: u64 = 0,
    /// The store walk that builds the caches, a chunk a tick
    /// (`stepWalk`): the definitions, then the runs.
    walk: Walk = .idle,
    /// The key the walk goes on after; empty for the first chunk.
    walk_after: std.ArrayListUnmanaged(u8) = .empty,
    /// Whether the runs are walked after the definitions: not when only
    /// the definitions are read again (`producers_lost`).
    walk_runs_next: bool = false,
    /// The term's first walk finished. Until then the leader steps no run
    /// and fires no trigger or schedule: what it hasn't read yet it would
    /// pass over, or fire again.
    caches_built: bool = false,
    /// A definition's trigger or schedule couldn't be built (out of
    /// memory): the definitions are walked again.
    producers_lost: bool = false,

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

    /// Plan executor health, by (namespace id, workflow, plan, executor)
    /// (`healthKey`): health-weighted selection orders by it, and each
    /// executor's breaker is kept in it. Changed only by a step that logs
    /// (`applyPlanEffects`).
    plan_health: std.StringHashMap(plan_types.ExecutorHealth),
    /// Each plan's passes this leader has started (`healthKey` with no
    /// executor): where round-robin starts the next, and what random's
    /// shuffle is drawn from.
    plan_cursors: std.StringHashMap(u32),
    /// The term `plan_health` and `plan_cursors` were kept in: both are
    /// this leader's memory, and a new term starts without them, so a node
    /// leading again doesn't act on what it saw before (`planMemory`).
    selection_term: u64 = 0,

    /// The longest workflow name: its runs' history names it as its owner.
    pub const MAX_WORKFLOW_NAME_LEN: usize = 256;
    comptime {
        std.debug.assert(MAX_WORKFLOW_NAME_LEN == @import("../protocol/history_entries.zig").MAX_OWNER);
    }
    /// The longest key a create or start entry carries.
    pub const MAX_START_KEY: usize = 600;
    /// The longest run id a start may choose: its key is the namespace's id
    /// and the run id.
    pub const MAX_RUN_ID: usize = MAX_START_KEY - KEY_PREFIX;
    comptime {
        std.debug.assert(MAX_RUN_ID == runs_machine.MAX_RUN_ID);
    }

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
        /// Its incarnation, as applied: a step names it, so one decided for
        /// a run since deleted, its id started again, applies to nothing.
        created_ns: u64,
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
        /// Its step's call is held by its action's shard's memory share:
        /// said once in its history, however long the hold.
        call_over_budget_noted: bool = false,
        call_over_quota_noted: bool = false,
        /// When this leader may next ask again for that call, and the index
        /// of the last time it did: until that applies, the step waits on
        /// it, so it can't give up as the start goes out again.
        call_retry_at_ms: i64 = 0,
        call_retry_index: u64 = 0,

        /// Retry attempt counter for the current step (reset on step transition).
        retry_count: u32 = 0,

        /// Index of the current executor within a plan (reset on step transition).
        plan_executor_idx: u8 = 0,

        /// Natural index into plan.executors[] for the active executor:
        /// plan_executor_idx is its position in the pass's `plan_order`.
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
        /// The signal types this decision's waits took, in order, owned:
        /// each the oldest of its type the run holds.
        takes: std.ArrayListUnmanaged([]u8) = .empty,
        /// The call this decision makes (an encoded `ae.Call`), logged with
        /// the step that waits on it.
        call_payload: ?[]const u8 = null,
        /// What this decision does to the leader's plan memory: kept until
        /// its step logs (`commitRun`), since one not logged is decided
        /// again and would count the same result, or start the same pass,
        /// twice.
        plan_effects: std.ArrayListUnmanaged(PlanEffect) = .empty,
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
        /// How far this leader has read in `proposed_term`, past starts it
        /// proposed that may not apply: a step-down can drop them from the
        /// log, so another term reads from the applied cursor again, and
        /// the starts made twice are one run each by their idempotency keys.
        /// `chain` is the event of the last start proposed, which the next
        /// start must find logged (`runs_machine.TriggerAt`): `at` can pass
        /// events no start was made for. Leader memory.
        proposed: ?struct { at: StreamID, term: u64, chain: StreamID } = null,
        /// A refused start of this leader's re-reads from the place logged:
        /// the log's last index then. The starts of the chain it broke are
        /// all at or below it, and their refusals change nothing more.
        resync_index: u64 = 0,
        /// Kept with `proposed` and `resync_index`, as one.
        hold: MemoryHold = .{},
        quota_hold: QuotaHold = .{},
        last_poll_ms: i64,
        /// A read of a stream another shard owns, in flight (0 for none):
        /// its number and the term it was asked in. At most one at a time.
        read_seq: u64 = 0,
        read_term: u64 = 0,
        read_sent_ms: i64 = 0,
        next_read_seq: u64 = 0,
        /// The namespace the read in flight was asked in.
        read_ns: NamespaceId = .invalid,
        /// Which stream the cursor is in (the stamp that made it), once a
        /// read or the logged place has said; a read of one made since
        /// starts the cursor over.
        stream_made_ns: ?u64 = null,
    };

    /// The stream a trigger's read read: its namespace, its made stamp and
    /// its name. A start from it carries it (`runs_machine.TriggerAt`).
    const ReadStream = struct { ns: NamespaceId, made: u64, name: []const u8 };

    /// Where the logged place (`workflow.per_def`) has a trigger in the
    /// stream named `name` in `ns`, made at `made`: (0, 0) when it is in
    /// another stream or none.
    fn appliedCursor(self: *const WorkflowHandler, def_key: []const u8, ns: NamespaceId, name: []const u8, made: ?u64) StreamID {
        const zero: StreamID = .{ .timestamp_ms = 0, .sequence = 0 };
        const per = runs_machine.perDefOf(self.defsRead(), def_key) orelse return zero;
        if (per.trig_ns != ns.int() or !std.mem.eql(u8, per.trig_stream, name) or made == null or per.trig_made != made.?) return zero;
        return .{ .timestamp_ms = per.trig_ts, .sequence = per.trig_seq };
    }

    /// Where a trigger reads from in `term`, in the stream in `ns`: past
    /// what this leader proposed in that term, else from the logged place.
    fn readFrom(self: *const WorkflowHandler, def_key: []const u8, trigger: *const StreamTriggerState, ns: NamespaceId, term: u64) StreamID {
        const applied = self.appliedCursor(def_key, ns, trigger.stream_name_owned, trigger.stream_made_ns);
        const p = trigger.proposed orelse return applied;
        if (p.term != term or applied.greaterThan(p.at)) return applied;
        return p.at;
    }

    /// The place the next start of a read `readFrom` began must find
    /// logged: the last start proposed in that read's run of them, else
    /// where the read began.
    fn chainFrom(self: *const WorkflowHandler, def_key: []const u8, trigger: *const StreamTriggerState, ns: NamespaceId, made: u64, term: u64) StreamID {
        const applied = self.appliedCursor(def_key, ns, trigger.stream_name_owned, made);
        const p = trigger.proposed orelse return applied;
        if (p.term != term or applied.greaterThan(p.at)) return applied;
        return p.chain;
    }

    /// The trigger's cursor in its stream in `ns`, as the logged place has
    /// it: what a read goes on from. For tests.
    pub fn triggerCursor(self: *WorkflowHandler, def_key: []const u8, ns: NamespaceId) StreamID {
        const t = self.stream_triggers.getPtr(def_key) orelse return .{ .timestamp_ms = 0, .sequence = 0 };
        self.seedMade(def_key, t, ns);
        return self.appliedCursor(def_key, ns, t.stream_name_owned, t.stream_made_ns);
    }

    /// What holds a schedule's or trigger's starts: its shard's memory
    /// share, or its namespace's run-history quota on the shard.
    pub const Bound = enum { memory, quota };

    /// A schedule's or trigger's starts held to one bound. At it, it
    /// proposes none. After a start refused there it waits before the next,
    /// each wait twice the last (`callRetryMs` for memory, `quotaRetryMs`
    /// for the quota): the bound is only checked before proposing, and a
    /// start just under it is still refused at apply. A start of it
    /// admitted, or a new term, begins again. Leader memory.
    pub fn StartHold(comptime bound: Bound) type {
        return struct {
            const Self = @This();
            term: u64 = 0,
            refusals: u32 = 0,
            until_ms: i64 = std.math.minInt(i64),
            /// The figure it is held at (the share, or the namespace's
            /// part), at it or waiting after a refusal there; null while it
            /// isn't held.
            share: ?u64 = null,
            /// Said in the log until then: at most once a minute, however
            /// often a shard hovering at the bound holds it, and across a
            /// new term.
            said_until_ms: i64 = std.math.minInt(i64),
            /// The quota's hold lasts until an operator acts: said once an
            /// episode, from its first hold until it clears or a run starts.
            said: bool = false,

            const SAY_EVERY_MS: i64 = 60_000;

            /// Begun again, in `term`: what was said stands.
            fn reset(h: *Self, term: u64) void {
                h.* = .{ .term = term, .said_until_ms = h.said_until_ms };
            }

            /// Whether it waits out a refusal now, in `term`.
            fn waiting(h: *Self, term: u64, now_ms: i64) bool {
                if (h.term != term) h.reset(term);
                return now_ms < h.until_ms;
            }

            /// Held at `share`; true when it is to be said.
            fn held(h: *Self, share: u64, now_ms: i64) bool {
                h.share = share;
                if (bound == .quota) {
                    if (h.said) return false;
                    h.said = true;
                    return true;
                }
                if (now_ms < h.said_until_ms) return false;
                h.said_until_ms = now_ms +| SAY_EVERY_MS;
                return true;
            }

            /// A start of it refused at the bound in `term`: it waits.
            fn refused(h: *Self, term: u64, now_ms: i64, share: u64) bool {
                if (h.term != term) h.reset(term);
                const first = h.held(share, now_ms);
                h.refusals +|= 1;
                const wait = switch (bound) {
                    .memory => callRetryMs(h.refusals),
                    .quota => quotaRetryMs(h.refusals),
                };
                h.until_ms = now_ms +| @as(i64, @intCast(wait));
                return first;
            }

            /// Not at the bound now; a wait still stands until a run starts.
            fn clear(h: *Self) void {
                h.share = null;
                h.said = false;
            }
        };
    }

    pub const MemoryHold = StartHold(.memory);
    pub const QuotaHold = StartHold(.quota);

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
        hold: MemoryHold = .{},
        quota_hold: QuotaHold = .{},
    };

    const Timer = struct { at_ms: i64, ns_key: []const u8 };
    /// What holds a run's step: its namespace's share of this shard's
    /// history lane (`lane`), its parent's outbox (`parent`, at
    /// `parent_shard`), or its namespace's run-history quota (`quota`).
    pub const Hold = struct {
        ns: NamespaceId,
        kind: enum { lane, parent, quota } = .lane,
        parent_shard: ?u16 = null,
    };
    /// A held run's hold, and where its key is in that hold's list.
    const Held = struct { hold: Hold, at: u32 };
    const Ending = struct { index: u64, term: u64 };

    fn timerCompare(_: void, a: Timer, b: Timer) std.math.Order {
        return std.math.order(a.at_ms, b.at_ms);
    }

    pub fn init(allocator: Allocator) WorkflowHandler {
        var own = Store.init(allocator);
        // Built without a shard, it is a group of one: a run's history goes
        // to its own outbox.
        own.place = .{ .group = 0, .count = 1 };
        return .{
            .allocator = allocator,
            .own_store = own,
            .def_scratch = .init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
            .stream_triggers = std.StringHashMap(StreamTriggerState).init(allocator),
            .schedules = std.StringHashMap(ScheduleState).init(allocator),
            .plan_health = std.StringHashMap(plan_types.ExecutorHealth).init(allocator),
            .plan_cursors = std.StringHashMap(u32).init(allocator),
        };
    }

    pub fn deinit(self: *WorkflowHandler) void {
        self.calls_sweep_after.deinit(self.allocator);
        self.freeChildStarting();
        self.child_starting.deinit(self.allocator);
        self.child_scan_after.deinit(self.allocator);
        self.own_store.deinit();
        self.def_scratch.deinit();
        self.walk_after.deinit(self.allocator);
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
        inline for (.{ &self.parked, &self.call_dirty, &self.call_timer, &self.to_step, &self.ending }) |map| {
            var k = map.keyIterator();
            while (k.next()) |key| self.allocator.free(key.*);
            map.deinit(self.allocator);
        }
        self.clearStepping();
        self.stepping.deinit(self.allocator);
        while (self.timers.pop()) |t| self.allocator.free(t.ns_key);
        self.timers.deinit(self.allocator);
        self.clearHeld();
        self.held.deinit(self.allocator);
        self.held_by.deinit(self.allocator);
        self.refusals.deinit(self.allocator);

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

        self.forgetPlans(null);
        self.plan_health.deinit();
        self.plan_cursors.deinit();
    }

    /// The leader's plan memory for workflow `of` (namespace id and name),
    /// or for every workflow, gone.
    fn forgetPlans(self: *WorkflowHandler, of: ?struct { ns: NamespaceId, workflow: []const u8 }) void {
        var buf: [16 + 24 + MAX_WORKFLOW_NAME_LEN]u8 = undefined;
        // `healthKey` up to the plan's name: no other workflow's keys start so.
        const prefix: []const u8 = if (of) |w|
            std.fmt.bufPrint(&buf, "{s}{d}:{s}", .{ &w.ns.bytes(), w.workflow.len, w.workflow }) catch return
        else
            "";
        if (of == null) {
            var hit = self.plan_health.iterator();
            while (hit.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit(self.allocator);
            }
            self.plan_health.clearRetainingCapacity();
            var cit = self.plan_cursors.keyIterator();
            while (cit.next()) |k| self.allocator.free(k.*);
            self.plan_cursors.clearRetainingCapacity();
            return;
        }
        // One workflow's: a removal moves no other entry (`removeByPtr`
        // leaves a tombstone), so one walk finds them all.
        var hit = self.plan_health.iterator();
        while (hit.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.key_ptr.*, prefix)) continue;
            const key = entry.key_ptr.*;
            entry.value_ptr.deinit(self.allocator);
            self.plan_health.removeByPtr(entry.key_ptr);
            self.allocator.free(key);
        }
        var cit = self.plan_cursors.keyIterator();
        while (cit.next()) |k| {
            if (!std.mem.startsWith(u8, k.*, prefix)) continue;
            const key = k.*;
            self.plan_cursors.removeByPtr(k);
            self.allocator.free(key);
        }
    }

    /// Starts this term's plan memory afresh if it was kept in another.
    fn planMemory(self: *WorkflowHandler, shard: *const Shard) void {
        const term = shard.raft_node.current_term;
        if (self.selection_term == term) return;
        self.forgetPlans(null);
        self.selection_term = term;
    }

    /// A change a decision makes to the leader's plan memory, under its
    /// key (`healthKey`; a pass's has no executor), owned.
    pub const PlanEffect = struct {
        key: []u8,
        what: union(enum) {
            /// An executor's call ended, succeeding or not.
            result: struct { success: bool, breaker: ?plan_types.CircuitBreakerConfig },
            /// A call was made as its half-open breaker's probe.
            probe,
            /// A pass started: the plan's next starts one further along.
            pass,
        },
        at_ms: i64,
    };

    /// `what` kept on `run`'s copy, for when its step logs. Short of memory
    /// it is not kept: a result counted once less, a pass's start not
    /// moved on.
    fn stagePlanEffect(self: *WorkflowHandler, run: *RunRecord, ns: NamespaceId, plan_name: []const u8, executor_name: []const u8, what: @FieldType(PlanEffect, "what"), at_ms: i64) void {
        const key = healthKey(self.allocator, ns, run.workflow_name_owned, plan_name, executor_name) catch return;
        run.plan_effects.append(self.allocator, .{ .key = key, .what = what, .at_ms = at_ms }) catch self.allocator.free(key);
    }

    /// The plan memory `run`'s logged step changes.
    fn applyPlanEffects(self: *WorkflowHandler, shard: *const Shard, run: *RunRecord) void {
        self.planMemory(shard);
        for (run.plan_effects.items) |e| switch (e.what) {
            .pass => {
                const got = self.plan_cursors.getOrPut(e.key) catch continue;
                if (got.found_existing) got.value_ptr.* +%= 1 else {
                    got.key_ptr.* = self.allocator.dupe(u8, e.key) catch {
                        self.plan_cursors.removeByPtr(got.key_ptr);
                        continue;
                    };
                    got.value_ptr.* = 1;
                }
            },
            .result, .probe => applyEffect(self.healthAt(e.key) orelse continue, e),
        };
    }

    fn applyEffect(health: *plan_types.ExecutorHealth, e: PlanEffect) void {
        switch (e.what) {
            .result => |r| {
                health.recordApiAttempt(r.success, 0, e.at_ms);
                if (r.breaker) |cfg| health.updateBreakerState(cfg, r.success, e.at_ms);
            },
            .probe => health.markProbe(e.at_ms),
            .pass => {},
        }
    }

    /// A fresh pass's executor order, by the plan's selection: definition
    /// order; health-weighted, healthiest first; round-robin, starting one
    /// further along each pass this leader logs; random, shuffled from the
    /// shard, term and pass. The order is logged with the run's progress,
    /// so a new leader resumes the pass as it was; only the next pass's
    /// start is this leader's.
    fn orderPass(self: *WorkflowHandler, shard: *const Shard, run: *RunRecord, plan: definition.InlinePlan, ns: NamespaceId, order: []u8) void {
        for (order, 0..) |*slot, i| slot.* = @intCast(i);
        switch (plan.selection) {
            .static_order => {},
            .health_weighted => self.sortExecutorsByHealth(order, plan, ns, run.workflow_name_owned),
            .round_robin, .random => {
                const key = healthKey(self.allocator, ns, run.workflow_name_owned, plan.name, "") catch return;
                defer self.allocator.free(key);
                const pass = self.plan_cursors.get(key) orelse 0;
                self.stagePlanEffect(run, ns, plan.name, "", .pass, 0);
                if (plan.selection == .round_robin) return std.mem.rotate(u8, order, pass % order.len);
                var h = std.hash.Wyhash.init(0);
                var head: [14]u8 = undefined;
                std.mem.writeInt(u64, head[0..8], shard.raft_node.current_term, .little);
                std.mem.writeInt(u16, head[8..10], shard.id, .little);
                std.mem.writeInt(u32, head[10..14], pass, .little);
                h.update(&head);
                h.update(key);
                var prng = std.Random.DefaultPrng.init(h.final());
                prng.random().shuffle(u8, order);
            },
        }
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
        for (run.takes.items) |t| self.allocator.free(t);
        run.takes.deinit(self.allocator);
        for (run.plan_effects.items) |e| self.allocator.free(e.key);
        run.plan_effects.deinit(self.allocator);
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
        dispatcher.registerWithRoute(.workflow_history, table_run.handler("workflow.history", history_ops.Ops, history_ops.Ops.workflowHistory), run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.workflow_run_delete, table_run.handler("workflow.run_delete", RunDelete, RunDelete.run), run_id_mod.preRouteByRunId);
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
        if (etype != .workflow_start and etype != .workflow_step and etype != .workflow_signal and etype != .workflow_complete) return;
        if (shard.raft_node.role != .leader) return;
        // A cancel is counted against the one this leader recorded, of any
        // term it led (it may be elected again with it in its log); the
        // rest only in the term that proposed them.
        if (etype != .workflow_complete and entry.header.term != shard.raft_node.current_term) return;
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        switch (etype) {
            .workflow_signal => {
                if (result.refused() == null) if (shard.metrics_registry) |m| m.workflow.recordSignalDelivered();
                return;
            },
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
                if (self.stepping.fetchRemove(cmd.key)) |kv| {
                    self.allocator.free(kv.key);
                    // Refused, nothing of the run's applied: a timer that
                    // took this step was popped, and is armed again.
                    if (result.refused() != null) self.recheckRefused(cmd.key);
                }
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

        // Its runs' history names it: no longer than that may.
        if (def.name.len > MAX_WORKFLOW_NAME_LEN) {
            var mb: [96]u8 = undefined;
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, std.fmt.bufPrint(&mb, "a workflow name is at most {d} bytes, not {d}", .{ MAX_WORKFLOW_NAME_LEN, def.name.len }) catch "workflow name too long");
            return null;
        }

        // A trigger reads from its stream without passing a client's
        // request check, so the namespace it names is checked here.
        if (def.trigger) |t| if (t.namespace) |ns| if (ns.len > 0) if (@import("../namespace/handler.zig").nameRefusal(ns)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return null;
        };
        // And so is its stream's name. Here, not in the parser: a create
        // is parsed again where it applies, and halts there if it doesn't.
        if (def.trigger) |t| if (stream_names.reservedStreamName(t.stream)) {
            shard.sendRefusal(conn, req.header.request_id, .unauthorized, .{ .reason = .reserved_key, .ran = .no, .message = stream_names.RESERVED });
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
        if (workflow_name.len > MAX_WORKFLOW_NAME_LEN) {
            var mb: [96]u8 = undefined;
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, std.fmt.bufPrint(&mb, "a workflow name is at most {d} bytes, not {d}", .{ MAX_WORKFLOW_NAME_LEN, workflow_name.len }) catch "workflow name too long");
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

        // A leading '_' is the server's (a schedule's or a trigger's start);
        // the applier refuses one on a client's start too.
        if (idempotency_key) |idem_key| if (runs_machine.serverKey(idem_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, runs_machine.SERVER_KEY_REFUSED);
            return null;
        };

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

        // The applier creates the run from the entry. It makes history at
        // once: refused while this shard's history lane has no room for it.
        if (shard.historyWait(req.ns_id)) |w| {
            var wb: [256]u8 = undefined;
            shard.sendRefusal(conn, req.header.request_id, .overloaded, .{ .reason = .unclassified, .ran = .no, .message = Shard.sendWaitText(w, &wb) });
            return null;
        }
        const proposed = self.proposeStart(shard, req.ns_id, run_id_str, workflow_name, version, input, idempotency_key, "workflow_started", null) catch |err| {
            shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "run not persisted"));
            return null;
        };
        return proposed;
    }

    pub fn hasDutyWork(self: *const WorkflowHandler) bool {
        return self.to_step.count() > 0 or self.parked.count() > 0 or self.timers.count() > 0 or self.held.count() > 0 or self.rewatch or self.walk != .idle;
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
    /// decided again by whichever node leads, from what applied, and its
    /// caches go (`clearCaches`): it builds them again when it next leads.
    pub fn dropStartedRuns(self: *WorkflowHandler, shard: *Shard) void {
        _ = shard;
        self.clearStepping();
        var it = self.ending.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.ending.clearRetainingCapacity();
        self.clearCaches();
    }

    /// A leader's caches of applied state: the runs it steps (`to_step`,
    /// `parked`, `call_dirty`, `call_timer`, `timers`) and the definitions'
    /// schedules and stream triggers. Built from the store once this node
    /// leads in `term`, and kept from then on by `entryApplied` as each
    /// entry applies on it. A follower keeps none, and nothing a leader
    /// held outlives its term: every term starts from the store. Called
    /// every tick the node leads: the store is walked a chunk a tick
    /// (`stepWalk`), so a large one doesn't hold up the Raft tick and the
    /// leader's heartbeats with it. An entry that applies mid-walk is seen
    /// by `entryApplied` against the store as it then is; the walk reaching
    /// its key later reads the same, so either order builds the same.
    pub fn leadCaches(self: *WorkflowHandler, term: u64) void {
        if (self.caches_term != term) {
            self.clearCaches();
            self.caches_term = term;
            self.startWalk(.defs, true);
        }
        if (self.walk == .idle) {
            if (self.producers_lost) self.startWalk(.defs, false) else if (self.rewatch) self.startWalk(.runs, false);
        }
        self.stepWalk();
    }

    const Walk = enum { idle, defs, runs };
    /// Most definitions a tick's walk reads: each is parsed.
    const DEFS_PER_TICK: usize = 64;
    /// Most runs a tick's walk reads.
    const RUNS_PER_TICK: usize = 1024;

    fn startWalk(self: *WorkflowHandler, w: Walk, runs_next: bool) void {
        self.walk = w;
        self.walk_runs_next = runs_next;
        self.walk_after.clearRetainingCapacity();
        switch (w) {
            .idle => {},
            .defs => self.producers_lost = false,
            .runs => {
                self.rewatch = false;
                // A call's timer that couldn't be armed may be one of these.
                var ct = self.call_timer.keyIterator();
                while (ct.next()) |k| self.allocator.free(k.*);
                self.call_timer.clearRetainingCapacity();
            },
        }
    }

    fn stepWalk(self: *WorkflowHandler) void {
        switch (self.walk) {
            .idle => {},
            .defs => if (self.walkChunk(defs_machine.DefFam, DEFS_PER_TICK)) {
                if (!self.walk_runs_next) return self.endWalk();
                // The runs' first chunk in the same tick: a small store is
                // built in one.
                self.startWalk(.runs, false);
                self.stepWalk();
            },
            .runs => if (self.walkChunk(runs_machine.RunFam, RUNS_PER_TICK)) self.endWalk(),
        }
    }

    fn endWalk(self: *WorkflowHandler) void {
        self.walk = .idle;
        self.walk_after.clearRetainingCapacity();
        self.caches_built = true;
    }

    /// Up to `max` more of family `F`'s keys into the caches; whether the
    /// walk reached the end. One that can't be read is logged and passed
    /// over, so the rest are still built.
    fn walkChunk(self: *WorkflowHandler, comptime F: type, max: usize) bool {
        const read = self.defsRead();
        var it = if (self.walk_after.items.len == 0) read.range(F, "") else read.rangeAfter(F, "", self.walk_after.items);
        var n: usize = 0;
        while (true) {
            const item = (it.next() catch {
                log.err("a stored workflow {s} could not be read; this leader goes on without it", .{if (F == defs_machine.DefFam) "definition" else "run"});
                continue;
            }) orelse return true;
            if (F == defs_machine.DefFam) self.syncProducers(item.key) else self.runApplied(.workflow_step, item.key);
            n += 1;
            if (n < max) continue;
            self.walk_after.clearRetainingCapacity();
            // Out of memory: this walk starts again from its first key.
            self.walk_after.appendSlice(self.allocator, item.key) catch self.walk_after.clearRetainingCapacity();
            return false;
        }
    }

    /// Every leader cache gone (`leadCaches`).
    fn clearCaches(self: *WorkflowHandler) void {
        self.caches_term = 0;
        self.walk = .idle;
        self.walk_after.clearRetainingCapacity();
        self.caches_built = false;
        self.producers_lost = false;
        inline for (.{ &self.parked, &self.call_dirty, &self.call_timer, &self.to_step }) |map| {
            var k = map.keyIterator();
            while (k.next()) |key| self.allocator.free(key.*);
            map.clearRetainingCapacity();
        }
        self.to_step_cursor = 0;
        self.call_dirty_cursor = 0;
        while (self.timers.pop()) |t| self.allocator.free(t.ns_key);
        self.clearHeld();
        self.rewatch = false;
        var tit = self.stream_triggers.iterator();
        while (tit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeTriggerState(entry.value_ptr);
            _ = global_stream_trigger_count.fetchSub(1, .monotonic);
        }
        self.stream_triggers.clearRetainingCapacity();
        var sit = self.schedules.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeScheduleState(entry.value_ptr);
        }
        self.schedules.clearRetainingCapacity();
        var pk = self.producers_built.keyIterator();
        while (pk.next()) |k| self.allocator.free(k.*);
        self.producers_built.clearRetainingCapacity();
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
        // A signal is said in its run's history.
        if (shard.historyWait(req.ns_id)) |w| {
            var wb: [256]u8 = undefined;
            shard.sendRefusal(conn, req.header.request_id, .overloaded, .{ .reason = .unclassified, .ran = .no, .message = Shard.sendWaitText(w, &wb) });
            return null;
        }
        const run_ns_key = self.nsKey(req.ns_id, run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return null;
        };
        defer self.allocator.free(run_ns_key);
        // What apply refuses, answered at once where this node can tell.
        const applied = self.appliedRun(run_ns_key) orelse {
            shard.sendRefusal(conn, req.header.request_id, .not_found, NO_SUCH_RUN);
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
            shard.sendRefusal(conn, req.header.request_id, .not_found, NO_SUCH_RUN);
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
        const c: runs_machine.Complete = .{ .status = .cancelled, .output = null, .event = terminalEventType(.cancelled), .detail = reason, .workflow = run.workflow, .created_ns = run.created_ns };
        var vbuf: [MAX_CANCEL_REASON + MAX_WORKFLOW_NAME_LEN + 72]u8 = undefined;
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

    /// `workflow.run_delete`: an ended run deleted, its history with it.
    /// Refused here for a live one, and at apply for one that isn't there
    /// by then or is another incarnation than the one read here.
    /// What every op by a run's id answers for a run no shard holds:
    /// status, history, signal, cancel and delete alike.
    pub const NO_SUCH_RUN: proto.Refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such run" };

    const RunDelete = struct {
        pub const Error = error{ NotFound, NotLeading, OutOfMemory };

        pub fn refusal(err: Error) table_run.Refused {
            return switch (err) {
                error.NotFound => .{ .status = .not_found, .refusal = NO_SUCH_RUN },
                error.NotLeading => .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: this node's leadership isn't settled yet — retry" } },
                error.OutOfMemory => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } },
            };
        }

        const Response = table_run.op("workflow.run_delete").response;

        fn run(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(Response) {
            const self = shard.workflow_handler;
            if (!shard.raft_node.leaderReady()) return error.NotLeading;
            if (req.key.len == 0) return error.NotFound;
            const run_key = self.nsKey(req.ns_id, req.key) orelse return error.OutOfMemory;
            defer self.allocator.free(run_key);
            const rec = self.appliedRun(run_key) orelse return error.NotFound;
            if (!appliedStatus(rec).isTerminal()) return .{ .refused = .{ .status = .bad_request, .refusal = .{ .reason = .run_live, .ran = .no, .message = runs_machine.RUN_LIVE } } };
            const d: runs_machine.Delete = .{ .workflow = rec.workflow, .idem = rec.idempotency_key, .created_ns = rec.created_ns };
            const vbuf = try self.allocator.alloc(u8, d.size());
            defer self.allocator.free(vbuf);
            const value = d.encode(vbuf).?;
            const proposed = persistence_mod.proposeEntryIn(shard, .workflow_run_delete, entry_mod.Flags.NONE, req.ns_id, run_key, value) catch |err|
                return .{ .refused = .{ .status = persistence_mod.failureStatus(err), .refusal = persistence_mod.failureRefusal(err, "delete not persisted") } };
            return .{ .park = .{ .proposed = proposed, .answer = answerEmpty } };
        }

        fn answerEmpty(shard: *Shard, conn: *Connection, request_id: u64, _: *const persistence_mod.Result) void {
            table_run.answer(Response, shard, conn, request_id, .{});
        }
    };

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
            shard.sendRefusal(conn, req.header.request_id, .not_found, NO_SUCH_RUN);
            return;
        };

        // Binary wire format:
        // [run_id_len:u16][run_id][workflow_len:u16][workflow][version_len:u16][version]
        // [status:u8][current_step_len:u16][current_step][input_len:u32][input]
        // [created_at:i64][has_started:u8][started_at?:i64][has_completed:u8][completed_at?:i64]
        // [has_wait_signal:u8][wait_signal_len:u16][wait_signal]?
        // [has_output:u8]([output_len:u32][output][output_truncated:u8])?
        // An ended run's input is empty, and its output its first
        // `OUTPUT_PREVIEW` bytes: its history holds both whole.
        const current_step = run.current_step_name_owned orelse "start";
        // Sized to the run. Each field is bounded where the run was made: the
        // start entry (key < 600 bytes, value <= 65000) and the completion
        // record (under 64 KiB) hold every one, so each length fits its field.
        const size = 2 + run.run_id_owned.len + 2 + run.workflow_name_owned.len + 2 + run.workflow_version_owned.len +
            1 + 2 + current_step.len + 4 + run.input_owned.len + 8 +
            1 + (if (run.started_at_ms != null) @as(usize, 8) else 0) + 1 + (if (run.completed_at_ms != null) @as(usize, 8) else 0) +
            1 + (if (run.wait_signal_type_owned) |sig| 2 + sig.len else 0) +
            1 + (if (run.output_owned) |o| 4 + o.len + 1 else 0);
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
            w.writeByte(@intFromBool(run.output_truncated)) catch unreachable;
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
        /// An ended run's output is its first `OUTPUT_PREVIEW` bytes: more
        /// were cut, and its history holds the whole.
        output_truncated: bool = false,
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
            .output_truncated = rec.output_truncated,
            .search_tags_owned = if (rec.search_tags.len > 0) rec.search_tags else null,
        };
    }

    pub const SeqEvent = history_read.SeqEvent;
    pub const HistoryPage = history_read.Page;
    pub const HistoryAnswer = history_read.Answer;
    pub const MAX_HISTORY_PAGE = history_read.MAX_PAGE;
    pub const HISTORY_LOST = history_read.LOST;
    pub const HISTORY_MISSING = history_read.MISSING;
    pub const HistoryRequest = history_read.Query;

    /// Run `run_key`'s history as applied, `rec` its run, as
    /// `history_read.read` answers it. Points into `a`.
    pub fn readRunHistory(self: *WorkflowHandler, a: Allocator, run_key: []const u8, rec: runs_machine.Run.Value, window: StreamHandler.HistoryWindow, asked: u32, final: bool) error{OutOfMemory}!HistoryAnswer {
        const stream = self.history orelse return .{ .page = .{ .events = &.{}, .more = null } };
        const ended: ?u64 = if (appliedStatus(rec).isTerminal()) @intCast(@max(rec.completed_at_ms, 0)) else null;
        return history_read.read(a, stream, self.defsRead(), .{
            .family = .workflow,
            .ns = keyNamespace(run_key),
            .owner = rec.workflow,
            .run = keyName(run_key),
            .created_ns = rec.created_ns,
            .ended_ms = ended,
            .flushed = rec.hist_flushed,
            .pending = rec.hist_pending,
        }, window, asked, final);
    }

    // ── LIST RUNS ───────────────────────────────────────────────────────

    pub const RunInfo = struct {
        run_id: []const u8,
        workflow: []const u8,
        status: []const u8,
        created_at: i64,
    };

    /// Run `run_key` (id `run_id`) as a list shows it, if it matches the
    /// status and the lowercased search; null for one that doesn't, or that
    /// isn't there. The search reads at most `SEARCH_SPAN` bytes of each
    /// field, so a page's cost is bounded whatever a run holds.
    pub fn listedRun(self: *WorkflowHandler, run_key: []const u8, run_id: []const u8, status_filter: []const u8, search_lower: []const u8) Listed {
        const run = self.shownRun(run_key, run_id) orelse return .missing;
        if (status_filter.len > 0 and !std.mem.eql(u8, run.status.toString(), status_filter)) return .filtered;
        if (search_lower.len > 0) {
            const matched = containsLower(span(run.run_id_owned), search_lower) or
                containsLower(span(run.workflow_name_owned), search_lower) or
                (if (run.current_step_name_owned) |step| containsLower(span(step), search_lower) else false) or
                containsLower(span(run.input_owned), search_lower) or
                (if (run.search_tags_owned) |tags| containsLower(span(tags), search_lower) else false);
            if (!matched) return .filtered;
        }
        return .{ .shown = .{
            .run_id = run.run_id_owned,
            .workflow = run.workflow_name_owned,
            .status = run.status.toString(),
            .created_at = run.started_at_ms orelse run.created_at_ms,
        } };
    }

    /// A list's verdict on one indexed run: shown, filtered out, or not there.
    pub const Listed = union(enum) { shown: RunInfo, filtered, missing };

    /// The most of one field a list's search reads.
    pub const SEARCH_SPAN: usize = 4096;

    fn span(field: []const u8) []const u8 {
        return field[0..@min(field.len, SEARCH_SPAN)];
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
        // At its namespace's quota: held without a copy, and out of
        // `to_step` until its hold clears, so no pass looks at it again.
        if (stepsHeld(shard, keyNamespace(run_ns_key))) return self.holdQuota(run_ns_key);
        const run = self.takeRun(shard, run_ns_key) orelse return;
        if (run.ending == null and !self.endingInFlight(shard, run_ns_key)) self.advanceCopy(shard, run_ns_key, run);
        switch (self.commitRun(shard, run_ns_key)) {
            .held => |h| if (h.kind == .quota) self.holdQuota(run_ns_key),
            else => {},
        }
    }

    /// Whether `ns`'s runs' steps are held at its quota, as committed.
    fn stepsHeld(shard: *Shard, ns: NamespaceId) bool {
        return hq.atUsage(.{ .store = &shard.defaultPartition().store }, ns);
    }

    /// Run `run_key` held at its namespace's quota: out of `to_step` and
    /// `call_dirty`, kept on its hold's list. Its release indexes it again
    /// from its wait as applied (`reindexReleased`).
    fn holdQuota(self: *WorkflowHandler, run_key: []const u8) void {
        self.unindexToStep(run_key);
        unindexKey(self, &self.call_dirty, run_key);
        self.holdTimer(.{ .ns = keyNamespace(run_key), .kind = .quota }, run_key);
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
                    // Whether a signal of its type applied already. Out of
                    // memory: the run stops at this step, taken again later.
                    const held = runs_machine.signalsHeld(self.allocator, self.defsRead(), run_ns_key, wait_step.signal_type) catch |err| switch (err) {
                        error.OutOfMemory => return,
                        error.Malformed => {
                            self.completeRun(shard, run_ns_key, run, .failed, "the run's signals won't decode", now_ms);
                            return;
                        },
                    };
                    // Less those this decision's waits took already.
                    var took: u64 = 0;
                    for (run.takes.items) |t| took += @intFromBool(std.mem.eql(u8, t, wait_step.signal_type));
                    const signal_found = held > took;
                    self.addHistoryEvent(run, "waiting_for_signal", wait_step.signal_type, now_ms);

                    // A signal's match brings the run back here, so this is
                    // how any wait ends too.
                    self.clearWaitTimeout(run);
                    if (signal_found) {
                        // It takes the signal: the step that logs this
                        // removes it, so another wait of its type needs
                        // another.
                        const owned_take = self.allocator.dupe(u8, wait_step.signal_type) catch return;
                        run.takes.append(self.allocator, owned_take) catch {
                            self.allocator.free(owned_take);
                            return;
                        };
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

    /// Execute a plan step: try its executors in the pass's order (`orderPass`),
    /// each one's circuit breaker consulted before it is attempted, whatever
    /// the selection.
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

        // The pass's executor order, by the plan's selection.
        // The parser refuses a longer list.
        std.debug.assert(plan.executors.len <= plan_types.MAX_PLAN_EXECUTORS);
        const exec_count: u8 = @intCast(plan.executors.len);
        self.planMemory(shard);
        if (run.plan_order_len != exec_count) {
            self.orderPass(shard, run, plan, ns, run.plan_order[0..exec_count]);
            run.plan_order_len = exec_count;
        }
        const exec_order = run.plan_order[0..exec_count];

        while (run.plan_executor_idx < exec_count) {
            const natural_idx = exec_order[run.plan_executor_idx];
            const executor = plan.executors[natural_idx];

            // Its breaker, under any selection, before attempting it, as
            // this decision's own results so far leave it: a retry isn't
            // made past the breaker its last failure opened. Half-open lets
            // one call through, the probe, until its result.
            const admit: plan_types.ExecutorHealth.Admit = if (executor.breaker) |breaker_cfg|
                self.admitsNow(run, ns, plan_name, executor.name, breaker_cfg, now_ms)
            else
                .yes;
            if (admit == .no) {
                self.addHistoryEvent(run, "plan_breaker_skip", executor.name, now_ms);
                run.plan_executor_idx += 1;
                run.plan_executor_retry_count = 0;
                continue;
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
            const can_wait = !self.anotherExecutorCanRun(run, plan, exec_order[run.plan_executor_idx + 1 ..], ns, plan_name, now_ms);
            const outcome = self.invokeAction(shard, run, resolved_action, step_input, step_label, ns, can_wait, now_ms);

            // The probe is the call made: one held for room is no call.
            if (admit == .probe and !(outcome == null and run.waiting_for_room))
                self.stagePlanEffect(run, ns, plan_name, executor.name, .probe, now_ms);

            // Parked for async action — will resume via resumeFromAction
            if (outcome == null) return null;

            const success = std.mem.eql(u8, outcome.?, definition.StepOutcome.success);

            // Health for synchronous completions: health-weighted orders by
            // it, and every breaker counts its failures.
            self.stageResult(run, ns, plan_name, executor, success, now_ms);

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
    fn anotherExecutorCanRun(self: *WorkflowHandler, run: *const RunRecord, plan: definition.InlinePlan, order: []const u8, ns: NamespaceId, plan_name: []const u8, now_ms: i64) bool {
        for (order) |i| {
            const executor = plan.executors[i];
            const cfg = executor.breaker orelse return true;
            if (self.admitsNow(run, ns, plan_name, executor.name, cfg, now_ms) != .no) return true;
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
        // A child's runs' history names its workflow: one named longer is
        // none that could exist. An action's name is bounded too.
        if (kind == .workflow and target.len > MAX_WORKFLOW_NAME_LEN) {
            self.addHistoryEvent(run, "step_call_too_large", "a workflow name is longer than a run's history may name", now_ms);
            return definition.StepOutcome.execution_failure;
        }
        if (kind == .action and target.len > ae.MAX_ACTION_NAME) {
            self.addHistoryEvent(run, "step_call_too_large", "an action name is longer than an action may have", now_ms);
            return definition.StepOutcome.execution_failure;
        }
        // Its start goes to the callee's shard: held, as for a full log,
        // while that outbox, this shard's, or the namespace's share of
        // either has no room, so one tenant's calls can't fill them.
        if (shard.sendWait(shard.router.slotToShard(slot), .main, ns) != null) return self.holdStepForRoom(run, step_label, now_ms);
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
                if (now_ns >= waitedOutNs(c, shard.actions_handler.missing_action_wait_ms)) return true;
                const wait_ms = heldRetryMs(c.state, c.attempts);
                if (now_ns < c.last_ns +| wait_ms * std.time.ns_per_ms or now_ms < run.call_retry_at_ms) return false;
                self.retryCall(shard, run, action_rid, c, now_ms);
                return false;
            },
            .over_budget => {
                // A step that can't wait fails at once, saying why. A child's
                // step never waits for its workflow to be created, but does
                // wait for memory to free on the child's shard.
                if (!c.can_wait and c.kind == .action) return true;
                // Said once, however long the hold: memory frees when it
                // frees, and each ask again adds nothing to history.
                if (!run.call_over_budget_noted) {
                    run.call_over_budget_noted = true;
                    self.addHistoryEvent(run, "call_held_over_budget", c.outcome, now_ms);
                }
                if (run.call_retry_index > shard.raft_node.last_applied) return false;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                const wait_ms = heldRetryMs(c.state, c.attempts);
                if (now_ns < c.last_ns +| wait_ms * std.time.ns_per_ms or now_ms < run.call_retry_at_ms) return false;
                self.retryCall(shard, run, action_rid, c, now_ms);
                return false;
            },
            .over_quota => {
                // It always waits, a step that can't wait too: room comes
                // as the callee's namespace deletes runs or raises its
                // quota, and failing would lose the step for a limit. Said
                // once, however long the hold.
                if (!run.call_over_quota_noted) {
                    run.call_over_quota_noted = true;
                    self.addHistoryEvent(run, "call_held_over_quota", c.outcome, now_ms);
                }
                if (run.call_retry_index > shard.raft_node.last_applied) return false;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                const wait_ms = heldRetryMs(c.state, c.attempts);
                if (now_ns < c.last_ns +| wait_ms * std.time.ns_per_ms or now_ms < run.call_retry_at_ms) return false;
                self.retryCall(shard, run, action_rid, c, now_ms);
                return false;
            },
        }
    }

    /// How long a call held at its callee's quota waits before asking again,
    /// after `attempts`: 5 s, doubling, at most 10 minutes. Room comes only
    /// as runs are deleted or the quota raised, so it asks less often than a
    /// memory hold does.
    pub fn quotaRetryMs(attempts: u32) u64 {
        const doublings: u6 = @intCast(@min(@max(attempts, 1) - 1, 16));
        return @min(CALL_RETRY_FIRST_MS << doublings, QUOTA_RETRY_MAX_MS);
    }

    pub const QUOTA_RETRY_MAX_MS: u64 = 600_000;

    /// How long a call in `state` waits before asking again: a quota hold
    /// on its own backoff, a missing action or a memory hold on the call's.
    /// Both sites that wait on a held call read it here.
    pub fn heldRetryMs(state: calls_mod.State, attempts: u32) u64 {
        return if (state == .over_quota) quotaRetryMs(attempts) else callRetryMs(attempts);
    }

    /// Asks again for a call whose action wasn't registered, or whose
    /// action's shard was at its memory share. Not proposed (a busy log, a
    /// step-down): asked again a second later.
    fn retryCall(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, action_rid: []const u8, c: calls_mod.Call.Value, now_ms: i64) void {
        run.call_retry_at_ms = now_ms +| 1_000;
        const ns = keyNamespace(run.ns_key);
        // Its start is sent again: not while its outbox has no room.
        if (shard.sendWait(shard.router.slotToShard(router.nsKeySlot(ns, c.target)), .main, ns) != null) return;
        var buf: [ae.CallRetry.max_size]u8 = undefined;
        const payload = ae.CallRetry.encodeInto(&buf, .{ .ns = ns.int(), .slot = calls_mod.callerSlot(ns, run.workflow_name_owned), .run = action_rid }) catch return;
        run.call_retry_index = (proposeCharged(shard, ns, .step_call_retry, payload) catch return).index;
        if (shard.metrics_registry) |m| m.workflow.recordActionCallRetry();
        // A hold over memory or the quota was said once, when it started.
        if (c.state == .over_budget or c.state == .over_quota) return;
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
        return shard.sendWait(shard.router.slotToShard(p.parent_slot), .main, ns) != null;
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
    /// version, or a run already holds the child's id, or the shard is at
    /// its memory share.
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
        // At its shard's memory share, or its namespace's quota here, the
        // start would be refused at apply: the parent is told now, and asks
        // again. The quota first, as at apply.
        const quota = if (taken or !held) null else hq.preCheck(self.defsRead(), ns, runs_machine.reserveOf(p.workflow, run_id));
        const answer: ?ae.Answer = if (taken) .conflict else if (!held) .no_action else if (quota != null) .over_quota else if (gate.atShare(&shard.defaultPartition().store) != null) .over_budget else null;
        if (answer) |a| {
            // A refusal is told to the parent: not past its outbox's room.
            if (shard.sendWait(shard.router.slotToShard(p.parent_slot), .main, ns) != null) return error.Overloaded;
            // The quota's figures, as an apply's refusal gives them.
            var wb: [128]u8 = undefined;
            const why: ?[]const u8 = if (quota) |o| hq.why(&wb, ns, shard.id, o) else null;
            var buf: [ae.ChildRefuse.max_size]u8 = undefined;
            const payload = try ae.ChildRefuse.encodeInto(&buf, .{ .ns = ns.int(), .run = run_id, .answer = a, .why = why });
            return shard.raft_node.propose(.child_refuse, entry_mod.Flags.NONE, payload);
        }
        return self.proposeStart(shard, ns, run_id, p.workflow, if (p.version.len > 0) p.version else "latest", p.input, null, "child_started", null);
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
        self.checkPendingActionsAt(shard, @import("stdx").time.milliTimestamp());
    }

    /// `checkPendingActions` at `now_ms`: a seam so tests needn't sleep.
    fn checkPendingActionsAt(self: *WorkflowHandler, shard: *Shard, now_ms: i64) void {
        self.sweepCalls(shard, now_ms);
        self.startChildren(shard);
        if (shard.raft_node.role != .leader) return;
        self.releaseHeld(shard, now_ms);

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

        // Parked runs whose call may need a step, decided from what applied
        // alone: a run is taken only when its step logs something. One not
        // due yet gets a timer; one that can't move leaves the set until
        // its call changes.
        var look_keys: [STEPS_PER_PASS][]const u8 = undefined;
        const look_count = self.dirtyToLook(shard, &look_keys);
        defer for (look_keys[0..look_count]) |k| self.allocator.free(k);
        for (look_keys[0..look_count]) |ns_key| {
            unindexKey(self, &self.call_dirty, ns_key);
            switch (self.callWork(shard, ns_key, now_ms)) {
                .none => {},
                .later => |at| self.armCallTimer(ns_key, at),
                .step => {
                    if (stepsHeld(shard, keyNamespace(ns_key))) {
                        self.holdQuota(ns_key);
                        continue;
                    }
                    const run = (self.tryTakeRun(shard, ns_key) catch {
                        // Out of the dirty set already: looked at again.
                        self.indexKey(&self.call_dirty, ns_key);
                        continue;
                    }) orelse continue;
                    const rid = run.pending_action_run_id_owned orelse {
                        self.dropRun(ns_key);
                        continue;
                    };
                    if (self.callSettled(shard, run, rid, now_ms)) self.resumeFromAction(shard, ns_key, now_ms);
                    switch (self.commitRun(shard, ns_key)) {
                        .held => |h| if (h.kind == .quota) {
                            self.holdQuota(ns_key);
                            continue;
                        },
                        else => {},
                    }
                    // Not logged (the log was full): looked at again.
                    if (self.steppingAt(shard, ns_key) == null) self.indexKey(&self.call_dirty, ns_key);
                },
            }
        }

        // A run can have two entries due at once, and what ran before this
        // may have moved it on: each is asked again as it fires.
        for (due_keys[0..due_count]) |ns_key| {
            const rec = self.appliedRun(ns_key) orelse continue;
            // A call's retry or its wait running out comes due.
            if (rec.wait == .call) {
                if (self.call_timer.get(ns_key)) |at| if (at <= now_ms) self.dropCallTimer(ns_key);
                if (self.parked.contains(ns_key)) self.indexKey(&self.call_dirty, ns_key);
                continue;
            }
            if (rec.wait_due_ms == 0 or rec.wait_due_ms > now_ms) continue;
            if (rec.wait != .timer and rec.wait != .signal) continue;
            if (self.steppingAt(shard, ns_key) != null) continue;
            if (self.endingInFlight(shard, ns_key)) {
                // Its timer stands if the cancel doesn't.
                self.armTimer(ns_key, now_ms +| ENDING_RECHECK_MS);
                continue;
            }
            // Held at its namespace's quota, or by its history share: kept
            // without a copy. A child's end held for its parent is known
            // only once decided.
            if (stepsHeld(shard, keyNamespace(ns_key))) {
                self.holdQuota(ns_key);
                continue;
            }
            if (shard.historyWait(keyNamespace(ns_key)) != null) {
                self.holdTimer(.{ .ns = keyNamespace(ns_key) }, ns_key);
                continue;
            }
            const r = (self.tryTakeRun(shard, ns_key) catch {
                // Its timer was popped: armed again, as an unlogged step's is.
                self.armTimer(ns_key, now_ms +| UNLOGGED_RECHECK_MS);
                continue;
            }) orelse continue;
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
            // A step held or not logged is decided again from its applied
            // wait: its timer was popped, and nothing else would wake it.
            // The popped entry is replaced, so a run keeps one.
            switch (self.commitRun(shard, ns_key)) {
                .held => |hold| self.holdTimer(hold, ns_key),
                .retry => self.armTimer(ns_key, now_ms +| UNLOGGED_RECHECK_MS),
                .logged, .unchanged, .failed => {},
            }
        }
    }

    /// Up to `out.len` dirty runs that may be looked at now (none with a
    /// step or cancel in flight), owned, picking up where the last pass
    /// stopped: runs held there (a step in flight, a full log) can't keep
    /// the rest waiting.
    fn dirtyToLook(self: *WorkflowHandler, shard: *Shard, out: [][]const u8) usize {
        var n: usize = 0;
        var visited: usize = 0;
        var it = self.call_dirty.iterator();
        // A cursor past the end (the map grew or shrank) starts over.
        const start: u32 = if (self.call_dirty_cursor <= self.call_dirty.capacity()) self.call_dirty_cursor else 0;
        it.index = start;
        var wrapped = start == 0;
        defer self.call_dirty_cursor = it.index;
        while (n < out.len and visited < VISITS_PER_PASS) {
            if (wrapped and start != 0 and it.index >= start) break;
            const entry = it.next() orelse {
                // At the end: once more from the top, up to where it began.
                if (wrapped) break;
                wrapped = true;
                it = self.call_dirty.iterator();
                continue;
            };
            visited += 1;
            const k = entry.key_ptr.*;
            if (self.steppingAt(shard, k) != null or self.endingInFlight(shard, k)) continue;
            out[n] = self.allocator.dupe(u8, k) catch break;
            n += 1;
        }
        return n;
    }

    const CallWork = union(enum) {
        /// Nothing to do until its call's record changes.
        none,
        /// Nothing to do before this stamp (ms): a retry, or its wait over.
        later: i64,
        /// A step to log: the call settled, or a note or a retry is due.
        step,
    };

    /// What parked run `run_key`'s call needs, read from what applied and
    /// its logged counters, with nothing allocated: what `callSettled`
    /// would do with a copy, without taking one.
    fn callWork(self: *WorkflowHandler, shard: *Shard, run_key: []const u8, now_ms: i64) CallWork {
        const rec = self.appliedRun(run_key) orelse return .none;
        if (rec.wait != .call or appliedStatus(rec).isTerminal()) return .none;
        const ns = keyNamespace(run_key);
        // Gone: the step takes it as lost.
        const c = calls_mod.callOf(self.defsRead(), ns, calls_mod.callerSlot(ns, rec.workflow), rec.wait_on) orelse return .step;
        switch (c.state) {
            .sent, .started => return .none,
            .done, .refused => return .step,
            .no_action => {
                const wait_ms = shard.actions_handler.missing_action_wait_ms;
                if (!c.can_wait or wait_ms == 0) return .step;
                // Counters that won't decode end the run when taken.
                const p = progress_mod.Progress.decode(rec.progress) catch return .step;
                if (!p.call_unregistered_noted) return .step;
                // A retry asked for and not applied yet: its apply moves the call.
                if (p.call_retry_index > shard.raft_node.last_applied) return .none;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                const waited_out_ns = waitedOutNs(c, wait_ms);
                if (now_ns >= waited_out_ns) return .step;
                const next_ns = c.last_ns +| heldRetryMs(c.state, c.attempts) * std.time.ns_per_ms;
                if (now_ns >= next_ns and now_ms >= p.call_retry_at_ms) return .step;
                const due_ms = @max(ceilMs(next_ns), p.call_retry_at_ms);
                return .{ .later = @min(due_ms, ceilMs(waited_out_ns)) };
            },
            .over_budget => {
                if (!c.can_wait and c.kind == .action) return .step;
                const p = progress_mod.Progress.decode(rec.progress) catch return .step;
                if (!p.call_over_budget_noted) return .step;
                if (p.call_retry_index > shard.raft_node.last_applied) return .none;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                const next_ns = c.last_ns +| heldRetryMs(c.state, c.attempts) * std.time.ns_per_ms;
                if (now_ns >= next_ns and now_ms >= p.call_retry_at_ms) return .step;
                return .{ .later = @max(ceilMs(next_ns), p.call_retry_at_ms) };
            },
            .over_quota => {
                // Always waits, `can_wait` or not (`callSettled`).
                const p = progress_mod.Progress.decode(rec.progress) catch return .step;
                if (!p.call_over_quota_noted) return .step;
                if (p.call_retry_index > shard.raft_node.last_applied) return .none;
                const now_ns: u64 = @intCast(@max(now_ms, 0) *| std.time.ns_per_ms);
                const next_ns = c.last_ns +| heldRetryMs(c.state, c.attempts) * std.time.ns_per_ms;
                if (now_ns >= next_ns and now_ms >= p.call_retry_at_ms) return .step;
                return .{ .later = @max(ceilMs(next_ns), p.call_retry_at_ms) };
            },
        }
    }

    /// When a call whose action isn't registered has waited `wait_ms` for
    /// one, its run failing then.
    fn waitedOutNs(c: calls_mod.Call.Value, wait_ms: u64) u64 {
        return c.first_ns +| wait_ms *| std.time.ns_per_ms;
    }

    /// The first whole ms at or after `ns`.
    fn ceilMs(ns: u64) i64 {
        return @intCast(std.math.divCeil(u64, ns, std.time.ns_per_ms) catch unreachable);
    }

    /// Drops a decision's copy without logging anything: nothing changed.
    fn dropRun(self: *WorkflowHandler, run_key: []const u8) void {
        var kv = self.runs.fetchRemove(run_key) orelse return;
        self.freeRunRecord(&kv.value);
        self.allocator.free(kv.key);
    }

    const ENDING_RECHECK_MS: i64 = 100;
    /// How soon a timer's step the log didn't take for now (full, or this
    /// node not leading or halted) is tried again: those last longer than
    /// a hold, and a retry each tick would only fail again.
    const UNLOGGED_RECHECK_MS: i64 = 1_000;

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
            // Settled only when its step can't wait: refused, saying why.
            .refused, .over_budget => .refused,
            // Not settled: `callSettled` resumes only a settled one. A call
            // held at its callee's quota never settles.
            .sent, .started, .over_quota => return,
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
        run.call_over_budget_noted = false;
        run.call_over_quota_noted = false;
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

                // Health for a plan executor's async completion, under any
                // selection, as for a synchronous one.
                if (run_step.isPlan()) {
                    if (def.getPlan(run_step.targetName())) |plan| {
                        if (run.plan_executor_natural_idx < plan.executors.len) {
                            const attempted_executor = plan.executors[run.plan_executor_natural_idx];
                            const is_success = std.mem.eql(u8, outcome, definition.StepOutcome.success);
                            self.stageResult(run, ns, run_step.targetName(), attempted_executor, is_success, now_ms);
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
                        // Use natural index: plan_executor_idx is the position in the
                        // pass's order, not the index into plan.executors[].
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
        return self.tryTakeRun(shard, run_key) catch null;
    }

    /// `takeRun`, saying when the copy ran out of memory, for a caller
    /// whose wake is gone once it fails: a popped timer, a call's look.
    /// Other callers' runs stay indexed and are taken again.
    fn tryTakeRun(self: *WorkflowHandler, shard: *Shard, run_key: []const u8) error{OutOfMemory}!?*RunRecord {
        std.debug.assert(!self.runs.contains(run_key));
        const rec = runs_machine.runOf(self.defsRead(), run_key) orelse return null;
        if (appliedStatus(rec).isTerminal()) return null;
        if (self.steppingAt(shard, run_key)) |rev| if (rev == rec.rev) return null;
        self.taken += 1;
        self.copyRun(run_key, rec) catch {
            log.err("workflow run {f}: not taken on (out of memory)", .{shown(run_key)});
            return error.OutOfMemory;
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
    /// What came of a run's step: logged; unchanged (nothing to log); held
    /// (its history lane, its parent's outbox); not logged for now
    /// (`retry`); or not logged, and won't be (`failed`, said once).
    pub const Committed = union(enum) { logged, unchanged, held: Hold, retry, failed };

    fn commitRun(self: *WorkflowHandler, shard: *Shard, run_key: []const u8) Committed {
        var kv = self.runs.fetchRemove(run_key) orelse return .unchanged;
        defer {
            self.freeRunRecord(&kv.value);
            self.allocator.free(kv.key);
        }
        const run = &kv.value;
        const index = self.proposeStep(shard, run) catch |err| {
            // An error that can pass belongs in `.retry`: one left to the
            // last arm is said once and its timer's run waits until the
            // next term. Classify each new error `proposeStep` can return.
            switch (err) {
                error.Unchanged => return .unchanged,
                error.HeldForHistory => return .{ .held = .{ .ns = keyNamespace(run_key) } },
                error.HeldForQuota => return .{ .held = .{ .ns = keyNamespace(run_key), .kind = .quota } },
                error.HeldForParent => {
                    const ns = keyNamespace(run_key);
                    const read: contract_view.ReadView = .{ .store = &shard.defaultPartition().store };
                    const p = children_mod.parentOf(read, ns, run.run_id_owned) orelse return .{ .held = .{ .ns = ns } };
                    return .{ .held = .{ .ns = ns, .kind = .parent, .parent_shard = shard.router.slotToShard(p.parent_slot) } };
                },
                // For now: tried again, quietly, as each try would say it.
                error.Overloaded, error.NotLeader, error.WritesStopped, error.Halted, error.OutOfMemory => {
                    // Counted on the halted shard's line of refused writes.
                    _ = shard.quietHalted(err);
                    return .retry;
                },
                else => {
                    log.warn("shard {d}: workflow run {f}: step not logged: {s}", .{ shard.id, shown(run_key), @errorName(err) });
                    return .failed;
                },
            }
        };
        _ = index;
        self.applyPlanEffects(shard, run);
        const owned = self.allocator.dupe(u8, run_key) catch return .logged;
        const gop = self.stepping.getOrPut(self.allocator, owned) catch {
            self.allocator.free(owned);
            return .logged;
        };
        if (gop.found_existing) self.allocator.free(owned);
        gop.value_ptr.* = run.rev;
        return .logged;
    }

    /// The `workflow_step` value for `run`'s copy, proposed.
    fn proposeStep(self: *WorkflowHandler, shard: *Shard, run: *RunRecord) !u64 {
        const ns = keyNamespace(run.ns_key);
        // A child's end goes to its parent's slot: held, and decided again,
        // while that outbox has no room. Each child is held for its own
        // parent, so one full parent holds no other run's end.
        if (run.ending != null and parentWait(shard, ns, run.run_id_owned)) return error.HeldForParent;
        // At its namespace's run-history quota: held, ending or not; only a
        // cancel, which is no step, goes past it. Its run's reserve counts
        // the one step a race lets through.
        if (stepsHeld(shard, ns)) return error.HeldForQuota;
        // Its events go on this shard's history lane: held, and decided
        // again, while that is past its cap and the namespace its share.
        if (shard.historyWait(ns) != null) return error.HeldForHistory;
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
            runs_machine.Event.encode(&events, .{ .event = MARKER_EVENT, .detail = detail, .at_ms = now_ms }) catch |err| return self.tooLargeOr(err, shard, run, a, limit, now_ms);
            event_count += 1;
        }
        for (new_events[skip..]) |e| {
            runs_machine.Event.encode(&events, .{ .event = e.event_type_owned, .detail = e.detail_owned, .at_ms = e.timestamp_ms }) catch |err| return self.tooLargeOr(err, shard, run, a, limit, now_ms);
            event_count += 1;
        }
        if (final) |f| {
            runs_machine.Event.encode(&events, f) catch |err| return self.tooLargeOr(err, shard, run, a, limit, now_ms);
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
                runs_machine.StepOutput.encode(&outputs, .{ .step = e.step_name, .outcome = e.outcome, .output = e.output }) catch |err| return self.tooLargeOr(err, shard, run, a, limit, now_ms);
                output_count += 1;
            }
        }

        var takes: wire_schema.Writer = .{ .buf = try a.alloc(u8, limit) };
        for (run.takes.items) |t| runs_machine.Take.encode(&takes, .{ .type = t }) catch |err| return self.tooLargeOr(err, shard, run, a, limit, now_ms);

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
        if (run.ending == null and event_count == 0 and output_count == 0 and run.call_payload == null and settles.len == 0 and run.takes.items.len == 0) {
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
        if (run.ending) |status| {
            if (status == .completed) output = self.workflowOutput(run, ns);
        }
        defer if (output) |o| self.allocator.free(o);

        const value: Step.Value = .{
            .created_ns = run.created_ns,
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
            .call = if (wait == .call) run.call_payload orelse "" else "",
            .takes = .{ .bytes = takes.written(), .count = @intCast(run.takes.items.len) },
        };
        const bytes = Step.encodeInto(try a.alloc(u8, limit), value) catch |err| switch (err) {
            error.NoSpaceLeft, error.TooLong => return self.proposeTooLarge(shard, run, a, limit, now_ms),
            else => return err,
        };
        return (try persistence_mod.proposeEntryIn(shard, .workflow_step, entry_mod.Flags.NONE, ns, run.ns_key, bytes)).index;
    }

    /// A part of a step past its buffer (events' details and outputs are
    /// unbounded) makes the step too large to log.
    fn tooLargeOr(self: *WorkflowHandler, err: anyerror, shard: *Shard, run: *RunRecord, a: Allocator, limit: usize, now_ms: i64) !u64 {
        return switch (err) {
            error.NoSpaceLeft, error.TooLong => self.proposeTooLarge(shard, run, a, limit, now_ms),
            else => err,
        };
    }

    /// A step too large to log ends its run failed, saying so: that always
    /// fits.
    fn proposeTooLarge(self: *WorkflowHandler, shard: *Shard, run: *RunRecord, a: Allocator, limit: usize, now_ms: i64) !u64 {
        _ = self;
        const ns = keyNamespace(run.ns_key);
        var events: wire_schema.Writer = .{ .buf = try a.alloc(u8, 512) };
        try runs_machine.Event.encode(&events, .{ .event = terminalEventType(.failed), .detail = "step record too large to log", .at_ms = now_ms });
        const value: runs_machine.Step.Value = .{
            .created_ns = run.created_ns,
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
            .takes = .{ .bytes = "", .count = 0 },
            .caller_slot = calls_mod.callerSlot(ns, run.workflow_name_owned),
            .workflow = run.workflow_name_owned,
            .ends = @backingInt(RunStatus.failed),
            .has_output = false,
            .output = "",
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

    /// How many stream triggers the node's leaders hold. For tests.
    pub fn streamTriggerCount() u32 {
        return global_stream_trigger_count.load(.monotonic);
    }

    /// Register a stream trigger for a workflow definition.
    /// Called from handleCreate when the parsed definition has a `trigger:` block.
    fn registerStreamTrigger(
        self: *WorkflowHandler,
        ns: NamespaceId,
        workflow_name: []const u8,
        trigger: definition.StreamTriggerDef,
    ) error{OutOfMemory}!void {
        const trigger_key = self.nsKey(ns, workflow_name) orelse return error.OutOfMemory;

        // The place in the stream is logged (`workflow.per_def`), so a
        // re-created definition keeps it, and one moved to another stream
        // reads that stream from its start. What this leader read ahead
        // stays only in the same stream. Read numbers go on across it: a
        // page asked before carries an older one, and isn't taken.
        var proposed: @FieldType(StreamTriggerState, "proposed") = null;
        var resync_index: u64 = 0;
        var hold: MemoryHold = .{};
        var next_read_seq: u64 = 0;
        var made: ?u64 = null;
        const stream_ns = self.otherStreamNamespace(ns, trigger.namespace);
        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            next_read_seq = state.next_read_seq;
            const same_ns = if (state.stream_namespace_owned) |a| (if (stream_ns) |b| std.mem.eql(u8, a, b) else false) else stream_ns == null;
            if (std.mem.eql(u8, state.stream_name_owned, trigger.stream) and same_ns) {
                proposed = state.proposed;
                resync_index = state.resync_index;
                hold = state.hold;
                made = state.stream_made_ns;
            }
            self.freeTriggerState(&state);
            _ = global_stream_trigger_count.fetchSub(1, .monotonic);
        }

        const owned_wf = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(trigger_key);
            return error.OutOfMemory;
        };
        const owned_stream = self.allocator.dupe(u8, trigger.stream) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            return error.OutOfMemory;
        };
        const owned_stream_ns: ?[]const u8 = if (stream_ns) |n| self.allocator.dupe(u8, n) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_stream);
            return error.OutOfMemory;
        } else null;

        self.stream_triggers.put(trigger_key, .{
            .workflow_name_owned = owned_wf,
            .ns = ns,
            .stream_name_owned = owned_stream,
            .stream_namespace_owned = owned_stream_ns,
            .batch_size = trigger.batch_size,
            .poll_interval_ms = trigger.batch_timeout_ms,
            .proposed = proposed,
            .resync_index = resync_index,
            .hold = hold,
            .next_read_seq = next_read_seq,
            .stream_made_ns = made,
            .last_poll_ms = 0,
        }) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_stream);
            if (owned_stream_ns) |n| self.allocator.free(n);
            return error.OutOfMemory;
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
    fn unregisterStreamTrigger(self: *WorkflowHandler, ns: NamespaceId, workflow_name: []const u8) error{OutOfMemory}!void {
        const trigger_key = self.nsKey(ns, workflow_name) orelse return error.OutOfMemory;
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
    ) error{OutOfMemory}!void {
        if (schedule.paused) return;
        // The validator lets through exactly one of a valid cron or an
        // interval; a stored definition holds the same.
        var parsed: ?cron_mod.Cron = null;
        const interval_ms: i64 = if (schedule.cron_expr) |expr| blk: {
            parsed = cron_mod.Cron.parse(expr) orelse return;
            break :blk 0;
        } else schedule.interval_ms orelse return;
        if (parsed == null and interval_ms <= 0) return;

        const sched_key = self.nsKey(ns, workflow_name) orelse return error.OutOfMemory;

        // A redefinition replaces the schedule; the slots it fired stay
        // fired.
        var fired_ms: i64 = std.math.minInt(i64);
        var hold: MemoryHold = .{};
        if (self.schedules.fetchRemove(sched_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            fired_ms = state.fired_ms;
            hold = state.hold;
            self.freeScheduleState(&state);
        }

        const owned_wf = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(sched_key);
            return error.OutOfMemory;
        };
        const owned_input: ?[]const u8 = if (schedule.input) |inp| self.allocator.dupe(u8, inp) catch {
            self.allocator.free(sched_key);
            self.allocator.free(owned_wf);
            return error.OutOfMemory;
        } else null;

        self.schedules.put(sched_key, .{
            .workflow_name_owned = owned_wf,
            .ns = ns,
            .interval_ms = interval_ms,
            .cron = parsed,
            .max_concurrent = schedule.max_concurrent,
            .input_owned = owned_input,
            .anchor_ms = anchor_ms,
            .fired_ms = fired_ms,
            .hold = hold,
        }) catch {
            self.allocator.free(sched_key);
            self.allocator.free(owned_wf);
            if (owned_input) |inp| self.allocator.free(inp);
            return error.OutOfMemory;
        };
    }

    fn unregisterSchedule(self: *WorkflowHandler, ns: NamespaceId, workflow_name: []const u8) error{OutOfMemory}!void {
        const sched_key = self.nsKey(ns, workflow_name) orelse return error.OutOfMemory;
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
        defer self.countDutiesHeld(shard);
        const at_share = gate.atShare(&shard.defaultPartition().store);

        var it = self.schedules.iterator();
        while (it.next()) |entry| {
            const schedule = entry.value_ptr;
            const slot = dueSlot(schedule, now_ms) orelse continue;
            if (slot <= schedule.fired_ms) continue;
            if (schedule.proposed_term == term and slot <= schedule.proposed_ms) continue;

            // Skip disabled workflows
            if (self.isDisabled(entry.key_ptr.*)) {
                schedule.hold.clear();
                schedule.quota_hold.clear();
                continue;
            }

            // Held at the share: the slot fires, once and late, when memory
            // frees, as one missed while no leader was ready.
            if (schedule.hold.waiting(term, now_ms)) continue;
            if (at_share) |o| {
                if (schedule.hold.held(o.share, now_ms)) self.noteStartsHeld("schedule", .memory, schedule.ns, schedule.workflow_name_owned, shard.id, o.share);
                continue;
            }
            schedule.hold.clear();
            // Held at its namespace's quota: the same, until runs end or
            // are deleted, or the quota is raised.
            if (schedule.quota_hold.waiting(term, now_ms)) continue;
            if (quotaHeld(self.defsRead(), schedule.ns, schedule.workflow_name_owned)) |part| {
                if (schedule.quota_hold.held(part, now_ms)) self.noteStartsHeld("schedule", .quota, schedule.ns, schedule.workflow_name_owned, shard.id, part);
                continue;
            }
            schedule.quota_hold.clear();

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

    fn noteStartsHeld(self: *WorkflowHandler, what: []const u8, bound: Bound, ns: NamespaceId, workflow: []const u8, shard_id: u16, at: u64) void {
        self.holds_said += 1;
        switch (bound) {
            .memory => log.warn("workflow {s} (namespace {d}): its {s} holds its starts while shard {d} is at its memory share ({d} bytes); they go on once memory frees", .{ workflow, ns.int(), what, shard_id, at }),
            .quota => log.warn("workflow {s} (namespace {d}): its {s} holds its starts while the namespace is at its run-history quota on shard {d} (a part of {d} bytes); they go on once runs are deleted or the quota is raised", .{ workflow, ns.int(), what, shard_id, at }),
        }
    }

    /// Whether a leader's start of `workflow` in `ns` would pass the
    /// namespace's quota here: its part when it would, null when it fits.
    /// The run's id isn't made yet, so the longest a server makes is
    /// counted: at most a few hundred bytes of the part above its own.
    fn quotaHeld(read: @import("../contract/view.zig").ReadView, ns: NamespaceId, workflow: []const u8) ?u64 {
        const own = @import("../budget/history_reserve.zig").workflow(workflow.len, run_id_mod.MAX_ID_LEN);
        return if (hq.preCheck(read, ns, own)) |o| o.part else null;
    }

    /// This node no longer runs the duties (it stepped down, or its shard
    /// halted): none is held at the share here, and the gauge drops this
    /// shard's count. The next term that leads begins its holds again.
    pub fn dropHolds(self: *WorkflowHandler, shard: *Shard) void {
        var sit = self.schedules.valueIterator();
        while (sit.next()) |s| {
            s.hold.reset(0);
            s.quota_hold.reset(0);
        }
        var tit = self.stream_triggers.valueIterator();
        while (tit.next()) |t| {
            t.hold.reset(0);
            t.quota_hold.reset(0);
        }
        self.countDutiesHeld(shard);
    }

    /// The schedules and triggers held at the share, into the gauge: each
    /// shard adds what moved since it last counted.
    fn countDutiesHeld(self: *WorkflowHandler, shard: *Shard) void {
        var n: u64 = 0;
        var q: u64 = 0;
        var sit = self.schedules.valueIterator();
        while (sit.next()) |s| {
            n += @intFromBool(s.hold.share != null);
            q += @intFromBool(s.quota_hold.share != null);
        }
        var tit = self.stream_triggers.valueIterator();
        while (tit.next()) |t| {
            n += @intFromBool(t.hold.share != null);
            q += @intFromBool(t.quota_hold.share != null);
        }
        defer self.memory_held = n;
        defer self.quota_held = q;
        const m = shard.metrics_registry orelse return;
        if (n > self.memory_held) _ = m.workflow.duties_memory_held.fetchAdd(n - self.memory_held, .monotonic) else _ = m.workflow.duties_memory_held.fetchSub(self.memory_held - n, .monotonic);
        if (q > self.quota_held) _ = m.workflow.duties_quota_held.fetchAdd(q - self.quota_held, .monotonic) else _ = m.workflow.duties_quota_held.fetchSub(self.quota_held - q, .monotonic);
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
        const idem = std.fmt.bufPrint(&key_buf, "_schedule:{d}:{s}:{d}", .{ schedule.ns.int(), schedule.workflow_name_owned, slot_ms }) catch return false;

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
        const at_share = gate.atShare(&shard.defaultPartition().store);

        var it = self.stream_triggers.iterator();
        while (it.next()) |entry| {
            const trigger = entry.value_ptr;

            // Enforce poll interval unless a push-wake forced this poll.
            if (!forced) {
                const poll_ms: i64 = @intCast(trigger.poll_interval_ms);
                if (now_ms - trigger.last_poll_ms < poll_ms) continue;
            }

            // Skip disabled workflows
            if (self.isDisabled(entry.key_ptr.*)) {
                trigger.hold.clear();
                trigger.quota_hold.clear();
                continue;
            }

            // Held at the share: its place stays where it is.
            if (trigger.hold.waiting(shard.raft_node.current_term, now_ms)) continue;
            if (at_share) |o| {
                if (trigger.hold.held(o.share, now_ms)) self.noteStartsHeld("trigger", .memory, trigger.ns, trigger.workflow_name_owned, shard.id, o.share);
                continue;
            }
            trigger.hold.clear();
            if (trigger.quota_hold.waiting(shard.raft_node.current_term, now_ms)) continue;
            if (quotaHeld(self.defsRead(), trigger.ns, trigger.workflow_name_owned)) |part| {
                if (trigger.quota_hold.held(part, now_ms)) self.noteStartsHeld("trigger", .quota, trigger.ns, trigger.workflow_name_owned, shard.id, part);
                continue;
            }
            trigger.quota_hold.clear();

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
            if (!self.sameStream(entry.key_ptr.*, trigger, stream_ns, made)) continue;
            const cursor = self.readFrom(entry.key_ptr.*, trigger, stream_ns, shard.raft_node.current_term);
            const result = sh.readPayloadsForStreamIn(trigger.stream_name_owned, stream_ns, cursor, limit);
            defer result.deinit(sh.allocator);
            self.takeRecords(shard, entry.key_ptr.*, trigger, .{ .ns = stream_ns, .made = made, .name = trigger.stream_name_owned }, result.payloads, result.ids, result.last_id);
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
    fn sameStream(self: *const WorkflowHandler, def_key: []const u8, trigger: *StreamTriggerState, ns: NamespaceId, made: u64) bool {
        self.seedMade(def_key, trigger, ns);
        const was = trigger.stream_made_ns orelse {
            trigger.stream_made_ns = made;
            return true;
        };
        if (made == was) return true;
        if (made < was) return false;
        trigger.stream_made_ns = made;
        trigger.proposed = null;
        trigger.last_poll_ms = 0;
        return false;
    }

    /// The stream the logged place is in, when it is this trigger's stream
    /// in `ns`: its made stamp is the one reads are checked against, unless
    /// this node has seen it made since. After a restart, a stream made
    /// again while the node was down is then read from its start.
    fn seedMade(self: *const WorkflowHandler, def_key: []const u8, trigger: *StreamTriggerState, ns: NamespaceId) void {
        const per = runs_machine.perDefOf(self.defsRead(), def_key) orelse return;
        if (per.trig_ns != ns.int() or !std.mem.eql(u8, per.trig_stream, trigger.stream_name_owned)) return;
        if (trigger.stream_made_ns == null or trigger.stream_made_ns.? < per.trig_made) trigger.stream_made_ns = per.trig_made;
    }

    /// Most a trigger's read of another shard's stream waits for its page
    /// before it is asked again: the answer may have found an inbox full.
    pub const TRIGGER_READ_DEADLINE_MS: i64 = 5_000;

    /// A stream another shard owns: ask that shard for a page from the
    /// trigger's cursor, at most one at a time. The page starts runs when it
    /// comes (`takeTriggerPage`).
    fn askTrigger(self: *WorkflowHandler, shard: *Shard, key: []const u8, trigger: *StreamTriggerState, ns: NamespaceId, owner: u16, limit: usize, now_ms: i64) void {
        const raft = shard.raft_node;
        if (raft.role != .leader) return;
        if (trigger.read_seq != 0 and trigger.read_term == raft.current_term and now_ms - trigger.read_sent_ms < TRIGGER_READ_DEADLINE_MS) return;
        const mailboxes = shard.peer_mailboxes orelse return;
        if (owner >= mailboxes.len) return;
        const ns_name = trigger.stream_namespace_owned orelse (shard.namespace_handler.table.nameOf(ns) orelse return);
        const seq = trigger.next_read_seq + 1;
        // From the same made stamp the page is taken against.
        self.seedMade(key, trigger, ns);
        const from = self.readFrom(key, trigger, ns, raft.current_term);
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
        trigger.read_ns = ns;
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
        // The stream it read is the one the trigger reads now.
        if (page.ns != trigger.read_ns.int() or !std.mem.eql(u8, page.source, trigger.stream_name_owned)) return;
        const from = self.readFrom(entry.key_ptr.*, trigger, trigger.read_ns, page.term);
        if (from.timestamp_ms != page.from_a or from.sequence != page.from_b) return;
        if (!self.sameStream(entry.key_ptr.*, trigger, trigger.read_ns, page.made_ns)) return;
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
        self.takeRecords(shard, entry.key_ptr.*, trigger, .{ .ns = trigger.read_ns, .made = page.made_ns, .name = page.source }, payloads, ids, .{ .timestamp_ms = page.last_a, .sequence = page.last_b });
    }

    /// Starts the runs for events read from the trigger's cursor through
    /// `last`, and moves this leader's read position there (`proposed`). A read that found no events but
    /// reached past appends it couldn't read moves the cursor past them, so
    /// the trigger doesn't stall. A start the log refuses (full for now)
    /// leaves the cursor where the read began: the next read takes these
    /// events again, and those already started are the same runs, by their
    /// idempotency keys.
    fn takeRecords(self: *WorkflowHandler, shard: *Shard, def_key: []const u8, trigger: *StreamTriggerState, read: ReadStream, payloads: []const []const u8, ids: []const StreamID, last: StreamID) void {
        const term = shard.raft_node.current_term;
        var chain = self.chainFrom(def_key, trigger, read.ns, read.made, term);
        if (payloads.len > 0) {
            if (trigger.batch_size <= 1) {
                // One run per event
                for (payloads, ids) |payload, event_id| {
                    if (!self.startRunFromTrigger(shard, trigger, read, payload, event_id, &chain)) return;
                }
            } else {
                // Batch mode: collect batch_size events into a JSON array per run
                const batch_limit: usize = @intCast(trigger.batch_size);
                var i: usize = 0;
                while (i < payloads.len) {
                    const end = @min(i + batch_limit, payloads.len);
                    // Out of memory: read again, as for a log full for now.
                    const batch_json = self.buildBatchJson(payloads[i..end]) orelse return;
                    defer self.allocator.free(batch_json);
                    if (!self.startRunFromTrigger(shard, trigger, read, batch_json, ids[end - 1], &chain)) return;
                    i = end;
                }
            }
        }
        trigger.proposed = .{ .at = last, .term = term, .chain = chain };
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
    /// A trigger-started run says the stream it read and the event it came
    /// from (a batch's last): its apply moves the trigger's logged place
    /// there, so a restart or a new leader neither re-runs consumed events
    /// nor skips unconsumed ones. Its idempotency key
    /// ("_trigger:<ns>:<made>:<stream>@<ts>:<seq>") makes the same event,
    /// read again, the same run. False when the start was refused for now:
    /// the trigger reads the event again next tick.
    fn startRunFromTrigger(
        self: *WorkflowHandler,
        shard: *Shard,
        trigger: *const StreamTriggerState,
        read: ReadStream,
        input: []const u8,
        event_id: StreamID,
        chain: *StreamID,
    ) bool {
        // Generate run ID with the definition's slot in its bits
        var run_id_buf: [run_id_mod.MAX_ID_LEN]u8 = undefined;
        const slot = router.nsKeySlot(trigger.ns, trigger.workflow_name_owned);
        const run_id_str = shard.run_id_gen.next(.workflow, slot, shard.raft_node.current_term, &run_id_buf);

        // Room for the longest stream name a trigger may have, and both
        // numbers at their widest.
        var idem_buf: [parser.MAX_TRIGGER_STREAM + "_trigger:::@:".len + 4 * 20]u8 = undefined;
        const idem = std.fmt.bufPrint(&idem_buf, "_trigger:{d}:{d}:{s}@{d}:{d}", .{ read.ns.int(), read.made, read.name, event_id.timestamp_ms, event_id.sequence }) catch {
            log.err("workflow {s}: trigger on stream of {d} bytes not started: its key doesn't fit", .{ trigger.workflow_name_owned, read.name.len });
            return true;
        };
        const at: runs_machine.TriggerAt = .{ .ns = read.ns.int(), .made = read.made, .stream = read.name, .ts = event_id.timestamp_ms, .seq = event_id.sequence, .after_ts = chain.timestamp_ms, .after_seq = chain.sequence };

        _ = self.proposeStart(shard, trigger.ns, run_id_str, trigger.workflow_name_owned, "latest", input, idem, "trigger_started", at) catch |err| {
            if (!shard.quietHalted(err)) log.err("workflow {s}: run {s} not started: {s}", .{ trigger.workflow_name_owned, run_id_str, @errorName(err) });
            // One that can pass (the log full for now, this node not leading
            // or not writing) is read again; one that can't is passed over,
            // or it would hold the trigger for good.
            return switch (err) {
                error.Overloaded, error.NotLeader, error.WritesStopped, error.Halted => false,
                else => true,
            };
        };
        chain.* = event_id;
        return true;
    }

    // ── Plan Health Helpers ─────────────────────────────────────────────

    /// The health record kept under `key` (`healthKey`), made if there is
    /// none; null when out of memory.
    fn healthAt(self: *WorkflowHandler, key: []const u8) ?*plan_types.ExecutorHealth {
        const got = self.plan_health.getOrPut(key) catch return null;
        if (got.found_existing) return got.value_ptr;
        const owned = self.allocator.dupe(u8, key) catch {
            self.plan_health.removeByPtr(got.key_ptr);
            return null;
        };
        got.key_ptr.* = owned;
        got.value_ptr.* = plan_types.ExecutorHealth.initWithName(&.{});
        return got.value_ptr;
    }

    /// A plan executor's health record, if a logged step made one: none is
    /// a closed breaker and full health.
    fn healthOf(self: *WorkflowHandler, ns: NamespaceId, workflow: []const u8, plan_name: []const u8, executor_name: []const u8) ?*const plan_types.ExecutorHealth {
        const key = healthKey(self.allocator, ns, workflow, plan_name, executor_name) catch return null;
        defer self.allocator.free(key);
        return self.plan_health.getPtr(key);
    }

    /// What an executor's breaker admits now: its logged health, with what
    /// `run`'s decision has staged for it so far. Out of memory, the
    /// logged health alone.
    fn admitsNow(self: *WorkflowHandler, run: *const RunRecord, ns: NamespaceId, plan_name: []const u8, executor_name: []const u8, cfg: plan_types.CircuitBreakerConfig, now_ms: i64) plan_types.ExecutorHealth.Admit {
        const key = healthKey(self.allocator, ns, run.workflow_name_owned, plan_name, executor_name) catch return .yes;
        defer self.allocator.free(key);
        var health: plan_types.ExecutorHealth = if (self.plan_health.get(key)) |h| h else .initWithName(&.{});
        for (run.plan_effects.items) |e| if (std.mem.eql(u8, e.key, key)) applyEffect(&health, e);
        return health.admits(cfg, now_ms);
    }

    /// An executor's call ended: counted, and its breaker moved, once the
    /// step that takes the result logs.
    fn stageResult(self: *WorkflowHandler, run: *RunRecord, ns: NamespaceId, plan_name: []const u8, executor: definition.ExecutorConfig, success: bool, now_ms: i64) void {
        self.stagePlanEffect(run, ns, plan_name, executor.name, .{ .result = .{ .success = success, .breaker = executor.breaker } }, now_ms);
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
        const proposed = self.proposeStart(shard, ns, run_id, wf_name, version, input, idempotency_key, event_type, null) catch |err| {
            // Held for room: asked again on a later tick.
            if (err == error.Overloaded) return null;
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
        trigger: ?runs_machine.TriggerAt,
    ) !persistence_mod.ProposeResult {
        _ = self;
        // A run makes history from its start: held while this shard's
        // history lane is past its cap, and the namespace past its share.
        if (shard.historyWait(ns) != null) return error.Overloaded;
        var ns_key_buf: [MAX_START_KEY]u8 = undefined;
        if (KEY_PREFIX + run_id.len > ns_key_buf.len) return error.KeyTooLarge;
        const ns_key = writeKey(&ns_key_buf, ns, run_id);
        // The run is created at the entry's stamp, by the applier.
        const start: runs_machine.Start = .{ .workflow = wf_name, .version = version, .status = .running, .event = event_type, .idem = idempotency_key, .trigger = trigger, .input = input };
        var value_buf: [65536]u8 = undefined;
        const value = start.encode(value_buf[0..65000]) orelse return error.PayloadTooLarge;
        return persistence_mod.proposeEntryIn(shard, .workflow_start, entry_mod.Flags.NONE, ns, ns_key, value);
    }

    const MARKER_EVENT = "history_trimmed";

    /// A cancel reason goes into the run's history and its completion record.
    const MAX_CANCEL_REASON: usize = runs_machine.MAX_COMPLETE_DETAIL;

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
            .workflow_start, .workflow_step, .workflow_signal, .workflow_complete, .workflow_run_delete, .child_start, .child_refuse, .step_call_retry, .step_call_clear, .call_reply, .call_done => &runs_machine.machine,
            else => return error.Halt,
        };
        try contract_machine.applyEntry(m, self.defsRead().store, &self.def_scratch, entry, out);
        self.entryApplied(entry, out, null);
    }

    // ── Definitions ─────────────────────────────────────────────────────

    pub fn defsRead(self: *const WorkflowHandler) contract_view.ReadView {
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

    /// After a workflow entry applied on the leader (`Shard.afterApply`),
    /// its caches (`leadCaches`) follow it: a definition's schedule and
    /// trigger follow its record, and a run's entry keeps `to_step`, the
    /// parked index and the timers, and the schedule's fired slot, as
    /// `workflow.run` now says. A follower keeps none of them.
    /// The leader an entry applied on: its term, and its log's last index.
    pub const Lead = struct { term: u64, last_index: u64 };

    pub fn entryApplied(self: *WorkflowHandler, e: *const entry_mod.Entry, out: *const Result, lead: ?Lead) void {
        const t = std.enums.fromInt(entry_mod.EntryType, e.header.entry_type) orelse return;
        if (t == .workflow_start) if (lead) |l| self.startDecided(e, out, l);
        // A call's message a repair skipped is refused, and its call settled
        // refused as it is: its run is looked at all the same.
        switch (t) {
            .call_reply, .call_done, .step_call_retry => return self.callChanged(t, e),
            else => {},
        }
        if (out.refused() != null) return;
        switch (t) {
            .workflow_create, .workflow_enabled, .workflow_start, .workflow_step, .workflow_signal, .workflow_complete, .workflow_run_delete => {},
            // A run waiting on an action not registered may now go on.
            .action_register => {
                var it = self.parked.keyIterator();
                while (it.next()) |k| self.indexKey(&self.call_dirty, k.*);
                return;
            },
            else => return,
        }
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
        switch (t) {
            .workflow_create, .workflow_enabled => self.syncProducers(cmd.key),
            else => self.runApplied(t, cmd.key),
        }
    }

    /// A schedule's or trigger's start of this leader's term applied, or
    /// was refused.
    fn startDecided(self: *WorkflowHandler, e: *const entry_mod.Entry, out: *const Result, lead: Lead) void {
        if (e.header.term != lead.term) return;
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
        const s = runs_machine.Start.parse(cmd.value) orelse return;
        // A client's or a child's start: its leader keeps nothing of it.
        if (s.trigger == null and !std.mem.eql(u8, s.event, "schedule_started")) return;
        if (cmd.key.len < KEY_PREFIX) return;
        const key = self.nsKey(keyNamespace(cmd.key), s.workflow) orelse return;
        defer self.allocator.free(key);
        const memory = if (out.refused()) |r| r.refusal.reason == .memory_budget else false;
        const quota = if (out.refused()) |r| r.refusal.reason == .history_quota else false;
        // A run made, not an answer from an earlier one.
        var r = out.reader();
        const made = out.refused() == null and (r.int(u8) orelse 0) == 1;
        const now_ms = @import("stdx").time.milliTimestamp();
        if (s.trigger != null) {
            const trigger = self.stream_triggers.getPtr(key) orelse return;
            if (out.refused() != null) return self.triggerRefused(trigger, e, lead, memory, quota, now_ms);
            if (made and e.header.index > trigger.resync_index) {
                trigger.hold.reset(lead.term);
                trigger.quota_hold.reset(lead.term);
            }
        } else if (std.mem.eql(u8, s.event, "schedule_started")) {
            const schedule = self.schedules.getPtr(key) orelse return;
            if (made) {
                schedule.hold.reset(lead.term);
                schedule.quota_hold.reset(lead.term);
            }
            if (!memory and !quota) return;
            // Its slot may be proposed again, once its wait is out.
            const idem = s.idem orelse return;
            const colon = std.mem.lastIndexOfScalar(u8, idem, ':') orelse return;
            const slot = std.fmt.parseInt(i64, idem[colon + 1 ..], 10) catch return;
            if (schedule.proposed_term != lead.term or schedule.proposed_ms != slot) return;
            schedule.proposed_ms = std.math.minInt(i64);
            if (memory) {
                if (schedule.hold.refused(lead.term, now_ms, self.boundShare())) self.noteStartsHeld("schedule", .memory, schedule.ns, schedule.workflow_name_owned, self.shardOf(), self.boundShare());
            } else {
                const part = self.boundPart(schedule.ns);
                if (schedule.quota_hold.refused(lead.term, now_ms, part)) self.noteStartsHeld("schedule", .quota, schedule.ns, schedule.workflow_name_owned, self.shardOf(), part);
            }
        }
    }

    /// A trigger's start this leader proposed was refused: the starts it
    /// proposed after it are refused too, so its next read begins at the
    /// place logged, a new chain. Refusals of the chain it broke, at or
    /// below the log's last index now, are passed over: one refusal is one
    /// re-read, and, for memory, one wait before it.
    fn triggerRefused(self: *WorkflowHandler, trigger: *StreamTriggerState, e: *const entry_mod.Entry, lead: Lead, memory: bool, quota: bool, now_ms: i64) void {
        if (e.header.index <= trigger.resync_index) return;
        trigger.proposed = null;
        trigger.resync_index = lead.last_index;
        if (memory) if (trigger.hold.refused(lead.term, now_ms, self.boundShare())) self.noteStartsHeld("trigger", .memory, trigger.ns, trigger.workflow_name_owned, self.shardOf(), self.boundShare());
        if (quota) {
            const part = self.boundPart(trigger.ns);
            if (trigger.quota_hold.refused(lead.term, now_ms, part)) self.noteStartsHeld("trigger", .quota, trigger.ns, trigger.workflow_name_owned, self.shardOf(), part);
        }
    }

    /// Namespace `ns`'s part bound on this shard, 0 for none.
    fn boundPart(self: *const WorkflowHandler, ns: NamespaceId) u64 {
        return if (@import("../settings/machine.zig").boundQuota(self.defsRead(), ns)) |q| q.part_bytes else 0;
    }

    fn shardOf(self: *const WorkflowHandler) u16 {
        return if (self.defsRead().store.place) |pl| pl.group else 0;
    }

    /// The share bound on this shard, 0 for none.
    fn boundShare(self: *const WorkflowHandler) u64 {
        return if (@import("../settings/machine.zig").boundShare(self.defsRead())) |b| b.share_bytes else 0;
    }

    /// A call's record changed (answered, ended, asked again): the run
    /// that made it is looked at.
    fn callChanged(self: *WorkflowHandler, t: entry_mod.EntryType, e: *const entry_mod.Entry) void {
        var diag: wire_schema.Diagnostic = .{};
        const ns: NamespaceId, const slot: u16, const callee: []const u8 = switch (t) {
            .call_reply => blk: {
                const p = ae.Reply.decodeAll(e.payload, &diag) catch return;
                break :blk .{ .of(p.ns), p.slot, p.run };
            },
            .call_done => blk: {
                const p = ae.Done.decodeAll(e.payload, &diag) catch return;
                break :blk .{ .of(p.ns), p.slot, p.run };
            },
            else => blk: {
                const p = ae.CallRetry.decodeAll(e.payload, &diag) catch return;
                break :blk .{ .of(p.ns), p.slot, p.run };
            },
        };
        const c = calls_mod.callOf(self.defsRead(), ns, slot, callee) orelse return;
        const key = self.nsKey(ns, c.caller_run) orelse return self.lostIndex(c.caller_run);
        defer self.allocator.free(key);
        if (self.parked.contains(key)) self.indexKey(&self.call_dirty, key);
    }

    /// A run its quota hold released, indexed again from its wait as
    /// applied, as an entry of its own would: no wait to `to_step`, a call
    /// to `call_dirty`, a timer or a due signal to `timers`. Gone or ended,
    /// nothing.
    fn reindexReleased(self: *WorkflowHandler, run_key: []const u8) void {
        const run = runs_machine.runOf(self.defsRead(), run_key) orelse return;
        if (appliedStatus(run).isTerminal()) return;
        switch (run.wait) {
            .none => self.indexToStep(run_key),
            .call => self.indexKey(&self.call_dirty, run_key),
            .timer => self.armTimer(run_key, run.wait_due_ms),
            .signal => if (run.wait_due_ms > 0) self.armTimer(run_key, run.wait_due_ms),
        }
    }

    fn runApplied(self: *WorkflowHandler, t: entry_mod.EntryType, run_key: []const u8) void {
        self.unhold(run_key);
        if (self.refusals.fetchRemove(run_key)) |kv| self.allocator.free(kv.key);
        // Gone (deleted): nothing of it stays indexed.
        const run = runs_machine.runOf(self.defsRead(), run_key) orelse {
            self.unindexToStep(run_key);
            unindexKey(self, &self.parked, run_key);
            unindexKey(self, &self.call_dirty, run_key);
            self.dropCallTimer(run_key);
            return;
        };
        const live = !appliedStatus(run).isTerminal();
        if (live and run.wait == .none) self.indexToStep(run_key) else self.unindexToStep(run_key);
        if (live and run.wait == .call) {
            self.indexKey(&self.parked, run_key);
            self.indexKey(&self.call_dirty, run_key);
        } else {
            unindexKey(self, &self.parked, run_key);
            unindexKey(self, &self.call_dirty, run_key);
            self.dropCallTimer(run_key);
        }
        if (live and (run.wait == .timer or (run.wait == .signal and run.wait_due_ms > 0))) self.armTimer(run_key, run.wait_due_ms);
        if (t == .workflow_start) {
            self.followFired(keyNamespace(run_key), run.workflow);
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

    /// Run `run_key`'s timer, popped while `hold` held its step, kept until
    /// the hold clears (`releaseHeld`).
    fn holdTimer(self: *WorkflowHandler, hold: Hold, run_key: []const u8) void {
        var key: []const u8 = undefined;
        if (self.held.getEntry(run_key)) |was| {
            if (std.meta.eql(was.value_ptr.hold, hold)) return;
            key = was.key_ptr.*;
            self.unlist(was.value_ptr.*);
        } else {
            key = self.allocator.dupe(u8, run_key) catch return self.lostIndex(run_key);
            self.held.put(self.allocator, key, undefined) catch {
                self.allocator.free(key);
                return self.lostIndex(run_key);
            };
        }
        const gop = self.held_by.getOrPut(self.allocator, hold) catch {
            // Listed nowhere, it would never be asked: walked again instead.
            return self.dropHeld(run_key);
        };
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        gop.value_ptr.append(self.allocator, key) catch {
            if (gop.value_ptr.items.len == 0) _ = self.held_by.remove(hold);
            return self.dropHeld(run_key);
        };
        self.held.getPtr(run_key).?.* = .{ .hold = hold, .at = @intCast(gop.value_ptr.items.len - 1) };
    }

    /// A held run off its hold's list; the hold goes with its last run.
    fn unlist(self: *WorkflowHandler, h: Held) void {
        const list = self.held_by.getPtr(h.hold) orelse return;
        _ = list.swapRemove(h.at);
        if (h.at < list.items.len) self.held.getPtr(list.items[h.at]).?.at = h.at;
        if (list.items.len == 0) {
            list.deinit(self.allocator);
            _ = self.held_by.remove(h.hold);
        }
    }

    /// A run in `held` that couldn't be listed: out, and its index lost.
    fn dropHeld(self: *WorkflowHandler, run_key: []const u8) void {
        const kv = self.held.fetchRemove(run_key) orelse return;
        self.lostIndex(run_key);
        self.allocator.free(kv.key);
    }

    /// How many runs `hold` holds.
    pub fn holdCount(self: *const WorkflowHandler, hold: Hold) usize {
        return if (self.held_by.get(hold)) |l| l.items.len else 0;
    }

    /// Run `run_key` out of the held set, if it's there.
    fn unhold(self: *WorkflowHandler, run_key: []const u8) void {
        const h = (self.held.get(run_key)) orelse return;
        self.unlist(h);
        const kv = self.held.fetchRemove(run_key).?;
        self.allocator.free(kv.key);
    }

    fn clearHeld(self: *WorkflowHandler) void {
        var it = self.held.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.held.clearRetainingCapacity();
        var lit = self.held_by.valueIterator();
        while (lit.next()) |l| l.deinit(self.allocator);
        self.held_by.clearRetainingCapacity();
        self.quota_eased = .{};
        var rit = self.refusals.keyIterator();
        while (rit.next()) |k| self.allocator.free(k.*);
        self.refusals.clearRetainingCapacity();
    }

    fn holds(shard: *Shard, hold: Hold) bool {
        return switch (hold.kind) {
            .parent => shard.sendWait(hold.parent_shard.?, .main, hold.ns) != null,
            .lane => shard.historyWait(hold.ns) != null,
            .quota => hq.atUsage(.{ .store = &shard.defaultPartition().store }, hold.ns),
        };
    }

    /// What the entry just applied eased, for the next release pass.
    pub fn noteEased(self: *WorkflowHandler, eased: @import("../contract/store.zig").Eased) void {
        self.quota_eased.merge(eased);
    }

    /// Held timers whose hold has cleared, put back on `timers`, due at
    /// `now_ms`: each is asked again against its run's wait as applied, as
    /// any timer is. Every hold is asked once; the runs are walked only
    /// when one clears, and at most `RELEASE_PER_PASS` go back a pass, so
    /// thousands released at once don't take every other run's turn at the
    /// due timers a pass handles. The rest wait for their hold to be asked
    /// again.
    fn releaseHeld(self: *WorkflowHandler, shard: *Shard, now_ms: i64) void {
        var cleared: [32]Hold = undefined;
        var n: usize = 0;
        var eased = self.quota_eased;
        self.quota_eased = .{};
        var hit = self.held_by.keyIterator();
        while (hit.next()) |h| {
            // A quota hold is asked only once its namespace eased: nothing
            // else lowers what holds it.
            if (h.kind == .quota and !eased.has(h.ns.int())) continue;
            if (holds(shard, h.*)) continue;
            if (n == cleared.len) {
                // Asked next pass: kept eased until it is.
                if (h.kind == .quota) self.quota_eased.add(h.ns.int());
                continue;
            }
            cleared[n] = h.*;
            n += 1;
        }
        var m: usize = 0;
        for (cleared[0..n]) |c| {
            while (m < RELEASE_PER_PASS) : (m += 1) {
                const list = self.held_by.getPtr(c) orelse break;
                const k = list.items[list.items.len - 1];
                self.unlist(self.held.get(k).?);
                const kv = self.held.fetchRemove(k).?;
                // A quota hold took runs from every path: each goes back to
                // what its wait as applied needs. A lane's took timers only.
                if (c.kind == .quota) {
                    self.reindexReleased(kv.key);
                    self.allocator.free(kv.key);
                    continue;
                }
                // Its key's ownership moves from the set to the heap.
                self.timers.push(self.allocator, .{ .at_ms = now_ms, .ns_key = kv.key }) catch {
                    self.lostIndex(kv.key);
                    self.allocator.free(kv.key);
                };
            }
            // A cleared quota hold with runs left past this pass's room is
            // asked again next pass.
            if (c.kind == .quota and self.held_by.contains(c)) self.quota_eased.add(c.ns.int());
        }
    }

    /// Held runs put back on `timers` a pass, at most: half the due timers
    /// a pass handles, so other runs' timers keep the rest. A tick runs up
    /// to four passes, so up to 128 a tick; the ratio is what holds.
    const RELEASE_PER_PASS: usize = 32;

    /// Run `run_key`'s step, which a timer may have taken, was refused at
    /// apply. Its timer is armed again against its wait as applied: a
    /// second out, doubling with each refusal in a row up to
    /// `REFUSED_RECHECK_MAX_MS`, with a warning once it gets there.
    fn recheckRefused(self: *WorkflowHandler, run_key: []const u8) void {
        const rec = self.appliedRun(run_key) orelse return;
        if (appliedStatus(rec).isTerminal() or rec.wait_due_ms == 0) return;
        if (rec.wait != .timer and rec.wait != .signal) return;
        const now = @import("stdx").time.milliTimestamp();
        self.armTimer(run_key, now +| self.refusedDelay(run_key));
    }

    /// The wait before run `run_key`'s next recheck, counting this refusal.
    /// Not counted (out of memory): the longest, the safe side.
    fn refusedDelay(self: *WorkflowHandler, run_key: []const u8) i64 {
        const n: u8 = blk: {
            if (self.refusals.getPtr(run_key)) |c| {
                c.* +|= 1;
                break :blk c.*;
            }
            const owned = self.allocator.dupe(u8, run_key) catch return REFUSED_RECHECK_MAX_MS;
            self.refusals.put(self.allocator, owned, 1) catch {
                self.allocator.free(owned);
                return REFUSED_RECHECK_MAX_MS;
            };
            break :blk 1;
        };
        const shift: u6 = @intCast(@min(n - 1, 16));
        const delay = @min(UNLOGGED_RECHECK_MS << shift, REFUSED_RECHECK_MAX_MS);
        if (n == REFUSED_WARN_AT)
            log.warn("workflow run {f}: its timer's step was refused {d} times in a row; looked at again every {d} s", .{ shown(run_key), n, @divTrunc(REFUSED_RECHECK_MAX_MS, 1000) });
        return delay;
    }

    const REFUSED_RECHECK_MAX_MS: i64 = 60_000;
    /// The refusal in a row whose recheck first waits the longest: warned
    /// at once, then not again for that run (the count saturates).
    const REFUSED_WARN_AT: u8 = blk: {
        var n: u8 = 1;
        while ((UNLOGGED_RECHECK_MS << (n - 1)) < REFUSED_RECHECK_MAX_MS) n += 1;
        break :blk n;
    };

    /// `armTimer` for a parked run's call, unless one at or before `at_ms`
    /// is armed already.
    fn armCallTimer(self: *WorkflowHandler, run_key: []const u8, at_ms: i64) void {
        if (self.call_timer.get(run_key)) |armed| if (armed <= at_ms) return;
        self.armTimer(run_key, at_ms);
        if (self.call_timer.getPtr(run_key)) |armed| {
            armed.* = at_ms;
            return;
        }
        const owned = self.allocator.dupe(u8, run_key) catch return;
        self.call_timer.put(self.allocator, owned, at_ms) catch self.allocator.free(owned);
    }

    fn dropCallTimer(self: *WorkflowHandler, run_key: []const u8) void {
        if (self.call_timer.fetchRemove(run_key)) |kv| self.allocator.free(kv.key);
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
        _ = self.commitRun(shard, run_key);
    }

    /// For tests: `run`'s history, all of it, as a read with no deadline
    /// left answers it, onto its copy, counted as logged.
    fn loadHistory(self: *WorkflowHandler, run: *RunRecord, run_key: []const u8, rec: runs_machine.Run.Value) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        var cursor: ?u64 = null;
        while (true) {
            const q: HistoryRequest = .{ .limit = MAX_HISTORY_PAGE, .cursor = cursor };
            const got = try self.readRunHistory(arena.allocator(), run_key, rec, q.window(), q.limit, true);
            const page = switch (got) {
                .page => |p| p,
                else => return error.Unavailable,
            };
            for (page.events) |e| {
                const event_type = try self.allocator.dupe(u8, e.event_type);
                errdefer self.allocator.free(event_type);
                const detail = try self.allocator.dupe(u8, e.detail);
                errdefer self.allocator.free(detail);
                try run.history.append(self.allocator, .{ .event_type_owned = event_type, .detail_owned = detail, .timestamp_ms = e.timestamp_ms });
            }
            cursor = page.more orelse break;
        }
        run.hist_logged = run.history.items.len;
    }

    /// For tests: run `run_key` as a decision would take it, built from
    /// what is applied, with its history. Read only; good until the
    /// handler's deinit.
    pub fn inspect(self: *WorkflowHandler, run_key: []const u8) ?*const RunRecord {
        const rec = runs_machine.runOf(self.defsRead(), run_key) orelse return null;
        var one = std.StringHashMap(RunRecord).init(self.allocator);
        defer one.deinit();
        self.copyRunInto(&one, run_key, rec) catch return null;
        var it = one.iterator();
        const kv = it.next().?;
        // Its history as a read answers it, what landed and what is pending.
        self.loadHistory(kv.value_ptr, run_key, rec) catch {
            self.allocator.free(kv.key_ptr.*);
            self.freeRunRecord(kv.value_ptr);
            return null;
        };
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
            .created_ns = rec.created_ns,
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
        // Its history isn't copied: no decision reads a past event, and a
        // step logs only the events its decision added (`hist_logged` on).
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
        run.call_over_budget_noted = p.call_over_budget_noted;
        run.call_over_quota_noted = p.call_over_quota_noted;
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
            .call_over_budget_noted = run.call_over_budget_noted,
            .call_over_quota_noted = run.call_over_quota_noted,
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
    /// when the definition has none. A leader's cache (`leadCaches`); only
    /// a ready leader fires them (`Shard.tick` runs producers only when it
    /// `leads`).
    fn syncProducers(self: *WorkflowHandler, def_key: []const u8) void {
        const d = self.defOf(def_key) orelse return;
        const ns = keyNamespace(def_key);
        const name = keyName(def_key);
        const built = self.producers_built.get(def_key);
        if (built == null or built.? != d.created_index) {
            // Redefined: its plans may not be the ones kept.
            if (built != null) self.forgetPlans(.{ .ns = ns, .workflow = name });
            self.buildProducers(def_key, d) catch |err| {
                // Not noted as built: the definitions are walked again.
                if (err == error.OutOfMemory) self.producers_lost = true;
                return;
            };
            self.noteBuilt(def_key, d.created_index);
        }
        // An enable after a disable moves the anchor without a create.
        if (self.schedules.getPtr(def_key)) |schedule| schedule.anchor_ms = d.anchor_ms;
        // The slots the starts applied so far fired.
        self.followFired(ns, name);
    }

    fn buildProducers(self: *WorkflowHandler, def_key: []const u8, d: DefinitionRecord) parser.ParseError!void {
        const ns = keyNamespace(def_key);
        const name = keyName(def_key);
        var def = try self.parseStored(d.yaml, def_key);
        defer def.deinit(self.allocator);
        errdefer log.err("workflow {f}: its trigger or schedule could not be built (out of memory); built again from the store", .{shown(def_key)});
        if (def.trigger) |named| {
            var trigger = named;
            if (d.own_namespace) trigger.namespace = null;
            try self.registerStreamTrigger(ns, name, trigger);
        } else try self.unregisterStreamTrigger(ns, name);
        if (def.schedule) |schedule| try self.registerSchedule(ns, name, schedule, d.anchor_ms) else try self.unregisterSchedule(ns, name);
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
    try testing.expect(dispatcher.hasHandler(OpCode.workflow_run_delete));

    try testing.expectEqual(@as(u16, 12), dispatcher.handler_count);
}

test "workflow handler: init and deinit" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    try testing.expectEqual(@as(usize, 0), handler.definitionCount());
    try testing.expectEqual(@as(usize, 0), handler.runCount());
}

test "workflow handler: a re-created trigger keeps the stream it read on the same stream only, and its read numbers always" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    try handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events" });
    const key = try allocator.dupe(u8, testKey(TEST_NS, "wf"));
    defer allocator.free(key);
    handler.stream_triggers.getPtr(key).?.stream_made_ns = 1700;
    handler.stream_triggers.getPtr(key).?.next_read_seq = 3;

    try handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events", .batch_size = 2 });
    const same = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(?u64, 1700), same.stream_made_ns);
    try testing.expectEqual(@as(u64, 3), same.next_read_seq);
    try testing.expectEqual(@as(u32, 2), same.batch_size);

    // Another stream: its own made stamp, from its first read. A page
    // asked for the old one carries a read number this one never asks with.
    try handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "other" });
    const moved = handler.stream_triggers.getPtr(key).?;
    try testing.expectEqual(@as(?u64, null), moved.stream_made_ns);
    try testing.expectEqual(@as(u64, 3), moved.next_read_seq);
}

test "workflow handler: a re-created trigger naming its own namespace, or none, keeps the stream it read; another namespace drops it" {
    const allocator = testing.allocator;
    var namespaces = try NamespaceHandler.init(allocator);
    defer namespaces.deinit();
    try testing.expectEqual(.created, namespaces.applyCreate("team-a"));
    try testing.expectEqual(.created, namespaces.applyCreate("team-b"));
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    handler.namespaces = &namespaces;
    const ns = namespaces.idOf("team-a").?;

    try handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .batch_size = 4 });
    const key = handler.nsKey(ns, "wf").?;
    defer allocator.free(key);
    for ([_]?[]const u8{ "team-a", null, "", "team-a" }) |named| {
        handler.stream_triggers.getPtr(key).?.stream_made_ns = 1700;
        try handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .namespace = named, .batch_size = 4 });
        try testing.expectEqual(@as(?u64, 1700), handler.stream_triggers.getPtr(key).?.stream_made_ns);
    }

    try handler.registerStreamTrigger(ns, "wf", .{ .stream = "events", .namespace = "team-b", .batch_size = 4 });
    try testing.expectEqual(@as(?u64, null), handler.stream_triggers.getPtr(key).?.stream_made_ns);
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
    handler.stream_triggers.getPtr(key).?.stream_made_ns = 1700;
    // Named, as the workflow's own: the entry says so, and the stream stays.
    try handler.replayCreate(testKey(TEST_NS_A, "wf"), head ++ ",\"namespace\":\"team-a\"" ++ tail, 2, entry_mod.Flags.OWN_NAMESPACE, &out);
    try testing.expect(handler.stream_triggers.getPtr(key).?.stream_namespace_owned == null);
    try testing.expectEqual(@as(?u64, 1700), handler.stream_triggers.getPtr(key).?.stream_made_ns);
    // Named, and not its own: another stream, read from its start.
    try handler.replayCreate(testKey(TEST_NS_A, "wf"), head ++ ",\"namespace\":\"team-b\"" ++ tail, 3, 0, &out);
    try testing.expectEqualStrings("team-b", handler.stream_triggers.getPtr(key).?.stream_namespace_owned.?);
    try testing.expectEqual(@as(?u64, null), handler.stream_triggers.getPtr(key).?.stream_made_ns);
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

/// Two waits on one type, one after the other.
const test_two_approvals_json =
    \\{"kind":"Workflow","name":"two-approvals","version":"1.0.0",
    \\"start":{"wait_for_signal":{"type":"approve"},"transitions":{"success":"again"}},
    \\"steps":{"again":{"wait_for_signal":{"type":"approve"},"transitions":{"success":"flo.Completed"}}}}
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
        const history = deliverHistoryToSelf(shard);
        if (!deliverToSelf(shard) and !history) return;
    }
}

/// The history lane's messages to this shard itself, proposed as one
/// inbound, as its driver would: logged, so a history row's locator finds
/// it. True when there were any.
fn deliverHistoryToSelf(shard: *Shard) bool {
    const channels = @import("../contract/channels.zig");
    const ce = @import("../protocol/channel_entries.zig");
    const read: contract_view.ReadView = .{ .store = &shard.defaultPartition().store };
    const next = channels.metaOf(read, 0, .history).next_seq;
    const first = (if (channels.inOf(read, 0, .history)) |i| i.applied else 0) + 1;
    if (first >= next) return false;
    var mb: [256 * 1024]u8 = undefined;
    var w: wire_schema.Writer = .{ .buf = &mb };
    var seq = first;
    // A few at a time, and a big one alone: this harness's log ring holds
    // 256 KiB.
    while (seq < next and seq < first + 8 and (seq == first or w.written().len < 64 * 1024)) : (seq += 1) {
        var kb: [channels.OUT_KEY]u8 = undefined;
        const o = (read.get(channels.OutFam, channels.outKey(&kb, 0, .history, seq)) catch unreachable).?.v;
        ce.Message.encode(&w, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload }) catch break;
    }
    if (seq == first) unreachable;
    const ib = std.testing.allocator.alloc(u8, w.written().len + 64) catch unreachable;
    defer std.testing.allocator.free(ib);
    const inbound = ce.Inbound.encodeInto(ib, .{ .from = 0, .lane = @backingInt(channels.Lane.history), .first_seq = first, .messages = .{ .bytes = w.written(), .count = @intCast(seq - first) } }) catch unreachable;
    _ = shard.raft_node.propose(.channel_inbound, @import("../storage/ual/entry.zig").Flags.NONE, inbound) catch unreachable;
    _ = shard.applyCommitted();
    // Delivered: trimmed from the outbox, as the driver's ack trims it.
    var tb: [ce.Trim.max_size]u8 = undefined;
    const trim = ce.Trim.encodeInto(&tb, .{ .to = 0, .lane = @backingInt(channels.Lane.history), .first = first, .through = seq - 1 }) catch unreachable;
    _ = shard.raft_node.propose(.channel_trim, @import("../storage/ual/entry.zig").Flags.NONE, trim) catch unreachable;
    _ = shard.applyCommitted();
    return true;
}

var test_inbound_index: u64 = 1 << 40;

fn deliverToSelf(shard: *Shard) bool {
    const channels = @import("../contract/channels.zig");
    const ce = @import("../protocol/channel_entries.zig");
    const part = shard.defaultPartition();
    const read: contract_view.ReadView = .{ .store = &part.store };
    const next = channels.metaOf(read, 0, .main).next_seq;
    const first = (if (channels.inOf(read, 0, .main)) |i| i.applied else 0) + 1;
    if (first >= next) return false;
    var mb: [128 * 1024]u8 = undefined;
    var w: wire_schema.Writer = .{ .buf = &mb };
    var seq = first;
    while (seq < next) : (seq += 1) {
        var kb: [channels.OUT_KEY]u8 = undefined;
        const o = (read.get(channels.OutFam, channels.outKey(&kb, 0, .main, seq)) catch unreachable).?.v;
        ce.Message.encode(&w, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload }) catch unreachable;
    }
    var ib: [130 * 1024]u8 = undefined;
    const inbound = ce.Inbound.encodeInto(&ib, .{ .from = 0, .lane = 0, .first_seq = first, .messages = .{ .bytes = w.written(), .count = @intCast(next - first) } }) catch unreachable;
    test_inbound_index += 1;
    var h = std.mem.zeroes(@import("../storage/ual/entry.zig").Header);
    h.entry_type = @backingInt(@import("../storage/ual/entry.zig").EntryType.channel_inbound);
    h.index = test_inbound_index;
    h.timestamp_ns = @intCast(@max(@import("stdx").time.milliTimestamp(), 0) *| std.time.ns_per_ms);
    h.payload_len = @intCast(inbound.len);
    const e: @import("../storage/ual/entry.zig").Entry = .{ .header = h, .payload = inbound };
    var out = persistence_mod.Result.start(&e);
    @import("../contract/machine.zig").applyEntry(&@import("../contract/machines.zig").Channels.machine, &part.store, &part.apply_scratch, &e, &out) catch unreachable;
    // Each message is then an entry applied to the workflows, as the
    // shard's applier has it: a reply wakes its parked caller.
    for (part.store.messages.items) |*m| shard.workflow_handler.entryApplied(&m.entry, &m.result, null);
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

/// For a handler built without a shard: run `run_key`'s history, the chunks
/// its own outbox holds for it then its pending events, oldest first. The
/// events point into the store and `events`' allocator's arena-less copies
/// are not made: good until the next apply.
fn appliedHistoryForTest(handler: *WorkflowHandler, run_key: []const u8, events: *std.ArrayListUnmanaged(WorkflowHandler.HistoryEvent)) !void {
    const channels = @import("../contract/channels.zig");
    const a = handler.def_scratch.allocator();
    const read = handler.defsRead();
    var pb: [channels.CHAN_KEY]u8 = undefined;
    var got: std.ArrayListUnmanaged(WorkflowHandler.SeqEvent) = .empty;
    var it = read.range(channels.OutFam, channels.outPrefix(&pb, 0, .history));
    while (try it.next()) |item| {
        var diag: wire_schema.Diagnostic = .{};
        const c = try @import("../protocol/history_entries.zig").Chunk.decodeAll(try a.dupe(u8, item.value.v.payload), &diag);
        if (!std.mem.eql(u8, c.run, keyName(run_key))) continue;
        try history_read.decodeRecords(a, c.records, .{}, &got);
    }
    const rec = runs_machine.runOf(read, run_key).?;
    try history_read.decodeRecords(a, rec.hist_pending, .{}, &got);
    for (got.items) |e| try events.append(testing.allocator, .{ .event_type_owned = e.event_type, .detail_owned = e.detail, .timestamp_ms = e.timestamp_ms });
}

/// Create a minimal test shard with an actions handler for unit tests.
/// Only `actions_handler` is usable — all other fields are undefined.
fn createTestShard(actions: *ActionsHandler) !Shard {
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
    // A run's history is read through its stream handler.
    const streams = try std.testing.allocator.create(StreamHandler);
    streams.* = StreamHandler.init(std.testing.allocator, partition);
    shard.stream_handler = streams;
    shard.channel = try @import("../contract/channel_driver.zig").Driver.init(std.testing.allocator, 0, 1);
    shard.outbox_caps = .{};
    return shard;
}

fn destroyTestShard(shard: *Shard) void {
    shard.stream_handler.deinit();
    std.testing.allocator.destroy(shard.stream_handler);
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
    // No event, no idempotency key.
    const value = (runs_machine.Start{ .workflow = wf_name, .version = "1.0.0", .status = .running, .event = "", .idem = null, .input = input }).encode(buf).?;
    var out: Result = .{};
    handler.replayStart(ns_key, value, 0, &out) catch |err| std.debug.panic("test run not started: {s}", .{@errorName(err)});
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
    handler.history = shard.stream_handler;
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
        handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    shard.outbox_caps = .{ .other = .{ .shard = 1 << 30, .channel = 1 } };
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

test "step executor: a full sink target on its action's shard holds no step; the step's own lane's pressure does" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-1"), "run-1", "test-wf");
    createTestRun(&handler, testKey(.default, "run-2"), "run-2", "test-wf");
    const now = @import("stdx").time.monotonicMs();
    const Lane = @import("../contract/channels.zig").Lane;
    // The action's shard (this one) has a sink target full, as its answers
    // on the sink lane say: a step's call starts on the main lane, so it
    // calls.
    const sink = &shard.channel.peers[0].lanes[@backingInt(Lane.sink)];
    sink.pressure[0] = .{ .shard = 0, .cause = @backingInt(@import("../contract/channel_driver.zig").Cause.target_full) };
    sink.pressure_len = 1;
    sink.pressure_ms = now;
    handler.advanceWorkflow(&shard, testKey(.default, "run-1"));
    settleTest(&shard);
    try testing.expect(handler.inspect(testKey(.default, "run-1")).?.pending_action_run_id_owned != null);
    // The same said on the main lane holds the next step.
    const main = &shard.channel.peers[0].lanes[@backingInt(Lane.main)];
    main.pressure = sink.pressure;
    main.pressure_len = 1;
    main.pressure_ms = now;
    handler.advanceWorkflow(&shard, testKey(.default, "run-2"));
    settleTest(&shard);
    const held = handler.inspect(testKey(.default, "run-2")).?;
    try testing.expect(held.waiting_for_room);
    try testing.expect(held.pending_action_run_id_owned == null);
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
    handler.history = shard.stream_handler;
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
    _ = handler.commitRun(&shard, testKey(.default, "run-r"));
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
    handler.history = shard.stream_handler;
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
    shard.outbox_caps = .{ .other = .{ .shard = 0, .channel = 1 << 30 } };
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
    handler.history = shard.stream_handler;
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

/// The leader's health record for an executor of a plan in the default
/// namespace, made in the shard's current term.
fn testHealth(handler: *WorkflowHandler, shard: *const Shard, workflow: []const u8, plan_name: []const u8, executor: []const u8) *plan_types.ExecutorHealth {
    handler.planMemory(shard);
    const key = WorkflowHandler.healthKey(testing.allocator, .default, workflow, plan_name, executor) catch unreachable;
    defer testing.allocator.free(key);
    return handler.healthAt(key).?;
}

test "step executor: a plan's executor waits when the only other's breaker is open, and not when it could run" {
    const allocator = testing.allocator;
    for ([_][]const u8{ "health-weighted", "static-order" }) |selection| for ([_]bool{ true, false }) |open| {
        var jb: [test_plan_exhaust_workflow_json.len]u8 = undefined;
        _ = std.mem.replace(u8, test_plan_exhaust_workflow_json, "health-weighted", selection, &jb);
        const json = jb[0..std.mem.replacementSize(u8, test_plan_exhaust_workflow_json, "health-weighted", selection)];
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        var actions = ActionsHandler.init(allocator);
        defer actions.deinit();
        var shard = try createTestShard(&actions);
        defer destroyTestShard(&shard);
        handler.store = &shard.defaultPartition().store;
        handler.history = shard.stream_handler;
        shard.workflow_handler = &handler;

        createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", json);
        createTestRun(&handler, testKey(.default, "run-b"), "run-b", "plan-wf");
        const paypal = testHealth(&handler, &shard, "plan-wf", "charge", "paypal");
        if (open) {
            paypal.breaker_state = .open;
            paypal.breaker_opened_at_ms = @import("stdx").time.milliTimestamp();
        }
        // stripe is tried first either way: as listed first, and as the
        // healthier when paypal's breaker is open. Under any selection.
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
    };
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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

test "workflow duty: a timer due while the history lane holds steps fires once the lane drains" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    createTestRun(&handler, testKey(.default, "noisy"), "noisy", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, testKey(.default, "due"), .{ .signal = "go", .due = now - 1 });
    // Another run's chunks on the lane, past its cap: steps wait.
    shard.outbox_caps.other.channel = 64;
    signalTestRun(&handler, testKey(.default, "noisy"), 4, 1000);
    try testing.expect(shard.historyWait(.default) != null);
    // Its timer comes due while steps wait: kept for when the hold clears,
    // not lost, and not copied to find out.
    const taken = handler.taken;
    handler.checkPendingActions(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "due")).?.status);
    try testing.expectEqual(@as(usize, 0), handler.timers.count());
    try testing.expect(handler.held.contains(testKey(.default, "due")));
    try testing.expectEqual(@as(usize, 1), handler.holdCount(.{ .ns = .default }));
    try testing.expect(handler.hasDutyWork());
    for (0..3) |_| handler.checkPendingActions(&shard);
    try testing.expectEqual(taken, handler.taken);
    // A new term starts its caches from the store: the set goes, and the
    // walk arms the run's timer again.
    handler.dropStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 0), handler.held.count());
    try testing.expectEqual(@as(usize, 0), handler.held_by.count());
    handler.leadCaches(shard.raft_node.current_term);
    while (handler.walk != .idle) handler.leadCaches(shard.raft_node.current_term);
    try testing.expect(handler.timers.count() >= 1);
    handler.checkPendingActions(&shard);
    try testing.expect(handler.held.contains(testKey(.default, "due")));
    // Drained: the wait it timed out of ends it.
    settleTest(&shard);
    try testing.expect(shard.historyWait(.default) == null);
    for (0..3) |_| {
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.inspect(testKey(.default, "due")).?.status);
}

test "workflow duty: runs held by one namespace's history share don't delay another namespace's timer" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestDef(&handler, testKey(TEST_NS_A, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "noisy"), "noisy", "test-wf");
    createTestRun(&handler, testKey(TEST_NS_A, "quiet"), "quiet", "test-wf");
    // More runs due in the noisy namespace than a pass takes, and the
    // quiet run's timer due well after them.
    const N = 100;
    const now = @import("stdx").time.milliTimestamp() + 60_000;
    var kb: [N][KEY_PREFIX + 8]u8 = undefined;
    for (0..N) |i| {
        var nb: [8]u8 = undefined;
        const k = writeKey(&kb[i], .default, std.fmt.bufPrint(&nb, "r{d}", .{i}) catch unreachable);
        createTestRun(&handler, k, "", "test-wf");
        testSetWait(&handler, &shard, k, .{ .signal = "go", .due = now });
    }
    testSetWait(&handler, &shard, testKey(TEST_NS_A, "quiet"), .{ .signal = "go", .due = now + 5_000 });
    // The noisy namespace's chunks on the lane past its share: its steps
    // wait, the quiet namespace's don't.
    shard.outbox_caps.other.channel = 64;
    signalTestRun(&handler, testKey(.default, "noisy"), 4, 1000);
    try testing.expect(shard.historyWait(.default) != null);
    try testing.expect(shard.historyWait(TEST_NS_A) == null);
    // Held as they come due, none copied to find out.
    const taken0 = handler.taken;
    handler.checkPendingActionsAt(&shard, now + 1);
    handler.checkPendingActionsAt(&shard, now + 2);
    try testing.expectEqual(taken0, handler.taken);
    try testing.expectEqual(@as(usize, N), handler.held.count());
    try testing.expectEqual(@as(usize, N), handler.holdCount(.{ .ns = .default }));
    // Once the quiet run's timer is due, one pass fires it: the held runs
    // are neither ahead of it nor copied again.
    const taken = handler.taken;
    handler.checkPendingActionsAt(&shard, now + 5_001);
    try testing.expectEqual(taken + 1, handler.taken);
    try testing.expectEqual(@as(usize, N), handler.held.count());
    // The lane drains: the hold clears, and every held run's timer fires.
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.inspect(testKey(TEST_NS_A, "quiet")).?.status);
    try testing.expect(shard.historyWait(.default) == null);
    // Back in batches: a pass puts back `RELEASE_PER_PASS`, not all.
    handler.checkPendingActionsAt(&shard, now + 5_002);
    try testing.expectEqual(N - WorkflowHandler.RELEASE_PER_PASS, handler.held.count());
    settleTest(&shard);
    for (0..8) |i| {
        handler.checkPendingActionsAt(&shard, now + 5_003 + @as(i64, @intCast(i)));
        settleTest(&shard);
    }
    try testing.expectEqual(@as(usize, 0), handler.held.count());
    try testing.expectEqual(@as(usize, 0), handler.held_by.count());
    for (kb[0..N], 0..) |*k, i| {
        var nb: [8]u8 = undefined;
        const name = std.fmt.bufPrint(&nb, "r{d}", .{i}) catch unreachable;
        try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.inspect(k[0 .. KEY_PREFIX + name.len]).?.status);
    }
}

test "workflow duty: a held run leaves the held set when an entry of its own applies" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    createTestRun(&handler, testKey(.default, "noisy"), "noisy", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, testKey(.default, "due"), .{ .signal = "go", .due = now + 10 });
    shard.outbox_caps.other.channel = 64;
    signalTestRun(&handler, testKey(.default, "noisy"), 4, 1000);
    handler.checkPendingActionsAt(&shard, now + 20);
    try testing.expect(handler.held.contains(testKey(.default, "due")));
    // The signal it waits on applies: it leaves, and its hold's count goes.
    try testSignal(&handler, testKey(.default, "due"), "go");
    try testing.expect(!handler.held.contains(testKey(.default, "due")));
    try testing.expectEqual(@as(usize, 0), handler.held_by.count());
}

test "workflow duty: a timer's step refused at apply is armed again" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, testKey(.default, "due"), .{ .signal = "go", .due = now - 1 });
    // Its timer pops and its step is logged, not yet applied.
    handler.checkPendingActionsAt(&shard, now);
    try testing.expectEqual(@as(usize, 0), handler.timers.count());
    try testing.expect(handler.stepping.contains(testKey(.default, "due")));
    // That step refused at apply: nothing of the run applied.
    const key = testKey(.default, "due");
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = .default, .key_length = @intCast(key.len), .value_length = 0, .key = key, .value = "" };
    var pb: [128]u8 = undefined;
    const len = cmd.serialize(&pb).?;
    const entry: entry_mod.Entry = .{ .header = .{ .magic = 0, .version = 0, .entry_type = @backingInt(entry_mod.EntryType.workflow_step), .flags = 0, .term = shard.raft_node.current_term, .index = shard.raft_node.log.lastIndex(), .timestamp_ns = 0, .payload_len = @intCast(len), .crc32c = 0 }, .payload = pb[0..len] };
    var out: Result = .{};
    out.refuse(.bad_request, .unclassified, "a step's counters are past their bound");
    handler.startApplied(&shard, &entry, &out);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    try testing.expect(handler.timers.peek().?.at_ms >= now + WorkflowHandler.UNLOGGED_RECHECK_MS);
}

/// Run `key`'s step, logged and not applied, refused at apply.
fn testRefuseStep(handler: *WorkflowHandler, shard: *Shard, key: []const u8) void {
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = .default, .key_length = @intCast(key.len), .value_length = 0, .key = key, .value = "" };
    var pb: [128]u8 = undefined;
    const len = cmd.serialize(&pb).?;
    const entry: entry_mod.Entry = .{ .header = .{ .magic = 0, .version = 0, .entry_type = @backingInt(entry_mod.EntryType.workflow_step), .flags = 0, .term = shard.raft_node.current_term, .index = shard.raft_node.log.lastIndex(), .timestamp_ns = 0, .payload_len = @intCast(len), .crc32c = 0 }, .payload = pb[0..len] };
    var out: Result = .{};
    out.refuse(.bad_request, .unclassified, "a step's counters are past their bound");
    handler.startApplied(shard, &entry, &out);
}

test "workflow duty: a timer's step refused every time backs off to a minute, and starts over once the run moves" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const key = testKey(.default, "due");
    createTestRun(&handler, key, "due", "test-wf");
    testSetWait(&handler, &shard, key, .{ .signal = "go", .due = @import("stdx").time.milliTimestamp() - 1 });
    // Each refusal in a row doubles the wait, up to the cap.
    const want = [_]i64{ 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000 };
    var at: i64 = @import("stdx").time.milliTimestamp();
    for (want) |delay| {
        handler.checkPendingActionsAt(&shard, at);
        try testing.expect(handler.stepping.contains(key));
        const before = @import("stdx").time.milliTimestamp();
        testRefuseStep(&handler, &shard, key);
        try testing.expectEqual(@as(usize, 1), handler.timers.count());
        at = handler.timers.peek().?.at_ms;
        try testing.expect(at >= before + delay);
        try testing.expect(at < before + delay + 1_000);
    }
    try testing.expectEqual(@as(?u8, want.len), handler.refusals.get(key));
    // An entry of its own applies: the count goes.
    try testSignal(&handler, key, "go");
    try testing.expect(handler.refusals.get(key) == null);
}

/// Fails the next allocation it is told to, and no other.
const FailOnce = struct {
    inner: Allocator,
    fail_next: bool = false,
    failed: bool = false,
    fn allocator(self: *FailOnce) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *FailOnce = @ptrCast(@alignCast(ctx));
        if (self.fail_next) {
            self.fail_next = false;
            self.failed = true;
            return null;
        }
        return self.inner.rawAlloc(len, a, ra);
    }
    fn resize(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) bool {
        const self: *FailOnce = @ptrCast(@alignCast(ctx));
        return self.inner.rawResize(m, a, n, ra);
    }
    fn remap(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
        const self: *FailOnce = @ptrCast(@alignCast(ctx));
        return self.inner.rawRemap(m, a, n, ra);
    }
    fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *FailOnce = @ptrCast(@alignCast(ctx));
        self.inner.rawFree(m, a, ra);
    }
};

test "workflow duty: a timer's run not copied (out of memory) is armed again, without walking the runs" {
    var fail: FailOnce = .{ .inner = testing.allocator };
    var handler = WorkflowHandler.init(fail.allocator());
    defer handler.deinit();
    var actions = ActionsHandler.init(testing.allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const key = testKey(.default, "due");
    createTestRun(&handler, key, "due", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, key, .{ .signal = "go", .due = now - 1 });
    // The pass's first allocation is the run's copy.
    const taken = handler.taken;
    fail.fail_next = true;
    handler.checkPendingActionsAt(&shard, now);
    try testing.expect(fail.failed);
    try testing.expectEqual(taken + 1, handler.taken);
    try testing.expect(!handler.stepping.contains(key));
    try testing.expect(!handler.rewatch);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    try testing.expectEqual(now + WorkflowHandler.UNLOGGED_RECHECK_MS, handler.timers.peek().?.at_ms);
    // Then it fires.
    handler.checkPendingActionsAt(&shard, now + WorkflowHandler.UNLOGGED_RECHECK_MS);
    try testing.expect(handler.stepping.contains(key));
}

test "workflow duty: a hold that clears puts back only its own runs" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "noisy"), "noisy", "test-wf");
    shard.outbox_caps.other.channel = 64;
    signalTestRun(&handler, testKey(.default, "noisy"), 4, 1000);
    try testing.expect(shard.historyWait(.default) != null);
    try testing.expect(shard.historyWait(TEST_NS_A) == null);
    // Two runs under a hold that holds, two under one that doesn't.
    handler.holdTimer(.{ .ns = .default }, testKey(.default, "a"));
    handler.holdTimer(.{ .ns = .default }, testKey(.default, "b"));
    handler.holdTimer(.{ .ns = TEST_NS_A }, testKey(TEST_NS_A, "c"));
    handler.holdTimer(.{ .ns = TEST_NS_A }, testKey(TEST_NS_A, "d"));
    // Held again under the same hold: still once.
    handler.holdTimer(.{ .ns = TEST_NS_A }, testKey(TEST_NS_A, "d"));
    try testing.expectEqual(@as(usize, 2), handler.holdCount(.{ .ns = TEST_NS_A }));
    const now = @import("stdx").time.milliTimestamp();
    handler.releaseHeld(&shard, now);
    try testing.expectEqual(@as(usize, 2), handler.held.count());
    try testing.expect(handler.held.contains(testKey(.default, "a")));
    try testing.expect(handler.held.contains(testKey(.default, "b")));
    try testing.expectEqual(@as(usize, 2), handler.holdCount(.{ .ns = .default }));
    try testing.expect(handler.holdCount(.{ .ns = TEST_NS_A }) == 0);
    try testing.expectEqual(@as(usize, 2), handler.timers.count());
    try testing.expectEqual(now, handler.timers.peek().?.at_ms);
}

test "workflow duty: a step whose events are past its buffer ends its run too large to log" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "big"), "big", "test-wf");
    const run = handler.takeRun(&shard, testKey(.default, "big")).?;
    // 40 events of 2 KiB: past the 64 KiB events buffer, under 64 events.
    const detail: [2048]u8 = @splat('d');
    for (0..40) |_| handler.addHistoryEvent(run, "noise", &detail, 1);
    try testing.expectEqual(WorkflowHandler.Committed.logged, handler.commitRun(&shard, testKey(.default, "big")));
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, handler.inspect(testKey(.default, "big")).?.status);
}

test "workflow duty: a child's end its timer decided, held for its parent's outbox, is logged once there is room" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "kid"), "kid", "test-wf");
    createTestRun(&handler, testKey(.default, "caller"), "caller", "test-wf");
    // A parent waits on "kid": its end goes to the parent's slot.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var ks = contract_view.KeySet.init(arena.allocator());
        var view = contract_view.View.init(&shard.defaultPartition().store, arena.allocator(), &ks);
        view.strict = false;
        var kb: [64]u8 = undefined;
        const k = try contract_view.idKey(&kb, .default, "kid");
        try view.put(children_mod.Parents, k, .{ .v = .{ .parent_slot = 0, .parent_run = "par", .parent_workflow = "par-wf", .workflow = "test-wf", .version = "", .input = "", .state = .started } });
        try view.commit();
    }
    // Its wait times out to its end.
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, testKey(.default, "kid"), .{ .signal = "go", .due = now - 1, .to = "flo.Completed" });
    // The parent's outbox at its cap: one call's start left in it.
    handler.advanceWorkflow(&shard, testKey(.default, "caller"));
    settleTest(&shard);
    shard.outbox_caps = .{ .other = .{ .shard = 1 << 30, .channel = 1 } };
    try testing.expect(shard.sendWait(0, .main, .default) != null);
    // Due while held: its end isn't logged, and its timer is kept for its
    // parent's outbox.
    const before = shard.raft_node.log.lastIndex();
    handler.checkPendingActions(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(WorkflowHandler.Hold{ .ns = .default, .kind = .parent, .parent_shard = 0 }, handler.held.get(testKey(.default, "kid")).?.hold);
    // Room: its end is logged.
    shard.outbox_caps = .{};
    handler.checkPendingActions(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, handler.inspect(testKey(.default, "kid")).?.status);
}

test "workflow duty: a timer's step the log doesn't take is tried again, once a second, not lost" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "due"), "due", "test-wf");
    const now = @import("stdx").time.milliTimestamp();
    testSetWait(&handler, &shard, testKey(.default, "due"), .{ .signal = "go", .due = now - 1 });
    shard.raft_node.writes_stopped = true;
    const before = shard.raft_node.log.lastIndex();
    handler.checkPendingActionsAt(&shard, now);
    // Not taken, and armed again, one entry, a second out.
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    try testing.expect(handler.timers.peek().?.at_ms >= now + WorkflowHandler.UNLOGGED_RECHECK_MS);
    // Passes before it is due do nothing and add no entry.
    for (0..5) |_| handler.checkPendingActionsAt(&shard, now);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    // Halted: the same, and counted on the halted shard's line.
    shard.raft_node.writes_stopped = false;
    shard.raft_node.halt_at = .{ .index = 1, .term = 1, .crc = 0 };
    const refusals = shard.halted_refusals;
    handler.checkPendingActionsAt(&shard, now + WorkflowHandler.UNLOGGED_RECHECK_MS);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(refusals + 1, shard.halted_refusals);
    try testing.expectEqual(@as(usize, 1), handler.timers.count());
    shard.raft_node.halt_at = null;
    handler.checkPendingActionsAt(&shard, now + 2 * WorkflowHandler.UNLOGGED_RECHECK_MS);
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.timed_out, handler.inspect(testKey(.default, "due")).?.status);
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    @memset(value[off..][0..5], 0); // no event type, no idempotency key, no trigger
    off += 5;
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
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = 1, .max_concurrent = 2 }, 0);
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

    const c: runs_machine.Complete = .{ .status = .cancelled, .output = null, .event = "workflow_cancelled", .detail = "stop", .workflow = "wf", .created_ns = handler.appliedRun(testKey(TEST_NS_A, "r1")).?.created_ns };
    var record_buf: [128]u8 = undefined;
    const record = c.encode(&record_buf).?;
    // Applied twice (a second cancel): one run finished.
    try handler.replayComplete(testKey(TEST_NS_A, "r1"), record, 7, &out);
    try handler.replayComplete(testKey(TEST_NS_A, "r1"), record, 7, &out);
    try testing.expectEqual(@as(?u32, 1), activeOf(&handler, testKey(TEST_NS_A, "wf")));
    // Room now: it fires once.
    try testing.expectEqual(@as(usize, 1), runsOf(&handler, &shard));
    // The ended run deleted through the same seam: gone.
    const r1 = handler.appliedRun(testKey(TEST_NS_A, "r1")).?;
    const d: runs_machine.Delete = .{ .workflow = r1.workflow, .idem = r1.idempotency_key, .created_ns = r1.created_ns };
    var del_buf: [128]u8 = undefined;
    try handler.applyDirect(.workflow_run_delete, 0, testKey(TEST_NS_A, "r1"), d.encode(&del_buf).?, 8, &out);
    try testing.expect(out.refused() == null);
    try testing.expect(handler.appliedRun(testKey(TEST_NS_A, "r1")) == null);
}

/// The runs of workflow `def_key` not ended, as applied; null before any started.
fn activeOf(handler: *WorkflowHandler, def_key: []const u8) ?u32 {
    const per = runs_machine.perDefOf(handler.defsRead(), def_key) orelse return null;
    return per.active;
}

/// A workflow_start's value, as `proposeStart` encodes it.
/// A trigger's start of run `wf` from event (`ts`, `seq`) of stream
/// `stream` in `ns`, made at `made`, with its key.
fn triggerStartValue(buf: []u8, key_buf: []u8, wf: []const u8, ns: NamespaceId, made: u64, stream: []const u8, ts: u64, seq: u64, after: [2]u64) []const u8 {
    const idem = std.fmt.bufPrint(key_buf, "_trigger:{d}:{d}:{s}@{d}:{d}", .{ ns.int(), made, stream, ts, seq }) catch unreachable;
    return (runs_machine.Start{ .workflow = wf, .version = "1", .status = .running, .event = "trigger_started", .idem = idem, .trigger = .{ .ns = ns.int(), .made = made, .stream = stream, .ts = ts, .seq = seq, .after_ts = after[0], .after_seq = after[1] }, .input = "" }).encode(buf).?;
}

fn startValue(buf: []u8, wf: []const u8, event_type: []const u8, idem: []const u8) []const u8 {
    // A trigger's start says where it read.
    const at: ?runs_machine.TriggerAt = if (std.mem.eql(u8, event_type, "trigger_started")) .{ .ns = NamespaceId.default.int(), .made = 1, .stream = "ev", .ts = 1, .seq = 0, .after_ts = 0, .after_seq = 0 } else null;
    return (runs_machine.Start{ .workflow = wf, .version = "1", .status = .running, .event = event_type, .idem = if (idem.len > 0) idem else null, .trigger = at, .input = "" }).encode(buf).?;
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
    handler.history = shard.stream_handler;
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
    try handler.replayStart(testKey(TEST_NS_A, "r-sched"), startValue(&buf, "wf", "schedule_started", "_schedule:16:wf:4000"), 4600, &out);
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
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;

    // Created ten intervals ago; slot 0 fired then, nothing since.
    const interval: i64 = 3_600_000;
    const now = @import("stdx").time.milliTimestamp();
    const anchor = now - 10 * interval;
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = interval, .max_concurrent = 0 }, anchor);
    var buf: [256]u8 = undefined;
    var key_buf: [128]u8 = undefined;
    var out: Result = .{};
    const slot0 = try std.fmt.bufPrint(&key_buf, "_schedule:16:wf:{d}", .{anchor});
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
    const key = try std.fmt.bufPrint(&key_buf, "_schedule:16:wf:{d}", .{latest});
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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

const SelectionOpts = struct {
    /// How many executors, from the first, have a breaker.
    breakers: usize = 1,
    /// Their failures to open, and then open for an hour.
    threshold: u32 = 1,
    /// The step's input is the run's `s` twice.
    doubled_input: bool = false,
};

/// A plan of `executors` executors e0, e1, ..., by `selection`, each tried
/// once a pass; the first `opts.breakers` with a breaker.
fn selectionWorkflowJson(buf: []u8, selection: []const u8, executors: usize, opts: SelectionOpts) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.print("{{\"kind\":\"Workflow\",\"name\":\"sel-wf\",\"version\":\"1.0.0\",\"plans\":{{\"p\":{{\"selection\":\"{s}\",\"executors\":[", .{selection}) catch unreachable;
    for (0..executors) |i| {
        if (i > 0) w.writeAll(",") catch unreachable;
        w.print("{{\"name\":\"e{d}\",\"run\":\"@actions/e{d}\",\"retry\":{{\"max_attempts\":1}}", .{ i, i }) catch unreachable;
        if (i < opts.breakers) w.print(",\"breaker\":{{\"failure_threshold\":{d},\"cooldown_ms\":3600000}}", .{opts.threshold}) catch unreachable;
        w.writeAll("}") catch unreachable;
    }
    w.writeAll("]}},\"start\":{\"run\":\"@plan/p\",") catch unreachable;
    if (opts.doubled_input) w.writeAll("\"input_mapping\":\"{\\\"a\\\":\\\"$.input.s\\\",\\\"b\\\":\\\"$.input.s\\\"}\",") catch unreachable;
    w.writeAll("\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}") catch unreachable;
    return w.buffered();
}

/// Runs one pass of a fresh run named `name`, completing each task with
/// `outcome`: the executors it started, in order, as "e0e2..." into `buf`.
fn runSelectionPass(handler: *WorkflowHandler, shard: *Shard, name: []const u8, outcome: []const u8, buf: []u8) []const u8 {
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, name);
    createTestRun(handler, key, name, "sel-wf");
    handler.advanceWorkflow(shard, key);
    settleTest(shard);
    var n: usize = 0;
    while (n < 16) : (n += 1) {
        const run = handler.inspect(key).?;
        if (run.status.isTerminal()) break;
        const rid = run.pending_action_run_id_owned orelse break;
        completeTestActionWith(shard, rid, outcome);
        handler.checkPendingActions(shard);
        settleTest(shard);
    }
    // One entry per executor: a retry starts the same one again.
    var pos: usize = 0;
    var last: []const u8 = "";
    for (handler.inspect(key).?.history.items) |evt| {
        if (!std.mem.eql(u8, evt.event_type_owned, "plan_executor_start")) continue;
        if (std.mem.eql(u8, evt.detail_owned, last)) continue;
        last = evt.detail_owned;
        @memcpy(buf[pos..][0..evt.detail_owned.len], evt.detail_owned);
        pos += evt.detail_owned.len;
    }
    return buf[0..pos];
}

const SelectionFixture = struct {
    handler: WorkflowHandler,
    actions: ActionsHandler,
    shard: Shard,

    fn init(self: *SelectionFixture, selection: []const u8, executors: usize, opts: SelectionOpts) !void {
        self.handler = WorkflowHandler.init(testing.allocator);
        self.actions = ActionsHandler.init(testing.allocator);
        self.shard = try createTestShard(&self.actions);
        self.handler.store = &self.shard.defaultPartition().store;
        self.handler.history = self.shard.stream_handler;
        self.shard.workflow_handler = &self.handler;
        var nb: [8]u8 = undefined;
        for (0..executors) |i| registerTestAction(&self.shard, std.fmt.bufPrint(&nb, "e{d}", .{i}) catch unreachable);
        var jb: [2048]u8 = undefined;
        createTestDef(&self.handler, testKey(.default, "sel-wf"), "sel-wf", selectionWorkflowJson(&jb, selection, executors, opts));
    }

    /// e`i`'s health record, if a logged step made one.
    fn health(self: *SelectionFixture, i: usize) ?*const plan_types.ExecutorHealth {
        var nb: [8]u8 = undefined;
        return self.handler.healthOf(.default, "sel-wf", "p", std.fmt.bufPrint(&nb, "e{d}", .{i}) catch unreachable);
    }

    fn deinit(self: *SelectionFixture) void {
        destroyTestShard(&self.shard);
        self.actions.deinit();
        self.handler.deinit();
    }
};

test "plan selection: round-robin starts one executor further along each pass, and again at the first in a new term" {
    var f: SelectionFixture = undefined;
    try f.init("round-robin", 3, .{ .threshold = 100 });
    defer f.deinit();
    var buf: [64]u8 = undefined;
    var nb: [8]u8 = undefined;
    // Every executor fails: each pass's whole order shows.
    for ([_][]const u8{ "e0e1e2", "e1e2e0", "e2e0e1", "e0e1e2" }, 0..) |want, i|
        try testing.expectEqualStrings(want, runSelectionPass(&f.handler, &f.shard, std.fmt.bufPrint(&nb, "rr{d}", .{i}) catch unreachable, "failure", &buf));
    // A new term's leader starts its count at 0.
    f.shard.raft_node.current_term += 1;
    try testing.expectEqualStrings("e0e1e2", runSelectionPass(&f.handler, &f.shard, "rr-term", "failure", &buf));
}

test "plan selection: a pass resumed by a new leader goes on in its logged order; the next starts at the first" {
    var f: SelectionFixture = undefined;
    try f.init("round-robin", 3, .{ .threshold = 100 });
    defer f.deinit();
    var buf: [64]u8 = undefined;
    _ = runSelectionPass(&f.handler, &f.shard, "first", "success", &buf);
    // The second pass starts at e1; its first call fails.
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, "mid");
    createTestRun(&f.handler, key, "mid", "sel-wf");
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    completeTestActionWith(&f.shard, f.handler.inspect(key).?.pending_action_run_id_owned.?, "failure");
    f.handler.checkPendingActions(&f.shard);
    settleTest(&f.shard);
    // A new leader: the pass goes on with e2, then e0, as logged.
    f.shard.raft_node.current_term += 1;
    for (0..4) |_| {
        const run = f.handler.inspect(key).?;
        if (run.status.isTerminal()) break;
        completeTestActionWith(&f.shard, run.pending_action_run_id_owned.?, "failure");
        f.handler.checkPendingActions(&f.shard);
        settleTest(&f.shard);
    }
    var pos: usize = 0;
    var last: []const u8 = "";
    for (f.handler.inspect(key).?.history.items) |evt| {
        if (!std.mem.eql(u8, evt.event_type_owned, "plan_executor_start") or std.mem.eql(u8, evt.detail_owned, last)) continue;
        last = evt.detail_owned;
        @memcpy(buf[pos..][0..evt.detail_owned.len], evt.detail_owned);
        pos += evt.detail_owned.len;
    }
    try testing.expectEqualStrings("e1e2e0", buf[0..pos]);
    try testing.expectEqualStrings("e0e1e2", runSelectionPass(&f.handler, &f.shard, "next", "failure", &buf));
}

test "plan selection: a pass not logged doesn't move round-robin's start on" {
    var f: SelectionFixture = undefined;
    try f.init("round-robin", 3, .{ .threshold = 100 });
    defer f.deinit();
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, "held");
    createTestRun(&f.handler, key, "held", "sel-wf");
    f.shard.raft_node.writes_stopped = true;
    const before = f.shard.raft_node.log.lastIndex();
    for (0..2) |_| f.handler.advanceWorkflow(&f.shard, key);
    try testing.expectEqual(before, f.shard.raft_node.log.lastIndex());
    f.shard.raft_node.writes_stopped = false;
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    // The logged pass started at e0, and the next at e1.
    const items = f.handler.inspect(key).?.history.items;
    try testing.expectEqualStrings("e0", items[items.len - 1].detail_owned);
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("e1e2e0", runSelectionPass(&f.handler, &f.shard, "after", "failure", &buf));
}

test "plan selection: random tries every executor once a pass, shuffled, the same way for the same shard and term" {
    var orders: [2][8][4]u8 = undefined;
    for (&orders) |*run_set| {
        var f: SelectionFixture = undefined;
        try f.init("random", 4, .{ .threshold = 100 });
        defer f.deinit();
        var buf: [64]u8 = undefined;
        var nb: [8]u8 = undefined;
        for (run_set, 0..) |*o, i| {
            // Every executor fails: the whole order shows.
            const got = runSelectionPass(&f.handler, &f.shard, std.fmt.bufPrint(&nb, "rn{d}", .{i}) catch unreachable, "failure", &buf);
            try testing.expectEqual(@as(usize, 8), got.len);
            for (o, 0..) |*c, k| c.* = got[2 * k + 1];
            // A permutation: each once.
            var sorted = o.*;
            std.mem.sort(u8, &sorted, {}, std.sort.asc(u8));
            try testing.expectEqualStrings("0123", &sorted);
        }
    }
    // Replayed for the same shard and term: the same orders.
    try testing.expectEqualDeep(orders[0], orders[1]);
    // And not all one order.
    var differs = false;
    for (orders[0][1..]) |o| if (!std.mem.eql(u8, &o, &orders[0][0])) {
        differs = true;
    };
    try testing.expect(differs);
}

test "plan selection: under every strategy an executor whose breaker opened is skipped, and never chosen while it's open" {
    for ([_][]const u8{ "static-order", "round-robin", "random", "health-weighted" }) |selection| {
        var f: SelectionFixture = undefined;
        try f.init(selection, 3, .{});
        defer f.deinit();
        var buf: [64]u8 = undefined;
        var nb: [16]u8 = undefined;
        // Fail every executor once: e0's breaker opens.
        _ = runSelectionPass(&f.handler, &f.shard, "open", "failure", &buf);
        var skips: usize = 0;
        for (0..6) |i| {
            const name = std.fmt.bufPrint(&nb, "after{d}", .{i}) catch unreachable;
            const got = runSelectionPass(&f.handler, &f.shard, name, "success", &buf);
            // Never started while its breaker is open.
            try testing.expect(std.mem.indexOf(u8, got, "e0") == null);
            var kb: [KEY_PREFIX + 32]u8 = undefined;
            for (f.handler.inspect(writeKey(&kb, .default, name)).?.history.items) |evt| {
                if (std.mem.eql(u8, evt.event_type_owned, "plan_breaker_skip") and std.mem.eql(u8, evt.detail_owned, "e0")) skips += 1;
            }
        }
        // Where its place came first, it was skipped by its breaker, not
        // left untried: every pass in definition order, some in the others.
        if (std.mem.eql(u8, selection, "static-order")) try testing.expectEqual(@as(usize, 6), skips);
        if (std.mem.eql(u8, selection, "round-robin")) try testing.expect(skips > 0);
    }
}

test "plan selection: a result is counted once, by the step that logs it, however often it is decided first" {
    var f: SelectionFixture = undefined;
    try f.init("static-order", 2, .{ .threshold = 3 });
    defer f.deinit();
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, "once");
    createTestRun(&f.handler, key, "once", "sel-wf");
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    completeTestActionWith(&f.shard, f.handler.inspect(key).?.pending_action_run_id_owned.?, "failure");
    // Its step decided, and not logged, a few times: nothing counted.
    f.shard.raft_node.writes_stopped = true;
    for (0..4) |_| f.handler.checkPendingActions(&f.shard);
    try testing.expect(f.health(0) == null);
    f.shard.raft_node.writes_stopped = false;
    f.handler.checkPendingActions(&f.shard);
    settleTest(&f.shard);
    try testing.expectEqual(@as(i64, 1), f.health(0).?.api_failures);
    try testing.expectEqual(plan_types.CircuitBreakerState.closed, f.health(0).?.breaker_state);
}

test "plan selection: a synchronous failure counts on its executor's breaker under any selection" {
    var f: SelectionFixture = undefined;
    try f.init("static-order", 2, .{ .doubled_input = true });
    defer f.deinit();
    // A step input no call may carry fails each executor without a call.
    const big = try testing.allocator.alloc(u8, ae.MAX_INPUT / 2 + 16);
    defer testing.allocator.free(big);
    @memset(big, 'x');
    @memcpy(big[0..6], "{\"s\":\"");
    @memcpy(big[big.len - 2 ..], "\"}");
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, "big");
    createTestRunWithInput(&f.handler, key, "big", "sel-wf", big);
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, f.handler.inspect(key).?.status);
    try testing.expectEqual(plan_types.CircuitBreakerState.open, f.health(0).?.breaker_state);
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("e1", runSelectionPass(&f.handler, &f.shard, "next", "success", &buf));
}

test "plan selection: with every breaker open the step fails once, each skipped" {
    var f: SelectionFixture = undefined;
    try f.init("round-robin", 3, .{ .breakers = 3 });
    defer f.deinit();
    const now = @import("stdx").time.milliTimestamp();
    for ([_][]const u8{ "e0", "e1", "e2" }) |e| {
        const h = testHealth(&f.handler, &f.shard, "sel-wf", "p", e);
        h.breaker_state = .open;
        h.breaker_opened_at_ms = now;
    }
    var kb: [KEY_PREFIX + 32]u8 = undefined;
    const key = writeKey(&kb, .default, "shut");
    createTestRun(&f.handler, key, "shut", "sel-wf");
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    const run = f.handler.inspect(key).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.failed, run.status);
    var skips: usize = 0;
    for (run.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "plan_breaker_skip")) skips += 1;
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "plan_executor_start"));
    }
    try testing.expectEqual(@as(usize, 3), skips);
}

test "plan selection: half-open lets one probe through; the others skip it until its result" {
    var f: SelectionFixture = undefined;
    try f.init("static-order", 2, .{});
    defer f.deinit();
    const h = testHealth(&f.handler, &f.shard, "sel-wf", "p", "e0");
    h.breaker_state = .open;
    h.breaker_opened_at_ms = @import("stdx").time.milliTimestamp() - 3_600_001;
    var keys: [3][KEY_PREFIX + 8]u8 = undefined;
    var nb: [8]u8 = undefined;
    var started: [3][]const u8 = undefined;
    for (&keys, 0..) |*kb, i| {
        const key = writeKey(kb, .default, std.fmt.bufPrint(&nb, "h{d}", .{i}) catch unreachable);
        createTestRun(&f.handler, key, "", "sel-wf");
        f.handler.advanceWorkflow(&f.shard, key);
        settleTest(&f.shard);
        const items = f.handler.inspect(key).?.history.items;
        started[i] = items[items.len - 1].detail_owned;
    }
    // The first is the probe; the rest go to e1.
    try testing.expectEqualStrings("e0", started[0]);
    try testing.expectEqualStrings("e1", started[1]);
    try testing.expectEqualStrings("e1", started[2]);
    // The probe succeeds: the breaker closes, and e0 is tried again.
    const probe = writeKey(&keys[0], .default, "h0");
    completeTestActionWith(&f.shard, f.handler.inspect(probe).?.pending_action_run_id_owned.?, "success");
    f.handler.checkPendingActions(&f.shard);
    settleTest(&f.shard);
    try testing.expectEqual(plan_types.CircuitBreakerState.closed, f.health(0).?.breaker_state);
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("e0", runSelectionPass(&f.handler, &f.shard, "closed", "success", &buf));
}

test "plan selection: a probe held for room is no probe: the breaker stays open until a call goes out" {
    var f: SelectionFixture = undefined;
    try f.init("static-order", 2, .{});
    defer f.deinit();
    // A call left in the outbox, before e0's breaker opens.
    var fb: [KEY_PREFIX + 8]u8 = undefined;
    const fill = writeKey(&fb, .default, "fill");
    createTestRun(&f.handler, fill, "fill", "sel-wf");
    f.handler.advanceWorkflow(&f.shard, fill);
    settleTest(&f.shard);
    try testing.expect(f.handler.inspect(fill).?.pending_action_run_id_owned != null);
    const h = testHealth(&f.handler, &f.shard, "sel-wf", "p", "e0");
    h.breaker_state = .open;
    h.breaker_opened_at_ms = @import("stdx").time.milliTimestamp() - 3_600_001;
    // e0's turn to probe, and no room: the step waits, and e0 is not marked.
    f.shard.outbox_caps = .{ .other = .{ .shard = 1 << 30, .channel = 1 } };
    var kb: [KEY_PREFIX + 8]u8 = undefined;
    const key = writeKey(&kb, .default, "held");
    createTestRun(&f.handler, key, "held", "sel-wf");
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    try testing.expect(f.handler.inspect(key).?.waiting_for_room);
    try testing.expectEqual(plan_types.CircuitBreakerState.open, f.health(0).?.breaker_state);
    // Room again: its next try is the probe.
    f.shard.outbox_caps = .{};
    f.handler.advanceWorkflow(&f.shard, key);
    settleTest(&f.shard);
    const run = f.handler.inspect(key).?;
    try testing.expect(run.pending_action_run_id_owned != null);
    try testing.expectEqualStrings("e0", run.history.items[run.history.items.len - 1].detail_owned);
    try testing.expectEqual(plan_types.CircuitBreakerState.half_open, f.health(0).?.breaker_state);
}

test "plan selection: a retry isn't made past the breaker its own failure opened" {
    var f: SelectionFixture = undefined;
    try f.init("static-order", 2, .{});
    defer f.deinit();
    var buf: [64]u8 = undefined;
    _ = runSelectionPass(&f.handler, &f.shard, "r", "failure", &buf);
    var starts: usize = 0;
    var skips: usize = 0;
    for (f.handler.inspect(testKey(.default, "r")).?.history.items) |evt| {
        if (!std.mem.eql(u8, evt.detail_owned, "e0")) continue;
        if (std.mem.eql(u8, evt.event_type_owned, "plan_executor_start")) starts += 1;
        if (std.mem.eql(u8, evt.event_type_owned, "plan_breaker_skip")) skips += 1;
    }
    // e0 has one retry, but its first failure opened its breaker.
    try testing.expectEqual(@as(usize, 1), starts);
    try testing.expectEqual(@as(usize, 1), skips);
}

test "plan selection: random's shuffle is the plan's own: two plans at the same pass don't share it" {
    var f: SelectionFixture = undefined;
    try f.init("random", 2, .{});
    defer f.deinit();
    var execs: [8]definition.ExecutorConfig = undefined;
    for (&execs) |*e| e.* = .{ .name = "x", .action_name = "@actions/x", .retry = null, .breaker = null };
    var orders: [2][8]u8 = undefined;
    for ([_][]const u8{ "p", "q" }, &orders) |name, *order| {
        var run: WorkflowHandler.RunRecord = undefined;
        run.workflow_name_owned = "sel-wf";
        run.plan_effects = .empty;
        defer {
            for (run.plan_effects.items) |e| testing.allocator.free(e.key);
            run.plan_effects.deinit(testing.allocator);
        }
        f.handler.orderPass(&f.shard, &run, .{ .name = name, .selection = .random, .executors = &execs }, .default, order);
    }
    try testing.expect(!std.mem.eql(u8, &orders[0], &orders[1]));
}

test "plan selection: a redefinition and a new term forget the leader's plan memory" {
    var f: SelectionFixture = undefined;
    try f.init("round-robin", 3, .{});
    defer f.deinit();
    f.handler.syncProducers(testKey(.default, "sel-wf"));
    var buf: [64]u8 = undefined;
    _ = runSelectionPass(&f.handler, &f.shard, "a", "failure", &buf);
    try testing.expect(f.handler.plan_health.count() > 0);
    try testing.expect(f.handler.plan_cursors.count() > 0);
    // Another workflow's memory stays when this one is redefined.
    f.handler.forgetPlans(.{ .ns = .default, .workflow = "other-wf" });
    try testing.expect(f.handler.plan_health.count() > 0);
    var jb: [2048]u8 = undefined;
    createTestDef(&f.handler, testKey(.default, "sel-wf"), "sel-wf", selectionWorkflowJson(&jb, "round-robin", 2, .{}));
    f.handler.syncProducers(testKey(.default, "sel-wf"));
    try testing.expectEqual(@as(u32, 0), f.handler.plan_health.count());
    try testing.expectEqual(@as(u32, 0), f.handler.plan_cursors.count());
    // e0's breaker opens again; a new term starts without it.
    _ = runSelectionPass(&f.handler, &f.shard, "b", "failure", &buf);
    try testing.expectEqual(plan_types.CircuitBreakerState.open, f.health(0).?.breaker_state);
    f.shard.raft_node.current_term += 1;
    try testing.expectEqualStrings("e0", runSelectionPass(&f.handler, &f.shard, "c", "success", &buf));
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    // [wf_len][wf][ver_len][ver][status][evt_len][idem_len][idem][no trigger]
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1, 0, 0, 3, 0, 'k', 'e', 'y', 0 };
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

/// A completion's incarnation, last in its value: a start at `ms`.
fn incarnationAt(comptime ms: u64) [8]u8 {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, ms * std.time.ns_per_ms, .little);
    return b;
}

test "workflow replay: a start answers with its run id whole, at the longest a run may have; a longer one is refused" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0, 0 };
    var long: [persistence_mod.MAX_ENTRY_KEY]u8 = undefined;
    _ = writeKey(&long, .default, &@as([long.len - KEY_PREFIX]u8, @splat('l')));
    var refused: Result = .{};
    try handler.replayStart(&long, &start, 1, &refused);
    try testing.expectEqualStrings(runs_machine.RUN_ID_TOO_LONG, refused.refused().?.refusal.message);
    var key: [KEY_PREFIX + runs_machine.MAX_RUN_ID]u8 = undefined;
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
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0, 0 };
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
        const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0, 0 };
        const stamp_ns: u64 = @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms;
        var pbuf: [128]u8 = undefined;
        // Through `replayEntry`, as the log applies them.
        var out: Result = .{};
        try handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_start, 0, 1, 1, stamp_ns, .default, testKey(.default, "r1"), &start, &pbuf).?, &out);
        try testing.expectEqual(stamp_ms, shownOf(&handler, testKey(.default, "r1")).?.created_at_ms);
        // completed, no output, no event, workflow "wf"
        // Its incarnation, the start's stamp, last.
        var complete = [_]u8{ 3, 0, 0, 0, 0, 0, 2, 0, 'w', 'f' } ++ @as([8]u8, @splat(0));
        std.mem.writeInt(u64, complete[10..18], stamp_ns, .little);
        try handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_complete, 0, 1, 2, stamp_ns + 9 * std.time.ns_per_ms, .default, testKey(.default, "r1"), &complete, &pbuf).?, &out);
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
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 7, 0, 's', 't', 'a', 'r', 't', 'e', 'd', 0, 0, 0 };
    var start_out: Result = .{};
    try handler.replayStart(testKey(.default, "r1"), &start, 1, &start_out);

    // completed, output "ok", last event "e"/"d", workflow "wf"
    // Its incarnation last: the start's stamp, 1 ms.
    const whole = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++ [_]u8{ 1, 0, 'e', 1, 0, 'd' } ++ [_]u8{ 2, 0, 'w', 'f' } ++ incarnationAt(1);

    var events: std.ArrayListUnmanaged(WorkflowHandler.HistoryEvent) = .empty;
    defer events.deinit(allocator);
    try appliedHistoryForTest(&handler, testKey(.default, "r1"), &events);
    try testing.expectEqual(@as(usize, 1), events.items.len);
    // Every cut is skipped whole.
    for (1..whole.len) |n| {
        try testing.expectError(error.Halt, handler.replayComplete(testKey(.default, "r1"), whole[0..n], 2, &replay_out_for_tests));
        const run = shownOf(&handler, testKey(.default, "r1")).?;
        try testing.expectEqual(WorkflowHandler.RunStatus.running, run.status);
        try testing.expect(run.output_owned == null);
        events.clearRetainingCapacity();
        try appliedHistoryForTest(&handler, testKey(.default, "r1"), &events);
        try testing.expectEqualStrings("started", events.items[0].event_type_owned);
    }

    try handler.replayComplete(testKey(.default, "r1"), &whole, 2, &replay_out_for_tests);
    const run = shownOf(&handler, testKey(.default, "r1")).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqualStrings("ok", run.output_owned.?);
    // An ended run keeps no search tags: a search covers live runs.
    try testing.expect(run.search_tags_owned == null);
    // Its last event follows the history it had.
    events.clearRetainingCapacity();
    try appliedHistoryForTest(&handler, testKey(.default, "r1"), &events);
    try testing.expectEqual(@as(usize, 2), events.items.len);
    try testing.expectEqualStrings("e", events.items[1].event_type_owned);
}

test "workflow replay: a cancel logged after a completion is refused and changes nothing, on every replica; a second cancel after a cancel succeeds" {
    // A completion a deposed leader logged commits ahead of a cancel the
    // next leader took while that completion hadn't applied there.
    const start = [_]u8{ 2, 0, 'w', 'f', 1, 0, '1', 1 } ++ [_]u8{ 0, 0, 0, 0, 0 };
    const wf = [_]u8{ 2, 0, 'w', 'f' };
    const completed = [_]u8{3} ++ [_]u8{ 1, 2, 0, 0, 0, 'o', 'k' } ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ wf ++ incarnationAt(1);
    const cancelled = [_]u8{5} ++ [_]u8{0} ++ [_]u8{ 0, 0 } ++ [_]u8{ 0, 0 } ++ wf ++ incarnationAt(1);

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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
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
    try testing.expect(!try runs_machine.hasSignal(testing.allocator, handler.defsRead(), testKey(.default, "r"), forged));
    try testing.expectEqual(events, after.history.items.len);

    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    const value = [_]u8{ 8, 0 } ++ "approval".*;
    try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", &value)) != null);
    settleTest(&shard);
    try testing.expect(try runs_machine.hasSignal(testing.allocator, handler.defsRead(), testKey(.default, "r"), "approval"));
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
    handler.history = shard.stream_handler;
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
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    // A step whose counters are of a version this build doesn't read.
    const step: runs_machine.Step.Value = .{
        .created_ns = handler.appliedRun(testKey(.default, "r")).?.created_ns,
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
        .takes = .{ .bytes = "", .count = 0 },
        .caller_slot = calls_mod.callerSlot(.default, "test-wf"),
        .workflow = "test-wf",
        .ends = 0,
        .has_output = false,
        .output = "",
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
    handler.history = shard.stream_handler;
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
    // What it steps it reads from the store, as it does when it leads.
    fresh.leadCaches(shard.raft_node.current_term + 1);
    try testing.expect(fresh.caches_built);
    fresh.advanceStartedRuns(&shard);
    settleTest(&shard);
    const run = fresh.inspect(testKey(.default, "r")).?;
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
    try testing.expectEqualStrings("second", run.wait_signal_type_owned.?);
}

test "a signal that commits between a wait's decision and its step is taken: the step is refused and the run decided again" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "two-waits"), "two-waits", test_two_waits_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "two-waits");
    // The signal is logged first; the wait is decided before it applies.
    const value = [_]u8{ 5, 0 } ++ "first".*;
    try testing.expect(handler.handleSignal(&shard, &conn, makeRequest(.workflow_signal, "r", &value)) != null);
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    settleTest(&shard);
    try testing.expect(try runs_machine.hasSignal(testing.allocator, handler.defsRead(), testKey(.default, "r"), "first"));
    handler.advanceStartedRuns(&shard);
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expectEqualStrings("second", run.current_step_name_owned.?);
    try testing.expectEqualStrings("second", run.wait_signal_type_owned.?);
}

test "a signal sent before its wait is taken by that wait alone: a second wait of its type needs a second signal" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const key = testKey(.default, "r");
    createTestDef(&handler, testKey(.default, "two-approvals"), "two-approvals", test_two_approvals_json);
    createTestRun(&handler, key, "r", "two-approvals");
    try testSignal(&handler, key, "approve");
    handler.advanceWorkflow(&shard, key);
    settleTest(&shard);
    // The first wait took it; the second waits.
    var run = handler.inspect(key).?;
    try testing.expectEqualStrings("again", run.current_step_name_owned.?);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expect(!try runs_machine.hasSignal(testing.allocator, handler.defsRead(), key, "approve"));
    handler.advanceStartedRuns(&shard);
    settleTest(&shard);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(key).?.status);
    // A second signal ends the second wait.
    try testSignal(&handler, key, "approve");
    handler.advanceStartedRuns(&shard);
    settleTest(&shard);
    run = handler.inspect(key).?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expectEqual(@as(u64, 0), runs_machine.runOf(handler.defsRead(), key).?.sig_held);
}

test "runs parked on actions never registered cost no step per tick, and don't hold up a run whose call settled" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    // They wait an hour for their action to be registered.
    actions.missing_action_wait_ms = 3_600_000;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    // More than one pass looks at, each parked on an action not registered.
    var kbs: [80][16]u8 = undefined;
    for (&kbs, 0..) |*kb, i| {
        var id_buf: [8]u8 = undefined;
        const key = writeKey(kb, .default, try std.fmt.bufPrint(&id_buf, "s{d:0>3}", .{i}));
        createTestRun(&handler, key, keyName(key), "test-wf");
        handler.advanceWorkflow(&shard, key);
        settleTest(&shard);
    }
    // Each says once that its action isn't registered.
    for (0..4) |_| {
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqual(@as(u32, 0), handler.call_dirty.count());

    // Then nothing: no step logged, no run taken, nothing left to look at.
    const logged = shard.raft_node.log.lastIndex();
    const taken = handler.taken;
    for (0..20) |_| handler.checkPendingActions(&shard);
    try testing.expectEqual(logged, shard.raft_node.log.lastIndex());
    try testing.expectEqual(taken, handler.taken);
    try testing.expectEqual(@as(u32, 0), handler.call_dirty.count());
    try testing.expectEqual(@as(u32, 0), handler.runs.count());

    // A run whose call settles, behind them all: it resumes.
    registerTestAction(&shard, "step-a");
    createTestRun(&handler, testKey(.default, "late"), "late", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "late"));
    settleTest(&shard);
    completeTestAction(&shard, handler.inspect(testKey(.default, "late")).?.pending_action_run_id_owned.?);
    // The registration has every parked run looked at again: more than one
    // pass holds them, and each pass starts where the last stopped.
    for (0..2) |_| {
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqualStrings("step_b", handler.inspect(testKey(.default, "late")).?.current_step_name_owned.?);
}

test "a run whose call a repair settled refused, while parked, is looked at and goes on" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "r"), "r", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "r"));
    _ = shard.applyCommitted();
    // Its call starts, and the run, looked at, has nothing to do yet.
    try testing.expect(deliverToSelf(&shard));
    _ = shard.applyCommitted();
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(u32, 0), handler.call_dirty.count());
    // The call's answer is skipped by a repair: the call settles refused.
    const part = shard.defaultPartition();
    part.store.inbound_skip = .all(77);
    try testing.expect(deliverToSelf(&shard));
    part.store.inbound_skip = null;
    settleTest(&shard);
    const rid = handler.inspect(testKey(.default, "r")).?.pending_action_run_id_owned.?;
    try testing.expectEqual(calls_mod.State.refused, calls_mod.callOf(handler.defsRead(), .default, calls_mod.callerSlot(.default, "test-wf"), rid).?.state);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "r")).?;
    try testing.expect(run.pending_action_run_id_owned == null);
    try testing.expect(run.status.isTerminal());
}

test "parked runs held by a step in flight don't keep one whose call settled from being looked at" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    actions.missing_action_wait_ms = 3_600_000;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    // More than a pass visits, each parked and each with a step in flight
    // that never applies.
    var kbs: [300][16]u8 = undefined;
    handler.stepping_term = shard.raft_node.current_term;
    for (&kbs, 0..) |*kb, i| {
        var id_buf: [8]u8 = undefined;
        const key = writeKey(kb, .default, try std.fmt.bufPrint(&id_buf, "h{d:0>3}", .{i}));
        createTestRun(&handler, key, keyName(key), "test-wf");
        handler.advanceWorkflow(&shard, key);
        settleTest(&shard);
        try handler.stepping.put(allocator, try allocator.dupe(u8, key), handler.appliedRun(key).?.rev);
    }
    try testing.expectEqual(@as(u32, 300), handler.call_dirty.count());
    registerTestAction(&shard, "step-a");
    createTestRun(&handler, testKey(.default, "late"), "late", "test-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "late"));
    settleTest(&shard);
    completeTestAction(&shard, handler.inspect(testKey(.default, "late")).?.pending_action_run_id_owned.?);
    for (0..2) |_| {
        handler.checkPendingActions(&shard);
        settleTest(&shard);
    }
    try testing.expectEqualStrings("step_b", handler.inspect(testKey(.default, "late")).?.current_step_name_owned.?);
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
    handler.history = shard.stream_handler;
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
    try testing.expect(!try runs_machine.hasSignal(testing.allocator, handler.defsRead(), testKey(.default, "r"), "go"));
}

test "workflow handler: a trigger cursor restored inside an append resumes with its next record" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    try handler.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events" });

    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
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
    var idem_buf: [128]u8 = undefined;
    var start_buf: [256]u8 = undefined;
    var out: Result = .{};
    try handler.replayStart(testKey(TEST_NS, "b"), triggerStartValue(&start_buf, &idem_buf, "wf", TEST_NS, 1, "events", all.ids[1].timestamp_ms, all.ids[1].sequence, .{ 0, 0 }), 1, &out);
    const cursor = handler.triggerCursor(testKey(TEST_NS, "wf"), TEST_NS);
    try testing.expect(cursor.eql(all.ids[1]));

    // The next poll reads c, d, e: the rest of that append, nothing skipped.
    const rest = streams.readPayloadsForStream("events", "default", cursor, 100);
    defer allocator.free(rest.payloads);
    defer allocator.free(rest.ids);
    try testing.expectEqual(@as(usize, 3), rest.payloads.len);
    for (rest.payloads, "cde") |p, want| try testing.expectEqual(want, p[0]);
}

test "workflow handler: a client's start with a key with a leading '_' is refused, and nothing is proposed" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    var conn = try Connection.init(allocator, -1, 0, 0);
    defer conn.deinit();
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    const before = shard.raft_node.log.lastIndex();
    for ([_][]const u8{ "_trigger:16:1:ev@9:0", "_schedule:16:test-wf:4000", "_x" }) |key| {
        var vb: [96]u8 = undefined;
        // [ver_len:u16 = 0][has_idem:u8 = 1][key_len:u16][key]
        vb[0] = 0;
        vb[1] = 0;
        vb[2] = 1;
        std.mem.writeInt(u16, vb[3..5], @intCast(key.len), .little);
        @memcpy(vb[5..][0..key.len], key);
        try testing.expect(handler.handleStart(&shard, &conn, makeRequest(.workflow_start, "test-wf", vb[0 .. 5 + key.len])) == null);
        const answer = try queuedAnswer(&conn);
        try testing.expectEqual(@backingInt(proto.StatusCode.bad_request), answer.header.status);
        try testing.expectEqualStrings(runs_machine.SERVER_KEY_REFUSED, (try answer.refused()).message);
    }
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
}

test "workflow handler: a trigger's place is read from what applied, so a handler built from the store alone, as from a snapshot, goes on from it" {
    const allocator = testing.allocator;
    var first = WorkflowHandler.init(allocator);
    defer first.deinit();
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    first.store = &partition.store;
    var out: Result = .{};
    try first.replayCreate(testKey(TEST_NS, "wf"), "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"run\":\"@actions/a\",\"transitions\":{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}", 1, 0, &out);
    var kb: [128]u8 = undefined;
    var sb: [256]u8 = undefined;
    try first.replayStart(testKey(TEST_NS, "r1"), triggerStartValue(&sb, &kb, "wf", TEST_NS, 4, "events", 1700, 3, .{ 0, 0 }), 2, &out);

    // None of the starts replayed into this one: only the store.
    var fresh = WorkflowHandler.init(allocator);
    defer fresh.deinit();
    fresh.store = &partition.store;
    try fresh.registerStreamTrigger(TEST_NS, "wf", .{ .stream = "events" });
    const at = fresh.triggerCursor(testKey(TEST_NS, "wf"), TEST_NS);
    try testing.expectEqual(@as(u64, 1700), at.timestamp_ms);
    try testing.expectEqual(@as(u64, 3), at.sequence);
    // The stream it read is its made stamp's: a read of an older making
    // is stale, and one made since starts it over.
    const t = fresh.stream_triggers.getPtr(testKey(TEST_NS, "wf")).?;
    try testing.expect(!fresh.sameStream(testKey(TEST_NS, "wf"), t, TEST_NS, 3));
    try testing.expectEqual(@as(?u64, 4), t.stream_made_ns);
    try testing.expect(fresh.sameStream(testKey(TEST_NS, "wf"), t, TEST_NS, 4));
    try testing.expect(!fresh.sameStream(testKey(TEST_NS, "wf"), t, TEST_NS, 9));
    try testing.expectEqual(@as(u64, 0), fresh.triggerCursor(testKey(TEST_NS, "wf"), TEST_NS).timestamp_ms);
    // In another namespace, the same name is another stream.
    try testing.expectEqual(@as(u64, 0), first.triggerCursor(testKey(TEST_NS, "wf"), TEST_NS_A).timestamp_ms);
}

test "workflow handler: a trigger's start of this leader's term refused reads again from the place logged, once for the chain it broke" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    const t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    const read_to: StreamID = .{ .timestamp_ms = 1800, .sequence = 0 };
    var buf: [256]u8 = undefined;
    var kb: [128]u8 = undefined;
    const value = triggerStartValue(&buf, &kb, "wf", .default, 1, "events", 1700, 0, .{ 0, 0 });
    var eb: [512]u8 = undefined;
    const refused = struct {
        fn at(b: []u8, v: []const u8, term: u64, index: u64) entry_mod.Entry {
            return entry_mod.buildCommandEntry(.workflow_start, entry_mod.Flags.NONE, term, index, 0, .default, testKey(.default, "r1"), v, b).?;
        }
    };
    const lead: WorkflowHandler.Lead = .{ .term = 3, .last_index = 40 };
    // An earlier leader's start: its chain isn't this one's.
    t.proposed = .{ .at = read_to, .term = 3, .chain = read_to };
    var e = refused.at(&eb, value, 2, 30);
    var out = Result.start(&e);
    out.refuse(.conflict, .unclassified, runs_machine.TRIGGER_OUT_OF_ORDER);
    handler.entryApplied(&e, &out, lead);
    try testing.expect(t.proposed != null);
    // This term's: read again from the place logged.
    e = refused.at(&eb, value, 3, 31);
    out = Result.start(&e);
    out.refuse(.conflict, .unclassified, runs_machine.TRIGGER_OUT_OF_ORDER);
    handler.entryApplied(&e, &out, lead);
    try testing.expect(t.proposed == null);
    try testing.expectEqual(@as(u64, 40), t.resync_index);
    // The chain it broke, refused after it: nothing more.
    t.proposed = .{ .at = read_to, .term = 3, .chain = read_to };
    e = refused.at(&eb, value, 3, 40);
    out = Result.start(&e);
    out.refuse(.conflict, .unclassified, runs_machine.TRIGGER_OUT_OF_ORDER);
    handler.entryApplied(&e, &out, .{ .term = 3, .last_index = 55 });
    try testing.expect(t.proposed != null);
    try testing.expectEqual(@as(u64, 40), t.resync_index);
    // A chain begun after it: read again.
    e = refused.at(&eb, value, 3, 41);
    out = Result.start(&e);
    out.refuse(.conflict, .unclassified, runs_machine.TRIGGER_OUT_OF_ORDER);
    handler.entryApplied(&e, &out, .{ .term = 3, .last_index = 55 });
    try testing.expect(t.proposed == null);
    try testing.expectEqual(@as(u64, 55), t.resync_index);
}

test "workflow handler: a trigger's start of an event in a stream made again, or of its name in another namespace, is another run than the old stream's" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    const t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    const event: StreamID = .{ .timestamp_ms = 1700, .sequence = 0 };
    for ([_]WorkflowHandler.ReadStream{
        .{ .ns = .default, .made = 1, .name = "events" },
        .{ .ns = .default, .made = 1, .name = "events" },
        .{ .ns = .default, .made = 2, .name = "events" },
        .{ .ns = TEST_NS_A, .made = 1, .name = "events" },
    }) |read| {
        // Each the first of its stream's chain.
        var chain: StreamID = .{ .timestamp_ms = 0, .sequence = 0 };
        try testing.expect(handler.startRunFromTrigger(&shard, t, read, "{}", event, &chain));
    }
    settleTest(&shard);
    // The same event of the same stream once; the others are their own.
    try testing.expectEqual(@as(usize, 3), handler.runCount());
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
    handler.history = shard.stream_handler;
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
        // [wf_len][wf][ver_len][ver][status][evt_len][idem_len][no trigger][input]
        const value = try std.fmt.allocPrint(allocator, "\x02\x00wf\x01\x001\x01\x00\x00\x00\x00\x00{{\"a\":{s}}}", .{arrays});
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
                .created_ns = handler.appliedRun(ns_key).?.created_ns,
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
                .takes = .{ .bytes = "", .count = 0 },
                .caller_slot = calls_mod.callerSlot(.default, wf_name),
                .workflow = wf_name,
                .ends = 0,
                .has_output = false,
                .output = "",
                .call = "",
            };
            var sb: [512]u8 = undefined;
            try handler.applyDirect(.workflow_step, 0, ns_key, try runs_machine.Step.encodeInto(&sb, step), started_at, &out);
        },
        else => {
            const c: runs_machine.Complete = .{ .status = status, .output = null, .event = "ended", .detail = "", .workflow = wf_name, .created_ns = handler.appliedRun(ns_key).?.created_ns };
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
    _ = handler.commitRun(shard, key);
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
    _ = handler.commitRun(shard, key);
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

/// Every run `ownPage` lists of `workflow` ("" for all) matching `search`,
/// a page at a time at `limit` and `room`, each page after the last's
/// stop, into `out` as owned ids (`ids_buf`): the list's whole walk.
fn walkAll(handler: *WorkflowHandler, workflow: []const u8, search: []const u8, limit: u32, room: u32, ids_buf: [][16]u8, out: [][]const u8) !struct { ids: []const []const u8, pages: usize, empty_pages: usize } {
    const buf = try testing.allocator.alloc(u8, system_ops.Runs.max_size);
    defer testing.allocator.free(buf);
    var n: usize = 0;
    var pages: usize = 0;
    var empty: usize = 0;
    var after_buf: [16]u8 = undefined;
    var after: ?list_runs.RunAt.Value = null;
    while (pages < 100_000) : (pages += 1) {
        const page = try list_runs.ownPage(handler, .default, workflow, .{ .limit = limit, .status = "", .search = search, .after = after }, room, buf);
        var it = page.runs.iterator();
        var got: usize = 0;
        while (try it.next()) |run| : (got += 1) {
            @memcpy(ids_buf[n][0..run.at.run_id.len], run.at.run_id);
            out[n] = ids_buf[n][0..run.at.run_id.len];
            n += 1;
        }
        if (got == 0) empty += 1;
        if (!page.more) return .{ .ids = out[0..n], .pages = pages + 1, .empty_pages = empty };
        const stop = page.stop.?;
        @memcpy(after_buf[0..stop.run_id.len], stop.run_id);
        after = .{ .created_at = stop.created_at, .run_id = after_buf[0..stop.run_id.len] };
    }
    return error.TestUnexpectedResult;
}

test "list runs: a search matching one run past the walk bound answers a page of none with more, and the next page finds it" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    // The one that matches is the oldest, behind more than a page reads.
    try createTestRunAt(&handler, "needle", "wf", 1, .running);
    var id: [16]u8 = undefined;
    for (0..list_runs.WALK_MAX + 4) |i| try createTestRunAt(&handler, try std.fmt.bufPrint(&id, "w-{d}", .{i}), "wf", @intCast(i + 2), .running);
    var ids_buf: [4][16]u8 = undefined;
    var out: [4][]const u8 = undefined;
    const got = try walkAll(&handler, "", "needle", 10, 1 << 20, &ids_buf, &out);
    try expectRunIds(&.{"needle"}, got.ids);
    try testing.expectEqual(@as(usize, 2), got.pages);
    try testing.expectEqual(@as(usize, 1), got.empty_pages);
}

test "list runs: a named workflow's list holds its own runs, each once in the list's order, at every limit and room, not another's sharing its prefix" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    // Started together, ids of other lengths; and "wfx", whose key's name
    // part starts as "wf"'s does.
    for ([_][]const u8{ "b", "ab", "zzz", "c" }) |r| try createTestRunAt(&handler, r, "wf", 7, .running);
    try createTestRunAt(&handler, "q", "wf", 8, .running);
    for ([_][]const u8{ "a", "bb" }) |r| try createTestRunAt(&handler, r, "wfx", 7, .running);
    const want = [_][]const u8{ "q", "ab", "b", "c", "zzz" };
    // One entry: [len]id[len]"wf"[len]"running"[i64].
    for ([_]u32{ 1, 2, 3, 10 }) |limit| for ([_]u32{ 2 + 3 + 2 + 2 + 2 + 7 + 8, 1 << 20 }) |room| {
        var ids_buf: [8][16]u8 = undefined;
        var out: [8][]const u8 = undefined;
        const got = try walkAll(&handler, "wf", "", limit, room, &ids_buf, &out);
        try expectRunIds(&want, got.ids);
    };
}

test "list runs: an index row whose run is gone is skipped, said once" {
    var handler = WorkflowHandler.init(testing.allocator);
    defer handler.deinit();
    for ([_][]const u8{ "r1", "r2", "r3" }) |r| try createTestRunAt(&handler, r, "wf", 5, .running);
    // r2's run row gone, its index rows left: a Flo bug a list survives.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ks = contract_view.KeySet.init(arena.allocator());
    try ks.addIn(runs_machine.RunFam.ID, .default, "r2");
    var view = contract_view.View.init(&handler.own_store, arena.allocator(), &ks);
    var kb: [64]u8 = undefined;
    try view.delete(runs_machine.RunFam, writeKey(&kb, .default, "r2"));
    try view.commit();
    var ids_buf: [4][16]u8 = undefined;
    var out: [4][]const u8 = undefined;
    const got = try walkAll(&handler, "", "", 10, 1 << 20, &ids_buf, &out);
    try expectRunIds(&.{ "r1", "r3" }, got.ids);
    // Each list meets it again; only the first logs it.
    const again = try walkAll(&handler, "", "", 10, 1 << 20, &ids_buf, &out);
    try expectRunIds(&.{ "r1", "r3" }, again.ids);
    try testing.expectEqual(@as(u64, 2), handler.missing_runs);
    try testing.expectEqual(@as(u32, 1), handler.missing_runs_logged);
}

test "list runs: runs started in one ms with ids of other lengths on two shards, walked through their index keys and merged, are each listed once in order" {
    var one = WorkflowHandler.init(testing.allocator);
    defer one.deinit();
    var two = WorkflowHandler.init(testing.allocator);
    defer two.deinit();
    for ([_][]const u8{ "b", "ab" }) |r| try createTestRunAt(&one, r, "wf", 7, .running);
    for ([_][]const u8{ "c", "zz" }) |r| try createTestRunAt(&two, r, "wg", 7, .running);
    const bufs = [_][]u8{ try testing.allocator.alloc(u8, system_ops.Runs.max_size), try testing.allocator.alloc(u8, system_ops.Runs.max_size) };
    defer for (bufs) |b| testing.allocator.free(b);
    var seen: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (seen.items) |x| testing.allocator.free(x);
        seen.deinit(testing.allocator);
    }
    var after_buf: [16]u8 = undefined;
    var after: ?list_runs.RunAt.Value = null;
    var rounds: usize = 0;
    while (rounds < 10) : (rounds += 1) {
        const q: list_runs.Query = .{ .limit = 2, .status = "", .search = "", .after = after };
        const pages = [_]list_runs.PageResponse.Value{
            try list_runs.ownPage(&one, .default, "", q, 1 << 20, bufs[0]),
            try list_runs.ownPage(&two, .default, "", q, 1 << 20, bufs[1]),
        };
        var entries: [4][]const u8 = undefined;
        const m = try list_runs.mergeRuns(&pages, 2, 1 << 20, &entries);
        // Each entry leads with its id: [len:u16][id].
        for (entries[0..m.taken]) |e| try seen.append(testing.allocator, try testing.allocator.dupe(u8, e[2..][0..std.mem.readInt(u16, e[0..2], .little)]));
        const c = m.cursor orelse break;
        @memcpy(after_buf[0..c.run_id.len], c.run_id);
        after = .{ .created_at = c.created_at, .run_id = after_buf[0..c.run_id.len] };
    }
    try expectRunIds(&.{ "ab", "b", "c", "zz" }, seen.items);
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
    // Where it stopped, read after the page is built: the run it showed.
    try testing.expectEqualStrings("r-2", page.stop.?.run_id);
    try testing.expectEqual(@as(i64, 20), page.stop.?.created_at);
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
        const pbuf2 = try allocator.alloc(u8, system_ops.Runs.max_size);
        defer allocator.free(pbuf2);
        const page = try list_runs.ownPage(&handler, ns, "", .{ .limit = 10, .status = "", .search = "", .after = null }, 1 << 20, pbuf2);
        var ids: [8][]const u8 = undefined;
        const listed_ids = try pageRunIds(page, &ids);
        try testing.expectEqual(@as(usize, 2), listed_ids.len);
        // Each run listed is one this namespace started.
        for (listed_ids) |listed| {
            var own = false;
            for (cases) |c| own = own or (c.ns == ns and std.mem.eql(u8, c.name, listed));
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
    const Step = union(enum) { def: struct { name: []const u8, yaml: []const u8, at: i64 }, start: struct { run: []const u8, wf: []const u8, event: []const u8, idem: []const u8 = "", at: ?[2]u64 = null, after: [2]u64 = .{ 0, 0 } } };
    const steps = [_]Step{
        .{ .def = .{ .name = "sched", .yaml = sched_v1, .at = 1000 } },
        .{ .def = .{ .name = "trig", .yaml = trig_v1, .at = 1000 } },
        .{ .start = .{ .run = "s1", .wf = "sched", .event = "schedule_started", .idem = "_schedule:16:sched:3000" } },
        .{ .start = .{ .run = "t1", .wf = "trig", .event = "trigger_started", .at = .{ 1700, 0 } } },
        .{ .def = .{ .name = "sched", .yaml = sched_v2, .at = 5000 } },
        .{ .def = .{ .name = "trig", .yaml = trig_v2, .at = 5000 } },
        .{ .start = .{ .run = "s2", .wf = "sched", .event = "schedule_started", .idem = "_schedule:16:sched:6000" } },
        .{ .start = .{ .run = "t2", .wf = "trig", .event = "trigger_started", .at = .{ 1800, 2 }, .after = .{ 1700, 0 } } },
    };
    {
        var handler = WorkflowHandler.init(allocator);
        defer handler.deinit();
        var buf: [256]u8 = undefined;
        var kb: [128]u8 = undefined;
        inline for (steps) |step| switch (step) {
            .def => |d| {
                var out: Result = .{};
                try handler.replayCreate(testKey(TEST_NS_A, d.name), d.yaml, d.at, 0, &out);
            },
            .start => |st| {
                var out: Result = .{};
                const value = if (st.at) |at| triggerStartValue(&buf, &kb, st.wf, TEST_NS_A, 1, "events", at[0], at[1], st.after) else startValue(&buf, st.wf, st.event, st.idem);
                try handler.replayStart(testKey(TEST_NS_A, st.run), value, 7000, &out);
                try testing.expect(out.refused() == null);
            },
        };
        const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "sched")).?;
        try testing.expectEqual(@as(i64, 6000), sched.fired_ms);
        try testing.expectEqual(@as(i64, 5000), sched.anchor_ms);
        const cursor = handler.triggerCursor(testKey(TEST_NS_A, "trig"), TEST_NS_A);
        try testing.expectEqual(@as(u64, 1800), cursor.timestamp_ms);
        try testing.expectEqual(@as(u64, 2), cursor.sequence);
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

fn createTestScheduled(handler: *WorkflowHandler, name: []const u8) !void {
    var key_buf: [64]u8 = undefined;
    var yaml_buf: [256]u8 = undefined;
    const yaml = try std.fmt.bufPrint(&yaml_buf,
        \\kind: Workflow
        \\name: {s}
        \\version: 1.0.0
        \\schedule:
        \\  interval: 1000
        \\start:
        \\  wait_for_signal:
        \\    type: never
        \\  transitions:
        \\    success: flo.Completed
    , .{name});
    var out: Result = .{};
    try handler.replayCreate(writeKey(&key_buf, .default, name), yaml, 1000, 0, &out);
}

test "workflow leader caches: a large store is built over several ticks, and the leader steps nothing until it is all read" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    const defs = WorkflowHandler.DEFS_PER_TICK + 1;
    const runs = WorkflowHandler.RUNS_PER_TICK + 1;
    var name_buf: [32]u8 = undefined;
    for (0..defs) |i| try createTestScheduled(&handler, try std.fmt.bufPrint(&name_buf, "wf-{d}", .{i}));
    for (0..runs) |i| try createTestRunAt(&handler, try std.fmt.bufPrint(&name_buf, "r-{d}", .{i}), "wf-0", 0, .running);

    // A chunk of the definitions, then the rest of them and a chunk of the
    // runs, then the last run.
    handler.leadCaches(3);
    try testing.expect(!handler.caches_built);
    try testing.expectEqual(WorkflowHandler.DEFS_PER_TICK, handler.schedules.count());
    try testing.expectEqual(@as(usize, 0), handler.to_step.count());
    handler.leadCaches(3);
    try testing.expect(!handler.caches_built);
    try testing.expectEqual(defs, handler.schedules.count());
    try testing.expectEqual(WorkflowHandler.RUNS_PER_TICK, handler.to_step.count());
    handler.leadCaches(3);
    try testing.expect(handler.caches_built);
    try testing.expectEqual(runs, handler.to_step.count());
    // Built: the same term reads nothing again.
    handler.leadCaches(3);
    try testing.expect(handler.caches_built);

    // A new term starts from the store again.
    handler.leadCaches(4);
    try testing.expect(!handler.caches_built);
    try testing.expectEqual(WorkflowHandler.DEFS_PER_TICK, handler.schedules.count());
}

test "workflow leader caches: a run the store can't decode is passed over, and those after it are still built" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    try createTestScheduled(&handler, "wf");
    for ([_][]const u8{ "a", "b", "c" }) |id| try createTestRunAt(&handler, id, "wf", 0, .running);
    var key_buf: [64]u8 = undefined;
    const store = handler.defsRead().store;
    const node = store.find(runs_machine.RunFam.ID, writeKey(&key_buf, .default, "b")).?;
    store.replace(runs_machine.RunFam.ID, node, try allocator.dupe(u8, &.{0xff}));

    handler.leadCaches(1);
    try testing.expect(handler.caches_built);
    try testing.expect(handler.to_step.contains(writeKey(&key_buf, .default, "a")));
    try testing.expect(!handler.to_step.contains(writeKey(&key_buf, .default, "b")));
    try testing.expect(handler.to_step.contains(writeKey(&key_buf, .default, "c")));
}

test "workflow leader caches: a schedule that couldn't be built for want of memory is built on a later tick, wherever the allocation failed" {
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var handler = WorkflowHandler.init(testing.allocator);
        defer handler.deinit();
        try createTestScheduled(&handler, "wf");
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_at });
        handler.allocator = failing.allocator();
        handler.leadCaches(1);
        handler.allocator = testing.allocator;
        // A few more ticks of the same term: whatever was lost is read again.
        for (0..3) |_| handler.leadCaches(1);
        var key_buf: [64]u8 = undefined;
        try testing.expect(handler.schedules.contains(writeKey(&key_buf, .default, "wf")));
        if (!failing.has_induced_failure) break;
    }
}

test "step executor: a call its action's shard holds over its memory share waits, says so once, and starts once memory frees" {
    const settings = @import("../settings/machine.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-m"), "run-m", "test-wf");
    // The shard has no room for a run.
    try settings.bindForTest(&shard.defaultPartition().store, 1);
    handler.advanceWorkflow(&shard, testKey(.default, "run-m"));
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const run = handler.takeRun(&shard, testKey(.default, "run-m")).?;
    const rid = run.pending_action_run_id_owned.?;
    const c = WorkflowHandler.callOf(&shard, run, rid).?;
    try testing.expectEqual(calls_mod.State.over_budget, c.state);
    try testing.expect(std.mem.indexOf(u8, c.outcome, "would pass its share") != null);
    // Held, not failed, and not taken for a missing action.
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    var held: usize = 0;
    for (handler.inspect(testKey(.default, "run-m")).?.history.items) |evt| {
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "action_not_registered"));
        if (std.mem.eql(u8, evt.event_type_owned, "call_held_over_budget")) held += 1;
    }
    try testing.expectEqual(@as(usize, 1), held);
    // Memory frees: asked again, it starts.
    try settings.bindForTest(&shard.defaultPartition().store, 1 << 40);
    handler.retryCall(&shard, run, rid, c, @import("stdx").time.milliTimestamp());
    _ = handler.commitRun(&shard, testKey(.default, "run-m"));
    settleTest(&shard);
    const again = handler.takeRun(&shard, testKey(.default, "run-m")).?;
    try testing.expectEqual(calls_mod.State.started, WorkflowHandler.callOf(&shard, again, again.pending_action_run_id_owned.?).?.state);
    // The ask again added nothing to history.
    held = 0;
    _ = handler.commitRun(&shard, testKey(.default, "run-m"));
    settleTest(&shard);
    for (handler.inspect(testKey(.default, "run-m")).?.history.items) |evt| {
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "action_call_retry"));
        if (std.mem.eql(u8, evt.event_type_owned, "call_held_over_budget")) held += 1;
    }
    try testing.expectEqual(@as(usize, 1), held);
}

test "a child its shard holds over its memory share is waited for, though its step can't wait, and starts once memory frees" {
    const settings = @import("../settings/machine.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    try settings.bindForTest(&shard.defaultPartition().store, 1);
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    startTestChildren(&handler, &shard);
    const run = handler.takeRun(&shard, testKey(.default, "par")).?;
    var kb: [ae.MAX_RUN_ID]u8 = undefined;
    const kid_id = kb[0..run.pending_action_run_id_owned.?.len];
    @memcpy(kid_id, run.pending_action_run_id_owned.?);
    const c = WorkflowHandler.callOf(&shard, run, kid_id).?;
    try testing.expectEqual(ae.CallKind.workflow, c.kind);
    try testing.expect(!c.can_wait);
    try testing.expectEqual(calls_mod.State.over_budget, c.state);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    try testing.expect(children_mod.parentOf(.{ .store = &shard.defaultPartition().store }, .default, kid_id) == null);
    var held: usize = 0;
    for (handler.inspect(testKey(.default, "par")).?.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "call_held_over_budget")) held += 1;
    }
    try testing.expectEqual(@as(usize, 1), held);
    // Memory frees: asked again, the child starts.
    try settings.bindForTest(&shard.defaultPartition().store, 1 << 40);
    handler.retryCall(&shard, run, kid_id, c, @import("stdx").time.milliTimestamp());
    try testing.expectEqual(WorkflowHandler.Committed.logged, handler.commitRun(&shard, testKey(.default, "par")));
    settleTest(&shard);
    startTestChildren(&handler, &shard);
    const again = handler.inspect(testKey(.default, "par")).?.*;
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, again.status);
    try testing.expectEqual(calls_mod.State.started, WorkflowHandler.callOf(&shard, &again, kid_id).?.state);
}

test "step executor: a call that can't wait, held over its action's shard's memory share, fails at once saying why" {
    const settings = @import("../settings/machine.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "stripe");
    registerTestAction(&shard, "paypal");
    // Each executor's call can't wait: the other could run.
    createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", test_plan_exhaust_workflow_json);
    createTestRun(&handler, testKey(.default, "run-c"), "run-c", "plan-wf");
    try settings.bindForTest(&shard.defaultPartition().store, 1);
    handler.advanceWorkflow(&shard, testKey(.default, "run-c"));
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "run-c")).?;
    var refused = false;
    for (run.history.items) |evt| {
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "action_not_found"));
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "call_held_over_budget"));
        if (std.mem.eql(u8, evt.event_type_owned, "action_refused") and std.mem.indexOf(u8, evt.detail_owned, "would pass its share") != null) refused = true;
    }
    try testing.expect(refused);
}

test "step executor: a call that can't wait, held at its callee's namespace's quota, waits anyway, says so once, and starts once there is room" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "stripe");
    registerTestAction(&shard, "paypal");
    // Each executor's call can't wait: held for memory it would fail.
    createTestDef(&handler, testKey(.default, "plan-wf"), "plan-wf", test_plan_exhaust_workflow_json);
    createTestRun(&handler, testKey(.default, "run-q"), "run-q", "plan-wf");
    // The namespace's usage is a byte under its part: its own steps go on,
    // but no call's reserve fits.
    try hq.setForTest(&shard.defaultPartition().store, .default, 1 << 30, (1 << 30) - 1);
    handler.advanceWorkflow(&shard, testKey(.default, "run-q"));
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    const run = handler.takeRun(&shard, testKey(.default, "run-q")).?;
    const rid = run.pending_action_run_id_owned.?;
    const c = WorkflowHandler.callOf(&shard, run, rid).?;
    try testing.expectEqual(calls_mod.State.over_quota, c.state);
    try testing.expect(std.mem.startsWith(u8, c.outcome, "limit: ns 1 history "));
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, run.status);
    var held: usize = 0;
    for (handler.inspect(testKey(.default, "run-q")).?.history.items) |evt| {
        try testing.expect(!std.mem.eql(u8, evt.event_type_owned, "action_refused"));
        if (std.mem.eql(u8, evt.event_type_owned, "call_held_over_quota")) held += 1;
    }
    try testing.expectEqual(@as(usize, 1), held);
    // Asked again and refused, past where memory's backoff stops at a
    // minute: both sites that wait on it wait the quota's, longer.
    var cur = run;
    var at = c;
    var tries: usize = 0;
    while (at.attempts < 6) : (tries += 1) {
        try testing.expect(tries < 16);
        handler.retryCall(&shard, cur, cur.pending_action_run_id_owned.?, at, @import("stdx").time.milliTimestamp());
        _ = handler.commitRun(&shard, testKey(.default, "run-q"));
        settleTest(&shard);
        cur = handler.takeRun(&shard, testKey(.default, "run-q")).?;
        at = WorkflowHandler.callOf(&shard, cur, cur.pending_action_run_id_owned.?).?;
        try testing.expectEqual(calls_mod.State.over_quota, at.state);
    }
    const wait_ms = WorkflowHandler.heldRetryMs(at.state, at.attempts);
    try testing.expect(wait_ms > WorkflowHandler.CALL_RETRY_MAX_MS);
    const last_ms: i64 = @intCast(at.last_ns / std.time.ns_per_ms);
    // Past memory's longest wait and short of the quota's: neither site asks.
    const between = last_ms + @as(i64, @intCast(WorkflowHandler.CALL_RETRY_MAX_MS)) + 1_000;
    cur.call_retry_at_ms = 0;
    const before_log = shard.raft_node.log.lastIndex();
    try testing.expect(!handler.callSettled(&shard, cur, cur.pending_action_run_id_owned.?, between));
    try testing.expectEqual(before_log, shard.raft_node.log.lastIndex());
    _ = handler.commitRun(&shard, testKey(.default, "run-q"));
    switch (handler.callWork(&shard, testKey(.default, "run-q"), between)) {
        .later => |due| try testing.expect(due >= last_ms + @as(i64, @intCast(wait_ms)) - 1),
        else => return error.TestUnexpectedResult,
    }
    settleTest(&shard);
    const run2 = handler.takeRun(&shard, testKey(.default, "run-q")).?;
    // Room again: asked again, it starts.
    try hq.setForTest(&shard.defaultPartition().store, .default, 1 << 30, 0);
    const rid2 = run2.pending_action_run_id_owned.?;
    handler.retryCall(&shard, run2, rid2, WorkflowHandler.callOf(&shard, run2, rid2).?, @import("stdx").time.milliTimestamp());
    _ = handler.commitRun(&shard, testKey(.default, "run-q"));
    settleTest(&shard);
    const again = handler.takeRun(&shard, testKey(.default, "run-q")).?;
    try testing.expectEqual(calls_mod.State.started, WorkflowHandler.callOf(&shard, again, again.pending_action_run_id_owned.?).?.state);
    _ = handler.commitRun(&shard, testKey(.default, "run-q"));
}

test "a held call waits on its own hold's backoff: the quota's up to 10 minutes, memory's and a missing action's up to one" {
    try testing.expectEqual(@as(u64, 600_000), WorkflowHandler.heldRetryMs(.over_quota, 8));
    try testing.expectEqual(WorkflowHandler.CALL_RETRY_MAX_MS, WorkflowHandler.heldRetryMs(.over_budget, 8));
    try testing.expectEqual(WorkflowHandler.CALL_RETRY_MAX_MS, WorkflowHandler.heldRetryMs(.no_action, 8));
}

test "a call held at its callee's quota asks again at 5 s, doubling, at most 10 minutes" {
    try testing.expectEqual(@as(u64, 5_000), WorkflowHandler.quotaRetryMs(0));
    try testing.expectEqual(@as(u64, 5_000), WorkflowHandler.quotaRetryMs(1));
    try testing.expectEqual(@as(u64, 10_000), WorkflowHandler.quotaRetryMs(2));
    try testing.expectEqual(@as(u64, 600_000), WorkflowHandler.quotaRetryMs(8));
    try testing.expectEqual(@as(u64, 600_000), WorkflowHandler.quotaRetryMs(std.math.maxInt(u32)));
}

test "step executor: a call first unregistered, then over its action's shard's share, then unregistered again, says each hold once" {
    const settings = @import("../settings/machine.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, testKey(.default, "run-u"), "run-u", "test-wf");
    const key = testKey(.default, "run-u");
    const S = struct {
        /// Each hold's note in `run`'s history, how many times.
        fn notes(run: *const WorkflowHandler.RunRecord) [2]usize {
            var n: [2]usize = .{ 0, 0 };
            for (run.history.items) |evt| {
                if (std.mem.eql(u8, evt.event_type_owned, "action_not_registered")) n[0] += 1;
                if (std.mem.eql(u8, evt.event_type_owned, "call_held_over_budget")) n[1] += 1;
            }
            return n;
        }
        /// Asked again, as the duty would, and answered.
        fn again(h: *WorkflowHandler, sh: *Shard, k: []const u8) void {
            const run = h.takeRun(sh, k).?;
            const rid = run.pending_action_run_id_owned.?;
            h.retryCall(sh, run, rid, WorkflowHandler.callOf(sh, run, rid).?, @import("stdx").time.milliTimestamp());
            _ = h.commitRun(sh, k);
            settleTest(sh);
            h.checkPendingActions(sh);
            settleTest(sh);
        }
    };
    // Not registered: said once.
    handler.advanceWorkflow(&shard, key);
    settleTest(&shard);
    handler.checkPendingActions(&shard);
    settleTest(&shard);
    try testing.expectEqual([2]usize{ 1, 0 }, S.notes(handler.inspect(key).?));
    // Registered, but its shard has no room: that hold said once too.
    registerTestAction(&shard, "step-a");
    try settings.bindForTest(&shard.defaultPartition().store, 1);
    S.again(&handler, &shard, key);
    S.again(&handler, &shard, key);
    try testing.expectEqual(calls_mod.State.over_budget, WorkflowHandler.callOf(&shard, handler.inspect(key).?, handler.inspect(key).?.pending_action_run_id_owned.?).?.state);
    try testing.expectEqual([2]usize{ 1, 1 }, S.notes(handler.inspect(key).?));
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(key).?.status);
    // Then room, but the action gone: unregistered again, and neither hold
    // said twice for the one call.
    applyTestActionEntry(&shard, .action_delete, test_action_entries.Delete.Value{ .ns = NamespaceId.default.int(), .name = "step-a" }, 1);
    try settings.bindForTest(&shard.defaultPartition().store, 1 << 40);
    S.again(&handler, &shard, key);
    try testing.expectEqual(calls_mod.State.no_action, WorkflowHandler.callOf(&shard, handler.inspect(key).?, handler.inspect(key).?.pending_action_run_id_owned.?).?.state);
    try testing.expectEqual([2]usize{ 1, 1 }, S.notes(handler.inspect(key).?));
}

/// `n` signals to run `key`, each with a type `size` bytes long, applied as
/// a shard applies them: their events fill its chunks fast.
fn signalTestRun(handler: *WorkflowHandler, key: []const u8, n: usize, size: usize) void {
    var tb: [1024]u8 = undefined;
    var vb: [1100]u8 = undefined;
    var pb: [1300]u8 = undefined;
    for (0..n) |i| {
        @memset(tb[0..size], 'a' + @as(u8, @intCast(i % 26)));
        const value = runs_machine.Signal.encodeInto(&vb, .{ .type = tb[0..size], .payload = "" }) catch unreachable;
        var out: Result = .{};
        handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_signal, 0, 1, 100 + i, (1 + i) * std.time.ns_per_ms, .default, key, value, &pb).?, &out) catch unreachable;
    }
}

/// Every seq a history read gives run `key`, page by page of `limit`.
fn pagedSeqs(handler: *WorkflowHandler, key: []const u8, limit: u32, newest_first: bool, out: []u64) ![]u64 {
    var n: usize = 0;
    var cursor: ?u64 = null;
    var pages: usize = 0;
    while (true) : (pages += 1) {
        try testing.expect(pages < 1000);
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const q: WorkflowHandler.HistoryRequest = .{ .limit = limit, .cursor = cursor, .newest_first = newest_first };
        const rec = runs_machine.runOf(handler.defsRead(), key).?;
        const page = (try handler.readRunHistory(arena.allocator(), key, rec, q.window(), limit, false)).page;
        try testing.expect(page.events.len <= limit);
        for (page.events) |e| {
            out[n] = e.seq;
            n += 1;
        }
        cursor = page.more orelse return out[0..n];
    }
}

test "workflow history: a run's events read back whole and in order, past its chunks' folds, paged by cursor either way" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    // Braces in its workflow's name route nothing: its slot is its own.
    const key = testKey(.default, "run-h");
    createTestRun(&handler, key, "run-h", "x{y}z");
    // About three events a chunk: past 16 chunks, so its rows fold.
    signalTestRun(&handler, key, 60, 1000);
    settleTest(&shard);
    const rec = runs_machine.runOf(handler.defsRead(), key).?;
    try testing.expectEqual(@as(u64, 60), rec.hist_next);
    try testing.expect(rec.hist_flushed >= 48);

    var seqs: [80]u64 = undefined;
    const all = try pagedSeqs(&handler, key, 1000, false, &seqs);
    try testing.expectEqual(@as(usize, 60), all.len);
    for (all, 0..) |s, i| try testing.expectEqual(@as(u64, i), s);
    // Paged, each once, oldest first or newest first.
    for ([_]u32{ 1, 7, 59, 60 }) |limit| {
        const up = try pagedSeqs(&handler, key, limit, false, &seqs);
        try testing.expectEqual(@as(usize, 60), up.len);
        for (up, 0..) |s, i| try testing.expectEqual(@as(u64, i), s);
        const down = try pagedSeqs(&handler, key, limit, true, &seqs);
        try testing.expectEqual(@as(usize, 60), down.len);
        for (down, 0..) |s, i| try testing.expectEqual(@as(u64, 59 - i), s);
    }
    // Each event as its signal applied it.
    const run = handler.inspect(key).?;
    try testing.expectEqual(@as(usize, 60), run.history.items.len);
    try testing.expectEqualStrings("signal_received", run.history.items[59].event_type_owned);
    try testing.expectEqual(@as(usize, 1000), run.history.items[59].detail_owned.len);
}

test "workflow history: a read whose chunks haven't landed waits, and at its deadline says which are missing" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const key = testKey(.default, "run-w");
    createTestRun(&handler, key, "run-w", "test-wf");
    // Sent, not delivered: its chunks are on the lane.
    signalTestRun(&handler, key, 8, 1000);
    const rec = runs_machine.runOf(handler.defsRead(), key).?;
    try testing.expect(rec.hist_flushed > 0);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expect(try handler.readRunHistory(a, key, rec, .{}, 100, false) == .wait);
    // A page wholly past the gap, the newest pending event, isn't held.
    try testing.expect(rec.hist_next > rec.hist_flushed);
    const newest = (try handler.readRunHistory(a, key, rec, .{ .newest_first = true }, 1, false)).page;
    try testing.expectEqual(rec.hist_next - 1, newest.events[0].seq);
    // At its deadline: what landed (none), the rest said missing, then
    // what is pending.
    const late = (try handler.readRunHistory(a, key, rec, .{}, 100, true)).page;
    try testing.expectEqualStrings(WorkflowHandler.HISTORY_MISSING, late.events[0].event_type);
    try testing.expectEqual(rec.hist_next - rec.hist_flushed + 1, late.events.len);
    try testing.expectEqual(rec.hist_flushed, late.events[1].seq);
    // Paged at its deadline one at a time, the gap is one page, and the
    // next starts past it.
    var cursor: ?u64 = null;
    var pages: usize = 0;
    while (true) : (pages += 1) {
        try testing.expect(pages <= late.events.len);
        const q: WorkflowHandler.HistoryRequest = .{ .limit = 1, .cursor = cursor };
        const p = (try handler.readRunHistory(a, key, rec, q.window(), 1, true)).page;
        if (pages == 0) try testing.expectEqualStrings(WorkflowHandler.HISTORY_MISSING, p.events[0].event_type);
        cursor = p.more orelse break;
    }
    try testing.expectEqual(late.events.len - 1, pages);
    // Landed: whole, with no wait.
    settleTest(&shard);
    const landed = (try handler.readRunHistory(a, key, rec, .{}, 100, false)).page;
    try testing.expectEqual(@as(usize, 8), landed.events.len);
    for (landed.events, 0..) |e, i| try testing.expectEqual(@as(u64, i), e.seq);
}

test "workflow history: a request is a limit, then a cursor and an order, as its schema has them" {
    const R = history_read.Request;
    var buf: [R.max_size]u8 = undefined;
    var diag: wire_schema.Diagnostic = .{};
    const full = try R.decodeAll(try R.encodeInto(&buf, .{ .limit = 5, .cursor = 9, .newest_first = true }), &diag);
    try testing.expectEqual(@as(u32, 5), full.limit);
    try testing.expectEqual(@as(?u64, 9), full.cursor);
    try testing.expect(full.newest_first);
    const plain = try R.decodeAll(try R.encodeInto(&buf, .{ .limit = 100, .cursor = null, .newest_first = false }), &diag);
    try testing.expect(plain.cursor == null and !plain.newest_first);
    // A presence byte that is neither absent nor present is refused.
    try testing.expectError(error.Refused, R.decodeAll(&.{ 2, 0, 0, 0, 0 }, &diag));
}

test "workflow history: past its history lane's cap, a shard holds new runs and steps until the lane drains" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    shard.outbox_caps.other.channel = 64;
    const key = testKey(.default, "run-q");
    createTestRun(&handler, key, "run-q", "test-wf");
    // Its chunks on the lane, past its cap: a start, and a step, wait.
    signalTestRun(&handler, key, 4, 1000);
    try testing.expect(shard.historyWait(.default) != null);
    try testing.expectError(error.Overloaded, handler.proposeStart(&shard, .default, "run-r", "test-wf", "latest", "{}", null, "workflow_started", null));
    try testing.expect(handler.proposeRun(&shard, .default, "run-r", "test-wf", "latest", "{}", null, "schedule_started") == null);
    const run = handler.takeRun(&shard, key).?;
    try testing.expectError(error.HeldForHistory, handler.proposeStep(&shard, run));
    _ = handler.commitRun(&shard, key);
    try testing.expect(handler.steppingAt(&shard, key) == null);
    // Drained: both go.
    settleTest(&shard);
    try testing.expect(shard.historyWait(.default) == null);
    try testing.expect(handler.proposeRun(&shard, .default, "run-r", "test-wf", "latest", "{}", null, "schedule_started") != null);
}

/// A step of run `key` (of `wf`) at `rev`, one event with `detail`, applied
/// as a shard applies it.
fn stepTestRun(handler: *WorkflowHandler, key: []const u8, wf: []const u8, rev: u64, detail: []const u8) void {
    const Step = runs_machine.Step;
    const eb = testing.allocator.alloc(u8, detail.len + 64) catch unreachable;
    defer testing.allocator.free(eb);
    var ew: wire_schema.Writer = .{ .buf = eb };
    runs_machine.Event.encode(&ew, .{ .event = "big", .detail = detail, .at_ms = 0 }) catch unreachable;
    const value: Step.Value = .{
        .created_ns = handler.appliedRun(key).?.created_ns,
        .rev = rev,
        .step = "s",
        .events = .{ .bytes = ew.written(), .count = 1 },
        .outputs = .{ .bytes = "", .count = 0 },
        .wait = .none,
        .wait_kind = .action,
        .wait_slot = 0,
        .wait_on = "",
        .wait_due_ms = 0,
        .wait_timeout_to = "",
        .progress = "",
        .settles = "",
        .caller_slot = calls_mod.callerSlot(.default, wf),
        .workflow = wf,
        .ends = 0,
        .has_output = false,
        .output = "",
        .call = "",
        .takes = .{ .bytes = "", .count = 0 },
    };
    const vb = testing.allocator.alloc(u8, detail.len + 1024) catch unreachable;
    defer testing.allocator.free(vb);
    const pb = testing.allocator.alloc(u8, persistence_mod.MAX_PERSIST_PAYLOAD) catch unreachable;
    defer testing.allocator.free(pb);
    var out: Result = .{};
    handler.replayEntry(&entry_mod.buildCommandEntry(.workflow_step, 0, 1, 500 + rev, (1 + rev) * std.time.ns_per_ms, .default, key, Step.encodeInto(vb, value) catch unreachable, pb).?, &out) catch unreachable;
}

test "workflow history: a page holds at most its event bound and its byte bound, and paging reads past both" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const key = testKey(.default, "run-b");
    createTestRun(&handler, key, "run-b", "test-wf");
    // Five events of 60 KiB: more than one answer's bytes.
    const detail = try allocator.alloc(u8, 60 * 1024);
    defer allocator.free(detail);
    @memset(detail, 'd');
    for (0..5) |i| stepTestRun(&handler, key, "test-wf", i, detail);
    settleTest(&shard);
    const rec = runs_machine.runOf(handler.defsRead(), key).?;
    try testing.expectEqual(@as(u64, 5), rec.hist_next);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // However many asked for, a page's bytes fit its answer.
    const first = (try handler.readRunHistory(a, key, rec, .{}, std.math.maxInt(u32), false)).page;
    try testing.expect(first.events.len < 5 and first.events.len > 0);
    try testing.expect(first.more != null);
    const v = try history_read.response(a, first);
    try testing.expect(try history_read.Response.encodedSize(v) <= ops.MAX_RESPONSE_BODY);
    // Paged, every event once.
    var seqs: [16]u64 = undefined;
    const all = try pagedSeqs(&handler, key, std.math.maxInt(u32), false, &seqs);
    try testing.expectEqual(@as(usize, 5), all.len);
    // A limit of none still reads one.
    try testing.expectEqual(@as(usize, 1), (try handler.readRunHistory(a, key, rec, .{}, 0, false)).page.events.len);
}

test "a step calling a child workflow named longer than a run's history may name fails, saying so, and starts nothing" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    var name: [WorkflowHandler.MAX_WORKFLOW_NAME_LEN + 1]u8 = undefined;
    @memset(&name, 'k');
    var db: [1024]u8 = undefined;
    const def = try std.fmt.bufPrint(&db, "{{\"kind\":\"Workflow\",\"name\":\"par\",\"version\":\"1.0.0\",\"start\":{{\"run\":\"@workflow/{s}\",\"transitions\":{{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}}}}", .{&name});
    createTestDef(&handler, testKey(.default, "par"), "par", def);
    createTestRun(&handler, testKey(.default, "p1"), "p1", "par");
    handler.advanceWorkflow(&shard, testKey(.default, "p1"));
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "p1")).?;
    try testing.expect(run.pending_action_run_id_owned == null);
    var said = false;
    for (run.history.items) |evt| {
        if (std.mem.eql(u8, evt.event_type_owned, "step_call_too_large") and std.mem.eql(u8, evt.detail_owned, "a workflow name is longer than a run's history may name")) said = true;
    }
    try testing.expect(said);
}

test "a step calling an action named longer than an action may be fails, saying so, and one at the bound makes its call" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    var name: [ae.MAX_ACTION_NAME + 1]u8 = undefined;
    @memset(&name, 'a');
    var db: [1024]u8 = undefined;
    inline for (.{ "at", "over" }, 0..) |wf, i| {
        const target: []const u8 = if (i == 0) name[0..ae.MAX_ACTION_NAME] else &name;
        const def = try std.fmt.bufPrint(&db, "{{\"kind\":\"Workflow\",\"name\":\"{s}\",\"version\":\"1.0.0\",\"start\":{{\"run\":\"@actions/{s}\",\"transitions\":{{\"success\":\"flo.Completed\",\"failure\":\"flo.Failed\"}}}}}}", .{ wf, target });
        createTestDef(&handler, testKey(.default, wf), wf, def);
        const key = testKey(.default, wf);
        createTestRun(&handler, key, wf, wf);
        handler.advanceWorkflow(&shard, key);
        settleTest(&shard);
        const run = handler.inspect(key).?;
        var said = false;
        for (run.history.items) |evt| {
            if (std.mem.eql(u8, evt.event_type_owned, "step_call_too_large") and std.mem.eql(u8, evt.detail_owned, "an action name is longer than an action may have")) said = true;
        }
        if (target.len <= ae.MAX_ACTION_NAME) {
            try testing.expect(!said);
            try testing.expect(run.pending_action_run_id_owned != null);
        } else {
            try testing.expect(said);
            try testing.expect(run.pending_action_run_id_owned == null);
        }
    }
}

/// A start of `run` as this leader's entry `index` in `term`, applied: refused
/// for memory, or admitted, `made` saying whether it created its run.
fn decidedStart(handler: *WorkflowHandler, eb: []u8, comptime ns: NamespaceId, comptime run: []const u8, value: []const u8, term: u64, index: u64, verdict: enum { memory, quota, made, idem }) void {
    const e = entry_mod.buildCommandEntry(.workflow_start, entry_mod.Flags.NONE, term, index, 0, ns, testKey(ns, run), value, eb).?;
    var out = Result.start(&e);
    switch (verdict) {
        .memory => out.refuse(.limit, .memory_budget, "shard 0 memory: past its share"),
        .quota => out.refuse(.limit, .history_quota, "limit: ns 16 history 1+1 of 1 on shard 0"),
        .made => out.putInt(u8, 1),
        .idem => out.putInt(u8, 0),
    }
    handler.entryApplied(&e, &out, .{ .term = term, .last_index = index + 9 });
}

test "workflow schedule: at its namespace's quota a slot isn't proposed; refused for the quota it waits its own, longer, backoff, then fires once, late" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const store = &shard.defaultPartition().store;

    const interval: i64 = 3_600_000;
    const now = @import("stdx").time.milliTimestamp();
    const anchor = now - 10 * interval;
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = interval, .max_concurrent = 0 }, anchor);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    const latest = anchor + 10 * interval;
    const term = shard.raft_node.current_term;

    // At the quota: nothing proposed, counted held at the quota alone.
    try hq.setForTest(store, TEST_NS_A, 1 << 30, 1 << 30);
    var before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(@as(?u64, 1 << 30), sched.quota_hold.share);
    try testing.expectEqual(@as(u64, 1), handler.quota_held);
    try testing.expectEqual(@as(u64, 0), handler.memory_held);
    // Said once for the episode, however long it lasts.
    const said = handler.holds_said;
    sched.quota_hold.said_until_ms = 0;
    handler.tickSchedules(&shard);
    handler.tickSchedules(&shard);
    try testing.expectEqual(said, handler.holds_said);
    // Another namespace's usage holds nothing of this one.
    try hq.setForTest(store, TEST_NS_A, 1 << 30, 0);
    try hq.setForTest(store, .default, 1 << 30, 1 << 30);
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    try testing.expectEqual(latest, sched.proposed_ms);
    try testing.expectEqual(@as(u64, 0), handler.quota_held);

    // Refused at apply for the quota: the slot is free again, after the
    // quota's wait, which is its own and longer than memory's past a minute.
    var buf: [256]u8 = undefined;
    var key_buf: [128]u8 = undefined;
    var eb: [512]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "_schedule:16:wf:{d}", .{latest});
    const value = startValue(&buf, "wf", "schedule_started", key);
    decidedStart(&handler, &eb, TEST_NS_A, "r1", value, term, 50, .quota);
    try testing.expectEqual(std.math.minInt(i64), sched.proposed_ms);
    try testing.expectEqual(@as(u32, 1), sched.quota_hold.refusals);
    try testing.expectEqual(@as(u32, 0), sched.hold.refusals);
    before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    sched.quota_hold.until_ms = 0;
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    // Refused again, it waits twice as long; past a minute the quota's
    // wait outgrows memory's, up to 10 minutes.
    decidedStart(&handler, &eb, TEST_NS_A, "r2", value, term, 60, .quota);
    try testing.expectEqual(@as(u32, 2), sched.quota_hold.refusals);
    try testing.expect(sched.quota_hold.until_ms >= now + @as(i64, @intCast(WorkflowHandler.quotaRetryMs(2))));
    try testing.expect(WorkflowHandler.quotaRetryMs(8) > WorkflowHandler.callRetryMs(8));
    // A run it started ends the wait.
    decidedStart(&handler, &eb, TEST_NS_A, "r3", value, term, 70, .made);
    try testing.expectEqual(@as(u32, 0), sched.quota_hold.refusals);
}

test "workflow schedule: at its shard's memory share a slot isn't proposed; refused for memory it waits, then fires once, late" {
    const settings = @import("../settings/machine.zig");
    const pool = @import("../budget/pool.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const store = &shard.defaultPartition().store;

    const interval: i64 = 3_600_000;
    const now = @import("stdx").time.milliTimestamp();
    const anchor = now - 10 * interval;
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = interval, .max_concurrent = 0 }, anchor);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    const latest = anchor + 10 * interval;
    const term = shard.raft_node.current_term;

    // At the share: nothing proposed, and the schedule counted held.
    try settings.bindForTest(store, pool.useOf(store));
    var before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expect(sched.hold.share != null);
    try testing.expectEqual(@as(u64, 1), handler.memory_held);

    // Memory frees: the latest slot is proposed.
    try settings.bindForTest(store, std.math.maxInt(u64));
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    try testing.expectEqual(latest, sched.proposed_ms);
    try testing.expectEqual(@as(u64, 0), handler.memory_held);

    // Refused at apply for memory: the slot is free to propose again, after
    // a wait. Another term's refusal, or another slot's, changes nothing.
    var buf: [256]u8 = undefined;
    var key_buf: [128]u8 = undefined;
    var eb: [512]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "_schedule:16:wf:{d}", .{latest});
    const value = startValue(&buf, "wf", "schedule_started", key);
    decidedStart(&handler, &eb, TEST_NS_A, "r1", value, term + 1, 50, .memory);
    try testing.expectEqual(latest, sched.proposed_ms);
    var other: [256]u8 = undefined;
    decidedStart(&handler, &eb, TEST_NS_A, "r1", startValue(&other, "wf", "schedule_started", "_schedule:16:wf:5"), term, 50, .memory);
    try testing.expectEqual(latest, sched.proposed_ms);
    decidedStart(&handler, &eb, TEST_NS_A, "r1", value, term, 50, .memory);
    try testing.expectEqual(std.math.minInt(i64), sched.proposed_ms);
    try testing.expectEqual(@as(u32, 1), sched.hold.refusals);
    before = shard.raft_node.log.lastIndex();
    handler.tickSchedules(&shard);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    // Its wait out: the same slot, once.
    sched.hold.until_ms = 0;
    handler.tickSchedules(&shard);
    try testing.expectEqual(before + 1, shard.raft_node.log.lastIndex());
    try testing.expectEqual(latest, sched.proposed_ms);
    // Refused again, it waits twice as long.
    decidedStart(&handler, &eb, TEST_NS_A, "r2", value, term, 60, .memory);
    try testing.expectEqual(@as(u32, 2), sched.hold.refusals);
    try testing.expect(sched.hold.until_ms >= now + @as(i64, @intCast(WorkflowHandler.callRetryMs(2))));
    // A run it started ends the wait.
    decidedStart(&handler, &eb, TEST_NS_A, "r3", value, term, 70, .made);
    try testing.expectEqual(@as(u32, 0), sched.hold.refusals);
}

test "workflow handler: a trigger's start refused for memory waits before reading again, once for the chain it broke; a run it starts ends the wait" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    const t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    const at: StreamID = .{ .timestamp_ms = 1800, .sequence = 0 };
    var buf: [256]u8 = undefined;
    var kb: [128]u8 = undefined;
    var eb: [512]u8 = undefined;
    const value = triggerStartValue(&buf, &kb, "wf", .default, 1, "events", 1700, 0, .{ 0, 0 });
    t.proposed = .{ .at = at, .term = 3, .chain = at };
    decidedStart(&handler, &eb, .default, "r1", value, 3, 31, .memory);
    try testing.expect(t.proposed == null);
    try testing.expectEqual(@as(u32, 1), t.hold.refusals);
    try testing.expect(t.hold.share != null);
    // The rest of its chain, refused: no more waiting.
    decidedStart(&handler, &eb, .default, "r2", value, 3, 35, .memory);
    try testing.expectEqual(@as(u32, 1), t.hold.refusals);
    // A chain begun after it, refused: waits longer.
    decidedStart(&handler, &eb, .default, "r3", value, 3, 41, .memory);
    try testing.expectEqual(@as(u32, 2), t.hold.refusals);
    // A start read again whose run was made already doesn't end it, nor
    // one of the broken chain; a run made above it does.
    decidedStart(&handler, &eb, .default, "r4", value, 3, 51, .idem);
    try testing.expectEqual(@as(u32, 2), t.hold.refusals);
    decidedStart(&handler, &eb, .default, "r5", value, 3, 49, .made);
    try testing.expectEqual(@as(u32, 2), t.hold.refusals);
    decidedStart(&handler, &eb, .default, "r6", value, 3, 51, .made);
    try testing.expectEqual(@as(u32, 0), t.hold.refusals);
    // Another term's start doesn't touch this one's.
    t.proposed = .{ .at = at, .term = 3, .chain = at };
    const e = entry_mod.buildCommandEntry(.workflow_start, entry_mod.Flags.NONE, 2, 60, 0, .default, testKey(.default, "r7"), value, &eb).?;
    var out = Result.start(&e);
    out.refuse(.limit, .memory_budget, "past its share");
    handler.entryApplied(&e, &out, .{ .term = 3, .last_index = 70 });
    try testing.expect(t.proposed != null);
    try testing.expectEqual(@as(u32, 0), t.hold.refusals);
}

test "workflow handler: a trigger at its namespace's quota reads nothing; its start refused for the quota waits once for the chain it broke, on the quota's hold alone" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    const t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    // At the quota: its poll reads nothing, and it is counted held.
    try hq.setForTest(&shard.defaultPartition().store, .default, 1 << 30, 1 << 30);
    handler.triggers_dirty = true;
    handler.tickStreamTriggers(&shard);
    try testing.expectEqual(@as(?u64, 1 << 30), t.quota_hold.share);
    try testing.expect(t.proposed == null);
    // A refusal at apply breaks its chain and waits, on the quota's hold.
    const at: StreamID = .{ .timestamp_ms = 1800, .sequence = 0 };
    var buf: [256]u8 = undefined;
    var kb: [128]u8 = undefined;
    var eb: [512]u8 = undefined;
    const value = triggerStartValue(&buf, &kb, "wf", .default, 1, "events", 1700, 0, .{ 0, 0 });
    t.proposed = .{ .at = at, .term = 3, .chain = at };
    decidedStart(&handler, &eb, .default, "r1", value, 3, 31, .quota);
    try testing.expect(t.proposed == null);
    try testing.expectEqual(@as(u32, 1), t.quota_hold.refusals);
    try testing.expectEqual(@as(u32, 0), t.hold.refusals);
    // The rest of its chain, refused: no more waiting.
    decidedStart(&handler, &eb, .default, "r2", value, 3, 35, .quota);
    try testing.expectEqual(@as(u32, 1), t.quota_hold.refusals);
    // A run made above it ends the wait.
    decidedStart(&handler, &eb, .default, "r3", value, 3, 51, .made);
    try testing.expectEqual(@as(u32, 0), t.quota_hold.refusals);
}

test "workflow handler: a trigger re-created on its stream keeps what its leader read, its resync and its wait; moved to another stream it begins again" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const json = "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}";
    createTestDef(&handler, testKey(.default, "wf"), "wf", json);
    var t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    const at: StreamID = .{ .timestamp_ms = 1800, .sequence = 0 };
    const chain: StreamID = .{ .timestamp_ms = 1700, .sequence = 4 };
    t.proposed = .{ .at = at, .term = 3, .chain = chain };
    t.resync_index = 40;
    t.hold = .{ .term = 3, .refusals = 2, .until_ms = 99, .share = 7 };
    createTestDef(&handler, testKey(.default, "wf"), "wf", json);
    t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    try testing.expectEqual(chain, t.proposed.?.chain);
    try testing.expectEqual(at, t.proposed.?.at);
    try testing.expectEqual(@as(u64, 40), t.resync_index);
    try testing.expectEqual(@as(u32, 2), t.hold.refusals);
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"other\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    try testing.expect(t.proposed == null);
    try testing.expectEqual(@as(u64, 0), t.resync_index);
    try testing.expectEqual(@as(u32, 0), t.hold.refusals);
}

test "workflow handler: at its shard's memory share a trigger reads nothing, its place where it is, and is counted held" {
    const settings = @import("../settings/machine.zig");
    const pool = @import("../budget/pool.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    createTestDef(&handler, testKey(.default, "wf"), "wf", "{\"kind\":\"Workflow\",\"name\":\"wf\",\"version\":\"1\",\"trigger\":{\"stream\":\"events\"},\"start\":{\"wait_for_signal\":{\"type\":\"never\"},\"transitions\":{\"success\":\"flo.Completed\"}}}");
    const t = handler.stream_triggers.getPtr(testKey(.default, "wf")).?;
    const store = &shard.defaultPartition().store;
    try settings.bindForTest(store, pool.useOf(store));
    for (0..5) |_| {
        handler.triggers_dirty = true;
        handler.tickStreamTriggers(&shard);
    }
    try testing.expectEqual(@as(i64, 0), t.last_poll_ms);
    try testing.expect(t.hold.share != null);
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(u64, 1), handler.memory_held);
    // Held five ticks: said once.
    try testing.expectEqual(@as(u64, 1), handler.holds_said);
    try settings.bindForTest(store, std.math.maxInt(u64));
    handler.triggers_dirty = true;
    handler.tickStreamTriggers(&shard);
    try testing.expectEqual(@as(?u64, null), t.hold.share);
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(u64, 0), handler.memory_held);
    // At the share again before a run of it started: the same episode.
    try settings.bindForTest(store, pool.useOf(store));
    handler.triggers_dirty = true;
    handler.tickStreamTriggers(&shard);
    try testing.expectEqual(@as(u64, 1), handler.holds_said);
    // A new term within the minute: not said again.
    shard.raft_node.current_term += 1;
    handler.triggers_dirty = true;
    handler.tickStreamTriggers(&shard);
    try testing.expectEqual(@as(u64, 1), handler.holds_said);
    // Past it: said again.
    t.hold.said_until_ms = 0;
    handler.triggers_dirty = true;
    handler.tickStreamTriggers(&shard);
    try testing.expectEqual(@as(u64, 2), handler.holds_said);
    // The node steps down: nothing of it is held here, the gauge drops.
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(u64, 1), handler.memory_held);
    handler.dropHolds(&shard);
    try testing.expectEqual(@as(u64, 0), handler.memory_held);
    try testing.expectEqual(@as(?u64, null), t.hold.share);
}

test "workflow schedule: re-created, a schedule keeps its hold, as a trigger does" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = 60_000, .max_concurrent = 0 }, 0);
    handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?.hold = .{ .term = 3, .refusals = 2, .until_ms = 99, .share = 7, .said_until_ms = 5 };
    try handler.registerSchedule(TEST_NS_A, "wf", .{ .interval_ms = 30_000, .max_concurrent = 0 }, 10);
    const h = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?.hold;
    try testing.expectEqual(@as(u32, 2), h.refusals);
    try testing.expectEqual(@as(i64, 5), h.said_until_ms);
}

test "a child at its namespace's quota is answered over_quota by its leader, not started, and its parent waits" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "par")).?;
    var kb: [ae.MAX_RUN_ID]u8 = undefined;
    const kid_id = kb[0..run.pending_action_run_id_owned.?.len];
    @memcpy(kid_id, run.pending_action_run_id_owned.?);
    const store = &shard.defaultPartition().store;
    try testing.expect(children_mod.parentOf(.{ .store = store }, .default, kid_id) != null);
    // The child's reserve won't fit, nor is there memory: the leader
    // answers the call itself, for the quota, with its figures.
    try hq.setForTest(store, .default, runs_machine.reserveOf("kid-wf", kid_id) - 1, 0);
    try @import("../settings/machine.zig").bindForTest(store, 1);
    const refusals = store.quota_refusals;
    startTestChildren(&handler, &shard);
    try testing.expectEqual(refusals + 1, store.quota_refusals);
    try testing.expect(children_mod.parentOf(.{ .store = store }, .default, kid_id) == null);
    var rk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    try testing.expect(handler.appliedRun(writeKey(&rk, .default, kid_id)) == null);
    const c = WorkflowHandler.callOf(&shard, handler.inspect(testKey(.default, "par")).?, kid_id).?;
    try testing.expectEqual(calls_mod.State.over_quota, c.state);
    try testing.expect(std.mem.startsWith(u8, c.outcome, "limit: ns 1 run reserve "));
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "par")).?.status);
}

test "a child its shard is at its memory share for is answered over_budget by its leader, not started, and its parent waits" {
    const settings = @import("../settings/machine.zig");
    const pool = @import("../budget/pool.zig");
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    registerTestAction(&shard, "step-a");
    createTestDef(&handler, testKey(.default, "par-wf"), "par-wf", test_parent_json);
    createTestDef(&handler, testKey(.default, "kid-wf"), "kid-wf", test_kid_json);
    createTestRun(&handler, testKey(.default, "par"), "par", "par-wf");
    handler.advanceWorkflow(&shard, testKey(.default, "par"));
    settleTest(&shard);
    const run = handler.inspect(testKey(.default, "par")).?;
    var kb: [ae.MAX_RUN_ID]u8 = undefined;
    const kid_id = kb[0..run.pending_action_run_id_owned.?.len];
    @memcpy(kid_id, run.pending_action_run_id_owned.?);
    const store = &shard.defaultPartition().store;
    try testing.expect(children_mod.parentOf(.{ .store = store }, .default, kid_id) != null);
    // At the share: the leader answers the call itself.
    try settings.bindForTest(store, pool.useOf(store));
    startTestChildren(&handler, &shard);
    try testing.expect(children_mod.parentOf(.{ .store = store }, .default, kid_id) == null);
    var rk: [KEY_PREFIX + ae.MAX_RUN_ID]u8 = undefined;
    try testing.expect(handler.appliedRun(writeKey(&rk, .default, kid_id)) == null);
    const c = WorkflowHandler.callOf(&shard, handler.inspect(testKey(.default, "par")).?, kid_id).?;
    try testing.expectEqual(calls_mod.State.over_budget, c.state);
    try testing.expectEqual(WorkflowHandler.RunStatus.waiting, handler.inspect(testKey(.default, "par")).?.status);
}

test "workflow quota: a step at its namespace's quota is held without a copy, asked again only once the namespace eases, and indexed again from its wait" {
    const Eased = @import("../contract/store.zig").Eased;
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const store = &shard.defaultPartition().store;
    const key = testKey(.default, "run-h");
    const quota: WorkflowHandler.Hold = .{ .ns = .default, .kind = .quota };
    createTestDef(&handler, testKey(.default, "test-wf"), "test-wf", test_workflow_json);
    createTestRun(&handler, key, "run-h", "test-wf");

    // At its part: no step proposed, no copy taken, out of `to_step`.
    try hq.setForTest(store, .default, 1 << 30, 1 << 30);
    const before = shard.raft_node.log.lastIndex();
    handler.advanceWorkflow(&shard, key);
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    try testing.expect(!handler.to_step.contains(key));
    try testing.expect(handler.runs.get(key) == null);
    // Passes ask nothing of it: still held, nothing proposed.
    handler.checkPendingActions(&shard);
    handler.advanceStartedRuns(&shard);
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    try testing.expectEqual(before, shard.raft_node.log.lastIndex());

    // Under the part, but nothing eased it: no poll finds it.
    try hq.setForTest(store, .default, 1 << 30, 0);
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    // Another namespace eased: this one's hold isn't asked.
    var other: Eased = .{};
    other.add(TEST_NS_A.int());
    handler.noteEased(other);
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    // Its own eased: released, back in `to_step` (it waits on nothing).
    var own: Eased = .{};
    own.add(NamespaceId.default.int());
    handler.noteEased(own);
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(usize, 0), handler.holdCount(quota));
    try testing.expect(handler.to_step.contains(key));
    try testing.expect(!handler.quota_eased.any());

    // Eased while still at the part: one ask, and it stays held.
    try hq.setForTest(store, .default, 1 << 30, 1 << 30);
    handler.advanceWorkflow(&shard, key);
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    handler.noteEased(own);
    handler.checkPendingActions(&shard);
    try testing.expectEqual(@as(usize, 1), handler.holdCount(quota));
    try testing.expect(!handler.quota_eased.any());
    // A new term drops the held set and the eased marks: the next leader
    // rebuilds from the store, so nothing stays held on a stale verdict.
    handler.noteEased(own);
    handler.clearCaches();
    try testing.expectEqual(@as(usize, 0), handler.holdCount(quota));
    try testing.expect(!handler.quota_eased.any());
}

test "workflow schedule: held at its namespace's quota, a disable clears the hold and its count, and a cleared hold says the next episode again" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = try createTestShard(&actions);
    defer destroyTestShard(&shard);
    handler.store = &shard.defaultPartition().store;
    handler.history = shard.stream_handler;
    shard.workflow_handler = &handler;
    const store = &shard.defaultPartition().store;
    try testing.expectEqual(.created, shard.namespace_handler.applyCreate("a"));
    var out: Result = .{};
    const now = @import("stdx").time.milliTimestamp();
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
    , now - 10_000, 0, &out);
    const sched = handler.schedules.getPtr(testKey(TEST_NS_A, "wf")).?;
    try hq.setForTest(store, TEST_NS_A, 1 << 30, 1 << 30);
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(?u64, 1 << 30), sched.quota_hold.share);
    try testing.expect(sched.quota_hold.said);
    try testing.expectEqual(@as(u64, 1), handler.quota_held);
    // Disabled: nothing of the hold is left, counted or said.
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{0}, now, &out);
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(?u64, null), sched.quota_hold.share);
    try testing.expect(!sched.quota_hold.said);
    try testing.expectEqual(@as(u64, 0), handler.quota_held);
    // Enabled again, still at the quota: a new episode, said again.
    const said = handler.holds_said;
    try handler.replayEnabled(testKey(TEST_NS_A, "wf"), &.{1}, now - 5_000, &out);
    handler.tickSchedules(&shard);
    try testing.expectEqual(@as(?u64, 1 << 30), sched.quota_hold.share);
    try testing.expectEqual(said + 1, handler.holds_said);
}
