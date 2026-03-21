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
//! | workflow_list_runs   | workflow_name     | [limit:u32][sf_len:u16][sf][cl:u16]  |
//! | workflow_get_def     | workflow_name     | version (optional)                  |
//! | workflow_disable     | workflow_name     | version (optional)                  |
//! | workflow_enable      | workflow_name     | version (optional)                  |
//! | workflow_list_defs   | (unused)          | (empty)                             |

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const parser = @import("parser.zig");
const definition = @import("definition.zig");
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

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// WorkflowHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const WorkflowHandler = struct {
    allocator: Allocator,

    /// Mutex for cross-shard thread safety. Acquired when this handler is
    /// accessed from a different shard's thread via forwardToShard dispatch.
    mu: std.Thread.Mutex,

    /// In-memory definition store: "namespace:name" → DefinitionRecord.
    /// Key is namespace-qualified (allocated separately from record fields).
    definitions: std.StringHashMap(DefinitionRecord),

    /// In-memory run store: "namespace:run_id" → RunRecord.
    runs: std.StringHashMap(RunRecord),

    /// Disabled workflows: "namespace:name" → void.
    disabled: std.StringHashMap(void),

    /// Active stream triggers: "namespace:workflow_name" → StreamTriggerState.
    /// Registered when a workflow with a `trigger:` block is created.
    stream_triggers: std.StringHashMap(StreamTriggerState),

    /// Active interval/cron schedules: "namespace:workflow_name" → ScheduleState.
    /// Registered when a workflow with a `schedule:` block is created.
    schedules: std.StringHashMap(ScheduleState),

    /// Monotonic run counter.
    next_run_id: u64,

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

        /// Per-step outputs for JSONPath resolution ($.steps.*).
        step_outputs: ?StepOutputMap = null,

        /// Action run ID when parked waiting for async action completion.
        pending_action_run_id_owned: ?[]const u8 = null,

        /// Step name that triggered the pending action.
        pending_step_name_owned: ?[]const u8 = null,

        /// Shard ID where the pending action run was created (for cross-shard lookup).
        pending_action_shard_id: ?u16 = null,

        /// Retry attempt counter for the current step (reset on step transition).
        retry_count: u32 = 0,

        /// Absolute deadline for wait_for_signal timeout (ms since epoch, 0 = none).
        wait_timeout_at_ms: i64 = 0,

        /// Transition target to follow when wait times out.
        wait_timeout_target_owned: ?[]const u8 = null,
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
            .next_run_id = 1,
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
        }
        self.stream_triggers.deinit();

        // Free schedule state
        var sit = self.schedules.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeScheduleState(entry.value_ptr);
        }
        self.schedules.deinit();
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
        // Run-based ops: key is run_id, NOT workflow name. Cannot pre-route
        // because we don't know which shard owns the run. Handlers search
        // peer shards when not found locally.
        dispatcher.register(.workflow_status, dispatchWorkflow);
        dispatcher.register(.workflow_signal, dispatchWorkflow);
        dispatcher.register(.workflow_cancel, dispatchWorkflow);
        dispatcher.register(.workflow_history, dispatchWorkflow);
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
        const op: OpCode = @enumFromInt(req.header.op_code);
        shard.workflow_handler.handleCommand(shard, conn, req);
        if (op == .workflow_create or op == .workflow_start) {
            shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        }
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        self.mu.lock();
        defer self.mu.unlock();

        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .workflow_create => self.handleCreate(shard, conn, req),
            .workflow_start => self.handleStart(shard, conn, req),
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
    }

    // ── Cross-Shard Run Forwarding ──────────────────────────────────────

    /// For run-based operations (status, signal, cancel, history) where
    /// the key is a run_id: when the run isn't found locally, forward to
    /// the peer that owns the run.
    ///
    /// Uses the shard hint encoded in the run_id (format wfrun-{shard}-{seq})
    /// for O(1) routing. Falls back to linear scan for client-supplied or
    /// legacy run_ids without a shard hint.
    fn forwardRunOp(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request, run_ns_key: []const u8) bool {
        _ = self;
        const peers = shard.peer_shards orelse return false;

        // Fast path: extract shard hint from run_id (format: wfrun-{shard}-{seq})
        if (parseRunShardHint(req.key)) |hint_shard| {
            if (hint_shard < peers.len and hint_shard != shard.id) {
                const peer = peers[hint_shard];
                peer.workflow_handler.handleCommand(peer, conn, req);
                return true;
            }
            // Hint points to us or is out of range — run should be local
            return false;
        }

        // Slow path: client-supplied or legacy run_id without shard hint — linear scan.
        for (peers) |peer| {
            if (peer == shard) continue;
            const peer_wh = peer.workflow_handler;
            peer_wh.mu.lock();
            const has_run = peer_wh.runs.contains(run_ns_key);
            peer_wh.mu.unlock();
            if (has_run) {
                peer_wh.handleCommand(peer, conn, req);
                return true;
            }
        }
        return false;
    }

    /// Parse the shard hint from a run_id with format "wfrun-{shard_id}-{seq}".
    /// Returns the shard_id if present, null for client-supplied or legacy IDs.
    fn parseRunShardHint(run_id: []const u8) ?u16 {
        const prefix = "wfrun-";
        if (run_id.len <= prefix.len) return null;
        if (!std.mem.startsWith(u8, run_id, prefix)) return null;
        const after_prefix = run_id[prefix.len..];
        // Find the next '-' separator: "{shard_id}-{seq}"
        const sep_pos = std.mem.indexOfScalar(u8, after_prefix, '-') orelse return null;
        if (sep_pos == 0 or sep_pos >= after_prefix.len - 1) return null;
        return std.fmt.parseInt(u16, after_prefix[0..sep_pos], 10) catch null;
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const yaml = req.value;

        if (yaml.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow definition is required");
            return;
        }

        // Parse the YAML/JSON definition
        var def = parser.parseWorkflow(self.allocator, yaml) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid workflow definition");
            return;
        };
        defer def.deinit(self.allocator);

        // Validate the definition
        var validation = validator.validateWorkflow(self.allocator, &def) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "validation failed");
            return;
        };
        defer validation.deinit();

        if (validation.hasErrors()) {
            // Build error message from first error
            if (validation.items().len > 0) {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, validation.items()[0].message);
            } else {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow validation failed");
            }
            return;
        }

        // Workflow name must be sent as req.key by all callers (CLI extracts
        // it from YAML client-side). The server validates it matches the
        // parsed definition to catch mismatches early.
        if (req.key.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required as key");
            return;
        }
        const name = req.key;
        const version = def.version;

        // Build namespace-qualified key for the definitions map
        const ns_key = self.makeNsKey(req.namespace, name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        // Remove old definition if exists
        if (self.definitions.fetchRemove(ns_key)) |old| {
            self.allocator.free(old.key); // free old ns-qualified key
            self.allocator.free(old.value.name_owned);
            self.allocator.free(old.value.version_owned);
            self.allocator.free(old.value.yaml_owned);
        }

        // Duplicate all owned data
        const owned_name = self.allocator.dupe(u8, name) catch {
            self.allocator.free(ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const owned_version = self.allocator.dupe(u8, version) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(owned_version);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const now_ms: i64 = std.time.milliTimestamp();

        self.definitions.put(ns_key, .{
            .name_owned = owned_name,
            .version_owned = owned_version,
            .yaml_owned = owned_yaml,
            .created_at_ms = now_ms,
        }) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(owned_version);
            self.allocator.free(owned_yaml);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "definition store failed");
            return;
        };

        // Return the workflow name
        self.persistCreate(shard, req.namespace, name, owned_yaml);

        // Register stream trigger if the definition has one
        if (def.trigger) |trigger| {
            self.registerStreamTrigger(req.namespace, name, trigger);
        } else {
            // Definition updated without trigger — remove any existing trigger
            self.unregisterStreamTrigger(req.namespace, name);
        }

        // Register schedule if the definition has one
        if (def.schedule) |schedule| {
            self.registerSchedule(req.namespace, name, schedule);
        } else {
            self.unregisterSchedule(req.namespace, name);
        }

        shard.sendOkResponse(conn, req.header.request_id, owned_name);
    }

    // ── START ───────────────────────────────────────────────────────────

    fn handleStart(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const workflow_name = req.key;

        if (workflow_name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        // Build namespace-qualified key for definition/disabled lookups
        const def_ns_key = self.makeNsKey(req.namespace, workflow_name) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(def_ns_key);

        // Check workflow exists
        if (!self.definitions.contains(def_ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return;
        }

        // Check workflow is not disabled
        if (self.disabled.contains(def_ns_key)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow is disabled");
            return;
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
                return;
            }
            const ver_len = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;
            if (offset + ver_len > value.len) {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                return;
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
                        return;
                    }
                    const key_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                    offset += 2;
                    if (offset + key_len > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return;
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
                        return;
                    }
                    const rid_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                    offset += 2;
                    if (offset + rid_len > value.len) {
                        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "malformed start request");
                        return;
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

        // If idempotency key is provided, check for duplicate
        if (idempotency_key) |idem_key| {
            var rit = self.runs.iterator();
            while (rit.next()) |entry| {
                const run = entry.value_ptr;
                if (run.idempotency_key_owned) |existing_key| {
                    if (std.mem.eql(u8, existing_key, idem_key) and
                        std.mem.eql(u8, run.workflow_name_owned, workflow_name))
                    {
                        // Return existing run_id (idempotent)
                        shard.sendOkResponse(conn, req.header.request_id, run.run_id_owned);
                        return;
                    }
                }
            }
        }

        // Generate or use explicit run ID.
        // Server-generated IDs encode the owning shard: wfrun-{shard}-{seq}
        // for O(1) cross-shard routing on status/signal/cancel/history.
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = if (explicit_run_id) |rid|
            rid
        else blk: {
            break :blk std.fmt.bufPrint(&run_id_buf, "wfrun-{d}-{d}", .{ shard.id, self.nextRunId() }) catch "wfrun-0";
        };

        // Build namespace-qualified key for the runs map
        const run_ns_key = self.makeNsKey(req.namespace, run_id_str) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        // Guard against ID collision (e.g. replayed entry with same key)
        if (self.runs.contains(run_ns_key)) {
            self.allocator.free(run_ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "run_id collision");
            return;
        }

        // Duplicate for storage
        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch {
            self.allocator.free(run_ns_key);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const owned_wf_name = self.allocator.dupe(u8, workflow_name) catch {
            self.allocator.free(owned_run_id);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_wf_name);

        const owned_version = self.allocator.dupe(u8, version) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_version);

        const owned_input = self.allocator.dupe(u8, input) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            self.allocator.free(owned_version);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_input);

        const owned_idem: ?[]const u8 = if (idempotency_key) |k|
            self.allocator.dupe(u8, k) catch {
                self.allocator.free(owned_run_id);
                self.allocator.free(owned_wf_name);
                self.allocator.free(owned_version);
                self.allocator.free(owned_input);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            }
        else
            null;

        const now_ms: i64 = std.time.milliTimestamp();

        var run = RunRecord{
            .run_id_owned = owned_run_id,
            .workflow_name_owned = owned_wf_name,
            .workflow_version_owned = owned_version,
            .status = .running,
            .input_owned = owned_input,
            .created_at_ms = now_ms,
            .started_at_ms = now_ms,
            .completed_at_ms = null,
            .idempotency_key_owned = owned_idem,
            .signals = .empty,
            .history = .empty,
        };

        // Add initial history event
        const evt_type = self.allocator.dupe(u8, "workflow_started") catch {
            self.freeRunRecord(&run);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        const evt_detail = self.allocator.dupe(u8, input) catch {
            self.allocator.free(evt_type);
            self.freeRunRecord(&run);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        run.history.append(self.allocator, .{
            .event_type_owned = evt_type,
            .detail_owned = evt_detail,
            .timestamp_ms = now_ms,
        }) catch {
            self.allocator.free(evt_type);
            self.allocator.free(evt_detail);
            self.freeRunRecord(&run);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "history store failed");
            return;
        };

        self.runs.put(run_ns_key, run) catch {
            self.allocator.free(run_ns_key);
            self.freeRunRecord(&run);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "run store failed");
            return;
        };

        // Return the run ID
        self.persistStart(shard, req.namespace, owned_run_id, owned_wf_name, owned_version, owned_input, now_ms);
        shard.sendOkResponse(conn, req.header.request_id, owned_run_id);

        // Begin step execution. The run is already in the map; advanceWorkflow
        // will drive it through the workflow graph until it reaches a terminal
        // or a wait_for_signal step.
        self.advanceWorkflow(shard, run_ns_key, req.namespace);
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
            // Run not on this shard — forward to the peer that owns it.
            if (self.forwardRunOp(shard, conn, req, run_ns_key)) return;
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

        const now_ms: i64 = std.time.milliTimestamp();

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
            // Run not on this shard — forward to the peer that owns it.
            if (self.forwardRunOp(shard, conn, req, run_ns_key)) return;
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        const now_ms: i64 = std.time.milliTimestamp();
        run.status = .cancelled;
        run.completed_at_ms = now_ms;

        const reason = if (req.value.len > 0) req.value else "cancelled by user";
        self.addHistoryEvent(run, "workflow_cancelled", reason, now_ms);

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
            // Run not on this shard — forward to the peer that owns it.
            if (self.forwardRunOp(shard, conn, req, run_ns_key)) return;
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
        var fbs = std.io.fixedBufferStream(&wire_buf);
        const w = fbs.writer();
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

        shard.sendOkResponse(conn, req.header.request_id, fbs.getWritten());
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
            // Run not on this shard — forward to the peer that owns it.
            if (self.forwardRunOp(shard, conn, req, run_ns_key)) return;
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

    fn handleListRuns(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const workflow_name = req.key;

        // Parse value: [limit:u32][status_len:u16][status]?[cursor_len:u16][cursor]?
        var limit: u32 = 100;
        var status_filter: ?[]const u8 = null;

        if (req.value.len >= 4) {
            limit = std.mem.readInt(u32, req.value[0..4], .little);
            var offset: usize = 4;

            // Optional status filter
            if (offset + 2 <= req.value.len) {
                const sf_len = std.mem.readInt(u16, req.value[offset..][0..2], .little);
                offset += 2;
                if (sf_len > 0 and offset + sf_len <= req.value.len) {
                    status_filter = req.value[offset .. offset + sf_len];
                    offset += sf_len;
                }
            }
        }

        // Build namespace prefix for filtering ("namespace:")
        const ns_prefix = self.makeNsKey(req.namespace, "") orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(ns_prefix);

        // Collect matching runs
        const RunInfo = struct { run_id: []const u8, workflow: []const u8, status: []const u8, created_at: i64 };
        var runs_list: std.ArrayListUnmanaged(RunInfo) = .{};
        defer runs_list.deinit(self.allocator);

        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            if (runs_list.items.len >= limit) break;
            const run = entry.value_ptr;
            const map_key = entry.key_ptr.*;

            if (!std.mem.startsWith(u8, map_key, ns_prefix)) continue;
            if (workflow_name.len > 0 and !std.mem.eql(u8, run.workflow_name_owned, workflow_name)) continue;
            if (status_filter) |sf| {
                if (!std.mem.eql(u8, run.status.toString(), sf)) continue;
            }

            runs_list.append(self.allocator, .{
                .run_id = run.run_id_owned,
                .workflow = run.workflow_name_owned,
                .status = run.status.toString(),
                .created_at = run.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }

        // Serialize to binary wire format:
        // [count:u32]([run_id_len:u16][run_id][workflow_len:u16][workflow]
        //  [status_len:u16][status][created_at:i64])*
        // [has_more:u8][cursor_len:u16]
        var total: usize = 4; // count
        for (runs_list.items) |r| {
            total += 2 + r.run_id.len + 2 + r.workflow.len + 2 + r.status.len + 8;
        }
        total += 1 + 2; // trailer

        const buf = self.allocator.alloc(u8, total) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);

        std.mem.writeInt(u32, buf[0..4], @intCast(runs_list.items.len), .little);
        var pos: usize = 4;
        for (runs_list.items) |r| {
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
        buf[pos] = 0; // has_more
        pos += 1;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little); // cursor_len

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
        var defs: std.ArrayListUnmanaged(DefInfo) = .{};
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

        const now_ms: i64 = std.time.milliTimestamp();
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
                    self.completeRun(run, .failed, "step not found in definition", now_ms);
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
                    const outcome_or_park = self.executeRunStep(shard, run, run_step, step_input, step_label, namespace, now_ms);

                    // Free resolved input after action invocation
                    if (resolved_input) |ri| self.allocator.free(ri);

                    // null outcome means run is parked (async action)
                    const outcome = outcome_or_park orelse return;

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

                    // Reset retry counter on step transition
                    run.retry_count = 0;

                    // Follow transition
                    const transition = run_step.resolveTransition(outcome) orelse {
                        // No matching transition — fail
                        self.completeRun(run, .failed, "no transition for outcome", now_ms);
                        return;
                    };

                    // Check if target is a terminal (builtin or custom)
                    if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                        self.completeRun(run, status, transition.target, now_ms);
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
                        const transition = wait_step.getTransition(definition.StepOutcome.success) orelse {
                            self.completeRun(run, .failed, "no success transition for wait step", now_ms);
                            return;
                        };

                        if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                            self.completeRun(run, status, transition.target, now_ms);
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
        self.completeRun(run, .failed, "max step limit reached", now_ms);
    }

    /// Execute a single run step. Returns the step outcome string, or null
    /// if the run was parked waiting for an async action.
    fn executeRunStep(
        self: *WorkflowHandler,
        shard: *Shard,
        run: *RunRecord,
        run_step: definition.RunStep,
        step_input: []const u8,
        step_label: []const u8,
        namespace: []const u8,
        now_ms: i64,
    ) ?[]const u8 {
        if (run_step.isAction()) {
            return self.invokeAction(shard, run, run_step.targetName(), step_input, step_label, namespace, now_ms);
        } else if (run_step.isPlan()) {
            // Plan execution: look up InlinePlan, select executor, invoke its action.
            // For now, plans succeed — full plan executor selection is a follow-up.
            if (run.step_outputs) |*so| {
                so.put(self.allocator, step_label, "{}", definition.StepOutcome.success) catch {};
            }
            return definition.StepOutcome.success;
        } else if (run_step.isChildWorkflow()) {
            // Child workflow invocation — not yet wired.
            return definition.StepOutcome.execution_failure;
        }
        return definition.StepOutcome.execution_failure;
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

        // Cross-shard: need to create run on the target shard's handler
        const target_handler = self.resolveActionHandler(shard, namespace, action_name);

        // Pre-generate a run ID for the cross-shard invocation
        var run_id_buf: [32]u8 = undefined;
        const ts = @as(u64, @bitCast(std.time.milliTimestamp()));
        const pre_run_id = std.fmt.bufPrint(&run_id_buf, "x{d}-{d}", .{ shard.id, ts }) catch {
            return definition.StepOutcome.execution_failure;
        };

        // Create the run on the target shard's handler under mutex
        target_handler.runs_mu.lock();
        const ok = target_handler.invokeByNameWithId(pre_run_id, action_name, input, run.run_id_owned, run.workflow_name_owned);
        target_handler.runs_mu.unlock();

        if (!ok) return definition.StepOutcome.execution_failure;

        // Dupe the pre_run_id for parking (it's on the stack)
        const owned_pre_id = self.allocator.dupe(u8, pre_run_id) catch {
            return definition.StepOutcome.execution_failure;
        };

        // Broadcast inbox notifications to all shards so action_await waiters
        // on any shard can try claiming this run.
        if (shard.peer_inboxes) |inboxes| {
            for (inboxes, 0..) |inbox, i| {
                _ = inbox.send(.{
                    .tag = .action_invoke,
                    .src_shard = @intCast(shard.id),
                });
                _ = i;
            }
        }

        // Park the workflow run, awaiting action completion
        self.parkForAction(run, owned_pre_id, step_label, action_name, target_shard_id, now_ms);
        // Free the duped ID since parkForAction dupes it again
        self.allocator.free(owned_pre_id);
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

        const action_run_id = shard.actions_handler.invokeByName(shard, action_name, input, run.run_id_owned, run.workflow_name_owned) orelse {
            return definition.StepOutcome.execution_failure;
        };

        if (shard.actions_handler.getRunResult(action_run_id)) |result| {
            switch (result.status) {
                .completed => {
                    if (run.step_outputs) |*so| {
                        so.put(self.allocator, step_label, result.output orelse "{}", definition.StepOutcome.success) catch {};
                    }
                    return definition.StepOutcome.success;
                },
                .failed => {
                    if (run.step_outputs) |*so| {
                        so.put(self.allocator, step_label, result.output orelse "{}", definition.StepOutcome.failure) catch {};
                    }
                    return definition.StepOutcome.failure;
                },
                .pending, .running => {
                    self.parkForAction(run, action_run_id, step_label, action_name, target_shard_id, now_ms);
                    return null;
                },
                else => return definition.StepOutcome.execution_failure,
            }
        }
        return definition.StepOutcome.execution_failure;
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
    fn parkForAction(self: *WorkflowHandler, run: *RunRecord, action_run_id: []const u8, step_label: []const u8, action_name: []const u8, target_shard_id: u16, now_ms: i64) void {
        run.status = .waiting;

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

    /// Check all waiting runs for completed async actions and timed-out signals.
    /// Called periodically by the shard's task scheduler.
    pub fn checkPendingActions(self: *WorkflowHandler, shard: *Shard) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now_ms: i64 = std.time.milliTimestamp();

        // Collect keys of runs that need resuming (can't modify map while iterating)
        var resume_keys: [64][]const u8 = undefined;
        var timeout_keys: [64][]const u8 = undefined;
        var resume_count: usize = 0;
        var timeout_count: usize = 0;

        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (run.status != .waiting) continue;

            // Check async action completion
            if (run.pending_action_run_id_owned) |action_rid| {
                if (getActionRunResult(shard, run.pending_action_shard_id, action_rid)) |result| {
                    if (result.status == .completed or result.status == .failed) {
                        if (resume_count < resume_keys.len) {
                            resume_keys[resume_count] = entry.key_ptr.*;
                            resume_count += 1;
                        }
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

        // Handle signal timeouts
        for (timeout_keys[0..timeout_count]) |ns_key| {
            self.handleWaitTimeout(shard, ns_key, now_ms);
        }
    }

    /// Resume a workflow run after its async action completed.
    fn resumeFromAction(self: *WorkflowHandler, shard: *Shard, run_ns_key: []const u8, now_ms: i64) void {
        const run = self.runs.getPtr(run_ns_key) orelse return;
        const action_rid = run.pending_action_run_id_owned orelse return;
        const step_label = run.pending_step_name_owned orelse "unknown";

        // Get the action result from the correct shard
        const result = getActionRunResult(shard, run.pending_action_shard_id, action_rid) orelse return;

        const outcome: []const u8 = switch (result.status) {
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
        };

        // Record completion before clearing state (step_label points into pending_step_name_owned)
        run.status = .running;
        self.addHistoryEvent(run, "action_completed", outcome, now_ms);
        self.addHistoryEvent(run, "step_completed", step_label, now_ms);

        // Clear pending action state
        if (run.pending_action_run_id_owned) |a| self.allocator.free(a);
        run.pending_action_run_id_owned = null;
        if (run.pending_step_name_owned) |s| self.allocator.free(s);
        run.pending_step_name_owned = null;
        run.pending_action_shard_id = null;
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
                const transition = run_step.resolveTransition(outcome) orelse {
                    self.completeRun(run, .failed, "no transition for outcome", now_ms);
                    return;
                };
                if (resolveTerminalStatus(transition.target, def.terminals)) |status| {
                    self.completeRun(run, status, transition.target, now_ms);
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
                self.completeRun(run, status, "signal timeout", now_ms);
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

    /// Transition the run to a terminal status.
    fn completeRun(self: *WorkflowHandler, run: *RunRecord, status: RunStatus, detail: []const u8, now_ms: i64) void {
        run.status = status;
        run.completed_at_ms = now_ms;
        const event_type = switch (status) {
            .completed => "workflow_completed",
            .failed => "workflow_failed",
            .cancelled => "workflow_cancelled",
            .timed_out => "workflow_timed_out",
            else => "workflow_ended",
        };
        self.addHistoryEvent(run, event_type, detail, now_ms);
    }

    /// Update the run's current step pointer.
    fn setCurrentStep(self: *WorkflowHandler, run: *RunRecord, step_name: []const u8) void {
        if (run.current_step_name_owned) |old| self.allocator.free(old);
        run.current_step_name_owned = self.allocator.dupe(u8, step_name) catch null;
    }

    // ── Stream Trigger Polling ─────────────────────────────────────────

    /// Register a stream trigger for a workflow definition.
    /// Called from handleCreate when the parsed definition has a `trigger:` block.
    fn registerStreamTrigger(
        self: *WorkflowHandler,
        namespace: []const u8,
        workflow_name: []const u8,
        trigger: definition.StreamTriggerDef,
    ) void {
        const trigger_key = self.makeNsKey(namespace, workflow_name) orelse return;

        // Remove old trigger if re-creating workflow
        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeTriggerState(&state);
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
        const stream_ns = trigger.namespace orelse namespace;
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
            .stream_cursor_ts = 0,
            .stream_cursor_seq = 0,
            .last_poll_ms = 0,
        }) catch {
            self.allocator.free(trigger_key);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ns);
            self.allocator.free(owned_stream);
            self.allocator.free(owned_stream_ns);
        };
    }

    /// Remove the stream trigger for a workflow definition.
    fn unregisterStreamTrigger(self: *WorkflowHandler, namespace: []const u8, workflow_name: []const u8) void {
        const trigger_key = self.makeNsKey(namespace, workflow_name) orelse return;
        defer self.allocator.free(trigger_key);

        if (self.stream_triggers.fetchRemove(trigger_key)) |old| {
            self.allocator.free(old.key);
            var state = old.value;
            self.freeTriggerState(&state);
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

        const now_ms: i64 = std.time.milliTimestamp();

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
        const now_ms: i64 = std.time.milliTimestamp();
        const input = schedule.input_owned orelse "{}";

        // Generate run ID — encode shard for O(1) cross-shard routing
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = std.fmt.bufPrint(&run_id_buf, "wfrun-{d}-{d}", .{ shard.id, self.nextRunId() }) catch return;

        // Build namespace-qualified run key
        const run_ns_key = self.makeNsKey(schedule.namespace_owned, run_id_str) orelse return;

        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch {
            self.allocator.free(run_ns_key);
            return;
        };
        const owned_wf_name = self.allocator.dupe(u8, schedule.workflow_name_owned) catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            return;
        };
        const owned_version = self.allocator.dupe(u8, "latest") catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            return;
        };
        const owned_input = self.allocator.dupe(u8, input) catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            self.allocator.free(owned_version);
            return;
        };

        var run = RunRecord{
            .run_id_owned = owned_run_id,
            .workflow_name_owned = owned_wf_name,
            .workflow_version_owned = owned_version,
            .status = .running,
            .input_owned = owned_input,
            .created_at_ms = now_ms,
            .started_at_ms = now_ms,
            .completed_at_ms = null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
        };

        // Add history event
        const evt_type = self.allocator.dupe(u8, "schedule_started") catch {
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };
        const evt_detail = self.allocator.dupe(u8, input) catch {
            self.allocator.free(evt_type);
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };
        run.history.append(self.allocator, .{
            .event_type_owned = evt_type,
            .detail_owned = evt_detail,
            .timestamp_ms = now_ms,
        }) catch {
            self.allocator.free(evt_type);
            self.allocator.free(evt_detail);
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };

        self.runs.put(run_ns_key, run) catch {
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };

        self.persistStart(shard, schedule.namespace_owned, owned_run_id, owned_wf_name, owned_version, owned_input, now_ms);
        self.advanceWorkflow(shard, run_ns_key, schedule.namespace_owned);
    }

    /// Poll all active stream triggers and start workflow runs for new events.
    /// Called from shard.tick() on every reactor cycle.
    pub fn tickStreamTriggers(self: *WorkflowHandler, shard: *Shard) void {
        const now_ms = std.time.milliTimestamp();

        var it = self.stream_triggers.iterator();
        while (it.next()) |entry| {
            const trigger = entry.value_ptr;

            // Enforce poll interval
            const poll_ms: i64 = @intCast(trigger.poll_interval_ms);
            if (now_ms - trigger.last_poll_ms < poll_ms) continue;
            trigger.last_poll_ms = now_ms;

            // Skip disabled workflows
            if (self.disabled.contains(entry.key_ptr.*)) continue;

            // Read from the source stream
            const batch_limit: usize = @intCast(trigger.batch_size);
            const cursor = StreamID{ .timestamp_ms = trigger.stream_cursor_ts, .sequence = trigger.stream_cursor_seq };
            const result = shard.stream_handler.readPayloadsForStream(
                trigger.stream_name_owned,
                trigger.stream_namespace_owned,
                cursor,
                @max(batch_limit, 1) * 10, // read ahead for batching
            );
            if (result.payloads.len == 0) continue;
            defer shard.stream_handler.allocator.free(result.payloads);

            // Start one run per event (or per batch if batch_size > 1)
            if (trigger.batch_size <= 1) {
                // One run per event
                for (result.payloads) |payload| {
                    self.startRunFromTrigger(shard, trigger, payload);
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
                    self.startRunFromTrigger(shard, trigger, batch_json);
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
    fn startRunFromTrigger(
        self: *WorkflowHandler,
        shard: *Shard,
        trigger: *const StreamTriggerState,
        input: []const u8,
    ) void {
        const now_ms: i64 = std.time.milliTimestamp();

        // Generate run ID — encode shard for O(1) cross-shard routing
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = std.fmt.bufPrint(&run_id_buf, "wfrun-{d}-{d}", .{ shard.id, self.nextRunId() }) catch return;

        // Build namespace-qualified run key
        const run_ns_key = self.makeNsKey(trigger.namespace_owned, run_id_str) orelse return;

        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch {
            self.allocator.free(run_ns_key);
            return;
        };
        const owned_wf_name = self.allocator.dupe(u8, trigger.workflow_name_owned) catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            return;
        };
        const owned_version = self.allocator.dupe(u8, "latest") catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            return;
        };
        const owned_input = self.allocator.dupe(u8, input) catch {
            self.allocator.free(run_ns_key);
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_wf_name);
            self.allocator.free(owned_version);
            return;
        };

        var run = RunRecord{
            .run_id_owned = owned_run_id,
            .workflow_name_owned = owned_wf_name,
            .workflow_version_owned = owned_version,
            .status = .running,
            .input_owned = owned_input,
            .created_at_ms = now_ms,
            .started_at_ms = now_ms,
            .completed_at_ms = null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
        };

        // Add history event
        const evt_type = self.allocator.dupe(u8, "trigger_started") catch {
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };
        const evt_detail = self.allocator.dupe(u8, input) catch {
            self.allocator.free(evt_type);
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };
        run.history.append(self.allocator, .{
            .event_type_owned = evt_type,
            .detail_owned = evt_detail,
            .timestamp_ms = now_ms,
        }) catch {
            self.allocator.free(evt_type);
            self.allocator.free(evt_detail);
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };

        self.runs.put(run_ns_key, run) catch {
            self.freeRunRecord(&run);
            self.allocator.free(run_ns_key);
            return;
        };

        self.persistStart(shard, trigger.namespace_owned, owned_run_id, owned_wf_name, owned_version, owned_input, now_ms);
        self.advanceWorkflow(shard, run_ns_key, trigger.namespace_owned);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextRunId(self: *WorkflowHandler) u64 {
        const id = self.next_run_id;
        self.next_run_id += 1;
        return id;
    }

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
    fn persistCreate(self: *WorkflowHandler, shard: *Shard, namespace: []const u8, name: []const u8, yaml: []const u8) void {
        _ = self;
        // Build ns-qualified key: "namespace:name"
        var key_buf: [600]u8 = undefined;
        const ns_key = std.fmt.bufPrint(&key_buf, "{s}:{s}", .{ namespace, name }) catch return;

        _ = persistence_mod.persistEntry(shard, .workflow_create, entry_mod.Flags.NONE, namespace, ns_key, yaml) catch {};
    }

    /// Persist a workflow_start entry to the UAL so the run survives restart.
    /// Key stored is "namespace:run_id". Value format: [wf_name_len:u16][wf_name][ver_len:u16][ver][status:u8][created_at_ms:i64][input...]
    fn persistStart(
        self: *WorkflowHandler,
        shard: *Shard,
        namespace: []const u8,
        run_id: []const u8,
        wf_name: []const u8,
        version: []const u8,
        input: []const u8,
        created_at_ms: i64,
    ) void {
        _ = self;
        // Build ns-qualified key: "namespace:run_id"
        var ns_key_buf: [600]u8 = undefined;
        const ns_key = std.fmt.bufPrint(&ns_key_buf, "{s}:{s}", .{ namespace, run_id }) catch return;

        // Serialize value: [wf_name_len:u16][wf_name][ver_len:u16][ver][status:u8][created_at:i64][input...]
        const value_len = 2 + wf_name.len + 2 + version.len + 1 + 8 + input.len;
        if (value_len > 65000) return;
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

        std.mem.writeInt(i64, value_buf[off..][0..8], created_at_ms, .little);
        off += 8;

        @memcpy(value_buf[off .. off + input.len], input);
        off += input.len;

        _ = persistence_mod.persistEntry(shard, .workflow_start, entry_mod.Flags.NONE, namespace, ns_key, value_buf[0..off]) catch {};
    }

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *WorkflowHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.workflow_create, @ptrCast(self), replayEntryThunk);
        registry.register(.workflow_start, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *WorkflowHandler = @ptrCast(@alignCast(ctx));
        self.replayEntry(entry);
    }

    /// Replay a persisted workflow entry (called during segment replay on startup).
    pub fn replayEntry(self: *WorkflowHandler, entry: *const entry_mod.Entry) void {
        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;

        switch (etype) {
            .workflow_create => self.replayCreate(cmd.key, cmd.value),
            .workflow_start => self.replayStart(cmd.key, cmd.value),
            else => {},
        }
    }

    /// Replay a workflow_create entry. The key is "namespace:name" (ns-qualified).
    fn replayCreate(self: *WorkflowHandler, ns_key_raw: []const u8, yaml: []const u8) void {
        // Extract raw name from "namespace:name"
        const raw_name = if (std.mem.indexOfScalar(u8, ns_key_raw, ':')) |idx|
            ns_key_raw[idx + 1 ..]
        else
            ns_key_raw;

        // Allocate ns-qualified key for map lookup
        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return;

        // Remove old definition if exists
        if (self.definitions.fetchRemove(ns_key)) |old| {
            self.allocator.free(old.key); // old ns-qualified key
            self.allocator.free(old.value.name_owned);
            self.allocator.free(old.value.version_owned);
            self.allocator.free(old.value.yaml_owned);
        }

        // Parse to get version
        var def = parser.parseWorkflow(self.allocator, yaml) catch {
            self.allocator.free(ns_key);
            return;
        };
        const version = self.allocator.dupe(u8, def.version) catch {
            def.deinit(self.allocator);
            self.allocator.free(ns_key);
            return;
        };
        def.deinit(self.allocator);

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
            .created_at_ms = 0,
        }) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_name);
            self.allocator.free(version);
            self.allocator.free(owned_yaml);
        };
    }

    /// Replay a workflow_start entry. The key is "namespace:run_id" (ns-qualified).
    fn replayStart(self: *WorkflowHandler, ns_key_raw: []const u8, value: []const u8) void {
        // Extract raw run_id from "namespace:run_id"
        const raw_run_id = if (std.mem.indexOfScalar(u8, ns_key_raw, ':')) |idx|
            ns_key_raw[idx + 1 ..]
        else
            ns_key_raw;

        // Deserialize: [wf_name_len:u16][wf_name][ver_len:u16][ver][status:u8][created_at:i64][input...]
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

        const input = if (off < value.len) value[off..] else "{}";

        // Allocate ns-qualified key for map
        const ns_key = self.allocator.dupe(u8, ns_key_raw) catch return;

        // Duplicate all record fields
        const owned_rid = self.allocator.dupe(u8, raw_run_id) catch {
            self.allocator.free(ns_key);
            return;
        };
        const owned_wf = self.allocator.dupe(u8, wf_name) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_rid);
            return;
        };
        const owned_ver = self.allocator.dupe(u8, version) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_rid);
            self.allocator.free(owned_wf);
            return;
        };
        const owned_inp = self.allocator.dupe(u8, input) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_rid);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ver);
            return;
        };

        // Skip if already replayed (idempotent)
        if (self.runs.contains(ns_key)) {
            self.allocator.free(ns_key);
            self.allocator.free(owned_rid);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ver);
            self.allocator.free(owned_inp);
            return;
        }

        self.runs.put(ns_key, .{
            .run_id_owned = owned_rid,
            .workflow_name_owned = owned_wf,
            .workflow_version_owned = owned_ver,
            .status = status,
            .input_owned = owned_inp,
            .created_at_ms = created_at_ms,
            .started_at_ms = created_at_ms,
            .completed_at_ms = null,
            .idempotency_key_owned = null,
            .signals = .empty,
            .history = .empty,
        }) catch {
            self.allocator.free(ns_key);
            self.allocator.free(owned_rid);
            self.allocator.free(owned_wf);
            self.allocator.free(owned_ver);
            self.allocator.free(owned_inp);
        };

        // Advance next_run_id past replayed IDs to prevent collisions
        // when new runs are started after recovery.
        if (std.mem.startsWith(u8, raw_run_id, "wfrun-")) {
            if (std.fmt.parseInt(u64, raw_run_id["wfrun-".len..], 10)) |n| {
                if (n >= self.next_run_id) {
                    self.next_run_id = n + 1;
                }
            } else |_| {}
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
                try std.fmt.format(writer, "\\u{x:0>4}", .{c});
            } else {
                try writer.writeByte(c);
            },
        }
    }
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
            .reserved = 0,
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

/// Register a test action as WASM (completes synchronously in test mode).
fn registerTestAction(actions: *ActionsHandler, name: []const u8) void {
    const alloc = actions.allocator;
    const owned_name = alloc.dupe(u8, name) catch return;
    // WASM magic header: \x00asm — enough for the test-mode validation
    const wasm_bytes = alloc.dupe(u8, &[_]u8{ 0x00, 0x61, 0x73, 0x6d }) catch {
        alloc.free(owned_name);
        return;
    };
    const owned_ns = alloc.dupe(u8, "default") catch {
        alloc.free(owned_name);
        alloc.free(wasm_bytes);
        return;
    };
    actions.actions.put(owned_name, .{
        .name_owned = owned_name,
        .namespace_owned = owned_ns,
        .action_type = 1, // wasm
        .version = 1,
        .enabled = true,
        .created_at_ns = 0,
        .wasm_blob_owned = wasm_bytes,
    }) catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        alloc.free(wasm_bytes);
    };
}

/// Register a test action with invalid WASM (fails on invocation).
fn registerFailingAction(actions: *ActionsHandler, name: []const u8) void {
    const alloc = actions.allocator;
    const owned_name = alloc.dupe(u8, name) catch return;
    // Invalid WASM magic — executeWasmAction will set .failed in test mode
    const wasm_bytes = alloc.dupe(u8, &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }) catch {
        alloc.free(owned_name);
        return;
    };
    const owned_ns = alloc.dupe(u8, "default") catch {
        alloc.free(owned_name);
        alloc.free(wasm_bytes);
        return;
    };
    actions.actions.put(owned_name, .{
        .name_owned = owned_name,
        .namespace_owned = owned_ns,
        .action_type = 1,
        .version = 1,
        .enabled = true,
        .created_at_ns = 0,
        .wasm_blob_owned = wasm_bytes,
    }) catch {
        alloc.free(owned_name);
        alloc.free(owned_ns);
        alloc.free(wasm_bytes);
    };
}

/// Create a minimal test shard with an actions handler for unit tests.
/// Only `actions_handler` is usable — all other fields are undefined.
fn createTestShard(actions: *ActionsHandler) Shard {
    var shard: Shard = undefined;
    shard.actions_handler = actions;
    shard.peer_shards = null;
    shard.router = router.Router.init(1, 1, 0);
    return shard;
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

test "step executor: linear workflow completes via action invocation" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    // Set up actions handler with test WASM actions
    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    registerTestAction(&actions, "step-a");
    registerTestAction(&actions, "step-b");
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-1", "run-1", "test-wf");

    try testing.expectEqual(@as(usize, 1), handler.runCount());
    handler.advanceWorkflow(&shard, "default:run-1", "default");

    const run = handler.runs.get("default:run-1").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
    try testing.expect(run.completed_at_ms != null);
    // History should have: step_started(start), step_completed(start),
    // step_started(step_b), step_completed(step_b), workflow_completed
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
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:wait-wf", "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, "default:run-2", "run-2", "wait-wf");

    handler.advanceWorkflow(&shard, "default:run-2", "default");

    const run = handler.runs.get("default:run-2").?;
    // Should be waiting after start → wait_approval
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
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:wait-wf", "wait-wf", test_wait_workflow_json);
    createTestRun(&handler, "default:run-3", "run-3", "wait-wf");

    // Advance until it parks
    handler.advanceWorkflow(&shard, "default:run-3", "default");
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

    // Resume execution
    handler.advanceWorkflow(&shard, "default:run-3", "default");

    const run = handler.runs.get("default:run-3").?;
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
}

test "step executor: missing definition does not crash" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = createTestShard(&actions);

    // No definition registered
    createTestRun(&handler, "default:run-4", "run-4", "nonexistent-wf");

    // Should gracefully no-op (no definition found)
    handler.advanceWorkflow(&shard, "default:run-4", "default");

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
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-5", "run-5", "test-wf");

    handler.advanceWorkflow(&shard, "default:run-5", "default");

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
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:test-wf", "test-wf", test_workflow_json);
    createTestRun(&handler, "default:run-6", "run-6", "test-wf");

    handler.advanceWorkflow(&shard, "default:run-6", "default");

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
    var shard = createTestShard(&actions);

    createTestDef(&handler, "default:retry-wf", "retry-wf", test_retry_workflow_json);
    createTestRun(&handler, "default:run-7", "run-7", "retry-wf");

    handler.advanceWorkflow(&shard, "default:run-7", "default");

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

test "step executor: checkPendingActions handles completed async action" {
    const allocator = testing.allocator;
    var handler = WorkflowHandler.init(allocator);
    defer handler.deinit();

    var actions = ActionsHandler.init(allocator);
    defer actions.deinit();
    var shard = createTestShard(&actions);

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

    // checkPendingActions should detect the completed action and resume
    handler.checkPendingActions(&shard);

    const run = handler.runs.get("default:run-8").?;
    // After resuming: start → step_b (success) → flo.Completed
    try testing.expectEqual(WorkflowHandler.RunStatus.completed, run.status);
}
