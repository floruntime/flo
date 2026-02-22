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
const validator = @import("validator.zig");

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// WorkflowHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const WorkflowHandler = struct {
    allocator: Allocator,

    /// In-memory definition store: workflow name → DefinitionRecord.
    /// For simplicity, stores the latest version per name.
    /// Key is owned (duped from name).
    definitions: std.StringHashMap(DefinitionRecord),

    /// In-memory run store: run_id (string) → RunRecord.
    runs: std.StringHashMap(RunRecord),

    /// Disabled workflows: workflow_name → void.
    disabled: std.StringHashMap(void),

    /// Monotonic run counter.
    next_run_id: u64,

    const MAX_WORKFLOW_NAME_LEN: usize = 256;
    const MAX_DEFINITIONS: usize = 10_000;
    const MAX_RUNS: usize = 100_000;

    pub const RunStatus = enum(u8) {
        pending = 0,
        running = 1,
        completed = 2,
        failed = 3,
        cancelled = 4,
        timed_out = 5,

        pub fn toString(self: RunStatus) []const u8 {
            return switch (self) {
                .pending => "pending",
                .running => "running",
                .completed => "completed",
                .failed => "failed",
                .cancelled => "cancelled",
                .timed_out => "timed_out",
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

        /// Signals received by this run.
        signals: std.ArrayList(Signal),

        /// History events for this run.
        history: std.ArrayList(HistoryEvent),
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

    pub fn init(allocator: Allocator) WorkflowHandler {
        return .{
            .allocator = allocator,
            .definitions = std.StringHashMap(DefinitionRecord).init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
            .disabled = std.StringHashMap(void).init(allocator),
            .next_run_id = 1,
        };
    }

    pub fn deinit(self: *WorkflowHandler) void {
        // Free all definition records
        var dit = self.definitions.iterator();
        while (dit.next()) |entry| {
            self.allocator.free(entry.value_ptr.name_owned);
            self.allocator.free(entry.value_ptr.version_owned);
            self.allocator.free(entry.value_ptr.yaml_owned);
        }
        self.definitions.deinit();

        // Free all run records
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.freeRunRecord(entry.value_ptr);
        }
        self.runs.deinit();

        // Free disabled keys
        var diit = self.disabled.iterator();
        while (diit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.disabled.deinit();
    }

    fn freeRunRecord(self: *WorkflowHandler, run: *RunRecord) void {
        self.allocator.free(run.run_id_owned);
        self.allocator.free(run.workflow_name_owned);
        self.allocator.free(run.workflow_version_owned);
        self.allocator.free(run.input_owned);
        if (run.idempotency_key_owned) |k| self.allocator.free(k);

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

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.workflow_create, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_start, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_signal, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_cancel, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_status, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_history, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_list_runs, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_get_definition, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_disable, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.registerWithRoute(.workflow_enable, dispatchWorkflow, preRouteByWorkflow);
        dispatcher.register(.workflow_list_definitions, dispatchWorkflow);
    }

    fn preRouteByWorkflow(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return std.hash.Wyhash.hash(0, req.key);
    }

    fn dispatchWorkflow(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        shard.workflow_handler.handleCommand(shard, conn, req);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
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

        // Store the definition (overwrite if name already exists)
        const name = def.name;
        const version = def.version;

        // Remove old definition if exists
        if (self.definitions.fetchRemove(name)) |old| {
            self.allocator.free(old.value.name_owned);
            self.allocator.free(old.value.version_owned);
            self.allocator.free(old.value.yaml_owned);
        }

        // Duplicate all owned data
        const owned_name = self.allocator.dupe(u8, name) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_name);

        const owned_version = self.allocator.dupe(u8, version) catch {
            self.allocator.free(owned_name);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_version);

        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(owned_name);
            self.allocator.free(owned_version);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const now_ms: i64 = std.time.milliTimestamp();

        self.definitions.put(owned_name, .{
            .name_owned = owned_name,
            .version_owned = owned_version,
            .yaml_owned = owned_yaml,
            .created_at_ms = now_ms,
        }) catch {
            self.allocator.free(owned_name);
            self.allocator.free(owned_version);
            self.allocator.free(owned_yaml);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "definition store failed");
            return;
        };

        // Return the workflow name
        shard.sendOkResponse(conn, req.header.request_id, owned_name);
    }

    // ── START ───────────────────────────────────────────────────────────

    fn handleStart(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const workflow_name = req.key;

        if (workflow_name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        // Check workflow exists
        if (!self.definitions.contains(workflow_name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return;
        }

        // Check workflow is not disabled
        if (self.disabled.contains(workflow_name)) {
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

        // Generate or use explicit run ID
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = if (explicit_run_id) |rid|
            rid
        else blk: {
            break :blk std.fmt.bufPrint(&run_id_buf, "wfrun-{d}", .{self.nextRunId()}) catch "wfrun-0";
        };

        // Duplicate for storage
        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_run_id);

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

        self.runs.put(owned_run_id, run) catch {
            self.freeRunRecord(&run);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "run store failed");
            return;
        };

        // Return the run ID
        shard.sendOkResponse(conn, req.header.request_id, owned_run_id);
    }

    // ── SIGNAL ──────────────────────────────────────────────────────────

    fn handleSignal(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run = self.runs.getPtr(run_id) orelse {
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

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── CANCEL ──────────────────────────────────────────────────────────

    fn handleCancel(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run = self.runs.getPtr(run_id) orelse {
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

        const run = self.runs.get(run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // Build JSON status
        var buf: [8192]u8 = undefined;
        const status_json = std.fmt.bufPrint(&buf,
            \\{{"run_id":"{s}","workflow":"{s}","version":"{s}","status":"{s}","input":{s},"created_at":{d}}}
        , .{
            run.run_id_owned,
            run.workflow_name_owned,
            run.workflow_version_owned,
            run.status.toString(),
            run.input_owned,
            run.created_at_ms,
        }) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "status serialization failed");
            return;
        };

        shard.sendOkResponse(conn, req.header.request_id, status_json);
    }

    // ── HISTORY ─────────────────────────────────────────────────────────

    fn handleHistory(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const run_id = req.key;

        if (run_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "run_id is required");
            return;
        }

        const run = self.runs.get(run_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            return;
        };

        // Parse limit from value
        var limit: u32 = 100;
        if (req.value.len >= 4) {
            limit = std.mem.readInt(u32, req.value[0..4], .little);
        }

        // Build JSON array of history events
        var result_buf: [65536]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&result_buf);
        const writer = fbs.writer();

        writer.writeByte('[') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "history serialization failed");
            return;
        };

        const events = run.history.items;
        const count = @min(events.len, limit);
        for (events[0..count], 0..) |evt, i| {
            if (i > 0) writer.writeByte(',') catch return;
            std.fmt.format(writer,
                \\{{"type":"{s}","detail":"{s}","timestamp":{d}}}
            , .{ evt.event_type_owned, evt.detail_owned, evt.timestamp_ms }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "history serialization failed");
                return;
            };
        }

        writer.writeByte(']') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "history serialization failed");
            return;
        };

        shard.sendOkResponse(conn, req.header.request_id, fbs.getWritten());
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

        // Build JSON array of matching runs
        var result_buf: [65536]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&result_buf);
        const writer = fbs.writer();

        writer.writeByte('[') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "list serialization failed");
            return;
        };

        var count: u32 = 0;
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            if (count >= limit) break;
            const run = entry.value_ptr;

            // Filter by workflow name if specified
            if (workflow_name.len > 0 and !std.mem.eql(u8, run.workflow_name_owned, workflow_name)) {
                continue;
            }

            // Filter by status if specified
            if (status_filter) |sf| {
                if (!std.mem.eql(u8, run.status.toString(), sf)) continue;
            }

            if (count > 0) writer.writeByte(',') catch return;
            std.fmt.format(writer,
                \\{{"run_id":"{s}","workflow":"{s}","status":"{s}","created_at":{d}}}
            , .{
                run.run_id_owned,
                run.workflow_name_owned,
                run.status.toString(),
                run.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "list serialization failed");
                return;
            };
            count += 1;
        }

        writer.writeByte(']') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "list serialization failed");
            return;
        };

        shard.sendOkResponse(conn, req.header.request_id, fbs.getWritten());
    }

    // ── GET DEFINITION ──────────────────────────────────────────────────

    fn handleGetDefinition(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "workflow name is required");
            return;
        }

        const def = self.definitions.get(name) orelse {
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

        // Check workflow exists
        if (!self.definitions.contains(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "workflow not found");
            return;
        }

        if (!self.disabled.contains(name)) {
            const owned_name = self.allocator.dupe(u8, name) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
            self.disabled.put(owned_name, {}) catch {
                self.allocator.free(owned_name);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "disable failed");
                return;
            };
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

        if (self.disabled.fetchRemove(name)) |old| {
            self.allocator.free(old.key);
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── LIST DEFINITIONS ────────────────────────────────────────────────

    fn handleListDefinitions(self: *WorkflowHandler, shard: *Shard, conn: *Connection, req: Request) void {
        var result_buf: [65536]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&result_buf);
        const writer = fbs.writer();

        writer.writeByte('[') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "list serialization failed");
            return;
        };

        var count: u32 = 0;
        var dit = self.definitions.iterator();
        while (dit.next()) |entry| {
            const def = entry.value_ptr;
            if (count > 0) writer.writeByte(',') catch return;
            std.fmt.format(writer,
                \\{{"name":"{s}","version":"{s}","created_at":{d}}}
            , .{
                def.name_owned,
                def.version_owned,
                def.created_at_ms,
            }) catch return;
            count += 1;
        }

        writer.writeByte(']') catch return;

        shard.sendOkResponse(conn, req.header.request_id, fbs.getWritten());
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextRunId(self: *WorkflowHandler) u64 {
        const id = self.next_run_id;
        self.next_run_id += 1;
        return id;
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

    pub fn definitionCount(self: *const WorkflowHandler) usize {
        return self.definitions.count();
    }

    pub fn runCount(self: *const WorkflowHandler) usize {
        return self.runs.count();
    }
};

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
