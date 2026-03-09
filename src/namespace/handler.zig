//! Namespace Handler — registers namespace management opcodes with Dispatcher.
//!
//! Namespace operations are controller-only commands that route to Shard 0.
//! They manage the namespace registry (create, delete, list, info).
//!
//! ## Opcode Range
//!
//!   Commands:   0xB0–0xB3  (create, delete, list, info)
//!   Responses:  0xB4–0xB7
//!
//! ## Namespace Semantics
//!
//! - The namespace name is passed in `req.key` (not `req.namespace`).
//! - All mutations go through Controller Raft on Shard 0 (when wired).
//! - No pre-route hooks — all namespace commands route to controller.
//! - Reserved namespaces (`_sys`, `_internal`, `_flo`) cannot be created/deleted.
//!
//! ## Namespace Key Utilities (public API for all subsystems)
//!
//! All subsystems (KV, Stream, Queue, Actions, TS, Processing, Workflow) should
//! use the public key utilities below for namespace isolation:
//!
//! ```zig
//! const ns = @import("../namespace/handler.zig");
//!
//! // Validate key size early (returns error message or null if valid)
//! if (ns.validateKeySize(req.namespace, req.key)) |msg| return error(msg);
//!
//! // Build namespace-qualified key: "myapp\x00mykey"
//! var qbuf: [ns.MAX_QUALIFIED_KEY]u8 = undefined;
//! const qkey = try ns.qualifyKey(&qbuf, req.namespace, req.key);
//!
//! // Strip prefix for display (scan results, error messages)
//! const display_key = ns.stripPrefix(stored_key, req.namespace);
//!
//! // Build prefix for scanning all keys in a namespace
//! const prefix = ns.namespacePrefix(&qbuf, req.namespace);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const coordinator_mod = @import("../cluster/coordinator.zig");
const Coordinator = coordinator_mod.Coordinator;
const NamespaceConfig = coordinator_mod.NamespaceConfig;
const connection_mod = @import("../node/connection.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const CommandResult = result_mod.CommandResult;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Key Utilities — public API for all subsystems
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum length of a namespace name (alphanumeric + _-. only).
pub const MAX_NAMESPACE_LEN: usize = 128;

/// Maximum length of a namespace-qualified key (namespace + '\x00' + raw key).
/// Sized as a practical stack-friendly limit: 128 (max namespace) + 1 (separator)
/// + ~3967 bytes for the raw key portion = 4096 bytes total.
///
/// The wire protocol allows u16 keys (65535 bytes) but keys beyond 4KB are
/// pathological — they bloat Raft log entries, dominate stack frames, and
/// degrade hash table performance. All subsystems should validate against
/// `MAX_KEY_LENGTH` at the dispatch layer.
pub const MAX_QUALIFIED_KEY: usize = 4096;

/// Maximum raw key length that a user can create (accounting for namespace
/// prefix overhead). This is the limit to validate at key creation time so
/// users get a clear error before data is stored.
pub const MAX_KEY_LENGTH: usize = MAX_QUALIFIED_KEY - MAX_NAMESPACE_LEN - 1; // 3967

/// Null byte separator between namespace and key in qualified keys.
/// Chosen because namespace names are restricted to alphanumeric + _-. characters,
/// so '\x00' can never appear in a valid namespace name.
pub const NAMESPACE_SEPARATOR: u8 = 0;

/// Build a namespace-qualified internal key: `"<namespace>\x00<key>"`.
///
/// For empty or "default" namespace, returns the raw key unchanged (no prefix).
/// This is the canonical function — all subsystems should use this for
/// namespace isolation rather than implementing their own qualification.
///
/// Returns `error.KeyTooLarge` if the combined length exceeds the buffer.
///
/// Lifetime: the returned slice borrows from `buf` (when prefixed) or from
/// `raw_key` (when no prefix). Safe for synchronous operations, waiter
/// registration (pool copies to inline buffer), and Raft propose (serializes
/// immediately).
pub fn qualifyKey(buf: *[MAX_QUALIFIED_KEY]u8, ns: []const u8, raw_key: []const u8) error{KeyTooLarge}![]const u8 {
    if (ns.len == 0 or std.mem.eql(u8, ns, "default")) return raw_key;
    const total = ns.len + 1 + raw_key.len;
    if (total > MAX_QUALIFIED_KEY) return error.KeyTooLarge;
    @memcpy(buf[0..ns.len], ns);
    buf[ns.len] = NAMESPACE_SEPARATOR;
    @memcpy(buf[ns.len + 1 ..][0..raw_key.len], raw_key);
    return buf[0..total];
}

/// Strip namespace prefix from a qualified key for display to the user.
///
/// Given a stored key like `"myapp\x00mykey"` and namespace `"myapp"`, returns
/// `"mykey"`. If the key doesn't have the expected prefix (wrong namespace,
/// default namespace, or malformed), returns the key unchanged.
pub fn stripPrefix(qualified: []const u8, ns: []const u8) []const u8 {
    if (ns.len == 0 or std.mem.eql(u8, ns, "default")) return qualified;
    const prefix_len = ns.len + 1;
    if (qualified.len > prefix_len and
        qualified[ns.len] == NAMESPACE_SEPARATOR and
        std.mem.eql(u8, qualified[0..ns.len], ns))
    {
        return qualified[prefix_len..];
    }
    return qualified;
}

/// Build the namespace prefix for scanning all keys belonging to a namespace.
///
/// Returns `"ns\x00"` for non-default namespaces (use as a `scanPrefix` argument),
/// or an empty slice for the default namespace (full scan).
pub fn namespacePrefix(buf: *[MAX_QUALIFIED_KEY]u8, ns: []const u8) []const u8 {
    if (ns.len == 0 or std.mem.eql(u8, ns, "default")) return &.{};
    @memcpy(buf[0..ns.len], ns);
    buf[ns.len] = NAMESPACE_SEPARATOR;
    return buf[0 .. ns.len + 1];
}

/// Validate that a key + namespace combination will fit within limits.
///
/// Call this at the dispatch layer (before any business logic) to give users
/// a clear error message upfront rather than a confusing qualification failure
/// deep in the handler.
///
/// Returns an error message string if invalid, or `null` if the key is valid.
pub fn validateKeySize(ns: []const u8, key: []const u8) ?[]const u8 {
    if (key.len == 0) return "key is required";
    const has_ns = ns.len > 0 and !std.mem.eql(u8, ns, "default");
    if (has_ns) {
        if (ns.len + 1 + key.len > MAX_QUALIFIED_KEY)
            return "key too large for namespace (max 3967 bytes with namespace prefix)";
    } else {
        if (key.len > MAX_QUALIFIED_KEY)
            return "key too large (max 4096 bytes)";
    }
    return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// NamespaceHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const NamespaceHandler = struct {
    allocator: Allocator,

    /// In-memory namespace registry. Keys are owned copies of namespace names.
    /// Will be replaced by Controller Raft storage when wired.
    namespaces: std.StringHashMap(NamespaceMeta),

    const MAX_NAMESPACES: usize = 1024;

    const reserved_prefixes = [_][]const u8{
        "_sys",
        "_internal",
        "_flo",
    };

    pub const NamespaceMeta = struct {
        created_at_ns: u64,
        /// Tracks whether data has been written to this namespace.
        /// Incremented by markNamespaceHasData(), used for non-empty delete check.
        data_count: u32 = 0,
        /// Per-namespace settings (synced from coordinator)
        config: NamespaceConfig = .{},
    };

    pub fn init(allocator: Allocator) NamespaceHandler {
        return .{
            .allocator = allocator,
            .namespaces = std.StringHashMap(NamespaceMeta).init(allocator),
        };
    }

    pub fn deinit(self: *NamespaceHandler) void {
        var it = self.namespaces.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.namespaces.deinit();
    }

    // ── Namespace Data Tracking ─────────────────────────────────────────

    /// Mark a namespace as having data written to it.
    /// Called by KV/Stream/Queue handlers after successful writes.
    /// Auto-creates the "default" namespace entry when called with empty or "default" name.
    pub fn markNamespaceHasData(self: *NamespaceHandler, name: []const u8) void {
        const effective = if (name.len == 0 or std.mem.eql(u8, name, "default")) "default" else name;
        if (self.namespaces.getPtr(effective)) |meta| {
            meta.data_count +|= 1; // saturating add
        } else {
            // Auto-create namespace entry (e.g., "default" on first bare-namespace write)
            const key = self.allocator.dupe(u8, effective) catch return;
            const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
            self.namespaces.put(key, .{
                .created_at_ns = timestamp,
                .data_count = 1,
            }) catch {
                self.allocator.free(key);
                return;
            };
        }
    }

    /// Check if a namespace has had data written to it.
    pub fn namespaceHasData(self: *NamespaceHandler, name: []const u8) bool {
        if (self.namespaces.get(name)) |meta| {
            return meta.data_count > 0;
        }
        return false;
    }

    // ── Raft-Replicated Apply ───────────────────────────────────────────
    // Called after Coordinator commits a namespace change via Raft.
    // Updates the local in-memory registry to match the committed state.

    /// Apply a Raft-committed namespace creation to the local registry.
    pub fn applyCreate(self: *NamespaceHandler, name: []const u8) void {
        if (self.namespaces.contains(name)) return; // idempotent
        const owned = self.allocator.dupe(u8, name) catch return;
        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, std.time.milliTimestamp()))) * 1_000_000);
        self.namespaces.put(owned, .{ .created_at_ns = now_ns }) catch {
            self.allocator.free(owned);
        };
    }

    /// Apply a Raft-committed namespace deletion to the local registry.
    pub fn applyDelete(self: *NamespaceHandler, name: []const u8) void {
        if (self.namespaces.fetchRemove(name)) |kv| {
            self.allocator.free(kv.key);
        }
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        // No pre-route hooks — namespace commands route to Controller (Shard 0).
        dispatcher.register(.namespace_create, dispatchNamespace);
        dispatcher.register(.namespace_delete, dispatchNamespace);
        dispatcher.registerWalk(.namespace_list, dispatchNamespace, localScanNamespaces);
        dispatcher.register(.namespace_info, dispatchNamespace);
        dispatcher.register(.namespace_config_set, dispatchNamespace);
        dispatcher.register(.namespace_config_get, dispatchNamespace);
    }

    /// ShardWalker LocalScanFn for namespace_list — returns namespace names
    /// from one shard's NamespaceHandler registry.
    fn localScanNamespaces(
        ctx: *anyopaque,
        _: []const u8, // namespace (ignored — namespaces are global)
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *NamespaceHandler = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [256][]const u8 = undefined;
        };

        var count: usize = 0;
        var it = handler.namespaces.iterator();
        while (it.next()) |entry| {
            if (count >= S.name_buf.len) break;
            S.name_buf[count] = entry.key_ptr.*;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    fn dispatchNamespace(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @enumFromInt(req.header.op_code);

        // Reads (list, info, config_get) always go to local handler
        if (op == .namespace_list or op == .namespace_info or op == .namespace_config_get) {
            const cmd_result = shard.namespace_handler.handleCommand(req);
            defer shard.namespace_handler.freeResult(cmd_result);
            sendNamespaceResponse(shard, conn, req.header.request_id, cmd_result);
            return;
        }

        // Mutations (create, delete, config_set): validate then persist via UAL
        switch (op) {
            .namespace_create => dispatchCreate(shard, conn, req),
            .namespace_delete => dispatchDelete(shard, conn, req),
            .namespace_config_set => dispatchConfigSet(shard, conn, req),
            else => shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown namespace mutation"),
        }
    }

    /// Max payload for a namespace UAL entry: command prefix(10) + name(128) + settings TLV
    const MAX_NS_PAYLOAD: usize = entry_mod.COMMAND_PREFIX_SIZE + MAX_NAMESPACE_LEN + NamespaceConfig.MAX_SETTINGS_SIZE;

    fn dispatchCreate(shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "namespace name is required");
            return;
        }
        if (name.len > MAX_NAMESPACE_LEN) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "namespace name too long");
            return;
        }
        if (!isValidName(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid namespace name");
            return;
        }
        if (isReserved(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "reserved namespace name");
            return;
        }
        if (shard.namespace_handler.namespaces.contains(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .conflict, "namespace already exists");
            return;
        }
        if (shard.namespace_handler.namespaces.count() >= MAX_NAMESPACES) {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "namespace limit reached");
            return;
        }

        // Propose namespace_create entry through Raft → UAL (persists via segment writer)
        _ = proposeNamespaceEntry(shard, .namespace_create, name, &.{}) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "raft propose failed");
            return;
        };

        // Apply in-memory state directly (Raft propose already persisted the entry)
        shard.namespace_handler.applyCreate(name);

        // Also propagate to coordinator if wired (cluster metadata)
        if (shard.coordinator) |coord| {
            _ = coord.proposeCreateNamespace(name, 32, 1) catch {};
            _ = coord.applyCommitted() catch {};
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    fn dispatchDelete(shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "namespace name is required");
            return;
        }
        if (isReserved(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "cannot delete reserved namespace");
            return;
        }
        if (std.mem.eql(u8, name, "default")) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "cannot delete default namespace");
            return;
        }

        const force = req.value.len > 0 and req.value[0] != 0;
        if (!force and shard.namespace_handler.namespaceHasData(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "namespace is not empty; use --force to delete");
            return;
        }
        if (!shard.namespace_handler.namespaces.contains(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "namespace not found");
            return;
        }

        // Propose namespace_delete entry through Raft → UAL (persists via segment writer)
        _ = proposeNamespaceEntry(shard, .namespace_delete, name, &.{}) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "raft propose failed");
            return;
        };

        // Apply in-memory state directly
        shard.namespace_handler.applyDelete(name);

        // Also propagate to coordinator if wired
        if (shard.coordinator) |coord| {
            _ = coord.proposeDeleteNamespace(name) catch {};
            _ = coord.applyCommitted() catch {};
        }

        // Clean up projection data on force-delete
        if (force and name.len > 0) {
            var prefix_buf: [256]u8 = undefined;
            @memcpy(prefix_buf[0..name.len], name);
            prefix_buf[name.len] = 0;
            _ = shard.defaultPartition().kv.clearByPrefix(prefix_buf[0 .. name.len + 1]);
            shard.defaultPartition().stream.reset();
            shard.defaultPartition().queue.reset();
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    fn dispatchConfigSet(shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (name.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "namespace name is required");
            return;
        }
        if (!shard.namespace_handler.namespaces.contains(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "namespace not found");
            return;
        }
        if (req.value.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "settings payload is required");
            return;
        }

        const parsed = NamespaceConfig.deserializeSettings(req.value);
        if (parsed.config.settingsEmpty()) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "no valid settings provided");
            return;
        }

        // Serialize settings as the value portion of the UAL entry
        var settings_buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
        const settings_len = parsed.config.serializeSettings(&settings_buf);

        _ = proposeNamespaceEntry(shard, .namespace_config, name, settings_buf[0..settings_len]) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "raft propose failed");
            return;
        };

        // Apply in-memory state directly
        shard.namespace_handler.applyConfigUpdate(name, parsed.config);

        // Also propagate to coordinator if wired
        if (shard.coordinator) |coord| {
            _ = coord.proposeUpdateNamespaceConfig(name, parsed.config) catch {};
            _ = coord.applyCommitted() catch {};
        }

        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// Propose a namespace mutation as a UAL entry through Raft.
    /// The entry uses CommandPayload format: key = namespace name, value = payload.
    /// Persistence happens via the segment writer callback on UAL append.
    /// In-memory state is applied directly by the caller after propose succeeds.
    fn proposeNamespaceEntry(
        shard: *Shard,
        entry_type: entry_mod.EntryType,
        name: []const u8,
        value: []const u8,
    ) !@import("../raft/node.zig").ProposeResult {
        var payload_buf: [MAX_NS_PAYLOAD]u8 = undefined;
        const cmd = entry_mod.CommandPayload{
            .namespace_hash = 0, // namespaces are global, no namespace-hash needed
            .key_length = @intCast(name.len),
            .value_length = @intCast(value.len),
            .key = name,
            .value = value,
        };
        const payload_len = cmd.serialize(&payload_buf) orelse return error.PayloadTooLarge;
        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        return shard.raft_node.propose(entry_type, entry_mod.Flags.NONE, timestamp_ns, payload_buf[0..payload_len]);
    }

    /// Replay a namespace UAL entry to rebuild in-memory state.
    /// Called during segment replay on startup and after Raft commit.
    pub fn replayEntry(self: *NamespaceHandler, entry: *const entry_mod.Entry) void {
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        const name = cmd.key;
        if (name.len == 0) return;

        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);
        switch (etype) {
            .namespace_create => self.applyCreate(name),
            .namespace_delete => self.applyDelete(name),
            .namespace_config => {
                if (cmd.value.len > 0) {
                    const parsed = NamespaceConfig.deserializeSettings(cmd.value);
                    self.applyConfigUpdate(name, parsed.config);
                }
            },
            else => {},
        }
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *NamespaceHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .namespace_create => self.handleCreate(req),
            .namespace_delete => self.handleDelete(req),
            .namespace_list => self.handleList(req),
            .namespace_info => self.handleInfo(req),
            .namespace_config_get => self.handleConfigGet(req),
            .namespace_config_set => self.handleConfigSet(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown namespace opcode" } },
        };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }
        if (name.len > MAX_NAMESPACE_LEN) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name too long" } };
        }
        if (!isValidName(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "invalid namespace name" } };
        }
        if (isReserved(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "reserved namespace name" } };
        }

        // Check if already exists
        if (self.namespaces.contains(name)) {
            return .{ .err = .{ .code = .already_exists, .message = "namespace already exists" } };
        }

        // Check capacity
        if (self.namespaces.count() >= MAX_NAMESPACES) {
            return .{ .err = .{ .code = .internal_error, .message = "namespace limit reached" } };
        }

        // Store owned copy of name
        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };
        errdefer self.allocator.free(owned_name);

        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, std.time.milliTimestamp()))) * 1_000_000);

        self.namespaces.put(owned_name, .{
            .created_at_ns = now_ns,
        }) catch {
            self.allocator.free(owned_name);
            return .{ .err = .{ .code = .internal_error, .message = "namespace store failed" } };
        };

        return .{ .namespace_created = {} };
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    fn handleDelete(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }
        if (isReserved(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "cannot delete reserved namespace" } };
        }

        // "default" namespace cannot be deleted
        if (std.mem.eql(u8, name, "default")) {
            return .{ .err = .{ .code = .invalid_request, .message = "cannot delete default namespace" } };
        }

        // Parse force flag from req.value[0]
        const force = req.value.len > 0 and req.value[0] != 0;

        // If not force, check if namespace has data
        if (!force and self.namespaceHasData(name)) {
            return .{ .err = .{ .code = .namespace_not_empty, .message = "namespace is not empty; use --force to delete" } };
        }

        if (self.namespaces.fetchRemove(name)) |kv| {
            self.allocator.free(kv.key);
            return .{ .namespace_deleted = {} };
        }

        // Non-existent namespace — return not_found error
        return .{ .err = .{ .code = .not_found, .message = "namespace not found" } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *NamespaceHandler, req: Request) CommandResult {
        // Check if system namespaces should be included
        const include_system = req.value.len > 0 and req.value[0] != 0;
        _ = include_system;

        const data = self.serializeNamespaceList() catch {
            return .{ .err = .{ .code = .internal_error, .message = "namespace list serialization failed" } };
        };

        return .{ .namespace_list = .{ .data = data, .allocated = true } };
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }

        const exists = self.namespaces.contains(name);

        // Duplicate the name for the response
        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };

        return .{ .namespace_info = .{
            .exists = exists,
            .name = owned_name,
            .allocated = true,
        } };
    }

    // ── CONFIG GET ──────────────────────────────────────────────────────

    fn handleConfigGet(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }

        if (self.namespaces.get(name)) |meta| {
            var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
            const len = meta.config.serializeSettings(&buf);
            const data = self.allocator.dupe(u8, buf[0..len]) catch {
                return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
            };
            return .{ .namespace_config_get = .{
                .data = data,
                .allocated = true,
            } };
        }

        return .{ .err = .{ .code = .not_found, .message = "namespace not found" } };
    }

    // ── CONFIG SET (local-only path, no Raft) ───────────────────────────

    pub fn handleConfigSet(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }

        if (!self.namespaces.contains(name)) {
            return .{ .err = .{ .code = .not_found, .message = "namespace not found" } };
        }

        if (req.value.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "settings payload is required" } };
        }

        const parsed = NamespaceConfig.deserializeSettings(req.value);
        if (parsed.config.settingsEmpty()) {
            return .{ .err = .{ .code = .invalid_request, .message = "no valid settings provided" } };
        }

        self.applyConfigUpdate(name, parsed.config);
        return .{ .namespace_config_set = {} };
    }

    /// Apply a config update to the local namespace registry.
    pub fn applyConfigUpdate(self: *NamespaceHandler, name: []const u8, config: NamespaceConfig) void {
        if (self.namespaces.getPtr(name)) |meta| {
            meta.config.mergeSettings(config);
        }
    }

    /// Get namespace settings for a given namespace name.
    /// Returns default config if namespace not found.
    pub fn getSettings(self: *NamespaceHandler, name: []const u8) NamespaceConfig {
        if (self.namespaces.get(name)) |meta| {
            return meta.config;
        }
        return .{};
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Wire format: [count:u32] ([name_len:u16][name:bytes])*
    fn serializeNamespaceList(self: *NamespaceHandler) ![]u8 {
        // Calculate total size
        var total_size: usize = 4; // count header
        var entry_count: u32 = 0;
        var it = self.namespaces.iterator();
        while (it.next()) |entry| {
            total_size += 2 + entry.key_ptr.*.len; // u16 name_len + name bytes
            entry_count += 1;
        }

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], entry_count, .little);
        offset += 4;

        var it2 = self.namespaces.iterator();
        while (it2.next()) |entry| {
            const name = entry.key_ptr.*;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(name.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + name.len], name);
            offset += name.len;
        }

        return buf;
    }

    // ── Validation ──────────────────────────────────────────────────────

    fn isValidName(name: []const u8) bool {
        if (name.len == 0) return false;
        for (name) |c| {
            switch (c) {
                'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => {},
                else => return false,
            }
        }
        // Must start with letter or underscore
        switch (name[0]) {
            'a'...'z', 'A'...'Z', '_' => return true,
            else => return false,
        }
    }

    fn isReserved(name: []const u8) bool {
        for (reserved_prefixes) |prefix| {
            if (name.len >= prefix.len and std.mem.eql(u8, name[0..prefix.len], prefix)) {
                // Exact match or prefix match with separator
                if (name.len == prefix.len) return true;
                if (name[prefix.len] == ':' or name[prefix.len] == '.') return true;
            }
        }
        return false;
    }

    // ── Free Result ─────────────────────────────────────────────────────

    pub fn freeResult(self: *NamespaceHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .namespace_list => |r| {
                if (r.allocated) self.allocator.free(r.data);
            },
            .namespace_info => |r| {
                if (r.allocated) self.allocator.free(r.name);
            },
            .namespace_config_get => |r| {
                if (r.allocated) self.allocator.free(r.data);
            },
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Map namespace CommandResult variants to wire responses.
fn sendNamespaceResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok, .namespace_created, .namespace_deleted, .namespace_config_set => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendErrorResponse(conn, request_id, errorCodeToStatus(e.code), e.message);
        },
        .namespace_list => |n| {
            shard.sendOkResponse(conn, request_id, n.data);
        },
        .namespace_info => |n| {
            // Wire format: [exists:u8][name_len:u16 LE][name:bytes]
            var buf: [3 + 128]u8 = undefined;
            buf[0] = if (n.exists) 1 else 0;
            std.mem.writeInt(u16, buf[1..3], @intCast(n.name.len), .little);
            if (n.name.len > 0) {
                @memcpy(buf[3 .. 3 + n.name.len], n.name);
            }
            shard.sendOkResponse(conn, request_id, buf[0 .. 3 + n.name.len]);
        },
        .namespace_config_get => |n| {
            shard.sendOkResponse(conn, request_id, n.data);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled namespace response");
        },
    }
}

/// Map CommandResult.ErrorCode to wire StatusCode.
fn errorCodeToStatus(code: CommandResult.ErrorCode) proto.StatusCode {
    return switch (code) {
        .invalid_request => .bad_request,
        .unauthorized => .unauthorized,
        .not_found => .not_found,
        .already_exists => .conflict,
        .namespace_not_empty => .conflict,
        .timeout => .internal_error,
        .internal_error => .internal_error,
        .unavailable => .internal_error,
        else => .internal_error,
    };
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
        .namespace = "",
        .key = key,
        .value = value,
        .options = "",
    };
}

test "namespace handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    NamespaceHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_create)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_info)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_config_set)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_config_get)] != null);

    try testing.expectEqual(@as(u16, 6), dispatcher.handler_count);
}

test "namespace handler: create" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "production", ""));
    switch (result) {
        .namespace_created => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: create duplicate fails" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    const result = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.already_exists, e.code),
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: create empty name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create invalid name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "has spaces", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create reserved name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "_sys", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }

    const result2 = handler.handleCommand(makeRequest(.namespace_create, "_internal:test", ""));
    switch (result2) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "staging", ""));
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());

    const result = handler.handleCommand(makeRequest(.namespace_delete, "staging", ""));
    switch (result) {
        .namespace_deleted => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
}

test "namespace handler: delete non-existent returns not_found" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "ghost", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete default namespace blocked" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "default", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete reserved blocked" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "_flo.meta", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete non-empty without force blocked" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "myns", ""));
    handler.markNamespaceHasData("myns");

    // Delete without force should fail
    const result = handler.handleCommand(makeRequest(.namespace_delete, "myns", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.namespace_not_empty, e.code),
        else => return error.TestUnexpectedResult,
    }

    // Namespace should still exist
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: delete non-empty with force succeeds" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "myns", ""));
    handler.markNamespaceHasData("myns");

    // Delete with force=1 should succeed
    const force_value = [_]u8{1};
    const result = handler.handleCommand(makeRequest(.namespace_delete, "myns", &force_value));
    switch (result) {
        .namespace_deleted => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
}

test "namespace handler: markNamespaceHasData" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "tracked", ""));
    try testing.expect(!handler.namespaceHasData("tracked"));

    handler.markNamespaceHasData("tracked");
    try testing.expect(handler.namespaceHasData("tracked"));

    // Default namespace is auto-created on first implicit write
    handler.markNamespaceHasData("default");
    try testing.expect(handler.namespaceHasData("default"));

    // Non-existent namespace is auto-created on write
    handler.markNamespaceHasData("ghost");
    try testing.expect(handler.namespaceHasData("ghost"));
}

test "namespace handler: list" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "alpha", ""));
    _ = handler.handleCommand(makeRequest(.namespace_create, "beta", ""));

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            try testing.expect(r.allocated);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: list empty" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: info existing" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "myns", ""));

    const result = handler.handleCommand(makeRequest(.namespace_info, "myns", ""));
    switch (result) {
        .namespace_info => |r| {
            defer handler.freeResult(result);
            try testing.expect(r.exists);
            try testing.expectEqualStrings("myns", r.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: info non-existing" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_info, "missing", ""));
    switch (result) {
        .namespace_info => |r| {
            defer handler.freeResult(result);
            try testing.expect(!r.exists);
            try testing.expectEqualStrings("missing", r.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: name validation" {
    // Valid names
    try testing.expect(NamespaceHandler.isValidName("production"));
    try testing.expect(NamespaceHandler.isValidName("test-env"));
    try testing.expect(NamespaceHandler.isValidName("stage_2"));
    try testing.expect(NamespaceHandler.isValidName("MyNs"));
    try testing.expect(NamespaceHandler.isValidName("_private"));
    try testing.expect(NamespaceHandler.isValidName("v1.0"));

    // Invalid names
    try testing.expect(!NamespaceHandler.isValidName(""));
    try testing.expect(!NamespaceHandler.isValidName("has spaces"));
    try testing.expect(!NamespaceHandler.isValidName("has/slash"));
    try testing.expect(!NamespaceHandler.isValidName("0starts-with-digit"));
    try testing.expect(!NamespaceHandler.isValidName("-starts-with-dash"));
}

test "namespace handler: reserved detection" {
    try testing.expect(NamespaceHandler.isReserved("_sys"));
    try testing.expect(NamespaceHandler.isReserved("_sys:meta"));
    try testing.expect(NamespaceHandler.isReserved("_sys.config"));
    try testing.expect(NamespaceHandler.isReserved("_internal"));
    try testing.expect(NamespaceHandler.isReserved("_flo"));
    try testing.expect(NamespaceHandler.isReserved("_flo:test"));

    try testing.expect(!NamespaceHandler.isReserved("_system"));
    try testing.expect(!NamespaceHandler.isReserved("production"));
    try testing.expect(!NamespaceHandler.isReserved("my_sys_ns"));
}

test "namespace handler: freeResult non-allocated is no-op" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    handler.freeResult(.ok);
    handler.freeResult(.{ .err = .{ .code = .invalid_request, .message = "test" } });
    handler.freeResult(.{ .namespace_created = {} });
    handler.freeResult(.{ .namespace_deleted = {} });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Key Utilities — Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "qualifyKey: default namespace returns raw key" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const raw = "mykey";
    try testing.expectEqualStrings(raw, try qualifyKey(&buf, "", raw));
    try testing.expectEqualStrings(raw, try qualifyKey(&buf, "default", raw));
}

test "qualifyKey: non-default namespace prefixes correctly" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const result = try qualifyKey(&buf, "myapp", "mykey");
    try testing.expectEqual(@as(usize, 11), result.len); // "myapp" + \0 + "mykey"
    try testing.expectEqualStrings("myapp", result[0..5]);
    try testing.expectEqual(@as(u8, 0), result[5]);
    try testing.expectEqualStrings("mykey", result[6..]);
}

test "qualifyKey: oversized key returns KeyTooLarge" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const big_key = &[_]u8{'x'} ** (MAX_QUALIFIED_KEY); // exactly fills buffer
    const result = qualifyKey(&buf, "ns", big_key);
    try testing.expectError(error.KeyTooLarge, result);
}

test "stripPrefix: removes namespace prefix" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const qualified = try qualifyKey(&buf, "myapp", "mykey");
    try testing.expectEqualStrings("mykey", stripPrefix(qualified, "myapp"));
}

test "stripPrefix: default namespace is no-op" {
    try testing.expectEqualStrings("mykey", stripPrefix("mykey", ""));
    try testing.expectEqualStrings("mykey", stripPrefix("mykey", "default"));
}

test "stripPrefix: wrong namespace returns key unchanged" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const qualified = try qualifyKey(&buf, "myapp", "mykey");
    try testing.expectEqualStrings(qualified, stripPrefix(qualified, "other"));
}

test "namespacePrefix: default returns empty" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expectEqual(@as(usize, 0), namespacePrefix(&buf, "").len);
    try testing.expectEqual(@as(usize, 0), namespacePrefix(&buf, "default").len);
}

test "namespacePrefix: non-default returns ns plus separator" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const prefix = namespacePrefix(&buf, "prod");
    try testing.expectEqual(@as(usize, 5), prefix.len);
    try testing.expectEqualStrings("prod", prefix[0..4]);
    try testing.expectEqual(@as(u8, 0), prefix[4]);
}

test "validateKeySize: valid keys" {
    try testing.expectEqual(@as(?[]const u8, null), validateKeySize("myapp", "mykey"));
    try testing.expectEqual(@as(?[]const u8, null), validateKeySize("", "mykey"));
    try testing.expectEqual(@as(?[]const u8, null), validateKeySize("default", "mykey"));
}

test "validateKeySize: empty key" {
    try testing.expect(validateKeySize("myapp", "") != null);
}

test "validateKeySize: oversized key with namespace" {
    const big = &[_]u8{'x'} ** MAX_QUALIFIED_KEY;
    try testing.expect(validateKeySize("ns", big) != null);
}

test "validateKeySize: oversized key without namespace" {
    const big = &[_]u8{'x'} ** (MAX_QUALIFIED_KEY + 1);
    try testing.expect(validateKeySize("", big) != null);
}

// ── Raft Apply Tests ────────────────────────────────────────────────────────

test "namespace handler: applyCreate adds to local registry" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
    handler.applyCreate("test-ns");
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
    try testing.expect(handler.namespaces.contains("test-ns"));
}

test "namespace handler: applyCreate is idempotent" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyCreate("test-ns");
    handler.applyCreate("test-ns"); // duplicate — should be no-op
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: applyDelete removes from local registry" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyCreate("test-ns");
    try testing.expect(handler.namespaces.contains("test-ns"));

    handler.applyDelete("test-ns");
    try testing.expect(!handler.namespaces.contains("test-ns"));
    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
}

test "namespace handler: applyDelete non-existent is no-op" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyDelete("does-not-exist"); // should not crash
    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
}

test "namespace handler: config set and get" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyCreate("myapp");

    const settings = NamespaceConfig{ .kv_max_hot_versions = 50, .stream_retention_s = 3600 };
    handler.applyConfigUpdate("myapp", settings);

    const retrieved = handler.getSettings("myapp");
    try testing.expectEqual(@as(?u32, 50), retrieved.kv_max_hot_versions);
    try testing.expectEqual(@as(?u64, 3600), retrieved.stream_retention_s);
    try testing.expectEqual(@as(?u64, null), retrieved.kv_version_ttl_s);
}

test "namespace handler: config merge preserves unset fields" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyCreate("myapp");

    // First update
    handler.applyConfigUpdate("myapp", .{ .kv_max_hot_versions = 50 });
    // Second update — should merge, not replace
    handler.applyConfigUpdate("myapp", .{ .queue_max_dlq_size = 200 });

    const retrieved = handler.getSettings("myapp");
    try testing.expectEqual(@as(?u32, 50), retrieved.kv_max_hot_versions); // preserved
    try testing.expectEqual(@as(?u32, 200), retrieved.queue_max_dlq_size); // added
}

test "namespace handler: config on nonexistent namespace is no-op" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    // Should not crash — namespace doesn't exist
    handler.applyConfigUpdate("nonexistent", .{ .kv_max_hot_versions = 50 });

    const retrieved = handler.getSettings("nonexistent");
    try testing.expect(retrieved.settingsEmpty());
}

test "namespace handler: getSettings defaults for new namespace" {
    var handler = NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyCreate("fresh");
    const settings = handler.getSettings("fresh");
    try testing.expect(settings.settingsEmpty());
}

test "namespace handler: handleConfigGet returns TLV" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    handler.applyCreate("myapp");
    handler.applyConfigUpdate("myapp", .{ .kv_max_hot_versions = 42 });

    const result = handler.handleCommand(makeRequest(.namespace_config_get, "myapp", ""));
    switch (result) {
        .namespace_config_get => |payload| {
            // Deserialize the returned TLV
            const de = NamespaceConfig.deserializeSettings(payload.data);
            try testing.expectEqual(@as(?u32, 42), de.config.kv_max_hot_versions);
            if (payload.allocated) {
                allocator.free(payload.data);
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: handleConfigSet via command" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    handler.applyCreate("myapp");

    // Build TLV for settings
    const config = NamespaceConfig{ .memory_budget_bytes = 1_073_741_824 };
    var tlv_buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const tlv_len = config.serializeSettings(&tlv_buf);

    const result = handler.handleCommand(makeRequest(.namespace_config_set, "myapp", tlv_buf[0..tlv_len]));
    switch (result) {
        .namespace_config_set => {},
        else => return error.TestUnexpectedResult,
    }

    // Verify it was applied
    const retrieved = handler.getSettings("myapp");
    try testing.expectEqual(@as(?u64, 1_073_741_824), retrieved.memory_budget_bytes);
}
