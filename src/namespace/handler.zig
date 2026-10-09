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
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const router = @import("../node/router.zig");

const CommandResult = result_mod.CommandResult;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Key Utilities — public API for all subsystems
// ═══════════════════════════════════════════════════════════════════════════════

/// Room reserved for the namespace in a qualified key; names are at most
/// `MAX_NAME_LEN`.
pub const MAX_NAMESPACE_LEN: usize = 128;

/// The longest namespace name.
pub const MAX_NAME_LEN: usize = 63;

/// Why `name` cannot name a namespace, or null if it can: 1–63 of
/// `[a-z0-9_-]`, starting with a letter or digit and not ending in `-`; a
/// leading `_` is the system's. Lowercase only, so no two names differ by
/// case alone. No `:` or NUL: workflow runs and definitions are keyed
/// "namespace:name" and read back to the first `:`, and keys are
/// "namespace\x00key". The applier checks it too, so it is part of the
/// replicated state: loosening it later is safe, tightening it is not.
pub fn nameRefusal(name: []const u8) ?[]const u8 {
    if (name.len == 0) return "namespace name is required";
    if (name.len > MAX_NAME_LEN) return "namespace name too long (at most 63 bytes)";
    if (name[0] == '_') return "reserved namespace name: a leading '_' is the system's";
    for (name) |c| switch (c) {
        'a'...'z', '0'...'9', '-', '_' => {},
        'A'...'Z' => return "invalid namespace name: lowercase only",
        else => return "invalid namespace name: letters, digits, '_' and '-' only",
    };
    if (name[0] == '-') return "invalid namespace name: must start with a letter or digit";
    if (name[name.len - 1] == '-') return "invalid namespace name: must not end with '-'";
    return null;
}

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

/// Null byte separator between namespace and key in qualified keys: no
/// namespace name can contain it (see `nameRefusal`).
pub const NAMESPACE_SEPARATOR: u8 = 0;

/// Build a namespace-qualified internal key: `"<namespace>\x00<key>"`.
///
/// For empty or "default" namespace, returns the raw key unchanged (no prefix).
/// This is the canonical function — all subsystems should use this for
/// namespace isolation rather than implementing their own qualification.
///
/// Returns `error.KeyHasNul` if raw_key contains the separator: a "default"
/// key is stored bare, so it could otherwise name another namespace's entry
/// (refused in every namespace so the rule is one rule). Callers must fail
/// closed; falling back to raw_key reopens that reach.
/// Returns `error.KeyTooLarge` if the combined length exceeds the buffer.
///
/// Lifetime: the returned slice borrows from `buf` (when prefixed) or from
/// `raw_key` (when no prefix). Safe for synchronous operations, waiter
/// registration (pool copies to inline buffer), and Raft propose (serializes
/// immediately).
pub fn qualifyKey(buf: *[MAX_QUALIFIED_KEY]u8, ns: []const u8, raw_key: []const u8) error{ KeyTooLarge, KeyHasNul }![]const u8 {
    if (std.mem.indexOfScalar(u8, raw_key, NAMESPACE_SEPARATOR) != null) return error.KeyHasNul;
    if (ns.len == 0 or std.mem.eql(u8, ns, "default")) return raw_key;
    const total = ns.len + 1 + raw_key.len;
    if (total > MAX_QUALIFIED_KEY) return error.KeyTooLarge;
    @memcpy(buf[0..ns.len], ns);
    buf[ns.len] = NAMESPACE_SEPARATOR;
    @memcpy(buf[ns.len + 1 ..][0..raw_key.len], raw_key);
    return buf[0..total];
}

/// Qualify a consumer-group name so it is scoped to a single stream:
/// `qualifyKey(ns, stream) ++ "\x00" ++ group`.
///
///   default ns:     `"<stream>\x00<group>"`
///   non-default ns: `"<ns>\x00<stream>\x00<group>"`
///
/// This is what keys a consumer group in the projection and in the durable
/// cg_commit/cg_delete entries, so folding the stream in here makes cursors and
/// PEL strictly per-(stream, group) — a group never spans streams. The prefix a
/// stream's groups share is exactly `qualifyKey(ns, stream) ++ "\x00"`, which
/// `deleteStream` uses to cascade-remove a deleted stream's groups.
pub fn qualifyGroupKey(buf: *[MAX_QUALIFIED_KEY]u8, ns: []const u8, stream: []const u8, group: []const u8) error{KeyTooLarge}![]const u8 {
    const has_ns = ns.len > 0 and !std.mem.eql(u8, ns, "default");
    const ns_prefix_len: usize = if (has_ns) ns.len + 1 else 0;
    const total = ns_prefix_len + stream.len + 1 + group.len;
    if (total > MAX_QUALIFIED_KEY) return error.KeyTooLarge;
    var off: usize = 0;
    if (has_ns) {
        @memcpy(buf[0..ns.len], ns);
        buf[ns.len] = NAMESPACE_SEPARATOR;
        off = ns.len + 1;
    }
    @memcpy(buf[off..][0..stream.len], stream);
    off += stream.len;
    buf[off] = NAMESPACE_SEPARATOR;
    off += 1;
    @memcpy(buf[off..][0..group.len], group);
    off += group.len;
    return buf[0..off];
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
/// or an empty slice for the default namespace (full scan). A name too long
/// to be one is refused, never cut.
pub fn namespacePrefix(buf: *[MAX_QUALIFIED_KEY]u8, ns: []const u8) error{NamespaceTooLong}![]const u8 {
    if (ns.len == 0 or std.mem.eql(u8, ns, "default")) return &.{};
    if (ns.len > MAX_NAME_LEN) return error.NamespaceTooLong;
    @memcpy(buf[0..ns.len], ns);
    buf[ns.len] = NAMESPACE_SEPARATOR;
    return buf[0 .. ns.len + 1];
}

/// Validate a key for namespace ns: non-empty, no NUL, and fits once qualified.
///
/// Call this at the dispatch layer (before any business logic) to give users
/// a clear error message upfront rather than a confusing qualification failure
/// deep in the handler.
///
/// Returns an error message string if invalid, or `null` if the key is valid.
pub fn validateKeySize(ns: []const u8, key: []const u8) ?[]const u8 {
    if (key.len == 0) return "key is required";
    if (std.mem.indexOfScalar(u8, key, NAMESPACE_SEPARATOR) != null) return "key must not contain NUL";
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
    /// Names whose create this leader proposed, for a write or explicitly,
    /// and has not yet applied: a burst of first writes proposes one, and
    /// they hold room and ids until they apply. Cleared when this node
    /// stops leading: an entry the log then drops is proposed again.
    pending_creates: std.StringHashMapUnmanaged(void) = .{},
    /// What the last applied create did (see `Shard.answering_index`): two
    /// creates can both pass the handler's checks before either applies.
    last_create: CreateOutcome = .created,
    implicit_failed: u64 = 0,
    implicit_warn_ms: i64 = 0,

    /// In-memory namespace registry. Keys are owned copies of namespace names.
    /// Will be replaced by Controller Raft storage when wired.
    namespaces: std.StringHashMap(NamespaceMeta),
    /// Namespace hash → name, for the appliers that see only the hash on
    /// every committed entry. Kept in step with `namespaces`.
    names_by_hash: std.AutoHashMap(u32, []const u8),

    const MAX_NAMESPACES: usize = 1024;

    pub const CreateOutcome = enum { created, existed, invalid, collision, full, failed };

    pub const LIMIT_MESSAGE = std.fmt.comptimePrint("namespace limit reached ({d} per shard)", .{MAX_NAMESPACES});
    pub const COLLISION_MESSAGE = "namespace name collides with an existing namespace's id; choose another name";

    /// Why a request may not create or write to a namespace.
    pub const Refusal = struct { status: @import("../protocol/proto.zig").StatusCode, message: []const u8 };

    pub const NamespaceMeta = struct {
        created_at_ns: u64,
        /// Tracks whether data has been written to this namespace.
        /// Incremented by markNamespaceHasData(), used for non-empty delete check.
        data_count: u32 = 0,
    };

    /// "default" is registered from the start, before replay, with no log
    /// entry: its id is held from the first request, so no other name can
    /// take it.
    pub fn init(allocator: Allocator) !NamespaceHandler {
        var self: NamespaceHandler = .{
            .allocator = allocator,
            .namespaces = std.StringHashMap(NamespaceMeta).init(allocator),
            .names_by_hash = std.AutoHashMap(u32, []const u8).init(allocator),
        };
        errdefer self.deinit();
        const owned = try allocator.dupe(u8, "default");
        self.insertNamespace(owned, .{ .created_at_ns = 0 }) catch |err| {
            allocator.free(owned);
            return err;
        };
        return self;
    }

    pub fn deinit(self: *NamespaceHandler) void {
        var ic = self.pending_creates.keyIterator();
        while (ic.next()) |k| self.allocator.free(k.*);
        self.pending_creates.deinit(self.allocator);
        var it = self.namespaces.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.namespaces.deinit();
        self.names_by_hash.deinit();
    }

    /// Insert an owned name into both maps; frees it if either insert fails.
    fn insertNamespace(self: *NamespaceHandler, owned: []const u8, meta: NamespaceMeta) !void {
        try self.names_by_hash.put(router.namespaceHash(owned), owned);
        self.namespaces.put(owned, meta) catch |err| {
            _ = self.names_by_hash.remove(router.namespaceHash(owned));
            return err;
        };
    }

    /// Remove from both maps; returns the owned name for the caller to free.
    fn removeNamespace(self: *NamespaceHandler, name: []const u8) ?[]const u8 {
        const kv = self.namespaces.fetchRemove(name) orelse return null;
        _ = self.names_by_hash.remove(router.namespaceHash(kv.key));
        return kv.key;
    }

    // ── Namespace Data Tracking ─────────────────────────────────────────

    /// Mark a namespace as having data written to it.
    /// Called by KV/Stream/Queue handlers after successful writes.
    /// Auto-creates the "default" namespace entry when called with empty or "default" name.
    /// When `shard` is provided and the namespace is new, persists a namespace_create
    /// entry to the segment log so the namespace survives restart.
    pub fn markNamespaceHasData(self: *NamespaceHandler, name: []const u8, shard: ?*Shard) void {
        const effective = if (name.len == 0 or std.mem.eql(u8, name, "default")) "default" else name;
        if (self.namespaces.getPtr(effective)) |meta| {
            meta.data_count +|= 1; // saturating add
            return;
        }
        if (shard) |s| {
            self.proposeImplicitCreate(effective, s, true);
        } else {
            _ = self.applyCreate(effective);
            if (self.namespaces.getPtr(effective)) |meta| meta.data_count = 1;
        }
    }

    /// The value of a namespace_create proposed for a write that has
    /// already applied (`counts_write`): its applier counts that write. An
    /// explicit create, or one proposed ahead of its write, carries an
    /// empty value.
    const IMPLICIT_CREATE = "implicit";

    /// Implicit creation (e.g. "default" on the first bare-namespace write):
    /// a namespace_create entry, applied like an explicit one so it
    /// survives restart and reaches followers. One in flight per name.
    /// `counts_write`: the write that caused it has already applied and
    /// its responder found no namespace to count against, so the create
    /// counts it; a write that proposes the create ahead of its own entry
    /// is counted by its responder.
    pub fn proposeImplicitCreate(self: *NamespaceHandler, name: []const u8, s: *Shard, counts_write: bool) void {
        const effective = if (name.len == 0 or std.mem.eql(u8, name, "default")) "default" else name;
        if (self.namespaces.contains(effective) or self.pending_creates.contains(effective)) return;
        // Not proposed when it would be refused; a sink checks `admission`
        // itself and drops the write.
        if (!self.admits(effective)) return;
        _ = proposeNamespaceEntry(s, .namespace_create, effective, if (counts_write) IMPLICIT_CREATE else "") catch |err| {
            // Every write to the namespace tries again, so said once per
            // interval rather than per write.
            self.implicit_failed += 1;
            const now = @import("stdx").time.milliTimestamp();
            if (now - self.implicit_warn_ms >= 30_000) {
                self.implicit_warn_ms = now;
                log.warn("namespace: implicit create of '{s}' not persisted: {s} ({d} failed so far); its writes are not metered until it is", .{ effective, @errorName(err), self.implicit_failed });
            }
            return;
        };
        self.notePending(effective);
    }

    /// Why a write may not name `name` (empty is "default"), or null: an
    /// existing namespace, or a valid name with room left whose id no
    /// other namespace holds. Implicit creates in flight count against the
    /// room, so a burst cannot overshoot. "default" always has room, so
    /// filling the registry cannot lock it out.
    pub fn admission(self: *const NamespaceHandler, name: []const u8) ?Refusal {
        const effective = if (name.len == 0) "default" else name;
        if (self.namespaces.contains(effective) or self.pending_creates.contains(effective)) return null;
        if (nameRefusal(effective)) |why| return .{ .status = .bad_request, .message = why };
        if (self.collides(effective)) return .{ .status = .bad_request, .message = COLLISION_MESSAGE };
        if (!isDefault(effective) and self.namespaces.count() + self.pending_creates.count() >= MAX_NAMESPACES)
            return .{ .status = .bad_request, .message = LIMIT_MESSAGE };
        return null;
    }

    pub fn admits(self: *const NamespaceHandler, name: []const u8) bool {
        return self.admission(name) == null;
    }

    fn isDefault(name: []const u8) bool {
        return std.mem.eql(u8, name, "default");
    }

    /// Whether another namespace holds, or is being created with, `name`'s
    /// id: entries carry the id, not the name, so two names on one id would
    /// share data. Id 0 is what an entry without a namespace carries, and
    /// is never a name's.
    fn collides(self: *const NamespaceHandler, name: []const u8) bool {
        return self.idTaken(name, router.namespaceHash(name));
    }

    fn idTaken(self: *const NamespaceHandler, name: []const u8, id: u32) bool {
        if (id == 0) return true;
        if (self.names_by_hash.get(id)) |other| return !std.mem.eql(u8, other, name);
        var it = self.pending_creates.keyIterator();
        while (it.next()) |pending| {
            if (router.namespaceHash(pending.*) == id and !std.mem.eql(u8, pending.*, name)) return true;
        }
        return false;
    }

    /// Note a create this leader proposed, until it applies.
    fn notePending(self: *NamespaceHandler, name: []const u8) void {
        if (self.pending_creates.contains(name)) return;
        const owned = self.allocator.dupe(u8, name) catch return;
        self.pending_creates.put(self.allocator, owned, {}) catch self.allocator.free(owned);
    }

    /// This node stopped leading: its implicit creates may have been
    /// dropped with the log's tail.
    pub fn forgetImplicitCreates(self: *NamespaceHandler) void {
        var it = self.pending_creates.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.pending_creates.clearRetainingCapacity();
    }

    /// Check if a namespace has had data written to it.
    pub fn namespaceHasData(self: *NamespaceHandler, name: []const u8) bool {
        if (self.namespaces.get(name)) |meta| {
            return meta.data_count > 0;
        }
        return false;
    }

    // ── Appliers ────────────────────────────────────────────────────────
    // Called from the registry for every committed namespace entry.

    /// Resolve a namespace name from its `router.namespaceHash`. Runs once per
    /// applied entry (entries carry the hash, not the string), so it is an
    /// index lookup. Returns null for a namespace this node has never
    /// registered, including "default" before its first write.
    pub fn nameForHash(self: *const NamespaceHandler, hash: u32) ?[]const u8 {
        return self.names_by_hash.get(hash);
    }

    /// Apply a committed namespace creation to the local registry. The
    /// name, its id and the cap are checked again here, the same on every
    /// replica, whatever proposed it.
    pub fn applyCreate(self: *NamespaceHandler, name: []const u8) CreateOutcome {
        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000);
        return self.applyCreateAt(name, now_ns);
    }

    /// `applyCreate` from a log entry: created when the entry was stamped,
    /// the same on every replica.
    pub fn applyCreateAt(self: *NamespaceHandler, name: []const u8, created_at_ns: u64) CreateOutcome {
        if (self.namespaces.contains(name)) return .existed; // idempotent
        if (nameRefusal(name) != null) return .invalid;
        if (self.collides(name)) return .collision;
        if (!isDefault(name) and self.namespaces.count() >= MAX_NAMESPACES) return .full;
        const owned = self.allocator.dupe(u8, name) catch return .failed;
        self.insertNamespace(owned, .{ .created_at_ns = created_at_ns }) catch {
            self.allocator.free(owned);
            return .failed;
        };
        return .created;
    }

    /// Apply a Raft-committed namespace deletion to the local registry.
    pub fn applyDelete(self: *NamespaceHandler, name: []const u8) void {
        if (self.removeNamespace(name)) |owned| {
            self.allocator.free(owned);
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

        if (op == .namespace_config_set or op == .namespace_config_get) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, SETTINGS_REFUSAL);
            return;
        }

        // Reads (list, info) always go to local handler
        if (op == .namespace_list or op == .namespace_info) {
            const cmd_result = shard.namespace_handler.handleCommand(req);
            defer shard.namespace_handler.freeResult(cmd_result);
            sendNamespaceResponse(shard, conn, req.header.request_id, cmd_result);
            return;
        }

        // Mutations (create, delete): validate then persist via UAL
        switch (op) {
            .namespace_create => dispatchCreate(shard, conn, req),
            .namespace_delete => dispatchDelete(shard, conn, req),
            else => shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown namespace mutation"),
        }
    }

    fn dispatchCreate(shard: *Shard, conn: *Connection, req: Request) void {
        const name = req.key;

        if (nameRefusal(name)) |why| {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
            return;
        }
        if (shard.namespace_handler.namespaces.contains(name)) {
            shard.sendErrorResponse(conn, req.header.request_id, .conflict, "namespace already exists");
            return;
        }
        if (shard.namespace_handler.admission(name)) |r| {
            shard.sendErrorResponse(conn, req.header.request_id, r.status, r.message);
            return;
        }

        const proposed = proposeNamespaceEntry(shard, .namespace_create, name, &.{}) catch |err| {
            shard.sendErrorResponse(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureMessage(err, "not persisted"));
            return;
        };
        shard.namespace_handler.notePending(name);
        shard.park(conn, req, proposed, respondCreate);
    }

    fn respondCreate(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        switch (shard.namespace_handler.last_create) {
            .created => {},
            .existed => return shard.sendErrorResponse(conn, req.header.request_id, .conflict, "namespace already exists"),
            .invalid => return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, nameRefusal(req.key) orelse "invalid namespace name"),
            .collision => return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, COLLISION_MESSAGE),
            .full => return shard.sendErrorResponse(conn, req.header.request_id, .bad_request, LIMIT_MESSAGE),
            .failed => return shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "namespace not registered: out of memory"),
        }
        shard.sendOkResponse(conn, req.header.request_id, "");
    }

    /// A namespace's data can live on any shard, keyed by a name that a
    /// later create could reuse: until it can be removed everywhere and
    /// the name retired, delete is refused.
    pub const DELETE_REFUSAL = "namespace delete isn't supported yet: a namespace's data can't be removed safely across shards";

    fn dispatchDelete(shard: *Shard, conn: *Connection, req: Request) void {
        shard.sendErrorResponse(conn, req.header.request_id, .bad_request, DELETE_REFUSAL);
    }

    /// No namespace setting has anything that enforces it, so none is
    /// accepted: one comes back together with the code that applies it.
    pub const SETTINGS_REFUSAL = "namespace settings aren't supported yet: none would take effect";

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// Propose a namespace mutation as a UAL entry through Raft.
    /// The entry uses CommandPayload format: key = namespace name, value = payload.
    /// Namespace entries use empty namespace ("") to get namespace_hash=0
    /// because namespaces are global, not scoped to a namespace.
    fn proposeNamespaceEntry(
        shard: *Shard,
        entry_type: entry_mod.EntryType,
        name: []const u8,
        value: []const u8,
    ) !persistence_mod.ProposeResult {
        return persistence_mod.proposeEntry(shard, entry_type, entry_mod.Flags.NONE, "", name, value);
    }

    /// Register this handler's entry types with the ReplayRegistry.
    pub fn registerReplay(self: *NamespaceHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.namespace_create, @ptrCast(self), replayEntryThunk);
        registry.register(.namespace_delete, @ptrCast(self), replayEntryThunk);
        registry.register(.namespace_config, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *NamespaceHandler = @ptrCast(@alignCast(ctx));
        self.replayEntry(entry);
    }

    /// Replay a namespace UAL entry to rebuild in-memory state.
    /// Called during segment replay on startup and after Raft commit.
    pub fn replayEntry(self: *NamespaceHandler, entry: *const entry_mod.Entry) void {
        self.last_create = .created;
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
        const name = cmd.key;
        if (name.len == 0) return;

        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);
        switch (etype) {
            .namespace_create => {
                self.last_create = self.applyCreateAt(name, entry.header.timestamp_ns);
                if (std.mem.eql(u8, cmd.value, IMPLICIT_CREATE)) {
                    if (self.namespaces.getPtr(name)) |meta| meta.data_count = @max(meta.data_count, 1);
                }
                if (self.pending_creates.fetchRemove(name)) |kv| self.allocator.free(kv.key);
            },
            .namespace_delete => self.applyDelete(name),
            .namespace_config => log.warn("namespace: skipped a settings entry for namespace={s}: namespace settings aren't supported", .{name}),
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
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown namespace opcode" } },
        };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;
        if (nameRefusal(name)) |why| {
            return .{ .err = .{ .code = .invalid_request, .message = why } };
        }
        return switch (self.applyCreate(name)) {
            .created => .{ .namespace_created = {} },
            .existed => .{ .err = .{ .code = .already_exists, .message = "namespace already exists" } },
            .invalid => .{ .err = .{ .code = .invalid_request, .message = "invalid namespace name" } },
            .collision => .{ .err = .{ .code = .invalid_request, .message = COLLISION_MESSAGE } },
            .full => .{ .err = .{ .code = .invalid_request, .message = LIMIT_MESSAGE } },
            .failed => .{ .err = .{ .code = .internal_error, .message = "namespace not registered: out of memory" } },
        };
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    fn handleDelete(_: *NamespaceHandler, _: Request) CommandResult {
        return .{ .err = .{ .code = .invalid_request, .message = DELETE_REFUSAL } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *NamespaceHandler, req: Request) CommandResult {
        _ = req;
        const data = self.serializeNamespaceList() catch {
            return .{ .err = .{ .code = .internal_error, .message = "namespace list serialization failed" } };
        };

        return .{ .namespace_list = .{ .data = data } };
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }
        // The answer is built in a fixed buffer sized for a valid name.
        if (nameRefusal(name)) |why| return .{ .err = .{ .code = .invalid_request, .message = why } };

        const exists = self.namespaces.contains(name);

        // Duplicate the name for the response
        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };

        return .{ .namespace_info = .{
            .exists = exists,
            .name = owned_name,
        } };
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

    // ── Free Result ─────────────────────────────────────────────────────

    pub fn freeResult(self: *NamespaceHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .namespace_list => |r| {
                self.allocator.free(r.data);
            },
            .namespace_info => |r| {
                self.allocator.free(r.name);
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
        .ok, .namespace_created, .namespace_deleted => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendErrorResponse(conn, request_id, e.code.toStatus(), e.message);
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
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled namespace response");
        },
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
            .reserved = .{0} ** 8,
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
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "production", ""));
    switch (result) {
        .namespace_created => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
}

test "namespace handler: create duplicate fails" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    const result = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.already_exists, e.code),
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
}

test "namespace handler: create empty name" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create invalid name" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "has spaces", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create reserved name" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
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

test "namespace handler: delete is refused, whatever the namespace holds, and nothing is removed" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    _ = handler.handleCommand(makeRequest(.namespace_create, "staging", ""));
    for ([_][]const u8{ "", "\x01" }) |force| {
        switch (handler.handleCommand(makeRequest(.namespace_delete, "staging", force))) {
            .err => |e| try testing.expectEqualStrings(NamespaceHandler.DELETE_REFUSAL, e.message),
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expect(handler.namespaces.contains("staging"));
}

test "namespace handler: markNamespaceHasData" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "tracked", ""));
    try testing.expect(!handler.namespaceHasData("tracked"));

    handler.markNamespaceHasData("tracked", null);
    try testing.expect(handler.namespaceHasData("tracked"));

    // Default namespace is auto-created on first implicit write
    handler.markNamespaceHasData("default", null);
    try testing.expect(handler.namespaceHasData("default"));

    // Non-existent namespace is auto-created on write
    handler.markNamespaceHasData("ghost", null);
    try testing.expect(handler.namespaceHasData("ghost"));
}

test "namespace handler: list" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "alpha", ""));
    _ = handler.handleCommand(makeRequest(.namespace_create, "beta", ""));

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: list holds default from the start" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: info existing" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
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
    var handler = try NamespaceHandler.init(allocator);
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

test "namespace handler: the name grammar, with a reason for each refusal" {
    for ([_][]const u8{ "production", "test-env", "stage_2", "0starts-with-digit", "a", "9", "default", "a" ** 63 }) |ok| {
        if (nameRefusal(ok)) |why| {
            std.debug.print("{s}: {s}\n", .{ ok, why });
            return error.TestUnexpectedResult;
        }
    }
    const Case = struct { name: []const u8, starts: []const u8 };
    for ([_]Case{
        .{ .name = "", .starts = "namespace name is required" },
        .{ .name = "a" ** 64, .starts = "namespace name too long" },
        .{ .name = "_private", .starts = "reserved" },
        .{ .name = "_sys:meta", .starts = "reserved" },
        .{ .name = "MyNs", .starts = "invalid namespace name: lowercase only" },
        .{ .name = "v1.0", .starts = "invalid namespace name: letters, digits" },
        .{ .name = "has spaces", .starts = "invalid namespace name: letters, digits" },
        .{ .name = "has/slash", .starts = "invalid namespace name: letters, digits" },
        .{ .name = "a:b", .starts = "invalid namespace name: letters, digits" },
        .{ .name = "a\x00b", .starts = "invalid namespace name: letters, digits" },
        .{ .name = "-starts-with-dash", .starts = "invalid namespace name: must start" },
        .{ .name = "ends-with-dash-", .starts = "invalid namespace name: must not end" },
    }) |c| {
        const why = nameRefusal(c.name) orelse {
            std.debug.print("accepted: {s}\n", .{c.name});
            return error.TestUnexpectedResult;
        };
        if (!std.mem.startsWith(u8, why, c.starts)) {
            std.debug.print("{s}: {s}\n", .{ c.name, why });
            return error.TestUnexpectedResult;
        }
    }
}

test "namespace handler: the applier refuses a bad name and a full registry, whatever proposed it" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    try testing.expectEqual(NamespaceHandler.CreateOutcome.invalid, handler.applyCreate("a" ** 300));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.invalid, handler.applyCreate("_flo"));
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());

    // "default" holds one of the 1024 from the start.
    var buf: [16]u8 = undefined;
    for (0..NamespaceHandler.MAX_NAMESPACES - 1) |i| {
        try testing.expectEqual(NamespaceHandler.CreateOutcome.created, handler.applyCreate(try std.fmt.bufPrint(&buf, "ns{d}", .{i})));
    }
    try testing.expect(handler.admits("ns0"));
    try testing.expect(!handler.admits("one-more"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.full, handler.applyCreate("one-more"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.existed, handler.applyCreate("ns0"));
    try testing.expectEqual(NamespaceHandler.MAX_NAMESPACES, handler.namespaces.count());
    // "default" is never locked out.
    try testing.expect(handler.admits(""));
    try testing.expect(handler.admits("default"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.existed, handler.applyCreate("default"));
}

test "namespace handler: default's id, and id 0, are never another name's" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    // Held before anything is created or replayed.
    try testing.expectEqualStrings("default", handler.nameForHash(router.namespaceHash("default")).?);
    try testing.expect(handler.idTaken("squatter", router.namespaceHash("default")));
    try testing.expect(!handler.idTaken("default", router.namespaceHash("default")));
    try testing.expect(handler.idTaken("squatter", 0));
}

test "namespace handler: a create in flight holds its id against another name" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    handler.notePending("pending-one");
    try testing.expect(handler.idTaken("other", router.namespaceHash("pending-one")));
    try testing.expect(!handler.idTaken("pending-one", router.namespaceHash("pending-one")));
}

test "namespace handler: a name whose id another namespace holds is refused, by the applier and at admission" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    // Find two valid names with one 32-bit id.
    var seen = std.AutoHashMap(u32, u32).init(testing.allocator);
    defer seen.deinit();
    var a: [16]u8 = undefined;
    var b: [16]u8 = undefined;
    const pair = for (0..1_000_000) |i| {
        const name = try std.fmt.bufPrint(&a, "c{d}", .{i});
        const got = try seen.getOrPut(router.namespaceHash(name));
        if (got.found_existing) break .{ name, try std.fmt.bufPrint(&b, "c{d}", .{got.value_ptr.*}) };
        got.value_ptr.* = @intCast(i);
    } else return error.NoCollisionFound;
    try testing.expectEqual(NamespaceHandler.CreateOutcome.created, handler.applyCreate(pair[1]));
    try testing.expect(!handler.admits(pair[0]));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.collision, handler.applyCreate(pair[0]));
    try testing.expectEqualStrings(pair[1], handler.nameForHash(router.namespaceHash(pair[0])).?);
}

test "namespace handler: freeResult non-allocated is no-op" {
    const allocator = testing.allocator;
    var handler = try NamespaceHandler.init(allocator);
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
    try testing.expectEqual(@as(usize, 0), (try namespacePrefix(&buf, "")).len);
    try testing.expectEqual(@as(usize, 0), (try namespacePrefix(&buf, "default")).len);
}

test "namespacePrefix: non-default returns ns plus separator" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    const prefix = try namespacePrefix(&buf, "prod");
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
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
    _ = handler.applyCreate("test-ns");
    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
    try testing.expect(handler.namespaces.contains("test-ns"));
}

test "namespace handler: a namespace created through the log was created at its entry's stamp" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    var buf: [128]u8 = undefined;
    const e = entry_mod.buildCommandEntry(.namespace_create, 0, 1, 1, 1234 * std.time.ns_per_s, 0, "stamped", "", &buf) orelse unreachable;
    handler.replayEntry(&e);
    try testing.expectEqual(@as(u64, 1234 * std.time.ns_per_s), handler.namespaces.get("stamped").?.created_at_ns);
}

test "namespace handler: applyCreate is idempotent" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    _ = handler.applyCreate("test-ns");
    _ = handler.applyCreate("test-ns"); // duplicate — should be no-op
    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
}

test "namespace handler: applyDelete removes from local registry" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    _ = handler.applyCreate("test-ns");
    try testing.expect(handler.namespaces.contains("test-ns"));

    handler.applyDelete("test-ns");
    try testing.expect(!handler.namespaces.contains("test-ns"));
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: applyDelete non-existent is no-op" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    handler.applyDelete("does-not-exist"); // should not crash
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace: a committed entry's hash resolves to the name until it is deleted" {
    var handler = try NamespaceHandler.init(std.testing.allocator);
    defer handler.deinit();

    const hash = router.namespaceHash("prod");
    try std.testing.expect(handler.nameForHash(hash) == null);

    _ = handler.applyCreate("prod");
    try std.testing.expectEqualStrings("prod", handler.nameForHash(hash).?);
    // Re-applying (a replay) neither duplicates nor loses the mapping.
    _ = handler.applyCreate("prod");
    try std.testing.expectEqualStrings("prod", handler.nameForHash(hash).?);

    handler.applyDelete("prod");
    try std.testing.expect(handler.nameForHash(hash) == null);
    try std.testing.expect(handler.nameForHash(router.namespaceHash("never")) == null);
}

test "namespace handler: a key holding a NUL is never qualified" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expectError(error.KeyHasNul, qualifyKey(&buf, "", "b\x00k"));
    try testing.expectError(error.KeyHasNul, qualifyKey(&buf, "a", "b\x00k"));
    try testing.expectEqualStrings("key must not contain NUL", validateKeySize("default", "b\x00x").?);
}
