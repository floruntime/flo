//! Namespace Handler — the namespace registry and its ops.
//!
//! The namespace ops are on the op table (protocol/namespace_ops.zig) and
//! route to shard 0. The namespace an op acts on is its key, not the
//! envelope's namespace. A name with a leading `_` is the system's and is
//! never created.
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
const stdx = @import("stdx");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const schema = @import("../protocol/schema.zig");
const namespace_ops = @import("../protocol/namespace_ops.zig");
const namespace_entries = @import("../protocol/namespace_entries.zig");
const table_run = @import("../node/table_run.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
const router = @import("../node/router.zig");
const ns_table = @import("table.zig");
const ReplyTo = @import("../node/reply_to.zig").ReplyTo;

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
pub const MAX_NAME_LEN: usize = @import("../protocol/limits.zig").MAX_NAMESPACE_NAME;

/// Why `name` cannot name a namespace, or null if it can: 1–63 of
/// `[a-z0-9_-]`, starting with a letter or digit and not ending in `-`; a
/// leading `_` is the system's. Lowercase only, so no two names differ by
/// case alone. No `:` or NUL: workflow runs and definitions are keyed
/// "namespace:name" and read back to the first `:`, and keys are
/// "namespace\x00key". The applier checks it too, so it is part of the
/// replicated state: loosening it later is safe, tightening it is not.
pub fn nameRefusal(name: []const u8) ?[]const u8 {
    if (name.len == 0) return "namespace name is required";
    if (name.len > MAX_NAME_LEN) return std.fmt.comptimePrint("namespace name too long (at most {d} bytes)", .{MAX_NAME_LEN});
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
pub const MAX_QUALIFIED_KEY: usize = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

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
            return std.fmt.comptimePrint("key too large (max {d} bytes)", .{MAX_QUALIFIED_KEY});
    }
    return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// NamespaceHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const NamespaceHandler = struct {
    allocator: Allocator,
    /// The node's namespace table, which shard 0 publishes and every shard
    /// reads.
    table: *ns_table.Table,
    owns_table: bool,
    /// Shard 0: the namespaces' one owner, where every create is logged.
    owner: bool,
    /// Names whose create (shard 0) or bind (any other shard) this leader
    /// proposed and has not yet applied: a burst of first writes proposes
    /// one. Cleared when this node stops leading: an entry the log then
    /// drops is proposed again.
    pending_creates: std.StringHashMapUnmanaged(void) = .{},
    /// Shard 0: the next id a create may take, as applied on every replica.
    next_id: ns_table.Id = ns_table.FIRST_ALLOCATED,
    /// Shard 0's leader: the next id it proposes, past the creates it has
    /// in flight, so two creates never take one id.
    next_alloc: ns_table.Id = 0,
    /// Shard 0: creates applied and not yet on disk, published at the next
    /// flush.
    unpublished: std.ArrayListUnmanaged(Unpublished) = .empty,
    /// Shard 0: requests waiting for a name's create to be published.
    waiters: std.ArrayListUnmanaged(Waiter) = .empty,
    /// Shard 0: creates the applier refused since the last settle, for
    /// their waiters.
    refused_creates: std.ArrayListUnmanaged(RefusedCreate) = .empty,
    flush_warn_ms: i64 = 0,
    implicit_failed: u64 = 0,
    implicit_warn_ms: i64 = 0,

    /// The namespaces this shard knows. Keys are owned copies of their names.
    namespaces: std.StringHashMap(NamespaceMeta),
    /// Namespace hash → name, for the appliers that see only the hash on
    /// every committed entry. Kept in step with `namespaces`.
    names_by_hash: std.AutoHashMap(u32, []const u8),
    /// Shard 0: hashes of names whose create this log refused for its id
    /// or the room, and that were never created after. A write proposed
    /// behind such a create is refused at apply, the same on every replica.
    refused_hashes: std.AutoHashMapUnmanaged(u32, void) = .empty,
    /// A page of names, built for each namespace.list answer.
    list_buf: ?[]u8 = null,

    /// Live namespaces, "default" among them.
    const MAX_NAMESPACES: usize = ns_table.MAX_LIVE;

    pub const CreateOutcome = enum { created, existed, invalid, collision, full, stale_id, failed };

    pub const LIMIT_MESSAGE = std.fmt.comptimePrint("namespace limit reached ({d})", .{MAX_NAMESPACES});
    /// A create whose id another took first, after a leader change: its
    /// leader proposes it again with a fresh id.
    pub const STALE_ID_MESSAGE = "unavailable: the namespace's id was taken by another create; retry";
    /// How long a request waits for its namespace's create to reach disk:
    /// a write held on another shard asks through a gather part, whose
    /// deadline this is, so a create and a held write are told at once,
    /// and both under the SDKs' 5 s request timeout.
    pub const PUBLISH_WAIT_MS: u64 = @import("../node/reply_pool.zig").DEADLINE_MS;
    /// A sink's write to a namespace this shard can't create, and the node
    /// doesn't know yet.
    pub const UNKNOWN_HERE_MESSAGE = "unavailable: the namespace isn't created yet; create it first (flo namespace create)";
    /// Every id is taken: ids aren't reused, and a create refused or lost
    /// to a leader change spends one, so this can come below the limit.
    pub const IDS_USED_UP_MESSAGE = "namespace not created: this node has no namespace id left";
    /// Shard 0's leader hasn't applied what earlier leaders committed, so
    /// it can't yet choose an id or count the room.
    pub const CATCHING_UP_MESSAGE = "unavailable: a new leader is catching up; the namespace wasn't created — retry";
    pub const FLUSH_MESSAGE = "unavailable: the namespace isn't on disk yet; it may still be created — retry";

    /// `name` is the registry's copy.
    const Unpublished = struct { index: u64, id: ns_table.Id, name: []const u8 };
    /// How a waiter is answered once its name's create settles: `create`
    /// made it (ok); `joined` asked to create a name another request was
    /// already creating (conflict once it exists, or that create's refusal);
    /// `ensure` wants the id.
    pub const WaiterKind = enum { create, joined, ensure };
    const Waiter = struct {
        /// Owned.
        name: []const u8,
        kind: WaiterKind,
        reply_to: ReplyTo,
        request_id: u64,
        deadline_ms: u64,
    };
    const RefusedCreate = struct { name: []const u8, status: proto.StatusCode, message: []const u8 };
    pub const COLLISION_MESSAGE = "namespace name collides with an existing namespace's id; choose another name";

    /// Why a request may not create or write to a namespace.
    pub const Refusal = struct { status: @import("../protocol/proto.zig").StatusCode, message: []const u8 };

    pub const NamespaceMeta = struct {
        /// Its dense id, the same on every shard.
        id: ns_table.Id,
        created_at_ns: u64,
        /// The log index of the entry that made it known here; 0 for
        /// "default", which is known before any entry.
        created_index: u64 = 0,
    };

    /// A handler that owns the namespaces and a table of its own: shard 0
    /// of a node built in a test.
    pub fn init(allocator: Allocator) !NamespaceHandler {
        return initFor(allocator, 0, null);
    }

    /// Shard `shard_id`'s handler, reading `table` (the node's), or a table
    /// of its own when there is none. "default" is registered from the
    /// start, before replay, with no log entry: its id is a constant.
    pub fn initFor(allocator: Allocator, shard_id: u16, table: ?*ns_table.Table) !NamespaceHandler {
        const t = table orelse try ns_table.Table.create(allocator);
        errdefer if (table == null) t.destroy();
        var self: NamespaceHandler = .{
            .allocator = allocator,
            .table = t,
            .owns_table = table == null,
            .owner = shard_id == 0,
            .namespaces = std.StringHashMap(NamespaceMeta).init(allocator),
            .names_by_hash = std.AutoHashMap(u32, []const u8).init(allocator),
        };
        errdefer self.deinit();
        const owned = try allocator.dupe(u8, "default");
        self.insertNamespace(owned, .{ .id = ns_table.DEFAULT, .created_at_ns = 0 }) catch |err| {
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
        self.refused_hashes.deinit(self.allocator);
        if (self.list_buf) |b| self.allocator.free(b);
        self.unpublished.deinit(self.allocator);
        for (self.waiters.items) |w| self.freeWaiter(w);
        self.waiters.deinit(self.allocator);
        for (self.refused_creates.items) |r| self.allocator.free(r.name);
        self.refused_creates.deinit(self.allocator);
        if (self.owns_table) self.table.destroy();
    }

    fn freeWaiter(self: *NamespaceHandler, w: Waiter) void {
        self.allocator.free(w.name);
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

    // ── Implicit creation ───────────────────────────────────────────────

    /// A write named `name` (empty is "default"), from a handler: the edge
    /// already made the namespace known here (`prepareWrite`), so this only
    /// covers a path that skipped it. Without a shard (tests), it is
    /// created directly.
    pub fn markNamespaceHasData(self: *NamespaceHandler, name: []const u8, shard: ?*Shard) void {
        if (shard) |s| return self.proposeImplicitCreate(name, s);
        const effective = if (name.len == 0) "default" else name;
        if (!self.namespaces.contains(effective)) _ = self.applyCreate(effective);
    }

    /// A namespace a handler is about to write to, made known here if it
    /// can be without waiting: created on shard 0, bound elsewhere if the
    /// node's table has it. A name only shard 0 can create, from a path
    /// that can't wait for it (a pipeline's sink), stays unknown, and the
    /// caller's `admission` check drops the write.
    pub fn proposeImplicitCreate(self: *NamespaceHandler, name: []const u8, s: *Shard) void {
        switch (self.prepareWrite(s, name)) {
            .go, .ensure => {},
            .refused => |r| {
                // Every write to the namespace tries again, so said once per
                // interval rather than per write.
                self.implicit_failed += 1;
                const now = @import("stdx").time.milliTimestamp();
                if (now - self.implicit_warn_ms >= 30_000) {
                    self.implicit_warn_ms = now;
                    log.warn("namespace: '{s}' not made known on shard {d}: {s} ({d} failed so far); each write to it tries again", .{ name, s.id, r.message, self.implicit_failed });
                }
            },
        }
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
        // Only shard 0 knows every namespace, so only it decides the id and
        // the room; another shard learns of a name it publishes.
        if (!self.owner) return if (self.table.lookup(effective) != null) null else .{ .status = .unavailable, .message = UNKNOWN_HERE_MESSAGE };
        if (self.collides(effective)) return .{ .status = .bad_request, .message = COLLISION_MESSAGE };
        if (self.namespaces.count() + self.pending_creates.count() >= MAX_NAMESPACES)
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
    pub fn forgetImplicitCreates(self: *NamespaceHandler, shard: *Shard) void {
        self.next_alloc = 0;
        // What waited for a create this node proposed may never see it.
        while (self.waiters.items.len > 0) {
            const name = self.allocator.dupe(u8, self.waiters.items[0].name) catch break;
            defer self.allocator.free(name);
            self.answerWaiters(shard, name, .stopped_leading);
        }
        var it = self.pending_creates.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.pending_creates.clearRetainingCapacity();
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

    /// Whether this log refused the create of the name with this hash.
    pub fn createRefused(self: *const NamespaceHandler, hash: u32) bool {
        return self.refused_hashes.contains(hash);
    }

    /// Apply a committed namespace creation to the local registry. The
    /// name, its id and the cap are checked again here, the same on every
    /// replica, whatever proposed it.
    /// For tests: create `name` with the next id, as a committed entry
    /// would, and publish it at once.
    pub fn applyCreate(self: *NamespaceHandler, name: []const u8) CreateOutcome {
        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000);
        const outcome = self.applyCreateAt(name, self.next_id, now_ns, 0);
        if (outcome == .created) self.publishUnpublished();
        return outcome;
    }

    /// A committed create of `name` as `id`, stamped `created_at_ns`, at
    /// log index `index`. Checked again here, the same on every replica,
    /// whatever proposed it: the name, the id, the room. The id must be at
    /// least the next free one, so replicas agree whatever their leader
    /// thought; one below it lost a race after a leader change. A created
    /// namespace is published once it is on disk (`settle`).
    pub fn applyCreateAt(self: *NamespaceHandler, name: []const u8, id: ns_table.Id, created_at_ns: u64, index: u64) CreateOutcome {
        if (self.namespaces.contains(name)) return .existed;
        if (nameRefusal(name) != null) return .invalid;
        if (self.collides(name)) return .collision;
        if (self.namespaces.count() >= MAX_NAMESPACES) return .full;
        if (id < self.next_id or id >= ns_table.CAPACITY) return .stale_id;
        const owned = self.allocator.dupe(u8, name) catch return .failed;
        self.insertNamespace(owned, .{ .id = id, .created_at_ns = created_at_ns, .created_index = index }) catch {
            self.allocator.free(owned);
            return .failed;
        };
        // The registry holds it now: if the queue can't, the create is
        // refused on every replica alike only if memory runs out on all,
        // so it is a local failure.
        self.unpublished.append(self.allocator, .{ .index = index, .id = id, .name = owned }) catch {
            self.allocator.free(self.removeNamespace(name).?);
            return .failed;
        };
        self.next_id = id + 1;
        return .created;
    }

    /// Publish every create applied so far, without a flush: for tests and
    /// a shard with nothing on disk.
    fn publishUnpublished(self: *NamespaceHandler) void {
        for (self.unpublished.items) |u| {
            self.table.publish(u.id, u.name, u.index) catch {};
        }
        self.unpublished.clearRetainingCapacity();
    }

    // ── Ops ─────────────────────────────────────────────────────────────
    // Every namespace op routes to shard 0, the namespaces' one owner.

    const ListResponse = namespace_ops.op("namespace.list").response;
    const InfoResponse = namespace_ops.op("namespace.info").response;
    const EnsureResponse = namespace_ops.op("namespace.ensure").response;
    const Empty = namespace_ops.op("namespace.create").response;

    pub fn register(dispatcher: *Dispatcher) void {
        inline for (.{
            .{ "namespace.create", OpCode.namespace_create, runCreate },
            .{ "namespace.delete", OpCode.namespace_delete, runDelete },
            .{ "namespace.list", OpCode.namespace_list, runList },
            .{ "namespace.info", OpCode.namespace_info, runInfo },
            .{ "namespace.ensure", OpCode.namespace_ensure, runEnsure },
        }) |o| {
            comptime std.debug.assert(namespace_ops.op(o[0]).route == .shard0);
            dispatcher.register(o[1], table_run.handler(o[0], NamespaceHandler, o[2]));
            dispatcher.setShardRoute(o[1], toShard0);
        }
    }

    fn toShard0(_: Request) ?u16 {
        return 0;
    }

    pub const Error = error{ OutOfMemory, Exists, NoNamespace, DeleteRefused, LeaderUnknown, WritesStopped, TooManyProposals, IdsUsedUp, Internal };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.OutOfMemory => refused(.internal_error, .internal, "internal error: out of memory"),
            error.Exists => refused(.conflict, .unclassified, "namespace already exists"),
            error.NoNamespace => refused(.no_namespace, .not_found, "namespace not found"),
            error.DeleteRefused => refused(.bad_request, .unclassified, DELETE_REFUSAL),
            error.LeaderUnknown => refused(.unavailable, .leader_unknown, persistence_mod.failureMessage(error.NotLeader, "")),
            error.WritesStopped => refused(.unavailable, .writes_stopped, persistence_mod.failureMessage(error.WritesStopped, "")),
            error.TooManyProposals => refused(.overloaded, .too_many_proposals, persistence_mod.failureMessage(error.Overloaded, "")),
            error.IdsUsedUp => refused(.bad_request, .unclassified, IDS_USED_UP_MESSAGE),
            error.Internal => refused(.internal_error, .internal, "internal error: the request couldn't be run"),
        };
    }

    fn refused(status: proto.StatusCode, reason: proto.Reason, message: []const u8) table_run.Refused {
        return .{ .status = status, .refusal = .{ .reason = reason, .ran = .no, .message = message } };
    }

    fn proposeError(err: anyerror) Error {
        return switch (err) {
            error.NotLeader => error.LeaderUnknown,
            error.WritesStopped => error.WritesStopped,
            error.Overloaded => error.TooManyProposals,
            error.OutOfMemory => error.OutOfMemory,
            error.NamespacesFull => error.IdsUsedUp,
            else => error.Internal,
        };
    }

    /// Refuses `name` with its grammar's own reason, or with the room or
    /// id it would need; null when it may be created.
    fn createRefusal(self: *const NamespaceHandler, name: []const u8) ?Refusal {
        if (nameRefusal(name)) |why| return .{ .status = .bad_request, .message = why };
        return self.admission(name);
    }

    /// namespace.create: answered once the create is on disk and
    /// published, so the namespace is usable on every shard by then.
    fn runCreate(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(Empty) {
        return shard.namespace_handler.createOrWait(shard, conn, req, .create);
    }

    /// namespace.ensure: create-or-get, answered with the id once it is
    /// published, for a shard about to bind the name.
    fn runEnsure(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(EnsureResponse) {
        const self = shard.namespace_handler;
        if (self.table.lookup(req.key)) |id| return .{ .answer = .{ .id = id, .published_index = self.table.publishedIndex() } };
        return switch (try self.createOrWait(shard, conn, req, .ensure)) {
            .refused => |r| .{ .refused = r },
            else => .wait,
        };
    }

    /// Shard 0: a published name answers now; a known or pending one waits
    /// for its publish; a new one is proposed, then waits. `kind` says how
    /// the waiter is answered.
    fn createOrWait(self: *NamespaceHandler, shard: *Shard, conn: *Connection, req: Request, kind: WaiterKind) Error!table_run.Outcome(Empty) {
        const name = req.key;
        // The grammar first: a reserved name ("_flo") is in the table, but
        // is refused as reserved, not as existing.
        if (nameRefusal(name)) |why| return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun(why) } };
        if (self.table.lookup(name) != null) {
            if (kind == .create) return error.Exists;
            unreachable; // ensure answers a published name itself
        }
        var as = kind;
        if (self.namespaces.contains(name) or self.pending_creates.contains(name)) {
            // Being created by another request: wait for its outcome, so
            // "exists" is never said of a create that then fails.
            if (kind == .create) as = .joined;
        } else {
            if (self.createRefusal(name)) |r| return .{ .refused = .{ .status = r.status, .refusal = proto.notRun(r.message) } };
            if (!shard.raft_node.leaderReady()) return .{ .refused = .{ .status = .unavailable, .refusal = proto.notRun(CATCHING_UP_MESSAGE) } };
            _ = proposeCreate(shard, name) catch |err| return proposeError(err);
            self.notePending(name);
        }
        try self.addWaiter(name, as, conn.replyTo(), req.header.request_id);
        return .wait;
    }

    fn addWaiter(self: *NamespaceHandler, name: []const u8, kind: WaiterKind, reply_to: ReplyTo, request_id: u64) Error!void {
        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);
        try self.waiters.append(self.allocator, .{
            .name = owned,
            .kind = kind,
            .reply_to = reply_to,
            .request_id = request_id,
            .deadline_ms = @import("stdx").time.monotonicMs() + PUBLISH_WAIT_MS,
        });
    }

    /// What a write naming a namespace this shard doesn't know needs first.
    pub const Prepared = union(enum) {
        /// Nothing more: run it.
        go,
        refused: Refusal,
        /// Unknown to the node: shard 0 must create it first (`ensure`).
        ensure,
    };

    /// A write that brings data into `name` (empty is "default"). Shard 0
    /// proposes the create ahead of the write: one log, so the write
    /// applies, and reaches disk, only after it. Another shard binds a name
    /// the node's table has published, ahead of its write in its own log;
    /// a name the table lacks needs shard 0 first.
    pub fn prepareWrite(self: *NamespaceHandler, shard: *Shard, name_or_empty: []const u8) Prepared {
        const name = if (name_or_empty.len == 0) "default" else name_or_empty;
        if (self.namespaces.contains(name) or self.pending_creates.contains(name)) return .go;
        if (self.owner) {
            if (self.createRefusal(name)) |r| return .{ .refused = r };
            if (!shard.raft_node.leaderReady()) return .{ .refused = .{ .status = .unavailable, .message = CATCHING_UP_MESSAGE } };
            _ = proposeCreate(shard, name) catch |err| return .{ .refused = refusalOf(proposeError(err)) };
            self.notePending(name);
            return .go;
        }
        const id = self.table.lookup(name) orelse return .ensure;
        _ = proposeBind(shard, name, id) catch |err| return .{ .refused = refusalOf(proposeError(err)) };
        return .go;
    }

    fn refusalOf(err: Error) Refusal {
        const r = refusal(err);
        return .{ .status = r.status, .message = r.refusal.message };
    }

    /// Shard 0, once a tick: put applied creates on disk and publish them,
    /// then answer or run what waited for them; answer what waited for a
    /// refused create; and answer what has waited too long.
    pub fn settle(self: *NamespaceHandler, shard: *Shard) void {
        if (!self.owner) return;
        for (self.refused_creates.items) |r| {
            defer self.allocator.free(r.name);
            // A create that lost its id to a leader change is proposed
            // again, by the leader, with a fresh one.
            if (r.status == .unavailable and shard.raft_node.leaderReady() and self.hasWaiter(r.name)) {
                if (proposeCreate(shard, r.name)) |_| {
                    self.notePending(r.name);
                    continue;
                } else |_| {}
            }
            self.answerWaiters(shard, r.name, .{ .refused = .{ .status = r.status, .message = r.message } });
        }
        self.refused_creates.clearRetainingCapacity();

        if (self.unpublished.items.len > 0) publish: {
            shard.flushSegmentToDisk() catch |err| {
                const now = @import("stdx").time.milliTimestamp();
                if (now - self.flush_warn_ms >= 30_000) {
                    self.flush_warn_ms = now;
                    log.warn("namespace: {d} created namespaces are not on disk ({s}); they are not published until they are", .{ self.unpublished.items.len, @errorName(err) });
                }
                break :publish;
            };
            for (self.unpublished.items, 0..) |u, i| {
                self.table.publish(u.id, u.name, u.index) catch {
                    // What was published stays published: drop it, and
                    // try the rest next tick.
                    self.unpublished.replaceRangeAssumeCapacity(0, i, &.{});
                    break :publish;
                };
                self.answerWaiters(shard, u.name, .published);
            }
            self.unpublished.clearRetainingCapacity();
        }

        const now = @import("stdx").time.monotonicMs();
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const w = self.waiters.items[i];
            if (now < w.deadline_ms) {
                i += 1;
                continue;
            }
            _ = self.waiters.swapRemove(i);
            defer self.freeWaiter(w);
            shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, .{ .reason = .unclassified, .ran = .unknown, .message = FLUSH_MESSAGE });
        }
    }

    fn hasWaiter(self: *const NamespaceHandler, name: []const u8) bool {
        for (self.waiters.items) |w| if (std.mem.eql(u8, w.name, name)) return true;
        return false;
    }

    const Settled = union(enum) { published, refused: Refusal, stopped_leading };

    /// Answer, or run, every request waiting on `name`.
    fn answerWaiters(self: *NamespaceHandler, shard: *Shard, name: []const u8, how: Settled) void {
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const w = self.waiters.items[i];
            if (!std.mem.eql(u8, w.name, name)) {
                i += 1;
                continue;
            }
            _ = self.waiters.swapRemove(i);
            defer self.freeWaiter(w);
            switch (how) {
                .refused => |r| shard.deliverDeferredRefusal(w.reply_to, w.request_id, r.status, proto.notRun(r.message)),
                .stopped_leading => shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .unknown, .message = "unavailable: lost leadership before the namespace was created; it may still be — retry" }),
                .published => switch (w.kind) {
                    .create => {
                        var buf: [@sizeOf(proto.ResponseHeader) + Empty.max_size]u8 = undefined;
                        shard.deliverDeferred(w.reply_to, table_run.encodeAnswer(Empty, &buf, w.request_id, .{}).?);
                    },
                    .joined => {
                        const r = refusal(error.Exists);
                        shard.deliverDeferredRefusal(w.reply_to, w.request_id, r.status, r.refusal);
                    },
                    .ensure => {
                        var buf: [@sizeOf(proto.ResponseHeader) + EnsureResponse.max_size]u8 = undefined;
                        const v: EnsureResponse.Value = .{ .id = self.table.lookup(name).?, .published_index = self.table.publishedIndex() };
                        shard.deliverDeferred(w.reply_to, table_run.encodeAnswer(EnsureResponse, &buf, w.request_id, v).?);
                    },
                },
            }
        }
    }

    /// A namespace's data can live on any shard, keyed by a name that a
    /// later create could reuse: until it can be removed everywhere and
    /// the name retired, delete is refused.
    pub const DELETE_REFUSAL = "namespace delete isn't supported yet: a namespace's data can't be removed safely across shards";

    fn runDelete(_: *Shard, _: *Connection, _: Request) Error!table_run.Outcome(Empty) {
        return error.DeleteRefused;
    }

    fn runList(shard: *Shard, _: *Connection, _: Request, limit: u32, cursor: []const u8) Error!table_run.Outcome(ListResponse) {
        return .{ .answer = try shard.namespace_handler.page(limit, cursor) };
    }

    /// The names after `cursor`, in name order, as many as `limit` allows.
    /// The cursor is the last name given when more are left.
    pub fn page(self: *NamespaceHandler, limit: u32, cursor: []const u8) Error!ListResponse.Value {
        const names = try self.allocator.alloc([]const u8, self.namespaces.count());
        defer self.allocator.free(names);
        var n: usize = 0;
        var it = self.namespaces.keyIterator();
        while (it.next()) |k| {
            if (std.mem.order(u8, k.*, cursor) != .gt) continue;
            names[n] = k.*;
            n += 1;
        }
        std.mem.sort([]const u8, names[0..n], {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        const Names = ListResponse.field_list[0].T;
        const buf = self.list_buf orelse blk: {
            const b = try self.allocator.alloc(u8, Names.byte_bound.?);
            self.list_buf = b;
            break :blk b;
        };
        var out = Names.Builder.init(buf);
        for (names[0..n]) |name| {
            if (out.count == limit or !(out.add(name) catch unreachable)) break; // a valid name never breaks its schema
        }
        const more = out.count < n;
        return .{ .names = out.value(), .cursor = if (more) names[out.count - 1] else null };
    }

    fn runInfo(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(InfoResponse) {
        const name = req.key;
        if (nameRefusal(name)) |why| return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun(why) } };
        const meta = shard.namespace_handler.namespaces.get(name) orelse return error.NoNamespace;
        return .{ .answer = .{ .created_at_ms = @intCast(meta.created_at_ns / std.time.ns_per_ms) } };
    }

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// Shard 0's leader proposes `name`'s create with the next id: past
    /// every id applied and every create it has in flight. Replicas never
    /// choose an id; they check the one given (`applyCreateAt`).
    fn proposeCreate(shard: *Shard, name: []const u8) !persistence_mod.ProposeResult {
        const self = shard.namespace_handler;
        std.debug.assert(self.owner);
        const id = @max(self.next_id, self.next_alloc);
        if (id >= ns_table.CAPACITY) return error.NamespacesFull;
        const P = namespace_entries.Create;
        var buf: [P.max_size]u8 = undefined;
        const bytes = P.encodeInto(&buf, .{ .name = name, .id = id }) catch return error.PayloadTooLarge;
        const proposed = try shard.raft_node.propose(.namespace_create, entry_mod.Flags.NONE, bytes);
        self.next_alloc = id + 1;
        return proposed;
    }

    /// A shard other than 0 records, in its own log, that `name` is `id`,
    /// ahead of its first write in it. Its leader only, and only for an id
    /// the node's table has published.
    fn proposeBind(shard: *Shard, name: []const u8, id: ns_table.Id) !persistence_mod.ProposeResult {
        const self = shard.namespace_handler;
        std.debug.assert(!self.owner);
        const P = namespace_entries.Bind;
        var buf: [P.max_size]u8 = undefined;
        const bytes = P.encodeInto(&buf, .{ .id = id, .name = name }) catch return error.PayloadTooLarge;
        const proposed = try shard.raft_node.propose(.namespace_bind, entry_mod.Flags.NONE, bytes);
        self.notePending(name);
        return proposed;
    }

    /// Register this handler's entry types with the ReplayRegistry.
    pub fn registerReplay(self: *NamespaceHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.namespace_create, @ptrCast(self), replayEntryThunk);
        registry.register(.namespace_bind, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        const self: *NamespaceHandler = @ptrCast(@alignCast(ctx));
        return self.replayEntry(entry, out);
    }

    /// Apply a namespace entry, at boot or after commit. A create refused
    /// for its name, its id or the cap is refused the same way on every
    /// replica. A bind that disagrees with what this shard holds breaks
    /// the log, on every replica alike, and halts.
    pub fn replayEntry(self: *NamespaceHandler, entry: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        var diag: schema.Diagnostic = .{};
        switch (@as(entry_mod.EntryType, @fromBackingInt(@intCast(entry.header.entry_type)))) {
            .namespace_create => {
                const c = namespace_entries.Create.decodeAll(entry.payload, &diag) catch return error.Halt;
                // Only shard 0 logs creates; one elsewhere was never proposed.
                if (!self.owner) return error.Halt;
                if (self.pending_creates.fetchRemove(c.name)) |kv| self.allocator.free(kv.key);
                const outcome = self.applyCreateAt(c.name, c.id, entry.header.timestamp_ns, entry.header.index);
                switch (outcome) {
                    .created => _ = self.refused_hashes.remove(router.namespaceHash(c.name)),
                    .stale_id, .full => try self.refused_hashes.put(self.allocator, router.namespaceHash(c.name), {}),
                    else => {},
                }
                const refused_create: ?RefusedCreate = switch (outcome) {
                    .created => null,
                    .existed => .{ .name = c.name, .status = .conflict, .message = "namespace already exists" },
                    .invalid => .{ .name = c.name, .status = .bad_request, .message = nameRefusal(c.name) orelse "invalid namespace name" },
                    .collision => .{ .name = c.name, .status = .bad_request, .message = COLLISION_MESSAGE },
                    .full => .{ .name = c.name, .status = .bad_request, .message = LIMIT_MESSAGE },
                    .stale_id => .{ .name = c.name, .status = .unavailable, .message = STALE_ID_MESSAGE },
                    .failed => return error.OutOfMemory,
                };
                if (refused_create) |r| {
                    out.refuse(r.status, .unclassified, r.message);
                    // For the waiters, if any wait here.
                    const owned = self.allocator.dupe(u8, r.name) catch return;
                    self.refused_creates.append(self.allocator, .{ .name = owned, .status = r.status, .message = r.message }) catch self.allocator.free(owned);
                }
            },
            .namespace_bind => {
                const b = namespace_entries.Bind.decodeAll(entry.payload, &diag) catch return error.Halt;
                // Shard 0 knows every namespace from its creates; a bind
                // there was never proposed.
                if (self.owner) return error.Halt;
                if (self.pending_creates.fetchRemove(b.name)) |kv| self.allocator.free(kv.key);
                if (self.namespaces.get(b.name)) |meta| {
                    if (meta.id != b.id) return error.Halt; // one name, two ids
                    return; // bound already: nothing to do
                }
                // Another name on this hash: entries carry the hash, so the
                // two would share data.
                if (self.names_by_hash.get(router.namespaceHash(b.name)) != null) return error.Halt;
                if (b.id < ns_table.FIRST_ALLOCATED or b.id >= ns_table.CAPACITY) return error.Halt;
                if (nameRefusal(b.name) != null) return error.Halt;
                const owned = self.allocator.dupe(u8, b.name) catch return error.OutOfMemory;
                self.insertNamespace(owned, .{ .id = b.id, .created_at_ns = entry.header.timestamp_ns, .created_index = entry.header.index }) catch {
                    self.allocator.free(owned);
                    return error.OutOfMemory;
                };
            },
            else => return error.Halt,
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// A namespace entry at `index`, its payload coded by its schema into `buf`.
fn testEntry(comptime t: entry_mod.EntryType, index: u64, stamp_ns: u64, v: namespace_entries.Payload(@backingInt(t)).Value, buf: []u8) !entry_mod.Entry {
    const bytes = try namespace_entries.Payload(@backingInt(t)).encodeInto(buf, v);
    return entry_mod.buildEntry(t, 0, 1, index, stamp_ns, bytes);
}

test "namespace handler: every namespace op is registered, routed to shard 0, and settings are gone" {
    var dispatcher = Dispatcher.init();
    NamespaceHandler.register(&dispatcher);
    const req: Request = .{ .header = proto.RequestHeader.init(.namespace_list, 1), .namespace = "", .key = "", .value = "", .options = "" };
    for ([_]OpCode{ .namespace_create, .namespace_delete, .namespace_list, .namespace_info, .namespace_ensure }) |op| {
        try testing.expect(dispatcher.hasHandler(op));
        try testing.expectEqual(@as(?u16, 0), dispatcher.shard_route[@backingInt(op)].?(req));
    }
    try testing.expectEqual(@as(u16, 5), dispatcher.handler_count);
    for ([_]u16{ 0x014, 0x015 }) |retired| try testing.expect(std.enums.tagName(OpCode, @fromBackingInt(retired)) == null);
}

test "namespace handler: list pages in name order, holding default from the start" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    const only = try handler.page(namespace_ops.MAX_LIST, "");
    try testing.expectEqual(@as(u32, 1), only.names.count);
    try testing.expect(only.cursor == null);

    for ([_][]const u8{ "gamma", "alpha", "beta" }) |n| _ = handler.applyCreate(n);
    // limit 2: alpha, beta, then a cursor; from it, default and gamma.
    const first = try handler.page(2, "");
    var it = first.names.iterator();
    try testing.expectEqualStrings("alpha", (try it.next()).?);
    try testing.expectEqualStrings("beta", (try it.next()).?);
    try testing.expectEqualStrings("beta", first.cursor.?);
    const rest = try handler.page(2, first.cursor.?);
    var it2 = rest.names.iterator();
    try testing.expectEqualStrings("default", (try it2.next()).?);
    try testing.expectEqualStrings("gamma", (try it2.next()).?);
    try testing.expect(rest.cursor == null);
    // A cursor past the last name: an empty last page.
    const past = try handler.page(2, "zzz");
    try testing.expectEqual(@as(u32, 0), past.names.count);
    try testing.expect(past.cursor == null);
}

test "namespace handler: a create takes the next dense id, and one below it is refused as stale" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    var buf: [namespace_entries.Create.max_size]u8 = undefined;
    const first = ns_table.FIRST_ALLOCATED;
    const e = try testEntry(.namespace_create, 7, 1234 * std.time.ns_per_s, .{ .name = "stamped", .id = first }, &buf);
    var out = Result.start(&e);
    try handler.replayEntry(&e, &out);
    try testing.expect(out.refused() == null);
    const meta = handler.namespaces.get("stamped").?;
    try testing.expectEqual(first, meta.id);
    try testing.expectEqual(@as(u64, 1234 * std.time.ns_per_s), meta.created_at_ns);
    try testing.expectEqual(@as(u64, 7), meta.created_index);
    try testing.expectEqual(first + 1, handler.next_id);
    // Applied, not yet published: that waits for the flush.
    try testing.expect(handler.table.lookup("stamped") == null);
    // A second create of the name is refused where it applies.
    var again = Result.start(&e);
    try handler.replayEntry(&e, &again);
    try testing.expectEqual(proto.StatusCode.conflict, again.refused().?.status);
    // An id another create took first: refused on every replica alike, and
    // its leader proposes it again with a fresh one.
    const stale = try testEntry(.namespace_create, 8, 0, .{ .name = "late", .id = first }, &buf);
    var st = Result.start(&stale);
    try handler.replayEntry(&stale, &st);
    try testing.expectEqual(proto.StatusCode.unavailable, st.refused().?.status);
    try testing.expect(!handler.namespaces.contains("late"));
    // A reserved id is never allocated.
    const reserved = try testEntry(.namespace_create, 9, 0, .{ .name = "sneaky", .id = ns_table.SYSTEM }, &buf);
    var rs = Result.start(&reserved);
    try handler.replayEntry(&reserved, &rs);
    try testing.expect(rs.refused() != null);
    // Only shard 0 logs creates.
    var other = try NamespaceHandler.initFor(testing.allocator, 1, null);
    defer other.deinit();
    const elsewhere = try testEntry(.namespace_create, 10, 0, .{ .name = "x", .id = first }, &buf);
    var el = Result.start(&elsewhere);
    try testing.expectError(error.Halt, other.replayEntry(&elsewhere, &el));
}

test "namespace handler: a bind registers a name once on a shard other than 0, and one that disagrees halts" {
    var handler = try NamespaceHandler.initFor(testing.allocator, 3, null);
    defer handler.deinit();
    var buf: [namespace_entries.Bind.max_size]u8 = undefined;
    const id = ns_table.FIRST_ALLOCATED + 4;
    const bind = try testEntry(.namespace_bind, 3, 0, .{ .id = id, .name = "orders" }, &buf);
    var out = Result.start(&bind);
    try handler.replayEntry(&bind, &out);
    try testing.expectEqualStrings("orders", handler.nameForHash(router.namespaceHash("orders")).?);
    try testing.expectEqual(id, handler.namespaces.get("orders").?.id);
    // The same bind again changes nothing.
    var again = Result.start(&bind);
    try handler.replayEntry(&bind, &again);
    try testing.expect(again.refused() == null);
    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
    // One name, another id: the log contradicts itself.
    var buf2: [namespace_entries.Bind.max_size]u8 = undefined;
    const other_id = try testEntry(.namespace_bind, 4, 0, .{ .id = id + 1, .name = "orders" }, &buf2);
    var w = Result.start(&other_id);
    try testing.expectError(error.Halt, handler.replayEntry(&other_id, &w));
    try testing.expectEqual(id, handler.namespaces.get("orders").?.id);
    // A reserved id is never bound.
    const reserved = try testEntry(.namespace_bind, 5, 0, .{ .id = ns_table.SYSTEM, .name = "x" }, &buf2);
    var r = Result.start(&reserved);
    try testing.expectError(error.Halt, handler.replayEntry(&reserved, &r));
    // Shard 0 knows its namespaces from their creates; a bind there halts.
    var owner = try NamespaceHandler.init(testing.allocator);
    defer owner.deinit();
    var o = Result.start(&bind);
    try testing.expectError(error.Halt, owner.replayEntry(&bind, &o));
}

test "namespace handler: a bind whose name's hash another name holds halts" {
    var handler = try NamespaceHandler.initFor(testing.allocator, 1, null);
    defer handler.deinit();
    // Two valid names with one 32-bit hash: entries carry the hash, so the
    // second would share the first's data.
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
    var buf: [namespace_entries.Bind.max_size]u8 = undefined;
    const first = try testEntry(.namespace_bind, 1, 0, .{ .id = ns_table.FIRST_ALLOCATED, .name = pair[1] }, &buf);
    var out = Result.start(&first);
    try handler.replayEntry(&first, &out);
    var buf2: [namespace_entries.Bind.max_size]u8 = undefined;
    const second = try testEntry(.namespace_bind, 2, 0, .{ .id = ns_table.FIRST_ALLOCATED + 1, .name = pair[0] }, &buf2);
    var out2 = Result.start(&second);
    try testing.expectError(error.Halt, handler.replayEntry(&second, &out2));
    try testing.expectEqualStrings(pair[1], handler.nameForHash(router.namespaceHash(pair[0])).?);
}

test "namespace handler: a write to an unknown namespace registers it" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    handler.markNamespaceHasData("ghost", null);
    try testing.expect(handler.namespaces.contains("ghost"));
    handler.markNamespaceHasData("", null);
    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
}

test "namespace handler: the name grammar, with a reason for each refusal" {
    for ([_][]const u8{ "production", "test-env", "stage_2", "0starts-with-digit", "a", "9", "default", stdx.repeat("a", 63) }) |ok| {
        if (nameRefusal(ok)) |why| {
            std.debug.print("{s}: {s}\n", .{ ok, why });
            return error.TestUnexpectedResult;
        }
    }
    const Case = struct { name: []const u8, starts: []const u8 };
    for ([_]Case{
        .{ .name = "", .starts = "namespace name is required" },
        .{ .name = stdx.repeat("a", 64), .starts = "namespace name too long" },
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
    try testing.expectEqual(NamespaceHandler.CreateOutcome.invalid, handler.applyCreate(stdx.repeat("a", 300)));
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
    const big_key = &@as([(MAX_QUALIFIED_KEY)]u8, @splat('x')); // exactly fills buffer
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
    const big = &@as([MAX_QUALIFIED_KEY]u8, @splat('x'));
    try testing.expect(validateKeySize("ns", big) != null);
}

test "validateKeySize: oversized key without namespace" {
    const big = &@as([(MAX_QUALIFIED_KEY + 1)]u8, @splat('x'));
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

test "namespace handler: applyCreate is idempotent" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    _ = handler.applyCreate("test-ns");
    _ = handler.applyCreate("test-ns"); // duplicate — should be no-op
    try testing.expectEqual(@as(usize, 2), handler.namespaces.count());
}

test "namespace: a committed entry's hash resolves to the name" {
    var handler = try NamespaceHandler.init(std.testing.allocator);
    defer handler.deinit();

    const hash = router.namespaceHash("prod");
    try std.testing.expect(handler.nameForHash(hash) == null);

    _ = handler.applyCreate("prod");
    try std.testing.expectEqualStrings("prod", handler.nameForHash(hash).?);
    // Re-applying (a replay) neither duplicates nor loses the mapping.
    _ = handler.applyCreate("prod");
    try std.testing.expectEqualStrings("prod", handler.nameForHash(hash).?);
    try std.testing.expect(handler.nameForHash(router.namespaceHash("never")) == null);
}

test "namespace handler: a key holding a NUL is never qualified" {
    var buf: [MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expectError(error.KeyHasNul, qualifyKey(&buf, "", "b\x00k"));
    try testing.expectError(error.KeyHasNul, qualifyKey(&buf, "a", "b\x00k"));
    try testing.expectEqualStrings("key must not contain NUL", validateKeySize("default", "b\x00x").?);
}
