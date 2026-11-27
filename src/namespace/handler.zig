//! Namespace Handler — the namespace registry and its ops.
//!
//! The namespace ops are on the op table (protocol/namespace_ops.zig) and
//! route to shard 0. The namespace an op acts on is its key, not the
//! envelope's namespace. A name with a leading `_` is the system's and is
//! never created.

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
const RaftNode = @import("../raft/node.zig").RaftNode;
const ns_machine = @import("machine.zig");
const contract_machine = @import("../contract/machine.zig");
const Store = @import("../contract/store.zig").Store;
const ReadView = @import("../contract/view.zig").ReadView;
const gather_mod = @import("../node/gather.zig");
const settings_machine = @import("../settings/machine.zig");
const hq = @import("../budget/history_quota.zig");
const hm = @import("../stream/history_machine.zig");

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// Names and limits
// ═══════════════════════════════════════════════════════════════════════════════

/// The longest namespace name.
pub const MAX_NAME_LEN: usize = ns_machine.MAX_NAME_LEN;

pub const nameRefusal = ns_machine.nameRefusal;

/// Longest key, or stream or queue name: a namespace's name doesn't count
/// against it, since those are keyed by the namespace's id. Keys beyond
/// 4 KiB bloat log entries and stack frames.
pub const MAX_QUALIFIED_KEY: usize = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

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
    /// Shard 0: names whose create this leader proposed and has not yet
    /// applied: a burst of first writes proposes one. Cleared when this
    /// node stops leading: a create the log then drops is proposed again.
    pending_creates: std.StringHashMapUnmanaged(ns_table.Id) = .{},
    /// The shard's contract state, where shard 0's namespaces machine keeps
    /// every create (`namespace/machine.zig`); another shard's holds none.
    store: *Store,
    /// A store of its own, for a handler built without a shard (tests).
    owned_store: ?*Store = null,
    /// What a create applied here directly (`applyCommitted`) stages in.
    scratch: std.heap.ArenaAllocator,
    /// Shard 0's leader: the next id it proposes, past the creates it has
    /// in flight, so two creates never take one id.
    next_alloc: u32 = 0,
    /// Shard 0: past every id another shard of this node has applied an
    /// entry under, set at boot before any shard runs. Data there is keyed
    /// by id, so if shard 0's log lost the tail that created one, a create
    /// reusing it would read that shard's old data as its own. Only what
    /// this leader proposes rises past it: replicas check ids against
    /// `nextId` alone, which every replica computes alike.
    id_floor: u32 = 0,
    /// Shard 0: ids past its highest create at boot that another shard of
    /// this node had applied entries under, `(unknown_above, unknown_top]`.
    /// Its log lost those creates (a damaged or salvaged tail), or on a
    /// follower hasn't caught up to them yet: the data under one the table
    /// never publishes is out of reach. Set at boot with `id_floor`.
    unknown_above: u32 = 0,
    unknown_top: u32 = 0,
    /// How many of them the table lacked when last reported.
    unknown_reported: u32 = 0,
    /// How many times they were logged as an error (each time they rose),
    /// and at info as all known again.
    unknown_logged: u32 = 0,
    unknown_cleared: u32 = 0,
    /// Shard 0: creates applied and not yet on disk, published at the next
    /// flush.
    unpublished: std.ArrayListUnmanaged(Unpublished) = .empty,
    /// Shard 0: an alter applied whose quota isn't on disk yet, so not
    /// published: every setting is published again at the next flush.
    quotas_unpublished: bool = false,
    /// Shard 0: the table may lack a create the store holds: one replayed
    /// at boot, which doesn't queue it (`publishAll` at boot clears this),
    /// or one applied that couldn't be queued for want of memory. The next
    /// settle publishes from the store.
    publish_from_store: bool = false,
    /// Shard 0: requests waiting for a name's create to be published.
    waiters: std.ArrayListUnmanaged(Waiter) = .empty,
    flush_warn_ms: i64 = 0,
    implicit_failed: u64 = 0,
    implicit_warn_ms: i64 = 0,
    /// A page of names, built for each namespace.list answer.
    list_buf: ?[]u8 = null,

    /// Live namespaces, "default" among them.
    pub const MAX_NAMESPACES: usize = ns_table.MAX_LIVE;

    pub const CreateOutcome = enum { created, existed, invalid, full, stale_id, failed };

    pub const LIMIT_MESSAGE = std.fmt.comptimePrint("namespace limit reached ({d})", .{MAX_NAMESPACES});
    /// How long a request waits for its namespace's create to reach disk:
    /// a write held on another shard asks through a gather part, whose
    /// deadline this is, so a create and a held write are told at once,
    /// and both under the SDKs' 5 s request timeout.
    pub const PUBLISH_WAIT_MS: u64 = @import("../node/reply_pool.zig").DEADLINE_MS;
    /// A write to a namespace this shard can't create, and the node
    /// doesn't know yet.
    pub const UNKNOWN_HERE_MESSAGE = "unavailable: the namespace isn't created yet; create it first (flo namespace create)";
    /// Every id is taken: ids aren't reused, and a create refused or lost
    /// to a leader change spends one, so this can come below the limit.
    pub const IDS_USED_UP_MESSAGE = "namespace not created: this node has no namespace id left";
    /// Shard 0's leader hasn't applied what earlier leaders committed, so
    /// it can't yet choose an id or count the room.
    pub const PUBLISHED_NOT_READY = "unavailable: this node isn't a leader that has caught up, so it can't say which namespaces exist; the read was not run — retry";
    /// A read waiting on a create that didn't end in one: it ran nothing.
    const READ_UNSETTLED = "unavailable: the namespace's create didn't settle in time; the read was not run — retry";
    pub const CATCHING_UP_MESSAGE = "unavailable: a new leader is catching up; the namespace wasn't created — retry";
    pub const FLUSH_MESSAGE = "unavailable: the namespace isn't on disk yet; it may still be created — retry";

    /// `name` is owned.
    const Unpublished = struct { index: u64, id: ns_table.Id, name: []const u8 };
    /// How a waiter is answered once its name's create settles: `create`
    /// made it (ok); `joined` asked to create a name another request was
    /// already creating (conflict once it exists, or that create's refusal);
    /// `ensure` wants the id.
    pub const WaiterKind = enum { create, joined, ensure, published };
    const Waiter = struct {
        /// Owned.
        name: []const u8,
        kind: WaiterKind,
        reply_to: ReplyTo,
        request_id: u64,
        deadline_ms: u64,
    };

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
        return initFor(allocator, 0, null, null);
    }

    /// Shard `shard_id`'s handler, reading `table` (the node's) and `store`
    /// (the shard's), or ones of its own when there are none. "default" is
    /// known from the start, with no log entry: its id is a constant.
    pub fn initFor(allocator: Allocator, shard_id: u16, table: ?*ns_table.Table, store: ?*Store) !NamespaceHandler {
        const t = table orelse try ns_table.Table.create(allocator);
        errdefer if (table == null) t.destroy();
        const owned_store: ?*Store = if (store == null) blk: {
            const st = try allocator.create(Store);
            st.* = Store.init(allocator);
            st.place = .{ .group = shard_id, .count = shard_id + 1 };
            break :blk st;
        } else null;
        return .{
            .allocator = allocator,
            .table = t,
            .owns_table = table == null,
            .owner = shard_id == 0,
            .publish_from_store = shard_id == 0,
            .store = store orelse owned_store.?,
            .owned_store = owned_store,
            .scratch = .init(allocator),
        };
    }

    pub fn deinit(self: *NamespaceHandler) void {
        var ic = self.pending_creates.keyIterator();
        while (ic.next()) |k| self.allocator.free(k.*);
        self.pending_creates.deinit(self.allocator);
        if (self.list_buf) |b| self.allocator.free(b);
        for (self.unpublished.items) |u| self.allocator.free(u.name);
        self.unpublished.deinit(self.allocator);
        self.scratch.deinit();
        if (self.owned_store) |st| {
            st.deinit();
            self.allocator.destroy(st);
        }
        for (self.waiters.items) |w| self.freeWaiter(w);
        self.waiters.deinit(self.allocator);
        if (self.owns_table) self.table.destroy();
    }

    fn freeWaiter(self: *NamespaceHandler, w: Waiter) void {
        self.allocator.free(w.name);
    }

    // ── Implicit creation ───────────────────────────────────────────────

    /// A write named `name` (empty is "default"), from a handler: the edge
    /// already made the namespace known here (`prepareWrite`), so this only
    /// covers a path that skipped it. Without a shard (tests), it is
    /// created directly.
    pub fn markNamespaceHasData(self: *NamespaceHandler, name: []const u8, shard: ?*Shard) void {
        if (shard) |s| return self.proposeImplicitCreate(name, s);
        const effective = if (name.len == 0) "default" else name;
        if (!self.known(effective)) _ = self.applyCreate(effective);
    }

    /// A namespace a handler is about to write to, created if this is shard
    /// 0. Elsewhere a name the node's table lacks stays unknown, and the
    /// caller's `admission` check refuses the write.
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
        if (self.known(effective) or self.pending_creates.contains(effective)) return null;
        if (nameRefusal(effective)) |why| return .{ .status = .bad_request, .message = why };
        // Only shard 0 knows every namespace, so only it decides the id and
        // the room; another shard learns of a name it publishes.
        if (!self.owner) return if (self.table.lookup(effective) != null) null else .{ .status = .unavailable, .message = UNKNOWN_HERE_MESSAGE };
        if (self.live() + self.pending_creates.count() >= MAX_NAMESPACES)
            return .{ .status = .bad_request, .message = LIMIT_MESSAGE };
        return null;
    }

    pub fn admits(self: *const NamespaceHandler, name: []const u8) bool {
        return self.admission(name) == null;
    }

    const isDefault = ns_machine.isDefault;

    fn read(self: *const NamespaceHandler) ReadView {
        return .{ .store = self.store };
    }

    /// Whether this shard's log created `name`, or it is "default": on a
    /// shard other than 0, "default" alone.
    pub fn known(self: *const NamespaceHandler, name: []const u8) bool {
        return self.metaOf(name) != null;
    }

    /// `name`'s record as this shard's log made it; "default"'s is fixed.
    pub fn metaOf(self: *const NamespaceHandler, name: []const u8) ?NamespaceMeta {
        if (isDefault(name)) return .{ .id = ns_table.DEFAULT, .created_at_ns = 0 };
        const m = ns_machine.meta(self.read(), name) orelse return null;
        return .{ .id = .of(m.id), .created_at_ns = m.created_at_ns, .created_index = m.created_index };
    }

    /// Live namespaces, "default" among them.
    pub fn live(self: *const NamespaceHandler) u32 {
        return ns_machine.counts(self.read()).live;
    }

    /// The next id a create may take, as applied on every replica.
    pub fn nextId(self: *const NamespaceHandler) u32 {
        return ns_machine.counts(self.read()).next_id;
    }

    /// Shard 0: room for a create this leader is about to propose, taken
    /// before it proposes: once proposed, the entry is in the log on every
    /// replica, and a pending entry lost to memory here would let the next
    /// write propose the same create again, which halts shard 0 everywhere
    /// when it applies. Out of memory refuses the propose instead.
    /// Null when `name` is already pending: the first id stands.
    fn reservePending(self: *NamespaceHandler, name: []const u8) error{OutOfMemory}!?[]const u8 {
        std.debug.assert(self.owner);
        if (self.pending_creates.contains(name)) return null;
        try self.pending_creates.ensureUnusedCapacity(self.allocator, 1);
        return try self.allocator.dupe(u8, name);
    }

    /// Note a create this leader proposed, with its id, until it applies:
    /// the writes behind it carry that id. `owned` is from `reservePending`.
    fn notePending(self: *NamespaceHandler, owned: ?[]const u8, id: ns_table.Id) void {
        if (owned) |k| self.pending_creates.putAssumeCapacityNoClobber(k, id);
    }

    /// This node stopped leading: its implicit creates may have been
    /// dropped with the log's tail.
    pub fn forgetImplicitCreates(self: *NamespaceHandler, shard: *Shard) void {
        // What waited for a create this node proposed may never see it.
        while (self.waiters.items.len > 0) {
            const name = self.allocator.dupe(u8, self.waiters.items[0].name) catch break;
            defer self.allocator.free(name);
            self.answerWaiters(shard, name, .stopped_leading);
        }
        self.stoppedLeading();
    }

    /// This node stopped leading: the ids and creates it had in flight
    /// are no longer its own.
    pub fn stoppedLeading(self: *NamespaceHandler) void {
        self.next_alloc = 0;
        var it = self.pending_creates.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.pending_creates.clearRetainingCapacity();
    }

    // ── Ids and names ──────────────────────────────────────────────────

    /// Shard 0: propose no id at or below `id` (`id_floor`), which another
    /// shard holds data under; past its own creates, the id is unknown here.
    pub fn raiseIdFloor(self: *NamespaceHandler, id: u32) void {
        std.debug.assert(self.owner);
        self.id_floor = @max(self.id_floor, id + 1);
        self.unknown_above = self.nextId() - 1;
        self.unknown_top = @max(self.unknown_top, id);
    }

    /// Shard 0: the ids other shards hold data under that the table doesn't
    /// have, lowest first, as many as `out` holds; returns how many there
    /// are in all.
    pub fn unknownIds(self: *const NamespaceHandler, out: []u32) u32 {
        var n: u32 = 0;
        var id = self.unknown_above + 1;
        while (id <= self.unknown_top) : (id += 1) {
            if (self.table.nameOf(.of(id)) != null) continue;
            if (n < out.len) out[n] = id;
            n += 1;
        }
        return n;
    }

    /// The name shard 0's log created as `id` and hasn't published yet;
    /// null on another shard, which reads names from `table` alone. For a
    /// read that misses the table in the moment between a create applying
    /// and its publish, so it walks only those creates: it runs on every
    /// miss, a dequeue's included.
    pub fn createdName(self: *const NamespaceHandler, id: ns_table.Id) ?[]const u8 {
        if (!self.owner) return null;
        for (self.unpublished.items) |u| if (u.id == id) return u.name;
        return null;
    }

    /// Shard 0: the id of a namespace by name, as its own log and this
    /// leader know it: created and not yet published, or proposed and not
    /// yet applied, as well as what the table shows. Other shards read the
    /// table alone.
    pub fn idOf(self: *const NamespaceHandler, name: []const u8) ?ns_table.Id {
        std.debug.assert(self.owner);
        const effective = if (name.len == 0) "default" else name;
        if (self.metaOf(effective)) |meta| return meta.id;
        return self.pending_creates.get(effective);
    }

    /// For tests: create `name` with the next id, as a committed entry at
    /// index 0 would, and publish it at once.
    pub fn applyCreate(self: *NamespaceHandler, name: []const u8) CreateOutcome {
        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, stdx.time.milliTimestamp()))) * std.time.ns_per_ms);
        const outcome = self.applyCreateAt(name, .of(self.nextId()), now_ns, 0);
        if (outcome == .created) self.publishUnpublished();
        return outcome;
    }

    /// For tests: a committed create of `name` as `id`, stamped
    /// `created_at_ns`, at log index `index`, applied as shard 0 applies
    /// one and queued to publish; one the machine would halt on is said
    /// instead.
    pub fn applyCreateAt(self: *NamespaceHandler, name: []const u8, id: ns_table.Id, created_at_ns: u64, index: u64) CreateOutcome {
        switch (ns_machine.verdict(ns_machine.counts(self.read()), ns_machine.meta(self.read(), name) != null, name, id.int())) {
            .created => {},
            .existed => return .existed,
            .invalid => return .invalid,
            .full => return .full,
            .stale_id => return .stale_id,
        }
        var buf: [namespace_entries.Create.max_size]u8 = undefined;
        const bytes = namespace_entries.Create.encodeInto(&buf, .{ .name = name, .id = id.int() }) catch return .invalid;
        const e = entry_mod.buildEntry(.namespace_create, 0, 1, index, created_at_ns, bytes);
        var out = Result.start(&e);
        self.applyCommitted(&e, &out) catch return .failed;
        return .created;
    }

    /// Apply a committed create as shard 0 does: through the namespaces
    /// machine, then what follows it here. For a handler with a store of its
    /// own (tests, the simulator); a shard applies through its applier.
    pub fn applyCommitted(self: *NamespaceHandler, e: *const entry_mod.Entry, out: *Result) persistence_mod.ApplyError!void {
        try contract_machine.applyEntry(&ns_machine.machine, self.store, &self.scratch, e, out);
        self.afterApply(e);
    }

    /// After a create applied on shard 0, on every replica: no longer in
    /// flight here, and queued to publish once it is on disk (`settle`).
    /// Boot replay doesn't come here; it publishes the lot (`publishAll`).
    pub fn afterApply(self: *NamespaceHandler, e: *const entry_mod.Entry) void {
        if (self.owner and e.header.entry_type == @backingInt(entry_mod.EntryType.namespace_alter)) {
            self.quotas_unpublished = true;
            return;
        }
        if (!self.owner or e.header.entry_type != @backingInt(entry_mod.EntryType.namespace_create)) return;
        var diag: schema.Diagnostic = .{};
        const c = namespace_entries.Create.decodeAll(e.payload, &diag) catch unreachable; // it applied
        if (self.pending_creates.fetchRemove(c.name)) |kv| self.allocator.free(kv.key);
        const owned = self.allocator.dupe(u8, c.name) catch {
            self.publish_from_store = true;
            return;
        };
        self.unpublished.append(self.allocator, .{ .index = e.header.index, .id = .of(c.id), .name = owned }) catch {
            self.allocator.free(owned);
            self.publish_from_store = true;
        };
    }

    /// Publish every create applied so far, without a flush: for tests and
    /// a shard with nothing on disk.
    fn publishUnpublished(self: *NamespaceHandler) void {
        for (self.unpublished.items) |u| {
            self.publishQuotaOf(u.id, u.name, u.index);
            self.table.publish(u.id, u.name, u.index) catch {};
            self.allocator.free(u.name);
        }
        self.unpublished.clearRetainingCapacity();
    }

    /// Shard 0: publish every create the store holds that the table lacks.
    /// At boot, once replay is done, since everything replayed is on disk;
    /// and at a settle after a create applied but couldn't be queued.
    pub fn publishAll(self: *NamespaceHandler) error{OutOfMemory}!void {
        std.debug.assert(self.owner);
        // Walked in name order, not log order: the published index moves
        // only once every name is in, so no reader takes it to cover a name
        // still to come.
        var top: u64 = 0;
        var it = self.read().range(ns_machine.NameFam, ns_machine.NAME_PREFIX);
        while (it.next() catch unreachable) |rec| { // written by the machine alone
            top = @max(top, rec.value.v.created_index);
            const id: ns_table.Id = .of(rec.value.v.id);
            const name = rec.key[ns_machine.NAME_PREFIX.len..];
            // Its quota before its name: a shard that sees the name finds
            // the quota published with it.
            self.publishQuotaOf(id, name, rec.value.v.created_index);
            if (self.table.nameOf(id) != null) continue;
            try self.table.publish(id, name, 0);
        }
        self.publishQuotaOf(ns_table.DEFAULT, "default", 0);
        self.table.notePublished(top);
    }

    /// Shard 0: publish `id`'s quota, its own as the store holds it, or the
    /// default under the index that made the namespace (`created_index`,
    /// at least 1, so the built-in default namespace's counts as published).
    fn publishQuotaOf(self: *NamespaceHandler, id: ns_table.Id, name: []const u8, created_index: u64) void {
        if (ns_machine.settings(self.read(), name)) |s|
            self.table.publishQuota(id, ns_machine.quotaOf(s), s.set_index)
        else
            self.table.publishQuota(id, ns_machine.DEFAULT_HISTORY_QUOTA, @max(created_index, 1));
    }

    /// Shard 0: publish every namespace's own quota the store holds, after
    /// an alter's is on disk. One already published at its index is left.
    fn publishQuotas(self: *NamespaceHandler) void {
        var it = self.read().range(ns_machine.SettingsFam, ns_machine.NAME_PREFIX);
        while (it.next() catch unreachable) |rec| { // written by the machine alone
            const name = rec.key[ns_machine.NAME_PREFIX.len..];
            const id = if (ns_machine.isDefault(name)) ns_table.DEFAULT else if (ns_machine.meta(self.read(), name)) |m| ns_table.Id.of(m.id) else continue;
            self.table.publishQuota(id, ns_machine.quotaOf(rec.value.v), rec.value.v.set_index);
        }
    }

    // ── Ops ─────────────────────────────────────────────────────────────
    // Every namespace op routes to shard 0, the namespaces' one owner.

    const ListResponse = namespace_ops.op("namespace.list").response;
    const ShardQuota = namespace_ops.ShardQuota;
    const InfoResponse = namespace_ops.op("namespace.info").response;
    const EnsureResponse = namespace_ops.op("namespace.ensure").response;
    const PublishedResponse = namespace_ops.op("namespace.published").response;
    const Empty = namespace_ops.op("namespace.create").response;

    pub fn register(dispatcher: *Dispatcher) void {
        inline for (.{
            .{ "namespace.create", OpCode.namespace_create, runCreate },
            .{ "namespace.delete", OpCode.namespace_delete, runDelete },
            .{ "namespace.list", OpCode.namespace_list, runList },
            .{ "namespace.info", OpCode.namespace_info, runInfo },
            .{ "namespace.alter", OpCode.namespace_alter, runAlter },
            .{ "namespace.ensure", OpCode.namespace_ensure, runEnsure },
            .{ "namespace.published", OpCode.namespace_published, runPublished },
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
        if (self.known(name) or self.pending_creates.contains(name)) return null;
        // Creates in flight count against the room, so a burst cannot
        // overshoot.
        if (self.live() + self.pending_creates.count() >= MAX_NAMESPACES)
            return .{ .status = .bad_request, .message = LIMIT_MESSAGE };
        return null;
    }

    /// namespace.create: answered once the create is on disk and
    /// published, so the namespace is usable on every shard by then.
    fn runCreate(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(Empty) {
        return shard.namespace_handler.createOrWait(shard, conn, req, .create);
    }

    /// namespace.ensure: create-or-get, answered with the id once it is
    /// published, for another shard's write waiting on the name.
    fn runEnsure(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(EnsureResponse) {
        const self = shard.namespace_handler;
        if (self.table.lookup(req.key)) |id| return .{ .answer = .{ .id = id.int(), .published_index = self.table.publishedIndex() } };
        return switch (try self.createOrWait(shard, conn, req, .ensure)) {
            .refused => |r| .{ .refused = r },
            else => .wait,
        };
    }

    /// namespace.published: how far this node's table is published, for a
    /// shard or follower that doesn't know the name asked about. It waits
    /// for its own table to reach this index, then resolves the name itself.
    /// A create of the name applied here or under way is answered once it
    /// is published, so the asker finds it.
    fn runPublished(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(PublishedResponse) {
        const self = shard.namespace_handler;
        // Only a ready leader's table has every create acknowledged: a
        // deposed node's is behind, and a new leader's may not have applied
        // its predecessor's yet.
        if (!shard.raft_node.leaderReady()) return .{ .refused = .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = PUBLISHED_NOT_READY } } };
        const name = req.key;
        const unpublished = self.table.lookup(name) == null and (self.known(name) or self.pending_creates.contains(name));
        if (!unpublished) return .{ .answer = .{ .published_index = self.table.publishedIndex() } };
        try self.addWaiter(name, .published, conn.replyTo(), req.header.request_id);
        return .wait;
    }

    /// Shard 0: the log index of a create of `name` applied here that the
    /// table doesn't show yet, or null.
    pub fn unpublishedIndex(self: *const NamespaceHandler, name: []const u8) ?u64 {
        if (!self.owner or self.table.lookup(name) != null) return null;
        const meta = self.metaOf(name) orelse return null;
        return meta.created_index;
    }

    /// Whether this shard has a create of `name` applied or under way that
    /// the table doesn't show yet: shard 0 only.
    pub fn creating(self: *const NamespaceHandler, name: []const u8) bool {
        return self.owner and (self.known(name) or self.pending_creates.contains(name));
    }

    /// Shard 0: a published name answers now; a known or pending one waits
    /// for its publish; a new one is proposed, then waits. `kind` says how
    /// the waiter is answered.
    fn createOrWait(self: *NamespaceHandler, shard: *Shard, conn: *Connection, req: Request, kind: WaiterKind) Error!table_run.Outcome(Empty) {
        std.debug.assert(!shard.applying); // as in prepareWrite
        const name = req.key;
        // The grammar first: a reserved name ("_flo") is in the table, but
        // is refused as reserved, not as existing.
        if (nameRefusal(name)) |why| return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun(why) } };
        if (self.table.lookup(name) != null) {
            if (kind == .create) return error.Exists;
            unreachable; // ensure answers a published name itself
        }
        var as = kind;
        if (self.known(name) or self.pending_creates.contains(name)) {
            // Being created by another request: wait for its outcome, so
            // "exists" is never said of a create that then fails.
            if (kind == .create) as = .joined;
        } else {
            switch (self.proposeCreateOn(shard.raft_node, name)) {
                .proposed => {},
                .refused => |r| return .{ .refused = .{ .status = r.status, .refusal = proto.notRun(r.message) } },
                .failed => |err| return err,
            }
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
    /// applies, and reaches disk, only after it. Another shard runs a write
    /// to a name the node's table has published, since the create is on
    /// shard 0's disk by then; a name the table lacks waits for shard 0 to
    /// create and publish it.
    pub fn prepareWrite(self: *NamespaceHandler, shard: *Shard, name_or_empty: []const u8) Prepared {
        // The apply loop advances last_applied before it applies an entry:
        // from inside it, leaderReady would say ready mid-apply.
        std.debug.assert(!shard.applying);
        const name = if (name_or_empty.len == 0) "default" else name_or_empty;
        if (!self.owner) return if (self.table.lookup(name) != null) .go else .ensure;
        if (self.known(name) or self.pending_creates.contains(name)) return .go;
        // Shard 0 proposes the create ahead of the write, in one log, and
        // the write carries the id the create proposed: every create a
        // ready leader proposes applies (`namespace/machine.zig`), or is dropped with
        // the log's tail, writes behind it included.
        return switch (self.proposeCreateOn(shard.raft_node, name)) {
            .proposed => .go,
            .refused => |r| .{ .refused = r },
            .failed => |err| .{ .refused = refusalOf(err) },
        };
    }

    fn refusalOf(err: Error) Refusal {
        const r = refusal(err);
        return .{ .status = r.status, .message = r.refusal.message };
    }

    /// Shard 0, once a tick: put applied creates on disk and publish them,
    /// then answer or run what waited for them; and answer what has waited
    /// too long.
    pub fn settle(self: *NamespaceHandler, shard: *Shard) void {
        if (!self.owner) return;
        // A quota published here is bound by every shard's walk: wake the
        // ones asleep rather than leave it to their next 100 ms tick.
        const epoch = self.table.quotaEpoch();
        defer if (self.table.quotaEpoch() != epoch) shard.wakePeerShards();

        if (self.unpublished.items.len > 0 or self.publish_from_store or self.quotas_unpublished) publish: {
            shard.flushSegmentToDisk() catch |err| {
                const now = @import("stdx").time.milliTimestamp();
                if (now - self.flush_warn_ms >= 30_000) {
                    self.flush_warn_ms = now;
                    if (self.unpublished.items.len > 0)
                        log.warn("namespace: {d} created namespaces are not on disk ({s}); they are not published until they are", .{ self.unpublished.items.len, @errorName(err) })
                    else
                        log.warn("namespace: a quota set is not on disk ({s}); it is not published until it is", .{@errorName(err)});
                }
                break :publish;
            };
            if (self.quotas_unpublished) {
                self.publishQuotas();
                self.quotas_unpublished = false;
            }
            for (self.unpublished.items, 0..) |u, i| {
                self.publishQuotaOf(u.id, u.name, u.index);
                // Published already from the store (`publishAll`).
                if (self.table.nameOf(u.id) == null) self.table.publish(u.id, u.name, u.index) catch {
                    // What was published stays published: drop it, and
                    // try the rest next tick.
                    self.unpublished.replaceRangeAssumeCapacity(0, i, &.{});
                    break :publish;
                };
                self.answerWaiters(shard, u.name, .published);
                self.allocator.free(u.name);
            }
            self.unpublished.clearRetainingCapacity();
            if (self.publish_from_store) {
                self.publishAll() catch break :publish;
                self.publish_from_store = false;
                self.answerPublishedWaiters(shard);
            }
        }

        const now = @import("stdx").time.monotonicMs();
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const w = self.waiters.items[i];
            if (now < w.deadline_ms) {
                i += 1;
                continue;
            }
            _ = self.waiters.orderedRemove(i);
            defer self.freeWaiter(w);
            if (w.kind == .published)
                shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, proto.notRun(READ_UNSETTLED))
            else
                shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, .{ .reason = .unclassified, .ran = .unknown, .message = FLUSH_MESSAGE });
        }
    }

    /// Answer every waiter whose name the table now has.
    fn answerPublishedWaiters(self: *NamespaceHandler, shard: *Shard) void {
        var name_buf: [MAX_NAME_LEN]u8 = undefined;
        while (true) {
            const w = for (self.waiters.items) |w| {
                if (self.table.lookup(w.name) != null) break w;
            } else return;
            const name = name_buf[0..w.name.len];
            @memcpy(name, w.name);
            self.answerWaiters(shard, name, .published);
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
            // In the order they came: a client's pipelined writes waiting
            // on one create run as it sent them.
            _ = self.waiters.orderedRemove(i);
            defer self.freeWaiter(w);
            switch (how) {
                // A read waiting on the create: refused, there is no
                // namespace; unsettled, it ran nothing either way.
                .refused => |r| if (w.kind == .published)
                    shard.deliverDeferredRefusal(w.reply_to, w.request_id, .no_namespace, .{ .reason = .not_found, .ran = .no, .message = "namespace not found" })
                else
                    shard.deliverDeferredRefusal(w.reply_to, w.request_id, r.status, proto.notRun(r.message)),
                .stopped_leading => if (w.kind == .published)
                    shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, proto.notRun(READ_UNSETTLED))
                else
                    shard.deliverDeferredRefusal(w.reply_to, w.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .unknown, .message = "unavailable: lost leadership before the namespace was created; it may still be — retry" }),
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
                        const v: EnsureResponse.Value = .{ .id = self.table.lookup(name).?.int(), .published_index = self.table.publishedIndex() };
                        shard.deliverDeferred(w.reply_to, table_run.encodeAnswer(EnsureResponse, &buf, w.request_id, v).?);
                    },
                    .published => {
                        var buf: [@sizeOf(proto.ResponseHeader) + PublishedResponse.max_size]u8 = undefined;
                        shard.deliverDeferred(w.reply_to, table_run.encodeAnswer(PublishedResponse, &buf, w.request_id, .{ .published_index = self.table.publishedIndex() }).?);
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
        const Names = ListResponse.field_list[0].T;
        const buf = self.list_buf orelse blk: {
            const b = try self.allocator.alloc(u8, Names.byte_bound.?);
            self.list_buf = b;
            break :blk b;
        };
        var out = Names.Builder.init(buf);
        // The store's names after the cursor, in key order, which is name
        // order; "default", which has no record, merged in at its place.
        var kb: [1 + std.math.maxInt(u8)]u8 = undefined;
        const after = @import("../contract/view.zig").key(&kb, "", cursor) catch null;
        var it = if (after) |k| self.read().rangeAfter(ns_machine.NameFam, ns_machine.NAME_PREFIX, k) else self.read().range(ns_machine.NameFam, ns_machine.NAME_PREFIX);
        var default_due = std.mem.order(u8, "default", cursor) == .gt;
        var next_stored = nameAfter(&it, cursor);
        var last: ?[]const u8 = null;
        var more = false;
        while (true) {
            const name = if (default_due and (next_stored == null or std.mem.lessThan(u8, "default", next_stored.?))) d: {
                default_due = false;
                break :d "default";
            } else if (next_stored) |n| n: {
                next_stored = nameAfter(&it, cursor);
                break :n n;
            } else break;
            if (out.count == limit or !(out.add(name) catch unreachable)) { // a valid name never breaks its schema
                more = true;
                break;
            }
            last = name;
        }
        return .{ .names = out.value(), .cursor = if (more) last else null };
    }

    fn nameAfter(it: anytype, cursor: []const u8) ?[]const u8 {
        while (it.next() catch unreachable) |rec| { // written by the machine alone
            const name = rec.key[ns_machine.NAME_PREFIX.len..];
            if (std.mem.order(u8, name, cursor) == .gt) return name;
        }
        return null;
    }

    /// namespace.info: its record, its quota, and each shard's part and
    /// usage, gathered from every shard; it fails rather than answer with
    /// part of them.
    fn runInfo(shard: *Shard, conn: *Connection, req: Request) Error!table_run.Outcome(InfoResponse) {
        const self = shard.namespace_handler;
        const name = req.key;
        if (!ns_machine.isDefault(name)) if (nameRefusal(name)) |why| return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun(why) } };
        const meta = self.metaOf(name) orelse return error.NoNamespace;
        const own = ns_machine.settings(self.read(), name);
        const quota = ns_machine.quotaOf(own);
        const state = try shard.allocator.create(InfoGather);
        state.* = .{ .created_at_ms = @intCast(meta.created_at_ns / std.time.ns_per_ms), .quota = if (quota == ns_machine.NO_QUOTA) null else quota, .own = ns_machine.quotaOwn(own) };
        var state_owned = true;
        defer if (state_owned) shard.allocator.destroy(state);
        const n = shard.shardCount();
        const parts = try shard.allocator.alloc(gather_mod.Part, n);
        defer shard.allocator.free(parts);
        var own_buf: [ShardQuota.max_size]u8 = undefined;
        const own_body = try shard.allocator.dupe(u8, ShardQuota.encodeInto(&own_buf, localQuota(shard, meta.id)) catch unreachable);
        var body_owned = true;
        defer if (body_owned) shard.allocator.free(own_body);
        for (parts, 0..) |*p, i| {
            const s: u16 = @intCast(i);
            p.* = if (s == shard.id) .{ .owner = s, .body = own_body } else .{ .owner = s, .body = "", .op = @backingInt(OpCode.system_history_quota), .key = name };
        }
        state_owned = false;
        body_owned = false;
        shard.startGather(conn, req, parts, &InfoGather.op, state) catch |err| return switch (err) {
            error.GatherBusy => .{ .refused = .{ .status = .overloaded, .refusal = .{ .reason = .shard_busy, .ran = .no, .message = "overloaded: too many requests across shards are under way; this request was not run — retry" } } },
            error.OutOfMemory => error.OutOfMemory,
        };
        return .wait;
    }

    /// A namespace's info, held while every shard says its part.
    const InfoGather = struct {
        created_at_ms: i64,
        quota: ?u64,
        own: bool,

        const op: gather_mod.Op = .{ .merge = merge, .deinit = free };

        fn free(state: *anyopaque, allocator: Allocator) void {
            const self: *InfoGather = @ptrCast(@alignCast(state));
            allocator.destroy(self);
        }

        fn merge(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
            const self: *InfoGather = @ptrCast(@alignCast(state));
            const buf = try shard.allocator.alloc(u8, namespace_ops.ShardQuotas.max_size);
            defer shard.allocator.free(buf);
            var w: schema.Writer = .{ .buf = buf };
            for (bodies) |body| {
                var diag: schema.Diagnostic = .{};
                try ShardQuota.encode(&w, try ShardQuota.decodeAll(body, &diag));
            }
            table_run.answer(InfoResponse, shard, conn, request_id, .{ .created_at_ms = self.created_at_ms, .history_quota = self.quota, .history_quota_own = self.own, .shards = .{ .bytes = w.written(), .count = @intCast(bodies.len) } });
        }
    };

    /// This shard's part of namespace `ns`'s quota, as its leader bound it,
    /// and the run history the namespace holds here.
    pub fn localQuota(shard: *Shard, ns: ns_table.Id) ShardQuota.Value {
        const rv: ReadView = .{ .store = &shard.defaultPartition().store };
        const bound = settings_machine.boundQuota(rv, ns);
        const u = hm.usageOf(rv, ns);
        return .{
            .shard = shard.id,
            .bound = bound != null,
            .part_bytes = if (bound) |b| (if (b.part_bytes == settings_machine.NO_QUOTA) null else b.part_bytes) else null,
            .usage_bytes = u.bytes,
            .orphans = u.orphans,
            .orphan_bytes = u.orphan_bytes,
            .reserved_workflow_bytes = hq.reservedOf(hq.WorkflowReserved, rv, ns),
            .reserved_action_bytes = hq.reservedOf(hq.ActionReserved, rv, ns),
            .in_flight_bytes = hq.inFlight(rv, ns),
            // Leader memory: only the shard's leader knows it.
            .held_runs = if (shard.raft_node.role == .leader) @intCast(shard.workflow_handler.holdCount(.{ .ns = ns, .kind = .quota })) else null,
        };
    }

    /// `system.history_quota`, on the shard asked: its part of the
    /// namespace the key names. One this node's table doesn't know yet is
    /// refused, so the info fails and is asked again.
    pub fn handleHistoryQuota(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const id = shard.namespace_handler.table.lookup(if (req.key.len == 0) "default" else req.key) orelse
            return shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun("unavailable: this node hasn't published the namespace yet; the info was not run — retry"));
        table_run.answer(ShardQuota, shard, conn, req.header.request_id, localQuota(shard, id));
    }

    /// A quota below one run's reserve on every shard, told as the total it
    /// must reach. Valid until the thread's next call.
    fn floorMessage(floor: u64, shards: usize) []const u8 {
        const S = struct {
            threadlocal var buf: [160]u8 = undefined;
        };
        return std.fmt.bufPrint(&S.buf, "bad request: a history quota is at least {d} bytes on {d} shards, one run's reserve ({d} bytes) on each", .{ floor, shards, ns_machine.PART_FLOOR }) catch "bad request: a history quota is at least one run's reserve on every shard";
    }

    /// namespace.alter: a namespace's quota set on shard 0, answered once
    /// it applies. Below one shard's floor on this node, refused here.
    fn runAlter(shard: *Shard, _: *Connection, req: Request, mode: ns_machine.QuotaMode, bytes: ?u64) Error!table_run.Outcome(Empty) {
        const self = shard.namespace_handler;
        const name = req.key;
        if (!ns_machine.isDefault(name)) {
            if (nameRefusal(name)) |why| return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun(why) } };
            if (self.metaOf(name) == null) return error.NoNamespace;
        }
        const floor = ns_machine.PART_FLOOR *| shard.shardCount();
        if (ns_machine.quotaRefusal(mode, bytes, floor)) |why| {
            const below = mode == .bytes and bytes != null and bytes.? != 0 and bytes.? < floor;
            return .{ .refused = .{ .status = .bad_request, .refusal = .{ .reason = why.reason, .ran = .no, .message = if (below) floorMessage(floor, shard.shardCount()) else why.message } } };
        }
        if (!shard.raft_node.leaderReady()) return .{ .refused = .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: a new leader is catching up; the quota wasn't set — retry" } } };
        const P = namespace_entries.Alter;
        var buf: [P.max_size]u8 = undefined;
        const payload = P.encodeInto(&buf, .{ .name = name, .mode = mode, .bytes = bytes orelse 0 }) catch return error.Internal;
        const proposed = shard.raft_node.propose(.namespace_alter, entry_mod.Flags.NONE, payload) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerAlter } };
    }

    fn answerAlter(shard: *Shard, conn: *Connection, request_id: u64, _: *const persistence_mod.Result) void {
        table_run.answer(Empty, shard, conn, request_id, .{});
    }

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// What `proposeCreateOn` came to.
    pub const CreateProposal = union(enum) {
        proposed: persistence_mod.ProposeResult,
        refused: Refusal,
        failed: Error,
    };

    /// Shard 0's leader proposes a create of `name`, which the caller has
    /// checked this node doesn't know, unless the name, the room or its id
    /// refuses it, or the leader hasn't applied every earlier term's
    /// entry: the id comes from what it has applied, so until then it may
    /// be one an earlier leader's create already took.
    pub fn proposeCreateOn(self: *NamespaceHandler, raft: *RaftNode, name: []const u8) CreateProposal {
        if (self.createRefusal(name)) |r| return .{ .refused = r };
        if (!raft.leaderReady()) return .{ .refused = .{ .status = .unavailable, .message = CATCHING_UP_MESSAGE } };
        const proposed = self.proposeCreate(raft, name) catch |err| return .{ .failed = proposeError(err) };
        return .{ .proposed = proposed };
    }

    /// Shard 0's leader proposes `name`'s create with the next id: past
    /// every id applied and every create it has in flight. Replicas never
    /// choose an id; they check the one given (`namespace/machine.zig`).
    fn proposeCreate(self: *NamespaceHandler, raft: *RaftNode, name: []const u8) !persistence_mod.ProposeResult {
        std.debug.assert(self.owner);
        const id: u32 = @max(self.nextId(), self.next_alloc, self.id_floor);
        if (id >= ns_table.CAPACITY) return error.NamespacesFull;
        const P = namespace_entries.Create;
        var buf: [P.max_size]u8 = undefined;
        const bytes = P.encodeInto(&buf, .{ .name = name, .id = id }) catch return error.PayloadTooLarge;
        const owned = try self.reservePending(name);
        errdefer if (owned) |k| self.allocator.free(k);
        const proposed = try raft.propose(.namespace_create, entry_mod.Flags.NONE, bytes);
        self.next_alloc = id + 1;
        self.notePending(owned, .of(id));
        return proposed;
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

test "namespace handler: every namespace op is registered and routed to shard 0, and 0x015 stays retired" {
    var dispatcher = Dispatcher.init();
    NamespaceHandler.register(&dispatcher);
    const req: Request = .{ .header = proto.RequestHeader.init(.namespace_list, 1), .namespace = "", .key = "", .value = "", .options = "" };
    for ([_]OpCode{ .namespace_create, .namespace_delete, .namespace_list, .namespace_info, .namespace_alter, .namespace_ensure, .namespace_published }) |op| {
        try testing.expect(dispatcher.hasHandler(op));
        try testing.expectEqual(@as(?u16, 0), dispatcher.shard_route[@backingInt(op)].?(req));
    }
    try testing.expectEqual(@as(u16, 7), dispatcher.handler_count);
    try testing.expect(std.enums.tagName(OpCode, @fromBackingInt(0x015)) == null);
}

test "namespace handler: a namespace's quota is published before its name, the default under its create's index" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    try testing.expectEqual(NamespaceHandler.CreateOutcome.created, handler.applyCreate("orders"));
    const id = handler.table.lookup("orders").?;
    const q = handler.table.quotaOf(id).?;
    try testing.expectEqual(ns_machine.DEFAULT_HISTORY_QUOTA, q.quota);
    try testing.expect(q.index >= 1);
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

test "namespace handler: a create takes the next dense id, and one its leader could never have proposed halts" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    var buf: [namespace_entries.Create.max_size]u8 = undefined;
    const first = ns_table.FIRST_ALLOCATED.int();
    const e = try testEntry(.namespace_create, 7, 1234 * std.time.ns_per_s, .{ .name = "stamped", .id = first }, &buf);
    var out = Result.start(&e);
    try handler.applyCommitted(&e, &out);
    try testing.expect(out.refused() == null);
    const meta = handler.metaOf("stamped").?;
    try testing.expectEqual(ns_table.Id.of(first), meta.id);
    try testing.expectEqual(@as(u64, 1234 * std.time.ns_per_s), meta.created_at_ns);
    try testing.expectEqual(@as(u64, 7), meta.created_index);
    try testing.expectEqual(first + 1, handler.nextId());
    // Applied, not yet published: that waits for the flush.
    try testing.expect(handler.table.lookup("stamped") == null);
    // A second create of the name: its leader checks the name first, so
    // the log is broken, and every replica halts on it alike.
    var again = Result.start(&e);
    try testing.expectError(error.Halt, handler.applyCommitted(&e, &again));
    // An id another create took first: no ready leader proposes one, so
    // the log is broken, and every replica halts on it alike.
    const stale = try testEntry(.namespace_create, 8, 0, .{ .name = "late", .id = first }, &buf);
    var st = Result.start(&stale);
    try testing.expectError(error.Halt, handler.applyCommitted(&stale, &st));
    try testing.expect(!handler.known("late"));
    // A reserved id is never allocated.
    const reserved = try testEntry(.namespace_create, 9, 0, .{ .name = "sneaky", .id = ns_table.SYSTEM.int() }, &buf);
    var rs = Result.start(&reserved);
    try testing.expectError(error.Halt, handler.applyCommitted(&reserved, &rs));
    // Only shard 0 logs creates.
    var other = try NamespaceHandler.initFor(testing.allocator, 1, null, null);
    defer other.deinit();
    const elsewhere = try testEntry(.namespace_create, 10, 0, .{ .name = "x", .id = first }, &buf);
    var el = Result.start(&elsewhere);
    try testing.expectError(error.Halt, other.applyCommitted(&elsewhere, &el));
}

test "namespace handler: a write to an unknown namespace registers it" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    handler.markNamespaceHasData("ghost", null);
    try testing.expect(handler.known("ghost"));
    handler.markNamespaceHasData("", null);
    try testing.expectEqual(@as(u32, 2), handler.live());
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
    try testing.expectEqual(@as(u32, 1), handler.live());

    // "default" holds one of the 1024 from the start.
    var buf: [16]u8 = undefined;
    for (0..NamespaceHandler.MAX_NAMESPACES - 1) |i| {
        try testing.expectEqual(NamespaceHandler.CreateOutcome.created, handler.applyCreate(try std.fmt.bufPrint(&buf, "ns{d}", .{i})));
    }
    try testing.expect(handler.admits("ns0"));
    try testing.expect(!handler.admits("one-more"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.full, handler.applyCreate("one-more"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.existed, handler.applyCreate("ns0"));
    try testing.expectEqual(@as(u32, NamespaceHandler.MAX_NAMESPACES), handler.live());
    // "default" is never locked out.
    try testing.expect(handler.admits(""));
    try testing.expect(handler.admits("default"));
    try testing.expectEqual(NamespaceHandler.CreateOutcome.existed, handler.applyCreate("default"));
}

test "namespace handler: default has its own id from the start, and id 0 names nothing" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();
    // Held before anything is created or replayed.
    try testing.expectEqualStrings("default", handler.table.nameOf(.default).?);
    try testing.expectEqual(@as(?ns_table.Id, .default), handler.idOf("default"));
    try testing.expectEqual(@as(?ns_table.Id, .default), handler.idOf(""));
    try testing.expect(handler.table.nameOf(.invalid) == null);
}

// ── Raft Apply Tests ────────────────────────────────────────────────────────

test "namespace handler: applyCreate adds to local registry" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    try testing.expectEqual(@as(u32, 1), handler.live());
    _ = handler.applyCreate("test-ns");
    try testing.expectEqual(@as(u32, 2), handler.live());
    try testing.expect(handler.known("test-ns"));
}

test "namespace handler: applyCreate is idempotent" {
    var handler = try NamespaceHandler.init(testing.allocator);
    defer handler.deinit();

    _ = handler.applyCreate("test-ns");
    _ = handler.applyCreate("test-ns"); // duplicate — should be no-op
    try testing.expectEqual(@as(u32, 2), handler.live());
}

test "namespace: a committed entry's id resolves to the name" {
    var handler = try NamespaceHandler.init(std.testing.allocator);
    defer handler.deinit();

    const id = ns_table.FIRST_ALLOCATED;
    try std.testing.expect(handler.table.nameOf(id) == null);

    _ = handler.applyCreate("prod");
    try std.testing.expectEqual(@as(?ns_table.Id, id), handler.idOf("prod"));
    try std.testing.expectEqualStrings("prod", handler.table.nameOf(id).?);
    // Re-applying (a replay) neither duplicates nor loses the mapping.
    _ = handler.applyCreate("prod");
    try std.testing.expectEqualStrings("prod", handler.table.nameOf(id).?);
    try std.testing.expect(handler.table.nameOf(.of(id.int() + 1)) == null);
    try std.testing.expect(handler.idOf("never") == null);
}
