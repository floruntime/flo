//! KV's applied state as reads see it: committed state only, through the
//! contract's store, which the KV machine (machine.zig) alone changes.
//! A key is read as a `machine.Key`: its namespace's id and the key the
//! client named, stored as `id ‖ key`.
//!
//! A read borrows the store's bytes: a value or key it returns is good on
//! the shard thread until the next apply.

const std = @import("std");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const machine = @import("machine.zig");
const kv_ops = @import("../protocol/kv_ops.zig");

pub const Current = machine.Current;
pub const Version = machine.Version;
pub const Counts = machine.Counts;
pub const ValueType = machine.ValueType;
pub const Key = machine.Key;
pub const NamespaceId = machine.NamespaceId;

pub const KvState = struct {
    store: *store_mod.Store,
    /// What "now" is when a read judges expiry.
    read_clock: ReadClock = .{},
    /// The `kv.ns_counts` commits the shard last published.
    published_commits: u64 = std.math.maxInt(u64),
    /// Where the shard publishes each namespace's counts, once looked up:
    /// node-local, never state.
    publish_to: std.AutoHashMapUnmanaged(NamespaceId, *anyopaque) = .empty,

    pub fn deinit(self: *KvState, allocator: std.mem.Allocator) void {
        self.publish_to.deinit(allocator);
    }

    /// The shard points this at its Raft node, so a read judges expiry by
    /// the same time line the entry stamps follow: a key a replica has
    /// already treated as expired at apply never reads back as live. The
    /// default, the wall clock, is for state on its own.
    pub const ReadClock = struct {
        ctx: ?*const anyopaque = null,
        now_ns: *const fn (?*const anyopaque) u64 = wallNs,

        fn wallNs(_: ?*const anyopaque) u64 {
            return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
        }
    };

    fn readNow(self: *const KvState) u64 {
        return self.read_clock.now_ns(self.read_clock.ctx);
    }

    fn read(self: *const KvState) view_mod.ReadView {
        return .{ .store = self.store };
    }

    /// The live, unexpired value at `stored` (`Key.stored`, as a waiter
    /// keeps it), or null.
    pub fn get(self: *const KvState, stored: []const u8) ?Current {
        const c = self.getRaw(stored) orelse return null;
        if (c.tombstone or c.expiredAt(self.readNow())) return null;
        return c;
    }

    /// The current version at `stored`, tombstone or expired included.
    pub fn getRaw(self: *const KvState, stored: []const u8) ?Current {
        // State that won't decode was written by this build's own encode.
        return self.read().get(machine.Values, stored) catch unreachable;
    }

    /// `raw`'s live, unexpired value in namespace `ns`, or null.
    pub fn getIn(self: *const KvState, ns: NamespaceId, raw: []const u8) ?Current {
        var kb: [@import("../protocol/limits.zig").MAX_KV_KEY]u8 = undefined;
        return self.get(Key.init(&kb, ns, raw).stored);
    }

    /// A key's versions, newest first, from below `before` (exclusive)
    /// when given.
    pub fn history(self: *const KvState, key: Key, before: ?u64) History {
        var h: History = .{ .current = self.getRaw(key.stored), .before = before };
        var pb: [machine.MAX_KEY]u8 = undefined;
        var r = self.read().range(machine.History, machine.historyPrefix(&pb, key));
        while (r.next() catch unreachable) |item| {
            std.debug.assert(h.len < h.chain.len);
            h.chain[h.len] = item.value;
            h.len += 1;
        }
        h.next_index = h.len;
        return h;
    }

    pub const History = struct {
        current: ?Current,
        before: ?u64,
        /// Oldest first, as the family keeps them.
        chain: [kv_ops.HISTORY_MAX_VERSIONS]Version = undefined,
        len: usize = 0,
        next_index: usize = 0,
        gave_current: bool = false,

        /// Whether the key has any history at all.
        pub fn exists(h: *const History) bool {
            return h.current != null or h.len > 0;
        }

        pub fn next(h: *History) ?Version {
            if (!h.gave_current) {
                h.gave_current = true;
                if (h.current) |c| if (h.before == null or c.version < h.before.?) return Version{
                    .version = c.version,
                    .lsn = c.lsn,
                    .term = c.term,
                    .timestamp_ns = c.timestamp_ns,
                    .tombstone = c.tombstone,
                    .value = c.value,
                };
            }
            while (h.next_index > 0) {
                h.next_index -= 1;
                const v = h.chain[h.next_index];
                if (h.before == null or v.version < h.before.?) return v;
            }
            return null;
        }
    };

    /// The names of `ns`'s live, unexpired keys that start with `prefix`,
    /// in key order, after `after` when given, into `out`: borrowed, without
    /// the namespace. Returns how many. The walk is the namespace's id
    /// prefix, so it never reaches another namespace's key.
    pub fn scanKeyNames(self: *const KvState, ns: NamespaceId, prefix: []const u8, after: ?[]const u8, out: [][]const u8) usize {
        if (ns == .invalid) return 0;
        var pb: [machine.MAX_KEY]u8 = undefined;
        const p = view_mod.idKey(&pb, ns, prefix) catch return 0;
        var ab: [machine.MAX_KEY]u8 = undefined;
        const a = if (after) |k| view_mod.idKey(&ab, ns, k) catch return 0 else null;
        const head = @import("../protocol/limits.zig").NAMESPACE_ID_BYTES;
        const now = self.readNow();
        var r = if (a) |from| self.read().rangeAfter(machine.Values, p, from) else self.read().range(machine.Values, p);
        var n: usize = 0;
        while (n < out.len) {
            const item = (r.next() catch unreachable) orelse break;
            if (item.value.tombstone or item.value.expiredAt(now)) continue;
            out[n] = item.key[head..];
            n += 1;
        }
        return n;
    }

    /// Keys stored, tombstones included: room enough for any scan.
    pub fn totalEntries(self: *const KvState) usize {
        return @intCast(self.store.family(machine.Values.ID).count);
    }

    /// `ns`'s counts.
    pub fn countsOf(self: *const KvState, ns: NamespaceId) Counts {
        var kb: [machine.MAX_KEY]u8 = undefined;
        return (self.read().get(machine.NsCounts, machine.countsKey(&kb, ns)) catch unreachable) orelse .{};
    }

    /// Live keys in `ns`.
    pub fn liveKeys(self: *const KvState, ns: NamespaceId) u64 {
        return self.countsOf(ns).live;
    }

    /// Whether some namespace's counts changed since `markPublished`.
    pub fn countsChanged(self: *const KvState) bool {
        return self.store.family(machine.NsCounts.ID).commits != self.published_commits;
    }

    pub fn markPublished(self: *KvState) void {
        self.published_commits = self.store.family(machine.NsCounts.ID).commits;
    }

    /// Every namespace's counts, in id order.
    pub fn namespaces(self: *const KvState) Namespaces {
        return .{ .inner = self.read().range(machine.NsCounts, "") };
    }

    pub const countLive = countLiveImpl;
    pub const putForTest = putForTestImpl;
    pub const setExpiryForTest = setExpiryForTestImpl;

    pub const Namespaces = struct {
        inner: view_mod.ReadRange(machine.NsCounts),

        pub fn next(self: *Namespaces) ?struct { ns: NamespaceId, counts: Counts } {
            const item = (self.inner.next() catch unreachable) orelse return null;
            return .{ .ns = machine.countsNamespace(item.key), .counts = item.value };
        }
    };
};

// ─── For tests ──────────────────────────────────────────────────────────

const entry_mod = @import("../storage/ual/entry.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const persistence = @import("../storage/persistence.zig");

/// Live keys across every namespace.
fn countLiveImpl(self: *const KvState) u64 {
    var n: u64 = 0;
    var it = self.namespaces();
    while (it.next()) |ns| n += ns.counts.live;
    return n;
}

/// For tests: a put of `value` at stored key `stored` (`id ‖ raw`),
/// applied through the KV machine as a committed entry would be, outside
/// any log.
fn putForTestImpl(self: *KvState, stored: []const u8, value: []const u8) !void {
    const allocator = self.store.allocator;
    const body: kv_entries.Put.Value = .{ .key = stored, .value = value, .ttl_ms = null, .condition = .always, .cas_version = null };
    const buf = try allocator.alloc(u8, try kv_entries.Put.encodedSize(body));
    defer allocator.free(buf);
    const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 1, 0, try kv_entries.Put.encodeInto(buf, body));
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var out: persistence.Result = .{};
    out.reset(&e);
    try @import("../contract/machine.zig").applyEntry(&machine.machine, self.store, &scratch, &e, &out);
}

/// For tests: the expiry at stored key `stored` set outright, past any TTL
/// an entry may carry.
fn setExpiryForTestImpl(self: *KvState, stored: []const u8, expiry_ns: u64) !void {
    var scratch = std.heap.ArenaAllocator.init(self.store.allocator);
    defer scratch.deinit();
    var keys = view_mod.KeySet.init(scratch.allocator());
    const q = Key.ofStored(stored) orelse return error.NotFound;
    try keys.addIn(machine.Values.ID, q.ns, q.raw);
    var view = view_mod.View.init(self.store, scratch.allocator(), &keys);
    const vk = machine.valueKey(q);
    var cur = (try view.get(machine.Values, vk)) orelse return error.NotFound;
    cur.expiry_ns = expiry_ns;
    try view.put(machine.Values, vk, cur);
    try view.commit();
}
