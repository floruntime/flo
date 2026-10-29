//! KV's applied state as reads see it: committed state only, through the
//! contract's store, which the KV machine (machine.zig) alone changes.
//! Keys are namespace-qualified (`ns\0key`) here as on the wire.
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
pub const Qualified = machine.Qualified;

pub const KvState = struct {
    store: *store_mod.Store,
    /// What "now" is when a read judges expiry.
    read_clock: ReadClock = .{},
    /// The `kv.ns_counts` commits the shard last published.
    published_commits: u64 = std.math.maxInt(u64),

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

    /// `qkey`'s live, unexpired value, or null.
    pub fn get(self: *const KvState, qkey: []const u8) ?Current {
        const c = self.getRaw(qkey) orelse return null;
        if (c.tombstone or c.expiredAt(self.readNow())) return null;
        return c;
    }

    /// `qkey`'s current version, tombstone or expired included.
    pub fn getRaw(self: *const KvState, qkey: []const u8) ?Current {
        var kb: [machine.MAX_KEY]u8 = undefined;
        // State that won't decode was written by this build's own encode.
        return self.read().get(machine.Values, machine.valueKey(&kb, Qualified.of(qkey))) catch unreachable;
    }

    /// A key's versions, newest first, from below `before` (exclusive)
    /// when given.
    pub fn history(self: *const KvState, qkey: []const u8, before: ?u64) History {
        var h: History = .{ .current = self.getRaw(qkey), .before = before };
        var pb: [machine.MAX_KEY]u8 = undefined;
        var r = self.read().range(machine.History, machine.historyPrefix(&pb, Qualified.of(qkey)));
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
    /// in key order, into `out`: borrowed, without the namespace. Returns
    /// how many.
    pub fn scanKeyNames(self: *const KvState, ns: []const u8, prefix: []const u8, out: [][]const u8) usize {
        var pb: [machine.MAX_KEY]u8 = undefined;
        const p = view_mod.key(&pb, ns, prefix) catch return 0;
        const head = 1 + ns.len;
        const now = self.readNow();
        var r = self.read().range(machine.Values, p);
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
    pub fn countsOf(self: *const KvState, ns: []const u8) Counts {
        var kb: [machine.MAX_KEY]u8 = undefined;
        return (self.read().get(machine.NsCounts, machine.countsKey(&kb, ns)) catch unreachable) orelse .{};
    }

    /// Live keys in `ns`.
    pub fn liveKeys(self: *const KvState, ns: []const u8) u64 {
        return self.countsOf(ns).live;
    }

    /// Whether some namespace's counts changed since `markPublished`.
    pub fn countsChanged(self: *const KvState) bool {
        return self.store.family(machine.NsCounts.ID).commits != self.published_commits;
    }

    pub fn markPublished(self: *KvState) void {
        self.published_commits = self.store.family(machine.NsCounts.ID).commits;
    }

    /// Every namespace's counts, in name order.
    pub fn namespaces(self: *const KvState) Namespaces {
        return .{ .inner = self.read().range(machine.NsCounts, "") };
    }

    pub const countLive = countLiveImpl;
    pub const putForTest = putForTestImpl;
    pub const setExpiryForTest = setExpiryForTestImpl;

    pub const Namespaces = struct {
        inner: view_mod.ReadRange(machine.NsCounts),

        pub fn next(self: *Namespaces) ?struct { ns: []const u8, counts: Counts } {
            const item = (self.inner.next() catch unreachable) orelse return null;
            return .{ .ns = item.key[1 .. 1 + item.key[0]], .counts = item.value };
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

/// For tests: a put of `value` at `qkey`, applied through the KV machine
/// as a committed entry would be, outside any log.
fn putForTestImpl(self: *KvState, qkey: []const u8, value: []const u8) !void {
    const allocator = self.store.allocator;
    const body: kv_entries.Put.Value = .{ .key = qkey, .value = value, .ttl_ms = null };
    const buf = try allocator.alloc(u8, try kv_entries.Put.encodedSize(body));
    defer allocator.free(buf);
    const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 1, 0, try kv_entries.Put.encodeInto(buf, body));
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var out: persistence.Result = .{};
    out.reset(&e);
    try @import("../contract/machine.zig").applyEntry(&machine.machine, self.store, &scratch, &e, &out);
}

/// For tests: `qkey`'s expiry set outright, past any TTL an entry may carry.
fn setExpiryForTestImpl(self: *KvState, qkey: []const u8, expiry_ns: u64) !void {
    var scratch = std.heap.ArenaAllocator.init(self.store.allocator);
    defer scratch.deinit();
    var keys = view_mod.KeySet.init(scratch.allocator());
    const q = Qualified.of(qkey);
    try keys.add(machine.Values.ID, q.ns, q.key);
    var view = view_mod.View.init(self.store, scratch.allocator(), &keys);
    var kb: [machine.MAX_KEY]u8 = undefined;
    const vk = machine.valueKey(&kb, q);
    var cur = (try view.get(machine.Values, vk)) orelse return error.NotFound;
    cur.expiry_ns = expiry_ns;
    try view.put(machine.Values, vk, cur);
    try view.commit();
}
