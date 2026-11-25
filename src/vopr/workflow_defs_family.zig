//! Workflow definitions as the server keeps them: the definitions machine
//! (`workflow/defs_machine.zig`) on a store of each replica's own, the
//! checker comparing its family's checksum, and each entry checked against
//! a ledger of the definitions the entries made, kept apart from the
//! machine:
//!
//! - a create stores its version, anchored at its stamp, and keeps whether
//!   the workflow it replaces was enabled (a new one is enabled);
//! - an enable or disable of a missing workflow is refused; one naming the
//!   state it has changes nothing; an enable after a disable moves the
//!   anchor to its stamp;
//! - the records the store holds are the ledger's.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const dm = @import("../workflow/defs_machine.zig");

const Allocator = std.mem.Allocator;
const EntryType = entry_mod.EntryType;
const NS: @import("../namespace/table.zig").Id = .default;

/// The workflows the workload names: one is never created, so its enables
/// are refused.
pub const NAMES = [_][]const u8{ "w0", "w1", "w2", "w3" };
const NEVER_CREATED = 3;
const VERSIONS = [_][]const u8{ "1.0.0", "2.0.0", "3.0.0" };

fn yamlOf(comptime version: []const u8) []const u8 {
    return "kind: Workflow\nname: w\nversion: " ++ version ++ "\nstart:\n  wait_for_signal:\n    type: never\n  transitions:\n    success: flo.Completed\n";
}

// The last is longer: under a memory share, a replace by it grows, and
// may be refused.
const YAMLS = [_][]const u8{ yamlOf(VERSIONS[0]), yamlOf(VERSIONS[1]), yamlOf(VERSIONS[2]) ++ "# " ++ @as([256]u8, @splat('x')) ++ "\n" };

pub const WorkflowDefs = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    ledger: [NAMES.len]?Held = @splat(null),

    const Held = struct { version: usize, enabled: bool, anchor_ms: i64, index: u64 };

    pub const FAMILIES = [_]u16{dm.DefFam.ID};
    pub const FAMILY_NAMES = [_][]const u8{"workflow.def"};

    pub const Kind = enum { create, enable, disable };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        /// Refused for memory: past the share.
        memory: bool = false,
        broke: ?[]const u8 = null,
    };

    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
        }
    };

    pub fn init(allocator: Allocator) WorkflowDefs {
        var w: WorkflowDefs = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
        w.store.strict_keys = true;
        // The runs machine shares this store, a group of one: a run's
        // history goes to its own group, which here is every workflow's.
        w.store.place = .{ .group = 0, .count = 1 };
        return w;
    }

    pub fn deinit(self: *WorkflowDefs) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// Replay re-applies from the start: the ledger goes with the store.
    pub fn reset(self: *WorkflowDefs) void {
        self.store.clear();
        self.ledger = @splat(null);
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(EntryType, @fromBackingInt(entry_type))) {
            .workflow_create, .workflow_enabled => true,
            else => false,
        };
    }

    fn nameIndex(key: []const u8) ?usize {
        if (key.len < 4) return null;
        for (NAMES, 0..) |name, i| if (std.mem.eql(u8, name, key[4..])) return i;
        return null;
    }

    fn versionIndex(yaml: []const u8) ?usize {
        for (YAMLS, 0..) |y, i| if (std.mem.eql(u8, y, yaml)) return i;
        return null;
    }

    /// What the ledger says the entry does: the record it leaves, or that
    /// it is refused.
    const Want = union(enum) { refused, held: Held };

    fn predict(self: *const WorkflowDefs, kind: Kind, w: usize, value: []const u8, stamp_ms: i64, index: u64) ?Want {
        const now = self.ledger[w];
        switch (kind) {
            .create => {
                const v = versionIndex(value) orelse return null;
                return .{ .held = .{ .version = v, .enabled = if (now) |h| h.enabled else true, .anchor_ms = stamp_ms, .index = index } };
            },
            .enable, .disable => {
                var h = now orelse return .refused;
                const enable = kind == .enable;
                if (h.enabled == enable) return .{ .held = h };
                if (enable) h.anchor_ms = stamp_ms;
                h.enabled = enable;
                return .{ .held = h };
            },
        }
    }

    /// Apply `e` as the shard does, then check it against the ledger. A
    /// refusal must leave the family as it was.
    pub fn apply(self: *WorkflowDefs, e: *const entry_mod.Entry) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
        const w = nameIndex(cmd.key) orelse return error.Halt;
        const kind: Kind = if (@as(EntryType, @fromBackingInt(e.header.entry_type)) == .workflow_create) .create else if (cmd.value.len == 1 and cmd.value[0] == 1) .enable else .disable;
        const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
        const want = self.predict(kind, w, cmd.value, stamp_ms, e.header.index) orelse return error.Halt;
        const before = self.store.families[dm.DefFam.ID].commits;
        var out: persistence.Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&dm.machine, &self.store, &self.scratch, e, &out);
        var effect: Effect = .{ .kind = kind, .refused = out.refused() != null };
        // Past its share a create is refused whatever it asks: the memory
        // checker judges that verdict (`memory_gate.zig`).
        if (out.refused()) |rf| if (rf.refusal.reason == .memory_budget) {
            effect.memory = true;
            if (before != self.store.families[dm.DefFam.ID].commits) return error.RefusalChangedState;
            if (kind != .create) effect.broke = "a definition's enable was refused for memory";
            return effect;
        };
        if (effect.refused) {
            if (before != self.store.families[dm.DefFam.ID].commits) return error.RefusalChangedState;
            if (want != .refused) effect.broke = "the definitions machine refused an entry the ledger applies";
        } else if (want == .refused) effect.broke = "the definitions machine applied an entry the ledger refuses";
        switch (want) {
            .refused => {},
            .held => |h| self.ledger[w] = h,
        }
        if (effect.broke == null) effect.broke = self.rule();
        return effect;
    }

    /// The records the store holds against the ledger's.
    fn rule(self: *WorkflowDefs) ?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = &self.store };
        for (NAMES, self.ledger) |name, want| {
            var kb: [16]u8 = undefined;
            const got = dm.get(rv, view_mod.idKey(&kb, NS, name) catch unreachable);
            const h = want orelse {
                if (got != null) return "a workflow the ledger doesn't hold is stored";
                continue;
            };
            const g = got orelse return "a workflow the ledger holds is gone";
            if (!std.mem.eql(u8, g.version, VERSIONS[h.version]) or g.created_index != h.index) return "a workflow's definition is not its last create's";
            if ((g.enabled != 0) != h.enabled) return "whether a workflow is enabled is not the ledger's";
            if (g.anchor_ms != h.anchor_ms) return "a workflow's schedule anchor is not the ledger's";
        }
        return null;
    }

    pub fn checksums(self: *WorkflowDefs) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

/// A definition entry, as its handler proposes it: creates (redefinitions
/// among them), disables and enables, some of a workflow never created.
pub fn make(r: std.Random, buf: []u8) ?Made {
    const roll = r.uintLessThan(u8, 100);
    const w = if (roll < 45) r.uintLessThan(usize, NEVER_CREATED) else r.uintLessThan(usize, NAMES.len);
    var kb: [16]u8 = undefined;
    const key = view_mod.idKey(&kb, NS, NAMES[w]) catch unreachable;
    const etype: EntryType, const value: []const u8 = if (roll < 45)
        .{ .workflow_create, YAMLS[r.uintLessThan(usize, YAMLS.len)] }
    else if (roll < 75)
        .{ .workflow_enabled, &.{0} }
    else
        .{ .workflow_enabled, &.{1} };
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = NS, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
    const n = cmd.serialize(buf) orelse return null;
    return .{ .entry_type = etype, .payload = buf[0..n] };
}
