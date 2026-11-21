//! Cluster-settings machine: the memory-budget policy and per-node RAM,
//! committed on shard 0. One record holds `pool_share`; one row per node,
//! keyed by node id, holds its `{ram_bytes, reserve}`. The node-level budget is
//! derived from these rows joined with the data-holding membership at apply
//! (`node/memory_budget.zig`); this machine only keeps the rows. Every shard
//! also keeps the share of that budget its own leader bound (`settings.bind`),
//! which the pool is held to. A settings entry never charges the pool
//! (`entry.zig hasKeyValue`).
//!
//! These are the first node-level (not per-namespace) families: their keys are
//! built with an empty namespace, so the composite key is just a length byte
//! and the bare key.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const settings_entries = @import("../protocol/settings_entries.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;
const Stored = delivery.Stored;

// ─── Stored records ────────────────────────────────────────────────────────

/// The committed policy: the share of each node's RAM-after-reserve the pool
/// may use, in per-mille (700 = 70%). One record, under `POLICY_KEY`.
pub const Policy = Record(&.{
    .{ .name = "share_permille", .T = Int(u16) },
});

/// One node's figures, keyed by node id.
pub const NodeMem = Record(&.{
    .{ .name = "ram_bytes", .T = Int(u64) },
    .{ .name = "reserve", .T = Int(u64) },
});

/// The last non-provisional budget, committed so a replica restarted while the
/// budget is frozen reconstructs the held value from the log. One record, under
/// `BUDGET_KEY`. `governing_node` is 0 for none (node ids are ≥ 1).
pub const BudgetClean = Record(&.{
    .{ .name = "budget_bytes", .T = Int(u64) },
    .{ .name = "governing_node", .T = Int(u32) },
});

/// The share this shard holds, as its leader last bound it. Absent until the
/// first bind applies: no share is enforced then.
pub const Bind = Record(&.{
    .{ .name = "share_bytes", .T = Int(u64) },
    .{ .name = "budget_bytes", .T = Int(u64) },
});

pub const PolicyFam = view_mod.Family(50, "settings.policy", .shard, Stored(Policy));
pub const NodeFam = view_mod.Family(51, "settings.node", .shard, Stored(NodeMem));
pub const BudgetFam = view_mod.Family(52, "settings.budget", .shard, Stored(BudgetClean));
// Its own family: `BudgetFam` decodes every key there as a `BudgetClean`.
pub const BindFam = view_mod.Family(68, "settings.bind", .shard, Stored(Bind));

/// The single key the policy record lives under.
pub const POLICY_KEY = "share";
/// The single key the clean-budget record lives under.
pub const BUDGET_KEY = "clean";
/// The single key the bound share lives under.
pub const BIND_KEY = "share";

// ─── Wire payloads (the entry values a proposer encodes) ───────────────────
// The logged layouts live in `protocol/settings_entries.zig`, where they are
// pinned against the data format; the machine proposes and decodes them here.

pub const SetShare = settings_entries.Share;
pub const SetNodeMem = settings_entries.NodeMem;
pub const DelNode = settings_entries.DelNode;
pub const SetBudgetClean = settings_entries.BudgetClean;
pub const SetBudgetBind = settings_entries.BudgetBind;

// ─── Keys ──────────────────────────────────────────────────────────────────

/// A node row's composite key: an empty namespace, then the id little-endian.
pub fn nodeKey(buf: *[1 + 4]u8, id: u32) []const u8 {
    var idb: [4]u8 = undefined;
    std.mem.writeInt(u32, &idb, id, .little);
    return view_mod.key(buf, "", &idb) catch unreachable;
}

fn policyKey(buf: *[1 + POLICY_KEY.len]u8) []const u8 {
    return view_mod.key(buf, "", POLICY_KEY) catch unreachable;
}

fn budgetKey(buf: *[1 + BUDGET_KEY.len]u8) []const u8 {
    return view_mod.key(buf, "", BUDGET_KEY) catch unreachable;
}

fn bindKey(buf: *[1 + BIND_KEY.len]u8) []const u8 {
    return view_mod.key(buf, "", BIND_KEY) catch unreachable;
}

/// The share bound on the store `read` reads, or null when none is.
pub fn boundShare(read: view_mod.ReadView) ?Bind.Value {
    var kb: [1 + BIND_KEY.len]u8 = undefined;
    const rec = (read.get(BindFam, bindKey(&kb)) catch null) orelse return null;
    return rec.v;
}

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    switch (typeOf(e)) {
        .settings_pool_share => try out.add(PolicyFam.ID, "", POLICY_KEY),
        .settings_node_mem => {
            var diag: schema.Diagnostic = .{};
            const p = SetNodeMem.decodeAll(e.payload, &diag) catch return;
            var idb: [4]u8 = undefined;
            std.mem.writeInt(u32, &idb, p.node_id, .little);
            try out.add(NodeFam.ID, "", &idb);
        },
        .settings_node_del => {
            var diag: schema.Diagnostic = .{};
            const p = DelNode.decodeAll(e.payload, &diag) catch return;
            var idb: [4]u8 = undefined;
            std.mem.writeInt(u32, &idb, p.node_id, .little);
            try out.add(NodeFam.ID, "", &idb);
        },
        .settings_budget_clean => try out.add(BudgetFam.ID, "", BUDGET_KEY),
        .settings_budget_bind => try out.add(BindFam.ID, "", BIND_KEY),
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────────

const BAD = "bad request: malformed cluster-settings entry";

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    switch (typeOf(e)) {
        .settings_pool_share => {
            const p = SetShare.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, BAD);
            // A share is a fraction of RAM: at most 1000 per-mille (100%).
            if (p.share_permille > 1000) return out.refuse(.bad_request, .malformed, "bad request: pool share over 1000 per-mille");
            var kb: [1 + POLICY_KEY.len]u8 = undefined;
            try view.put(PolicyFam, policyKey(&kb), .{ .v = .{ .share_permille = p.share_permille } });
        },
        .settings_node_mem => {
            const p = SetNodeMem.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, BAD);
            var kb: [1 + 4]u8 = undefined;
            try view.put(NodeFam, nodeKey(&kb, p.node_id), .{ .v = .{ .ram_bytes = p.ram_bytes, .reserve = p.reserve } });
        },
        .settings_node_del => {
            const p = DelNode.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, BAD);
            var kb: [1 + 4]u8 = undefined;
            try view.delete(NodeFam, nodeKey(&kb, p.node_id));
        },
        .settings_budget_clean => {
            const p = SetBudgetClean.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, BAD);
            var kb: [1 + BUDGET_KEY.len]u8 = undefined;
            try view.put(BudgetFam, budgetKey(&kb), .{ .v = .{ .budget_bytes = p.budget_bytes, .governing_node = p.governing_node } });
        },
        .settings_budget_bind => {
            const p = SetBudgetBind.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, BAD);
            if (p.share_bytes == 0) return out.refuse(.bad_request, .malformed, "bad request: a memory share of 0 bytes");
            var kb: [1 + BIND_KEY.len]u8 = undefined;
            try view.put(BindFam, bindKey(&kb), .{ .v = .{ .share_bytes = p.share_bytes, .budget_bytes = p.budget_bytes } });
        },
        else => {},
    }
}

pub const machine: machine_mod.Machine = .{
    .name = "settings",
    .entry_types = &.{ .settings_pool_share, .settings_node_mem, .settings_node_del, .settings_budget_clean, .settings_budget_bind },
    .families = &.{ machine_mod.info(PolicyFam), machine_mod.info(NodeFam), machine_mod.info(BudgetFam), machine_mod.info(BindFam) },
    .keys = keys,
    .apply = apply,
};
