//! The workflow definitions machine: each workflow as its latest create
//! made it, and whether it is enabled.
//!
//! - `workflow.def`, keyed (namespace id, workflow name): its name, version,
//!   definition and created-at stamp, the idempotency mode it asks of a
//!   start, whether it is enabled, and the stamp its schedule counts slots
//!   from (`anchor_ms`): its create's, or the last enable that followed a
//!   disable. A redefinition keeps whether it is enabled.
//!
//! Its schedule and stream trigger are not here: they are node memory,
//! built from this record (`WorkflowHandler.syncProducers`), and only a
//! ready leader fires them.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const parser = @import("parser.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;
const Stored = delivery.Stored;
const MAX_PERSIST_PAYLOAD = persistence.MAX_PERSIST_PAYLOAD;

const log = @import("stdx").log;
const plant = @import("../plant.zig");

pub const Def = Record(&.{
    .{ .name = "name", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "version", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "yaml", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "created_at_ms", .T = Int(i64) },
    // The log index of the create that wrote it: a producer built from an
    // older one is built again.
    .{ .name = "created_index", .T = Int(u64) },
    .{ .name = "idempotency", .T = Int(u8) },
    // Its trigger reads a stream in its own namespace, whatever the
    // definition names (the create's `OWN_NAMESPACE` flag).
    .{ .name = "own_namespace", .T = Int(u8) },
    .{ .name = "enabled", .T = Int(u8) },
    .{ .name = "anchor_ms", .T = Int(i64) },
});

pub const DefFam = view_mod.Family(27, "workflow.def", .id, Stored(Def));

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

/// A definition key's namespace and name: `id ‖ name`, as `idKey` lays it.
const KEY_PREFIX = @sizeOf(u32);

fn keyNamespace(k: []const u8) NamespaceId {
    return .of(std.mem.readInt(u32, k[0..KEY_PREFIX], .big));
}

pub fn get(read: ReadView, def_key: []const u8) ?Def.Value {
    const got = read.get(DefFam, def_key) catch unreachable; // written by this machine alone
    return if (got) |s| s.v else null;
}

/// The entry's command, whose key leads with the namespace it is stamped
/// with; null for one that doesn't.
fn command(e: *const Entry) ?entry_mod.CommandPayload {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
    if (cmd.key.len <= KEY_PREFIX or keyNamespace(cmd.key) != cmd.namespace_id) return null;
    return cmd;
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    switch (typeOf(e)) {
        .workflow_create, .workflow_enabled => {
            const cmd = command(e) orelse return;
            try out.addIn(DefFam.ID, keyNamespace(cmd.key), cmd.key[KEY_PREFIX..]);
        },
        else => {},
    }
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = command(e) orelse return error.Halt;
    const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
    const prev = try view.get(DefFam, cmd.key);
    switch (typeOf(e)) {
        .workflow_create => {
            // Its runs' history names it as their owner: a longer name is
            // refused, as its handler refuses it, never stored.
            if (cmd.key.len - KEY_PREFIX > @import("../protocol/history_entries.zig").MAX_OWNER) return out.refuse(.bad_request, .unclassified, "a workflow name is longer than its runs' history may name");
            out.putBytes(cmd.key[KEY_PREFIX..]);
            // Its proposer parsed it; one that won't parse here breaks the
            // log, and halts on every replica alike.
            var diag: parser.Diagnostic = .{};
            var def = parser.parseWorkflow(view.arena, cmd.value, &diag) catch |err| {
                if (err == error.OutOfMemory) return error.OutOfMemory;
                log.err("workflow definition at index {d} doesn't parse: {s}", .{ e.header.index, diag.message() });
                return error.Halt;
            };
            defer def.deinit(view.arena);
            try view.put(DefFam, cmd.key, .{ .v = .{
                .name = cmd.key[KEY_PREFIX..],
                .version = def.version,
                .yaml = cmd.value,
                .created_at_ms = stamp_ms,
                .created_index = e.header.index,
                .idempotency = @backingInt(def.idempotency),
                .own_namespace = @intFromBool(e.header.flags & entry_mod.Flags.OWN_NAMESPACE != 0),
                .enabled = if (comptime plant.active == .defs_redefine_enables) 1 else if (prev) |p| p.v.enabled else 1,
                .anchor_ms = stamp_ms,
            } });
        },
        .workflow_enabled => {
            if (cmd.value.len != 1 or cmd.value[0] > 1) return error.Halt;
            // The definition may have gone since its enable was proposed.
            var rec = (prev orelse return out.refuse(.not_found, .not_found, "workflow not found")).v;
            const enable = cmd.value[0] == 1;
            const moves_always = comptime plant.active == .defs_enable_moves_anchor;
            if (@intFromBool(enable) == rec.enabled and !(moves_always and enable)) return;
            // Enabled again, its schedule counts slots from now: those
            // missed while disabled don't fire.
            if (enable) rec.anchor_ms = stamp_ms;
            rec.enabled = @intFromBool(enable);
            try view.put(DefFam, cmd.key, .{ .v = rec });
        },
        else => return error.Halt,
    }
}

pub const machine: machine_mod.Machine = .{
    .name = "workflow definitions",
    .entry_types = &.{ .workflow_create, .workflow_enabled },
    .families = &.{machine_mod.info(DefFam)},
    .keys = keys,
    .apply = apply,
};

// ─── Tests ───────────────────────────────────────────────────────────────────

const testing = std.testing;
const Store = @import("../contract/store.zig").Store;

const Fixture = struct {
    store: Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init() Fixture {
        var f: Fixture = .{ .store = Store.init(testing.allocator), .scratch = .init(testing.allocator) };
        f.store.strict_keys = true;
        return f;
    }

    fn deinit(f: *Fixture) void {
        f.store.deinit();
        f.scratch.deinit();
    }

    fn apply(f: *Fixture, t: EntryType, name: []const u8, value: []const u8, stamp_ms: u64) !Result {
        var kb: [600]u8 = undefined;
        const k = view_mod.idKey(&kb, .default, name) catch unreachable;
        var pbuf: [2048]u8 = undefined;
        f.index += 1;
        const e = entry_mod.buildCommandEntry(t, entry_mod.Flags.NONE, 1, f.index, stamp_ms * std.time.ns_per_ms, .default, k, value, &pbuf).?;
        var out = Result.start(&e);
        try machine_mod.applyEntry(&machine, &f.store, &f.scratch, &e, &out);
        return out;
    }

    fn def(f: *Fixture, name: []const u8) ?Def.Value {
        var kb: [600]u8 = undefined;
        return get(.{ .store = &f.store }, view_mod.idKey(&kb, .default, name) catch unreachable);
    }
};

fn yamlOf(comptime version: []const u8) []const u8 {
    return "kind: Workflow\nname: wf\nversion: " ++ version ++ "\nstart:\n  wait_for_signal:\n    type: never\n  transitions:\n    success: flo.Completed\n";
}

test "workflow definitions machine: a create stores the definition, anchored at its stamp; a redefinition keeps whether it is enabled" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.apply(.workflow_create, "wf", yamlOf("1.0.0"), 1000);
    var d = f.def("wf").?;
    try testing.expectEqualStrings("wf", d.name);
    try testing.expectEqualStrings("1.0.0", d.version);
    try testing.expectEqual(@as(i64, 1000), d.created_at_ms);
    try testing.expectEqual(@as(u64, 1), d.created_index);
    try testing.expectEqual(@as(u8, 1), d.enabled);
    try testing.expectEqual(@as(i64, 1000), d.anchor_ms);

    _ = try f.apply(.workflow_enabled, "wf", &.{0}, 2000);
    _ = try f.apply(.workflow_create, "wf", yamlOf("2.0.0"), 3000);
    d = f.def("wf").?;
    try testing.expectEqualStrings("2.0.0", d.version);
    try testing.expectEqual(@as(u64, 3), d.created_index);
    try testing.expectEqual(@as(u8, 0), d.enabled);
    try testing.expectEqual(@as(i64, 3000), d.anchor_ms);
}

test "workflow definitions machine: only an enable after a disable moves the anchor, and one of a missing workflow is refused, writing nothing" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.apply(.workflow_create, "wf", yamlOf("1.0.0"), 1000);
    _ = try f.apply(.workflow_enabled, "wf", &.{1}, 1500);
    try testing.expectEqual(@as(i64, 1000), f.def("wf").?.anchor_ms);
    _ = try f.apply(.workflow_enabled, "wf", &.{0}, 2000);
    _ = try f.apply(.workflow_enabled, "wf", &.{0}, 2500);
    try testing.expectEqual(@as(i64, 1000), f.def("wf").?.anchor_ms);
    _ = try f.apply(.workflow_enabled, "wf", &.{1}, 4000);
    try testing.expectEqual(@as(i64, 4000), f.def("wf").?.anchor_ms);
    try testing.expectEqual(@as(u8, 1), f.def("wf").?.enabled);

    const commits = f.store.families[DefFam.ID].commits;
    const gone = try f.apply(.workflow_enabled, "nope", &.{1}, 5000);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.not_found, gone.refused().?.status);
    try testing.expectEqual(commits, f.store.families[DefFam.ID].commits);
    try testing.expect(f.def("nope") == null);
}

test "workflow definitions machine: a definition that won't parse, or a malformed enable, halts and leaves the record" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.apply(.workflow_create, "wf", yamlOf("1.0.0"), 1000);
    try testing.expectError(error.Halt, f.apply(.workflow_create, "wf", "kind: [", 2000));
    try testing.expectError(error.Halt, f.apply(.workflow_enabled, "wf", &.{2}, 2000));
    try testing.expectError(error.Halt, f.apply(.workflow_enabled, "wf", "", 2000));
    try testing.expectEqualStrings("1.0.0", f.def("wf").?.version);
}

test "workflow definitions machine: a workflow named longer than its runs' history may name is refused, writing nothing" {
    var f = Fixture.init();
    defer f.deinit();
    var name: [@import("../protocol/history_entries.zig").MAX_OWNER + 1]u8 = undefined;
    @memset(&name, 'n');
    try testing.expect((try f.apply(.workflow_create, name[0 .. name.len - 1], yamlOf("1.0.0"), 1)).refused() == null);
    try testing.expect(f.def(name[0 .. name.len - 1]) != null);
    try testing.expect((try f.apply(.workflow_create, &name, yamlOf("1.0.0"), 1)).refused() != null);
    try testing.expect(f.def(&name) == null);
}

test "workflow definitions machine: past its shard's share a new definition is refused, writing nothing, and a replace that shrinks one lands" {
    const pool = @import("../budget/pool.zig");
    const settings = @import("../settings/machine.zig");
    var f = Fixture.init();
    defer f.deinit();
    const long = comptime yamlOf("1.0.0") ++ "# " ++ @as([512]u8, @splat('x')) ++ "\n";
    try testing.expect((try f.apply(.workflow_create, "wf", long, 1000)).refused() == null);
    try settings.bindForTest(&f.store, pool.useOf(&f.store));
    const use = pool.useOf(&f.store);
    const r = try f.apply(.workflow_create, "other", yamlOf("1.0.0"), 2000);
    try testing.expectEqual(@import("../protocol/reason.zig").Reason.memory_budget, r.refused().?.refusal.reason);
    try testing.expect(f.def("other") == null);
    try testing.expectEqual(use, pool.useOf(&f.store));
    // Replaced by a shorter one: it frees, so it lands.
    try testing.expect((try f.apply(.workflow_create, "wf", yamlOf("2.0.0"), 3000)).refused() == null);
    try testing.expectEqualStrings("2.0.0", f.def("wf").?.version);
    try testing.expect(pool.useOf(&f.store) < use);
}
