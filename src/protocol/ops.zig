//! The op table's types: what an op declaration holds, and the comptime
//! checks every table passes. An op is declared once, here; the server
//! binds a handler to it by name, the CLI and SDKs are generated from it,
//! and its request and response are schemas (schema.zig), so nothing else
//! describes its bytes.
//!
//! This file imports nothing from the server, so the CLI, the Zig SDK and
//! the generator can read the table without compiling the server.

const std = @import("std");
const schema = @import("schema.zig");
const reason_mod = @import("reason.zig");

/// Who may run an op: reading, writing, creating a namespace, acting on the
/// node, or administering it. The edge authorizes a principal on this.
pub const Class = enum { read, write, ns_create, node, admin };

/// Whether a client may send the op, or only a peer or another shard.
/// An internal op on the client port is answered exactly like an unknown one.
pub const Origin = enum { client, internal };

/// Whether the op names a namespace (resolved at the edge) or acts on the
/// server as a whole.
pub const Scope = enum { namespace, server };

/// How a request finds the shard that runs it.
pub const Route = union(enum) {
    /// The request's routing key if it has one, else its key.
    key,
    /// A decoded request field (never a namespace field). An optional one
    /// that's absent leaves the request on the connection's shard.
    field: []const u8,
    /// The partition bits in a run id; an id that doesn't decode is refused.
    run_id,
    /// The connection's own shard.
    local,
    /// Shard 0.
    shard0,
};

/// Whether the op runs on one shard or every one.
pub const Fanout = enum {
    none,
    /// Every shard, answers merged.
    walk,
    /// Any shard with a claimable item (an action await).
    any_shard_claim,
};

/// An op that may block: the request field that sets how long, its default
/// and its cap. The refusal text and `--help` render these values.
pub const Wait = struct {
    field: []const u8,
    default_ms: u64,
    max_ms: u64,
};

/// The envelope key as a positional argument.
pub const KeyArg = struct {
    /// Its name in usage and errors.
    name: []const u8 = "key",
    /// May be left out (an empty key).
    optional: bool = false,
    doc: []const u8 = "The key",
};

/// A short flag: `-letter` for the field's flag.
pub const Short = struct { field: []const u8, letter: u8 };

/// How the CLI exposes an op.
pub const Cli = struct {
    /// `command`: generated from this table. `hand`: a hand-written
    /// command, one that sends more than one request or in a family not
    /// generated yet; it still encodes through the op. `none`: a person
    /// never types it (worker and task ops).
    expose: enum { command, hand, none } = .command,
    /// The envelope key as a positional argument, or null when the command
    /// sends an empty key.
    key: ?KeyArg = .{},
    /// Request fields taken as positional arguments after the key, in
    /// order. A list field takes the rest of the arguments.
    args: []const []const u8 = &.{},
    /// Short flags, field name → letter.
    short: []const Short = &.{},
    /// A field read from a file or stdin rather than an argument.
    from_file: ?[]const u8 = null,
    /// How the answer prints without `-o`: `record`, one `name: value`
    /// line per field; `table`, a list field as rows; `raw`, the `show`
    /// field's bytes alone.
    render: enum { table, record, raw } = .record,
    /// The response field `raw` prints.
    show: ?[]const u8 = null,
    /// A boolean response field whose false exits 1, as not found does.
    absent: ?[]const u8 = null,
    /// Opaque byte fields (a cursor) a person reads and gives back as hex,
    /// in the request and the response alike.
    hex: []const []const u8 = &.{},
    examples: []const []const u8 = &.{},
};

/// One op. Every column but `name`, `code`, `class`, `request`, `response`
/// and `doc` has a default, so an ordinary op is a few lines.
pub const Op = struct {
    /// e.g. "kv.set": the SDK method stem and the CLI verb path, so it is
    /// user vocabulary.
    name: []const u8,
    /// Stable, inside its family's range; assigned once, never reused.
    code: u16,
    class: Class,
    origin: Origin = .client,
    scope: Scope = .namespace,
    /// Goes to the leader. Defaults from the class.
    writes: ?bool = null,
    /// May create its namespace; needs `writes`.
    creates: bool = false,
    /// The answer carries leased or claimed items, so it isn't dropped at
    /// the ordinary deadline.
    takes_items: bool = false,
    wait: ?Wait = null,
    fanout: Fanout = .none,
    route: Route = .key,
    /// The request field naming a transaction. When it is present the
    /// request goes wherever that transaction lives, its pinned shard's
    /// leader, whatever the op's class: a read in a transaction sees the
    /// transaction's writes.
    txn: ?[]const u8 = null,
    /// The request body: a `schema.Record`. The namespace and key are the
    /// envelope's, so they aren't listed.
    request: type,
    /// The success body: a `schema.Record`.
    response: type,
    cli: Cli = .{},
    doc: []const u8,

    /// Whether it goes to the leader: as declared, else from its class.
    pub fn isWrite(comptime op: Op) bool {
        return op.writes orelse switch (op.class) {
            .write, .ns_create, .admin => true,
            .read, .node => false,
        };
    }
};

/// The op-code families: each family's ops take codes inside its range.
pub const Family = enum {
    namespace,
    cluster,
    kv,
    stream,
    queue,
    ts,
    action,
    worker,
    workflow,
    processing,

    pub fn range(f: Family) struct { lo: u16, hi: u16 } {
        return switch (f) {
            .namespace => .{ .lo = 0x010, .hi = 0x02F },
            .cluster => .{ .lo = 0x030, .hi = 0x04F },
            .kv => .{ .lo = 0x100, .hi = 0x12F },
            .stream => .{ .lo = 0x140, .hi = 0x17F },
            .queue => .{ .lo = 0x180, .hi = 0x19F },
            .ts => .{ .lo = 0x1A0, .hi = 0x1BF },
            .action => .{ .lo = 0x300, .hi = 0x31F },
            .worker => .{ .lo = 0x320, .hi = 0x33F },
            .workflow => .{ .lo = 0x340, .hi = 0x37F },
            .processing => .{ .lo = 0x380, .hi = 0x3BF },
        };
    }

    /// The family an op name belongs to: the part before its first dot.
    pub fn of(comptime name: []const u8) ?Family {
        const dot = std.mem.indexOfScalar(u8, name, '.') orelse return null;
        return std.meta.stringToEnum(Family, name[0..dot]);
    }
};

const proto = @import("proto.zig");

/// The most a request body may hold: the frame cap less its header and the
/// largest namespace and key envelope.
pub const MAX_REQUEST_BODY: usize = proto.MAX_REQUEST_BYTES - @sizeOf(proto.RequestHeader) - (2 + schema.MAX_NAMESPACE_NAME) - (2 + MAX_KEY);

/// The longest key an envelope carries.
pub const MAX_KEY: usize = @import("limits.zig").MAX_QUALIFIED_KEY;

/// The most a success body may hold: an answer less its header.
pub const MAX_RESPONSE_BODY: usize = proto.MAX_ANSWER_BYTES - @sizeOf(proto.ResponseHeader);

/// Checks a table at comptime, failing the build with a message naming the
/// op and what's wrong. Call it once on every table.
pub fn check(comptime table: []const Op) void {
    if (comptime tableRule(table)) |message| @compileError(message);
}

/// What's wrong with a table, or null: `check`'s rules, callable from tests.
pub fn tableRule(comptime table: []const Op) ?[]const u8 {
    @setEvalBranchQuota(100_000);
    for (table, 0..) |op, i| {
        if (opRule(op, table[0..i])) |message| return "op '" ++ op.name ++ "': " ++ message;
    }
    return null;
}

fn opRule(comptime op: Op, comptime before: []const Op) ?[]const u8 {
    if (nameRule(op.name)) |m| return m;
    const family = Family.of(op.name) orelse return "its name names no family (e.g. kv.set)";
    const r = family.range();
    if (op.code < r.lo or op.code > r.hi) return std.fmt.comptimePrint("code 0x{x} is outside its family's 0x{x}-0x{x}", .{ op.code, r.lo, r.hi });
    for (before) |other| {
        if (other.code == op.code) return std.fmt.comptimePrint("code 0x{x} is also '{s}'", .{ op.code, other.name });
        if (std.mem.eql(u8, other.name, op.name)) return "declared twice";
    }
    if (op.creates and !op.isWrite()) return "creates its namespace but doesn't write";
    if (op.request.kind != .record) return "request is not a schema.Record";
    if (op.response.kind != .record) return "response is not a schema.Record";
    if (schema.fitsRule(op.request, MAX_REQUEST_BODY, "request")) |m| return m;
    if (schema.fitsRule(op.response, MAX_RESPONSE_BODY, "response")) |m| return m;
    switch (op.route) {
        .field => |name| {
            const f = fieldOf(op.request, name) orelse return "routes on field '" ++ name ++ "', which its request doesn't have";
            if (f.T.kind == .namespace) return "routes on namespace field '" ++ name ++ "'; a namespace never picks a shard";
        },
        else => {},
    }
    if (op.wait) |w| {
        const f = fieldOf(op.request, w.field) orelse return "waits on field '" ++ w.field ++ "', which its request doesn't have";
        const T = if (@hasDecl(f.T, "Inner")) f.T.Inner else f.T;
        if (!@hasDecl(T, "hi") or T.hi != w.max_ms) return "wait field '" ++ w.field ++ "' must be a DurationMs capped at the wait's max_ms";
        if (w.default_ms > w.max_ms) return "wait default is past its cap";
        if (f.T.kind == .defaulted and f.T.default_value != w.default_ms) return std.fmt.comptimePrint("wait default {d} ms disagrees with field '{s}', which defaults to {d}", .{ w.default_ms, w.field, f.T.default_value });
    }
    if (op.fanout == .walk and op.route != .key) return "a walk runs on every shard, so it has no route";
    if (op.txn) |name| {
        const f = fieldOf(op.request, name) orelse return "names transaction field '" ++ name ++ "', which its request doesn't have";
        const T = if (f.T.kind == .optional) f.T.Inner else f.T;
        if (T.kind != .integer or T.Value != u64) return "transaction field '" ++ name ++ "' must be a u64";
        if (op.fanout != .none) return "a transaction lives on one shard, so a walk can't take one";
    }
    if (op.doc.len == 0) return "has no doc line";
    if (cliRule(op)) |m| return m;
    return null;
}

/// What's wrong with an op's `cli` column, or null. Every field it names
/// exists, and a generated command can take every request field: one it
/// doesn't take positionally becomes a flag, which a list can't be.
fn cliRule(comptime op: Op) ?[]const u8 {
    const c = op.cli;
    for (c.args, 0..) |name, i| {
        const f = fieldOf(op.request, name) orelse return "cli takes field '" ++ name ++ "' positionally, which its request doesn't have";
        if (f.T.kind == .list and i != c.args.len - 1) return "cli list argument '" ++ name ++ "' must be the last";
    }
    for (c.short, 0..) |s, i| {
        if (fieldOf(op.request, s.field) == null) return "cli short flag for '" ++ s.field ++ "', which its request doesn't have";
        for (c.short[0..i]) |o| if (o.letter == s.letter) return "cli short flag letter used twice";
    }
    if (c.from_file) |name| if (fieldOf(op.request, name) == null) return "cli reads field '" ++ name ++ "' from a file, which its request doesn't have";
    if (c.show) |name| if (fieldOf(op.response, name) == null) return "cli shows field '" ++ name ++ "', which its response doesn't have";
    if (c.render == .raw and c.show == null) return "cli renders raw but shows no field";
    if (c.absent) |name| {
        const f = fieldOf(op.response, name) orelse return "cli exits absent on field '" ++ name ++ "', which its response doesn't have";
        if (f.T.kind != .boolean) return "cli absent field '" ++ name ++ "' must be a bool";
    }
    for (c.hex) |name| {
        const f = fieldOf(op.request, name) orelse (fieldOf(op.response, name) orelse return "cli shows field '" ++ name ++ "' as hex, which neither its request nor response has");
        const T = if (f.T.kind == .optional or f.T.kind == .defaulted) f.T.Inner else f.T;
        if (T.kind != .bytes) return "cli hex field '" ++ name ++ "' must be bytes";
    }
    const command = commandOf(op.name);
    for (c.examples) |ex| {
        if (!std.mem.startsWith(u8, ex, command) or (ex.len > command.len and ex[command.len] != ' '))
            return "cli example '" ++ ex ++ "' doesn't run '" ++ command ++ "'";
    }
    if (c.expose == .command) for (op.request.field_list) |f| {
        if (f.T.kind == .list and !isArg(c, f.name)) return "field '" ++ f.name ++ "' is a list, so the command must take it positionally";
        if (f.doc.len == 0) return "field '" ++ f.name ++ "' has no doc, which its help line needs";
    };
    return null;
}

/// The command a person types for an op: `flo`, its family, its group if it
/// has one, and its verb (`kv.txn.begin` is `flo kv txn begin`).
pub fn commandOf(comptime name: []const u8) []const u8 {
    comptime {
        var out: []const u8 = "flo";
        var parts = std.mem.splitScalar(u8, name, '.');
        while (parts.next()) |part| out = out ++ " " ++ dashed(part);
        return out;
    }
}

fn isArg(comptime c: Cli, comptime name: []const u8) bool {
    for (c.args) |a| if (std.mem.eql(u8, a, name)) return true;
    return false;
}

fn fieldOf(comptime Schema: type, comptime name: []const u8) ?schema.Field {
    for (Schema.field_list) |f| if (std.mem.eql(u8, f.name, name)) return f;
    return null;
}

fn nameRule(comptime name: []const u8) ?[]const u8 {
    for (name) |c| if (!(std.ascii.isLower(c) or std.ascii.isDigit(c) or c == '_' or c == '.')) return "its name must be lower case: [a-z0-9_.]";
    const dots = std.mem.count(u8, name, ".");
    if (dots < 1 or dots > 2) return "its name is family.verb or family.group.verb";
    var parts = std.mem.splitScalar(u8, name, '.');
    while (parts.next()) |part| if (part.len == 0) return "its name has an empty part";
    return null;
}

/// A group of ops one level below a family (`kv.txn` holds `kv.txn.begin`
/// and its siblings): its CLI subcommand and help line.
pub const Group = struct {
    /// e.g. "kv.txn".
    name: []const u8,
    doc: []const u8,
};

/// An op's group, `family.group`, if its name has one.
pub fn groupOf(comptime name: []const u8) ?[]const u8 {
    const last = std.mem.lastIndexOfScalar(u8, name, '.') orelse return null;
    if (std.mem.indexOfScalar(u8, name, '.') == last) return null;
    return name[0..last];
}

/// The verb a person types for an op: the last part of its name, `_` as `-`.
pub fn verbOf(comptime name: []const u8) []const u8 {
    const last = std.mem.lastIndexOfScalar(u8, name, '.').?;
    return dashed(name[last + 1 ..]);
}

/// `name` with each `_` as `-`: a field's flag, or an op's verb.
pub fn dashed(comptime name: []const u8) []const u8 {
    comptime {
        var out: [name.len]u8 = undefined;
        for (name, 0..) |c, i| out[i] = if (c == '_') '-' else c;
        const final = out;
        return &final;
    }
}

/// Checks that every grouped op in `table` has its group declared, and that
/// every group declared holds an op.
pub fn checkGroups(comptime table: []const Op, comptime groups: []const Group) void {
    for (table) |op| if (groupOf(op.name)) |g| {
        for (groups) |d| {
            if (std.mem.eql(u8, d.name, g)) break;
        } else @compileError("op '" ++ op.name ++ "': its group '" ++ g ++ "' isn't declared");
    };
    for (groups) |d| {
        for (table) |op| {
            if (groupOf(op.name)) |g| if (std.mem.eql(u8, g, d.name)) break;
        } else @compileError("group '" ++ d.name ++ "' holds no op");
        if (d.doc.len == 0) @compileError("group '" ++ d.name ++ "' has no doc line");
    }
}

/// The op a code names, if the table has one.
pub fn byCode(comptime table: []const Op, code: u16) ?usize {
    inline for (table, 0..) |op, i| if (op.code == code) return i;
    return null;
}

// ── The table hash ──────────────────────────────────────────────────────

/// A hash of everything a peer or client must agree on: every op's name,
/// code, flags, route and wait, the layout of its request and response
/// (types, bounds, defaults, modes, parsers), every entry payload's layout,
/// and the refusal reasons a client decodes. A change to any of them, or
/// reverting one, changes it, so a client or node built from another table
/// is refused, not misread.
pub fn tableHash(comptime ops: []const Op, comptime entries: anytype) u64 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var h = std.hash.Wyhash.init(0x666c6f2d6f707321); // "flo-ops!"
        hashEnum(&h, reason_mod.Reason);
        hashEnum(&h, reason_mod.Ran);
        hashNum(&h, ops.len);
        for (ops) |op| {
            hashStr(&h, op.name);
            hashNum(&h, op.code);
            hashNum(&h, @backingInt(op.class));
            hashNum(&h, @backingInt(op.origin));
            hashNum(&h, @backingInt(op.scope));
            hashNum(&h, @intFromBool(op.isWrite()));
            hashNum(&h, @intFromBool(op.creates));
            hashNum(&h, @intFromBool(op.takes_items));
            hashNum(&h, @backingInt(op.fanout));
            hashStr(&h, @tagName(op.route));
            if (op.route == .field) hashStr(&h, op.route.field);
            hashStr(&h, op.txn orelse "no txn");
            if (op.wait) |w| {
                hashStr(&h, w.field);
                hashNum(&h, w.default_ms);
                hashNum(&h, w.max_ms);
            } else hashStr(&h, "no wait");
            hashSchema(&h, op.request);
            hashSchema(&h, op.response);
        }
        hashEntries(&h, entries);
        return h.final();
    }
}

/// A hash of the entry payload layouts alone: what a segment stores. The
/// refusal reasons and ops in `tableHash` are never stored, so a change to
/// them must not move this.
pub fn entryTableHash(comptime entries: anytype) u64 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var h = std.hash.Wyhash.init(0x666c6f2d656e7421); // "flo-ent!"
        hashEntries(&h, entries);
        return h.final();
    }
}

fn hashEntries(comptime h: *std.hash.Wyhash, comptime entries: anytype) void {
    hashNum(h, entries.len);
    for (entries) |e| {
        hashStr(h, e.name);
        hashNum(h, e.type_code);
        hashSchema(h, e.payload);
    }
}

fn hashStr(comptime h: *std.hash.Wyhash, comptime s: []const u8) void {
    hashNum(h, s.len);
    h.update(s);
}

fn hashNum(comptime h: *std.hash.Wyhash, comptime n: anytype) void {
    const wide: i128 = n;
    h.update(std.mem.asBytes(&wide));
}

fn hashEnum(comptime h: *std.hash.Wyhash, comptime E: type) void {
    const info = @typeInfo(E).@"enum";
    hashNum(h, info.field_names.len);
    for (info.field_names, info.field_values) |name, value| {
        hashStr(h, name);
        hashNum(h, value);
    }
}

fn hashSchema(comptime h: *std.hash.Wyhash, comptime T: type) void {
    hashStr(h, @tagName(T.kind));
    hashNum(h, T.min_size);
    hashNum(h, T.max_size);
    if (T.kind == .record) {
        hashNum(h, T.field_list.len);
        for (T.field_list) |f| {
            hashStr(h, f.name);
            hashSchema(h, f.T);
        }
    }
    if (@hasDecl(T, "Inner")) hashSchema(h, T.Inner);
    if (@hasDecl(T, "Element")) hashSchema(h, T.Element);
    if (@hasDecl(T, "byte_bound")) hashNum(h, @as(i128, T.byte_bound orelse -1));
    if (T.kind == .enumeration) hashEnum(h, T.Value);
    if (@hasDecl(T, "lo")) {
        hashNum(h, T.lo);
        hashNum(h, T.hi);
    }
    if (@hasDecl(T, "nonfinite")) hashNum(h, @intFromBool(T.nonfinite));
    if (@hasDecl(T, "mode")) hashStr(h, @tagName(T.mode));
    if (@hasDecl(T, "parser_name")) hashStr(h, T.parser_name);
    if (@hasDecl(T, "default_value")) {
        // The default as it encodes, so any value type is covered.
        var buf: [T.Inner.max_size]u8 = undefined;
        var w: schema.Writer = .{ .buf = &buf };
        T.Inner.encode(&w, T.default_value) catch @compileError("a field's default doesn't encode as its own type");
        hashStr(h, w.written());
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

const testing = std.testing;
const fields = struct {
    const key_value = schema.Bytes(64 * 1024);
    const version = schema.Int(u64);
};

const test_table = [_]Op{
    .{
        .name = "kv.set",
        .code = 0x100,
        .class = .write,
        .creates = true,
        .request = schema.Record(&.{
            .{ .name = "value", .T = fields.key_value, .doc = "The value" },
            .{ .name = "ttl_ms", .T = schema.Optional(schema.DurationMs(1 << 40)), .doc = "Time to live" },
        }),
        .response = schema.Record(&.{.{ .name = "version", .T = fields.version }}),
        .cli = .{ .args = &.{"value"} },
        .doc = "Set a key's value.",
    },
    .{
        .name = "kv.get",
        .code = 0x101,
        .class = .read,
        .wait = .{ .field = "wait_ms", .default_ms = 0, .max_ms = 300_000 },
        .request = schema.Record(&.{.{ .name = "wait_ms", .T = schema.Defaulted(schema.DurationMs(300_000), 0), .doc = "How long to wait" }}),
        .response = schema.Record(&.{ .{ .name = "version", .T = fields.version }, .{ .name = "value", .T = fields.key_value } }),
        .cli = .{ .render = .raw, .show = "value" },
        .doc = "Get a key's value.",
    },
};

test "ops: the cli column names only fields the op has, and a command can take every field" {
    const base = test_table[0];
    const cases = [_]Cli{
        .{ .args = &.{"nope"} },
        .{ .short = &.{.{ .field = "nope", .letter = 'n' }} },
        .{ .render = .raw },
        .{ .render = .raw, .show = "nope" },
        .{ .absent = "version" },
        .{ .hex = &.{"ttl_ms"} },
        .{ .examples = &.{"flo kv sett k v"} },
        .{ .examples = &.{"flo kv get k"} },
    };
    inline for (cases) |c| {
        comptime var op = base;
        op.cli = c;
        try testing.expect(comptime cliRule(op) != null);
    }
    comptime var undocumented = base;
    undocumented.request = schema.Record(&.{.{ .name = "value", .T = fields.key_value }});
    try testing.expect(comptime cliRule(undocumented) != null);
    try testing.expectEqualStrings("txn", comptime groupOf("kv.txn.begin").?[3..]);
    try testing.expectEqual(@as(?[]const u8, null), comptime groupOf("kv.get"));
    try testing.expectEqualStrings("json-get", comptime verbOf("kv.json_get"));
    try testing.expectEqualStrings("flo kv txn begin", comptime commandOf("kv.txn.begin"));
    try testing.expect(comptime nameRule("kv.a.b.c") != null);
}

test "ops: a table that follows the rules passes, and classes give writes" {
    comptime check(&test_table);
    try testing.expect(comptime test_table[0].isWrite());
    try testing.expect(!comptime test_table[1].isWrite());
    try testing.expectEqual(@as(?usize, 1), byCode(&test_table, 0x101));
    try testing.expectEqual(@as(?usize, null), byCode(&test_table, 0x1ff));
}

test "ops: the table hash changes with a layout, a code or a flag" {
    const empty: [0]struct { name: []const u8, type_code: u8, payload: type } = .{};
    const base = comptime tableHash(&test_table, empty);

    comptime var widened = test_table;
    widened[0].request = schema.Record(&.{
        .{ .name = "value", .T = schema.Bytes(128 * 1024) },
        .{ .name = "ttl_ms", .T = schema.Optional(schema.DurationMs(1 << 40)) },
    });
    try testing.expect(base != comptime tableHash(&widened, empty));

    comptime var recoded = test_table;
    recoded[1].code = 0x102;
    try testing.expect(base != comptime tableHash(&recoded, empty));

    comptime var reflagged = test_table;
    reflagged[1].takes_items = true;
    try testing.expect(base != comptime tableHash(&reflagged, empty));

    comptime var renamed = test_table;
    renamed[0].request = schema.Record(&.{
        .{ .name = "data", .T = fields.key_value },
        .{ .name = "ttl_ms", .T = schema.Optional(schema.DurationMs(1 << 40)) },
    });
    try testing.expect(base != comptime tableHash(&renamed, empty));

    try testing.expectEqual(base, comptime tableHash(&test_table, empty));
}

const NoEntries = [0]struct { name: []const u8, type_code: u8, payload: type }{};

fn oneOp(comptime request: type) [1]Op {
    return .{.{ .name = "kv.x", .code = 0x110, .class = .read, .request = request, .response = schema.Record(&.{}), .doc = "x" }};
}

fn hashOf(comptime request: type) u64 {
    return comptime tableHash(&oneOp(request), NoEntries);
}

fn rec(comptime T: type) type {
    return schema.Record(&.{.{ .name = "a", .T = T }});
}

test "ops: the hash covers each part of a field's meaning on the wire" {
    const E1 = enum(u8) { a = 1, b = 2 };
    const E2 = enum(u8) { a = 1, b = 3 };
    const pairs = .{
        .{ rec(schema.Range(u32, 0, 10)), rec(schema.Range(u32, 1, 10)) },
        .{ rec(schema.Range(u32, 0, 10)), rec(schema.Range(u32, 0, 11)) },
        .{ rec(schema.Enum(E1)), rec(schema.Enum(E2)) },
        .{ rec(schema.Optional(schema.Int(u32))), rec(schema.Optional(schema.Int(i32))) },
        .{ rec(schema.Defaulted(schema.Int(u32), 5)), rec(schema.Defaulted(schema.Int(u32), 6)) },
        .{ rec(schema.Float(false)), rec(schema.Float(true)) },
        .{ rec(schema.Namespace(.read)), rec(schema.Namespace(.write)) },
        .{ rec(schema.Parsed("floql", 8)), rec(schema.Parsed("yaml", 8)) },
        .{ rec(schema.List(schema.Int(u8), 4)), rec(schema.List(schema.Int(i8), 4)) },
    };
    inline for (pairs) |p| try testing.expect(hashOf(p[0]) != hashOf(p[1]));
}

test "ops: the hash covers route, wait and entry payloads" {
    const R = schema.Record(&.{
        .{ .name = "wait_ms", .T = schema.Defaulted(schema.DurationMs(1000), 0) },
        .{ .name = "who", .T = schema.String(8) },
    });
    const base = [_]Op{.{ .name = "kv.x", .code = 0x110, .class = .read, .request = R, .response = schema.Record(&.{}), .doc = "x" }};
    const h0 = comptime tableHash(&base, NoEntries);
    comptime var routed = base;
    routed[0].route = .{ .field = "who" };
    try testing.expect(h0 != comptime tableHash(&routed, NoEntries));
    comptime var waited = base;
    waited[0].wait = .{ .field = "wait_ms", .default_ms = 0, .max_ms = 1000 };
    try testing.expect(h0 != comptime tableHash(&waited, NoEntries));

    const e1 = [_]struct { name: []const u8, type_code: u8, payload: type }{.{ .name = "kv.put", .type_code = 1, .payload = rec(schema.Int(u32)) }};
    const e2 = [_]struct { name: []const u8, type_code: u8, payload: type }{.{ .name = "kv.put", .type_code = 1, .payload = rec(schema.Int(u64)) }};
    try testing.expect(comptime tableHash(&base, e1) != tableHash(&base, e2));
    try testing.expect(comptime tableHash(&base, e1) != h0);
}

test "ops: the table rules refuse what check would, naming why" {
    const R = rec(schema.Int(u8));
    const W = schema.Record(&.{.{ .name = "wait_ms", .T = schema.Defaulted(schema.DurationMs(300_000), 5) }});
    const Case = struct { op: Op, why: []const u8 };
    const cases = [_]Case{
        .{ .op = .{ .name = "kv.a", .code = 0x100, .class = .read, .creates = true, .request = R, .response = R, .doc = "x" }, .why = "creates its namespace but doesn't write" },
        .{ .op = .{ .name = "kv.a", .code = 0x100, .class = .read, .request = rec(schema.Bytes(300_000)), .response = R, .doc = "x" }, .why = "request: its largest value" },
        .{ .op = .{ .name = "kv.a", .code = 0x100, .class = .read, .wait = .{ .field = "wait_ms", .default_ms = 999, .max_ms = 300_000 }, .request = W, .response = R, .doc = "x" }, .why = "wait default 999 ms disagrees" },
        .{ .op = .{ .name = "kv.a", .code = 0x200, .class = .read, .request = R, .response = R, .doc = "x" }, .why = "outside its family" },
        .{ .op = .{ .name = "kv.a", .code = 0x100, .class = .read, .request = R, .response = R, .doc = "" }, .why = "has no doc line" },
        .{ .op = .{ .name = "zz.a", .code = 0x100, .class = .read, .request = R, .response = R, .doc = "x" }, .why = "names no family" },
    };
    inline for (cases) |c| {
        const message = comptime tableRule(&.{c.op}) orelse return error.TestExpectedRefusal;
        try testing.expect(std.mem.indexOf(u8, message, c.why) != null);
    }
    try testing.expectEqual(@as(?[]const u8, null), comptime tableRule(&test_table));
}

test "ops: admin, write and ns_create go to the leader; read and node don't" {
    const R = rec(schema.Int(u8));
    inline for (.{ .{ Class.admin, true }, .{ Class.write, true }, .{ Class.ns_create, true }, .{ Class.read, false }, .{ Class.node, false } }) |c| {
        const op: Op = .{ .name = "kv.a", .code = 0x100, .class = c[0], .request = R, .response = R, .doc = "x" };
        try testing.expectEqual(c[1], comptime op.isWrite());
    }
}

test "ops: the table hash of a fixed table is pinned" {
    // Changing anything the hash covers, the refusal reasons included,
    // changes this value; so does an accidental change to how it's computed.
    // Update it only together with a deliberate change to the hash.
    try testing.expectEqual(@as(u64, 0xf907165f6f97bbbd), comptime tableHash(&test_table, NoEntries));
}

test "ops: the hash covers which field a route names and a wait's default" {
    const R = schema.Record(&.{
        .{ .name = "wait_ms", .T = schema.Defaulted(schema.DurationMs(1000), 0) },
        .{ .name = "who", .T = schema.String(8) },
        .{ .name = "whom", .T = schema.String(8) },
    });
    const R2 = schema.Record(&.{
        .{ .name = "wait_ms", .T = schema.Defaulted(schema.DurationMs(1000), 7) },
        .{ .name = "who", .T = schema.String(8) },
        .{ .name = "whom", .T = schema.String(8) },
    });
    const base = [_]Op{.{ .name = "kv.x", .code = 0x110, .class = .read, .route = .{ .field = "who" }, .wait = .{ .field = "wait_ms", .default_ms = 0, .max_ms = 1000 }, .request = R, .response = schema.Record(&.{}), .doc = "x" }};
    const h0 = comptime tableHash(&base, NoEntries);
    comptime var other_field = base;
    other_field[0].route = .{ .field = "whom" };
    try testing.expect(h0 != comptime tableHash(&other_field, NoEntries));
    comptime var other_default = base;
    other_default[0].request = R2;
    other_default[0].wait = .{ .field = "wait_ms", .default_ms = 7, .max_ms = 1000 };
    try testing.expect(h0 != comptime tableHash(&other_default, NoEntries));
}
