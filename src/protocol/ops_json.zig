//! `ops.json`: every client op, field, type, bound, answer status, refusal
//! reason and group, read from the op table and the protocol. SDK codecs and
//! the reference docs are generated from it, so it describes the wire, not
//! the server: internal ops are left out. `zig build gen` writes it; a
//! release publishes it.
//!
//! Integers past 2^53 (u64 bounds, an i64 minimum) are written as decimal
//! strings, so a JSON reader that parses numbers as doubles can't round
//! them.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");
const reason_mod = @import("reason.zig");
const proto = @import("proto.zig");
const table = @import("table.zig");
const kv_ops = @import("kv_ops.zig");

const Stringify = std.json.Stringify;

/// The layout of this file; a change a reader must know about bumps it.
pub const FORMAT: u32 = 1;

/// Writes `ops.json` for this build's table.
pub fn write(w: *std.Io.Writer) Stringify.Error!void {
    var jw: Stringify = .{ .writer = w, .options = .{ .whitespace = .indent_2 } };
    try jw.beginObject();
    try jw.objectField("format");
    try jw.write(FORMAT);
    try jw.objectField("protocol");
    try jw.write(proto.VERSION);
    try jw.objectField("table_hash");
    try jw.print("\"0x{x:0>16}\"", .{table.HASH});

    // The status every answer carries in its header.
    try jw.objectField("statuses");
    try jw.beginArray();
    const statuses = @typeInfo(proto.StatusCode).@"enum";
    inline for (statuses.field_names, statuses.field_values) |name, value| {
        try jw.beginObject();
        try jw.objectField("code");
        try jw.write(value);
        try jw.objectField("name");
        try jw.write(name);
        try jw.endObject();
    }
    try jw.endArray();

    try jw.objectField("reasons");
    try jw.beginArray();
    const reasons = @typeInfo(reason_mod.Reason).@"enum";
    inline for (reasons.field_names, reasons.field_values) |name, value| {
        try jw.beginObject();
        try jw.objectField("code");
        try jw.write(value);
        try jw.objectField("name");
        try jw.write(name);
        try jw.endObject();
    }
    try jw.endArray();

    try jw.objectField("groups");
    try jw.beginArray();
    inline for (table.op_groups) |g| {
        try jw.beginObject();
        try jw.objectField("name");
        try jw.write(g.name);
        try jw.objectField("doc");
        try jw.write(g.doc);
        try jw.endObject();
    }
    try jw.endArray();

    try jw.objectField("ops");
    try jw.beginArray();
    inline for (table.op_table) |op| if (op.origin == .client) try writeOp(&jw, op);
    try jw.endArray();
    try jw.endObject();
}

fn writeOp(jw: *Stringify, comptime op: ops.Op) Stringify.Error!void {
    try jw.beginObject();
    try jw.objectField("name");
    try jw.write(op.name);
    try jw.objectField("code");
    try jw.write(op.code);
    try jw.objectField("class");
    try jw.write(@tagName(op.class));
    try jw.objectField("writes");
    try jw.write(comptime op.isWrite());
    try jw.objectField("creates");
    try jw.write(op.creates);
    try jw.objectField("route");
    switch (op.route) {
        .field => |name| {
            try jw.beginObject();
            try jw.objectField("field");
            try jw.write(name);
            try jw.endObject();
        },
        else => try jw.write(@tagName(op.route)),
    }
    try jw.objectField("fanout");
    try jw.write(@tagName(op.fanout));
    try jw.objectField("wait");
    if (op.wait) |wt| {
        try jw.beginObject();
        try jw.objectField("field");
        try jw.write(wt.field);
        try jw.objectField("default_ms");
        try writeInt(jw, wt.default_ms);
        try jw.objectField("max_ms");
        try writeInt(jw, wt.max_ms);
        try jw.endObject();
    } else try jw.write(null);
    try jw.objectField("txn");
    try jw.write(op.txn);
    try jw.objectField("doc");
    try jw.write(op.doc);
    try jw.objectField("request");
    try writeFields(jw, op.request);
    try jw.objectField("response");
    try writeFields(jw, op.response);
    try jw.endObject();
}

fn writeFields(jw: *Stringify, comptime Record: type) Stringify.Error!void {
    try jw.beginArray();
    inline for (Record.field_list) |f| {
        try jw.beginObject();
        try jw.objectField("name");
        try jw.write(f.name);
        try jw.objectField("doc");
        try jw.write(f.doc);
        try jw.objectField("type");
        try writeType(jw, f.T);
        try jw.endObject();
    }
    try jw.endArray();
}

/// A schema type: its kind and whatever bounds it, recursively.
fn writeType(jw: *Stringify, comptime T: type) Stringify.Error!void {
    try jw.beginObject();
    try jw.objectField("kind");
    try jw.write(@tagName(T.kind));
    switch (T.kind) {
        .integer => {
            const info = @typeInfo(T.Value).int;
            try jw.objectField("bits");
            try jw.write(info.bits);
            try jw.objectField("signed");
            try jw.write(info.signedness == .signed);
            try jw.objectField("min");
            try writeInt(jw, T.lo);
            try jw.objectField("max");
            try writeInt(jw, T.hi);
        },
        .enumeration => {
            try jw.objectField("bits");
            try jw.write(@bitSizeOf(@typeInfo(T.Value).@"enum".tag_type));
            try jw.objectField("values");
            try jw.beginArray();
            const info = @typeInfo(T.Value).@"enum";
            inline for (info.field_names, info.field_values) |name, value| {
                try jw.beginObject();
                try jw.objectField("name");
                try jw.write(name);
                try jw.objectField("value");
                try jw.write(value);
                try jw.endObject();
            }
            try jw.endArray();
        },
        .bytes, .string => {
            try jw.objectField("max");
            try jw.write(T.bound);
        },
        .parsed => {
            try jw.objectField("parser");
            try jw.write(T.parser_name);
            try jw.objectField("max");
            try jw.write(T.bound);
        },
        .optional => {
            try jw.objectField("of");
            try writeType(jw, T.Inner);
        },
        .defaulted => {
            try jw.objectField("of");
            try writeType(jw, T.Inner);
            try jw.objectField("default");
            try writeValue(jw, T.Inner, T.default_value);
        },
        .list => {
            try jw.objectField("of");
            try writeType(jw, T.Element);
            try jw.objectField("max");
            try jw.write(T.bound);
            try jw.objectField("max_bytes");
            try jw.write(T.byte_bound);
        },
        .record => {
            try jw.objectField("fields");
            try writeFields(jw, T);
        },
        else => {},
    }
    try jw.endObject();
}

fn writeValue(jw: *Stringify, comptime T: type, comptime v: T.Value) Stringify.Error!void {
    switch (T.kind) {
        .integer => try writeInt(jw, v),
        .enumeration => try jw.write(@tagName(v)),
        .boolean => try jw.write(v),
        .bytes, .string, .parsed => try jw.write(v),
        else => @compileError("no JSON default for a " ++ @tagName(T.kind)),
    }
}

/// An integer as a number, or as a decimal string past what a double holds
/// exactly.
fn writeInt(jw: *Stringify, v: anytype) Stringify.Error!void {
    const exact: i128 = 1 << 53;
    const wide: i128 = v;
    if (wide > -exact and wide < exact) return jw.write(v);
    try jw.print("\"{d}\"", .{v});
}

const testing = std.testing;

test "ops.json: it parses, lists every client op with its fields, and leaves internal ops out" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try write(&out.writer);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try testing.expectEqual(@as(i64, FORMAT), root.get("format").?.integer);
    const listed = root.get("ops").?.array.items;
    var client_ops: usize = 0;
    inline for (table.op_table) |op| {
        if (op.origin == .client) client_ops += 1;
        const entry = for (listed) |o| {
            if (std.mem.eql(u8, o.object.get("name").?.string, op.name)) break o;
        } else null;
        try testing.expectEqual(op.origin == .client, entry != null);
        if (entry) |o| try testing.expectEqual(op.request.field_list.len, o.object.get("request").?.array.items.len);
    }
    try testing.expectEqual(client_ops, listed.len);
    try testing.expectEqual(@typeInfo(reason_mod.Reason).@"enum".field_names.len, root.get("reasons").?.array.items.len);
}

test "ops.json: it lists every answer status by code and name" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try write(&out.writer);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const listed = parsed.value.object.get("statuses").?.array.items;
    const statuses = @typeInfo(proto.StatusCode).@"enum";
    try testing.expectEqual(statuses.field_names.len, listed.len);
    inline for (statuses.field_names, statuses.field_values, 0..) |name, value, i| {
        try testing.expectEqual(@as(i64, value), listed[i].object.get("code").?.integer);
        try testing.expectEqualStrings(name, listed[i].object.get("name").?.string);
    }
    const unavailable = listed[@backingInt(proto.StatusCode.unavailable)].object;
    try testing.expectEqualStrings("unavailable", unavailable.get("name").?.string);
}

test "ops.json: a field's bound is its schema's, and a wide integer is a string" {
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try write(&out.writer);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    for (parsed.value.object.get("ops").?.array.items) |o| {
        if (!std.mem.eql(u8, o.object.get("name").?.string, "kv.set")) continue;
        const value = o.object.get("request").?.array.items[0].object;
        try testing.expectEqualStrings("value", value.get("name").?.string);
        try testing.expectEqual(@as(i64, kv_ops.MAX_VALUE), value.get("type").?.object.get("max").?.integer);
        const ttl = o.object.get("request").?.array.items[1].object.get("type").?.object.get("of").?.object;
        try testing.expectEqual(@as(i64, kv_ops.MAX_TTL_MS), ttl.get("max").?.integer);
        const cas = o.object.get("request").?.array.items[3].object;
        try testing.expectEqualStrings("cas_version", cas.get("name").?.string);
        try testing.expectEqualStrings("18446744073709551615", cas.get("type").?.object.get("of").?.object.get("max").?.string);
        return;
    }
    return error.TestUnexpectedResult;
}
