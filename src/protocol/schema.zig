//! Typed wire schemas. An op's request, its response and a log entry's
//! payload are each a `Record` of fields, and every field is one of the
//! types below. Each type knows its encoded size bounds and decodes and
//! encodes itself, so a schema is the only description of its bytes.
//!
//! What decoding guarantees, for any input:
//! - every length is bounded by the schema, and checked before it is used;
//! - a list's count is checked against the bytes left before its elements
//!   are read, so a large count can't drive work or allocation;
//! - nothing is allocated: strings, bytes and lists are views into the
//!   input, validated whole before decode returns;
//! - a value is decoded whole or refused with a reason naming its field:
//!   truncation, trailing bytes, an unknown enum value, a number out of
//!   range, an over-long field, invalid UTF-8 or a non-finite float;
//! - nothing panics: an impossible case is an error, never `unreachable`.
//!
//! What the types guarantee at comptime: no list of zero-size elements, a
//! `tail` only as a record's last field, and each record's worst-case
//! encoded size, so a caller can refuse to compile a schema whose biggest
//! value doesn't fit its buffer (`fitsIn`).

const std = @import("std");
const reason_mod = @import("reason.zig");

pub const Reason = reason_mod.Reason;

/// Why a decode failed, and in which field. `field` is a schema field name,
/// so a refusal can name it without echoing stored state.
pub const Diagnostic = struct {
    reason: Reason = .malformed,
    field: []const u8 = "",
    /// The bound a field broke, where one applies (a length or a range).
    bound: u64 = 0,
};

pub const DecodeError = error{Refused};
pub const EncodeError = error{ NoSpaceLeft, TooLong, OutOfRange };

/// Reads a frame front to back. Every read checks its length first.
pub const Reader = struct {
    bytes: []const u8,
    pos: usize = 0,
    diag: *Diagnostic,

    pub fn remaining(r: *const Reader) usize {
        return r.bytes.len - r.pos;
    }

    pub fn take(r: *Reader, n: usize, field: []const u8) DecodeError![]const u8 {
        if (n > r.remaining()) return r.refuse(.malformed, field, n);
        const out = r.bytes[r.pos..][0..n];
        r.pos += n;
        return out;
    }

    pub fn int(r: *Reader, comptime T: type, field: []const u8) DecodeError!T {
        const b = try r.take(@sizeOf(T), field);
        return std.mem.readInt(T, b[0..@sizeOf(T)], .little);
    }

    pub fn refuse(r: *Reader, reason: Reason, field: []const u8, bound: u64) DecodeError {
        r.diag.* = .{ .reason = reason, .field = field, .bound = bound };
        return error.Refused;
    }
};

/// Writes into a fixed buffer.
pub const Writer = struct {
    buf: []u8,
    pos: usize = 0,

    pub fn put(w: *Writer, bytes: []const u8) EncodeError!void {
        if (bytes.len > w.buf.len - w.pos) return error.NoSpaceLeft;
        @memcpy(w.buf[w.pos..][0..bytes.len], bytes);
        w.pos += bytes.len;
    }

    pub fn int(w: *Writer, comptime T: type, v: T) EncodeError!void {
        var b: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &b, v, .little);
        try w.put(&b);
    }

    pub fn written(w: *const Writer) []const u8 {
        return w.buf[0..w.pos];
    }
};

/// The smallest unsigned integer that holds `max`: the width of a length
/// or count prefix for a field bounded by it.
fn PrefixFor(comptime max: u64) type {
    if (max <= std.math.maxInt(u8)) return u8;
    if (max <= std.math.maxInt(u16)) return u16;
    if (max <= std.math.maxInt(u32)) return u32;
    @compileError("a length bound past u32");
}

// ── Scalars ─────────────────────────────────────────────────────────────

/// A fixed-width little-endian integer, every value allowed.
pub fn Int(comptime T: type) type {
    return Range(T, std.math.minInt(T), std.math.maxInt(T));
}

/// An integer refused outside `[min, max]`, so a value that would overflow
/// a later conversion is caught where it arrives.
pub fn Range(comptime T: type, comptime min: T, comptime max: T) type {
    switch (@typeInfo(T)) {
        .int => |i| if (i.bits == 0 or i.bits > 64 or i.bits % 8 != 0) @compileError("schema integers are 8 to 64 bits, whole bytes"),
        else => @compileError("Range takes an integer type"),
    }
    if (min > max) @compileError("Range min is past its max");
    return struct {
        pub const Value = T;
        pub const min_size = @sizeOf(T);
        pub const max_size = @sizeOf(T);
        pub const lo = min;
        pub const hi = max;
        pub const kind = .integer;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            const v = try r.int(T, field);
            if (v < min) return r.refuse(.out_of_range, field, boundOf(min));
            if (v > max) return r.refuse(.out_of_range, field, boundOf(max));
            return v;
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            if (v < min or v > max) return error.OutOfRange;
            try w.int(T, v);
        }
    };
}

/// A bound as a diagnostic carries it: a negative one reads as 0.
fn boundOf(v: anytype) u64 {
    return if (v < 0) 0 else @intCast(v);
}

/// A duration in milliseconds, refused past `max_ms`. Every duration on the
/// wire is milliseconds.
pub fn DurationMs(comptime max_ms: u64) type {
    return Range(u64, 0, max_ms);
}

/// A little-endian f64. NaN and infinities are refused unless allowed.
pub fn Float(comptime allow_nonfinite: bool) type {
    return struct {
        pub const Value = f64;
        pub const min_size = 8;
        pub const max_size = 8;
        pub const kind = .float;
        pub const nonfinite = allow_nonfinite;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            const v: f64 = @bitCast(try r.int(u64, field));
            if (!allow_nonfinite and !std.math.isFinite(v)) return r.refuse(.out_of_range, field, 0);
            return v;
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            if (!allow_nonfinite and !std.math.isFinite(v)) return error.OutOfRange;
            try w.int(u64, @bitCast(v));
        }
    };
}

/// One byte, 0 or 1; anything else is refused.
pub const Bool = struct {
    pub const Value = bool;
    pub const min_size = 1;
    pub const max_size = 1;
    pub const kind = .boolean;

    pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
        return switch ((try r.take(1, field))[0]) {
            0 => false,
            1 => true,
            else => r.refuse(.malformed, field, 1),
        };
    }

    pub fn encode(w: *Writer, v: Value) EncodeError!void {
        try w.put(&.{@intFromBool(v)});
    }
};

/// An enum carried as its tag; a tag the enum doesn't name is refused.
pub fn Enum(comptime E: type) type {
    const Tag = @typeInfo(E).@"enum".tag_type;
    if (@typeInfo(E).@"enum".is_exhaustive == false) @compileError("a schema enum is exhaustive, so an unknown value can be refused");
    if (@typeInfo(Tag).int.bits % 8 != 0 or @typeInfo(Tag).int.bits > 64) @compileError("a schema enum's tag is 8 to 64 bits, whole bytes");
    return struct {
        pub const Value = E;
        pub const min_size = @sizeOf(Tag);
        pub const max_size = @sizeOf(Tag);
        pub const kind = .enumeration;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            const tag = try r.int(Tag, field);
            return std.enums.fromInt(E, tag) orelse r.refuse(.malformed, field, 0);
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            try w.int(Tag, @intFromEnum(v));
        }
    };
}

// ── Variable length ─────────────────────────────────────────────────────

/// Opaque bytes, length-prefixed, at most `max`.
pub fn Bytes(comptime max: u32) type {
    return LengthPrefixed(max, false);
}

/// UTF-8 text, length-prefixed, at most `max` bytes; invalid UTF-8 is refused.
pub fn String(comptime max: u32) type {
    return LengthPrefixed(max, true);
}

fn LengthPrefixed(comptime max: u32, comptime utf8: bool) type {
    const P = PrefixFor(max);
    return struct {
        pub const Value = []const u8;
        pub const min_size = @sizeOf(P);
        pub const max_size = @sizeOf(P) + max;
        pub const bound = max;
        pub const kind = if (utf8) .string else .bytes;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            const len: usize = try r.int(P, field);
            if (len > max) return r.refuse(.field_too_long, field, max);
            const v = try r.take(len, field);
            if (utf8 and !std.unicode.utf8ValidateSlice(v)) return r.refuse(.malformed, field, 0);
            return v;
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            if (v.len > max) return error.TooLong;
            try w.int(P, @intCast(v.len));
            try w.put(v);
        }
    };
}

/// How a namespace named in a body is used: read from or written to. The
/// edge authorizes it for this mode.
pub const Access = enum { read, write };

/// A namespace named in a request body (a pipeline's source or sink), with
/// the access it's used for. A namespace field has no other form, so one
/// can't be declared without its mode.
pub fn Namespace(comptime access: Access) type {
    return struct {
        pub const Value = []const u8;
        const Name = String(MAX_NAMESPACE_NAME);
        pub const min_size = Name.min_size;
        pub const max_size = Name.max_size;
        pub const mode = access;
        pub const kind = .namespace;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            return Name.decode(r, field);
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            return Name.encode(w, v);
        }
    };
}

/// The longest namespace name a body may carry: the server's own limit.
pub const MAX_NAMESPACE_NAME: u32 = @import("limits.zig").MAX_NAMESPACE_NAME;

/// A stream record id: milliseconds, then a sequence.
pub const StreamId = struct {
    pub const Value = struct { ms: u64, seq: u64 };
    pub const min_size = 16;
    pub const max_size = 16;
    pub const kind = .stream_id;

    pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
        return .{ .ms = try r.int(u64, field), .seq = try r.int(u64, field) };
    }

    pub fn encode(w: *Writer, v: Value) EncodeError!void {
        try w.int(u64, v.ms);
        try w.int(u64, v.seq);
    }
};

/// A presence byte, then the value if present.
pub fn Optional(comptime T: type) type {
    return struct {
        pub const Value = ?T.Value;
        pub const min_size = 1;
        pub const max_size = 1 + T.max_size;
        pub const Inner = T;
        pub const kind = .optional;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            return switch ((try r.take(1, field))[0]) {
                0 => null,
                1 => try T.decode(r, field),
                else => r.refuse(.malformed, field, 1),
            };
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            if (v) |x| {
                try w.put(&.{1});
                try T.encode(w, x);
            } else try w.put(&.{0});
        }
    };
}

/// An optional field that reads as `default` when absent, so a handler
/// gets a plain value. It always encodes present: an absent field and one
/// carrying the default decode to the same value, but not to the same bytes,
/// so a decoded record re-encodes to bytes that decode equal, not to its
/// input.
pub fn Defaulted(comptime T: type, comptime default: T.Value) type {
    return struct {
        pub const Value = T.Value;
        pub const min_size = 1;
        pub const max_size = 1 + T.max_size;
        pub const Inner = T;
        pub const default_value = default;
        pub const kind = .defaulted;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            return (try Optional(T).decode(r, field)) orelse default;
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            try Optional(T).encode(w, v);
        }
    };
}

/// A count-prefixed list of at most `max` elements, decoded as a validated
/// view: every element is checked before decode returns, and none is
/// copied.
pub fn List(comptime T: type, comptime max: u32) type {
    if (listRule(T)) |message| @compileError(message);
    const P = PrefixFor(max);
    return struct {
        pub const min_size = @sizeOf(P);
        pub const max_size = @sizeOf(P) + @as(usize, max) * T.max_size;
        pub const Element = T;
        pub const bound = max;
        pub const kind = .list;

        /// The list's elements, already validated.
        pub const Value = struct {
            bytes: []const u8,
            count: u32,

            pub fn iterator(v: Value) Iterator {
                return .{ .bytes = v.bytes, .left = v.count };
            }
        };

        pub const Iterator = struct {
            bytes: []const u8,
            left: u32,
            pos: usize = 0,

            /// The next element. Decode validated them all, so a failure
            /// here is a bug, reported as an error rather than a panic.
            pub fn next(it: *Iterator) error{Corrupt}!?T.Value {
                if (it.left == 0) return null;
                var diag: Diagnostic = .{};
                var r: Reader = .{ .bytes = it.bytes, .pos = it.pos, .diag = &diag };
                const v = T.decode(&r, "") catch return error.Corrupt;
                it.pos = r.pos;
                it.left -= 1;
                return v;
            }
        };

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            const count = try r.int(P, field);
            if (count > max) return r.refuse(.field_too_long, field, max);
            // Before any element is read: can the bytes left hold this many?
            const least = std.math.mul(usize, count, T.min_size) catch return r.refuse(.malformed, field, count);
            if (least > r.remaining()) return r.refuse(.malformed, field, count);
            const start = r.pos;
            for (0..count) |_| _ = try T.decode(r, field);
            return .{ .bytes = r.bytes[start..r.pos], .count = count };
        }

        /// Encodes `items`, a slice of element values.
        pub fn encodeSlice(w: *Writer, items: []const T.Value) EncodeError!void {
            if (items.len > max) return error.TooLong;
            try w.int(P, @intCast(items.len));
            for (items) |item| try T.encode(w, item);
        }

        /// Re-encodes a decoded view (for forwarding or round-trip checks).
        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            try w.int(P, @intCast(v.count));
            try w.put(v.bytes);
        }
    };
}

/// The rest of the frame, opaque, at most `max` bytes. Only a record's last
/// field may be a tail.
pub fn Tail(comptime max: u32) type {
    return struct {
        pub const Value = []const u8;
        pub const min_size = 0;
        pub const max_size = max;
        pub const bound = max;
        pub const kind = .tail;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            if (r.remaining() > max) return r.refuse(.field_too_long, field, max);
            return r.take(r.remaining(), field);
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            if (v.len > max) return error.TooLong;
            try w.put(v);
        }
    };
}

/// Bytes handed to a named parser (YAML, FloQL, line protocol). The schema
/// bounds them; the parser gives them meaning and has its own fuzz target.
pub fn Parsed(comptime parser: []const u8, comptime max: u32) type {
    return struct {
        const B = Bytes(max);
        pub const Value = []const u8;
        pub const min_size = B.min_size;
        pub const max_size = B.max_size;
        pub const parser_name = parser;
        pub const bound = max;
        pub const kind = .parsed;

        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            return B.decode(r, field);
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            return B.encode(w, v);
        }
    };
}

// ── Records ─────────────────────────────────────────────────────────────

/// One field of a record: its name and type. The name is public vocabulary:
/// an SDK attribute and a CLI flag are spelled from it.
pub const Field = struct {
    name: [:0]const u8,
    T: type,
    doc: []const u8 = "",
};

/// A record: fields in wire order. Decoding reads each in turn; a record
/// decoded as a whole frame refuses trailing bytes.
///
/// Records can nest (a list of records), but a record can't contain itself:
/// these functions build each type from finished ones, so a schema type is
/// never recursive.
pub fn Record(comptime fields: []const Field) type {
    if (recordRule(fields)) |message| @compileError(message);
    const RecordValue = blk: {
        var names: [fields.len][]const u8 = undefined;
        var types: [fields.len]type = undefined;
        var attrs: [fields.len]std.builtin.Type.StructField.Attributes = undefined;
        for (fields, 0..) |f, i| {
            names[i] = f.name;
            types[i] = f.T.Value;
            attrs[i] = .{};
        }
        break :blk @Struct(.auto, null, &names, &types, &attrs);
    };
    return struct {
        pub const field_list = fields;
        pub const Value = RecordValue;
        pub const min_size = sum: {
            var n: usize = 0;
            for (fields) |f| n += f.T.min_size;
            break :sum n;
        };
        pub const max_size = sum: {
            var n: usize = 0;
            for (fields) |f| n += f.T.max_size;
            break :sum n;
        };
        pub const kind = .record;

        /// Decodes this record from the reader, leaving it after the record.
        pub fn decode(r: *Reader, field: []const u8) DecodeError!Value {
            _ = field;
            var v: Value = undefined;
            inline for (fields) |f| @field(v, f.name) = try f.T.decode(r, f.name);
            return v;
        }

        /// Decodes `bytes` as exactly one record: trailing bytes are refused.
        pub fn decodeAll(bytes: []const u8, diag: *Diagnostic) DecodeError!Value {
            var r: Reader = .{ .bytes = bytes, .diag = diag };
            const v = try decode(&r, "");
            if (r.remaining() != 0) return r.refuse(.malformed, "", r.remaining());
            return v;
        }

        pub fn encode(w: *Writer, v: Value) EncodeError!void {
            inline for (fields) |f| try f.T.encode(w, @field(v, f.name));
        }

        /// Encodes `v` into `buf`, returning the bytes written.
        pub fn encodeInto(buf: []u8, v: Value) EncodeError![]const u8 {
            var w: Writer = .{ .buf = buf };
            try encode(&w, v);
            return w.written();
        }
    };
}

fn validName(comptime name: []const u8) bool {
    if (name.len == 0 or !std.ascii.isLower(name[0])) return false;
    for (name) |c| if (!(std.ascii.isLower(c) or std.ascii.isDigit(c) or c == '_')) return false;
    return true;
}

/// Fails the build if `Schema`'s largest value doesn't fit `budget` bytes,
/// naming what it is for. Call it at comptime for every schema and every
/// buffer it must fit.
pub fn fitsIn(comptime Schema: type, comptime budget: usize, comptime what: []const u8) void {
    if (fitsRule(Schema, budget, what)) |message| @compileError(message);
}

// ── Rules ───────────────────────────────────────────────────────────────
// Each returns what's wrong, or null; the type constructors turn a message
// into a compile error, and tests call the rules directly.

/// Whether a value of `T` can end in a tail: a tail, or an optional,
/// default, list or record that holds one.
pub fn containsTail(comptime T: type) bool {
    if (T.kind == .tail) return true;
    if (@hasDecl(T, "Inner")) return containsTail(T.Inner);
    if (@hasDecl(T, "Element")) return containsTail(T.Element);
    if (T.kind == .record) {
        for (T.field_list) |f| if (containsTail(f.T)) return true;
    }
    return false;
}

pub fn listRule(comptime T: type) ?[]const u8 {
    if (T.min_size == 0) return "a list element must take at least one byte, or its count is unbounded by the frame";
    if (containsTail(T)) return "a list element can't contain a tail: it would take the rest of the frame";
    return null;
}

pub fn recordRule(comptime fields: []const Field) ?[]const u8 {
    for (fields, 0..) |f, i| {
        if (containsTail(f.T) and i != fields.len - 1) return "field '" ++ f.name ++ "' holds a tail, which only a record's last field may";
        if (!validName(f.name)) return "field name '" ++ f.name ++ "' must be lower snake_case: [a-z][a-z0-9_]*";
        for (fields[0..i]) |g| if (std.mem.eql(u8, f.name, g.name)) return "field '" ++ f.name ++ "' is declared twice";
    }
    return null;
}

pub fn fitsRule(comptime Schema: type, comptime budget: usize, comptime what: []const u8) ?[]const u8 {
    if (Schema.max_size > budget) return std.fmt.comptimePrint(
        "{s}: its largest value is {d} bytes, past the {d} it must fit",
        .{ what, Schema.max_size, budget },
    );
    return null;
}

// ── Tests ───────────────────────────────────────────────────────────────

const testing = std.testing;

const Color = enum(u8) { red = 1, green = 2 };
const Item = Record(&.{
    .{ .name = "name", .T = String(8) },
    .{ .name = "weight", .T = Range(u32, 1, 1000) },
});
const Sample = Record(&.{
    .{ .name = "id", .T = Int(u64) },
    .{ .name = "ttl_ms", .T = Optional(DurationMs(86_400_000)) },
    .{ .name = "color", .T = Enum(Color) },
    .{ .name = "active", .T = Bool },
    .{ .name = "score", .T = Float(false) },
    .{ .name = "items", .T = List(Item, 4) },
    .{ .name = "limit", .T = Defaulted(Int(u32), 100) },
    .{ .name = "since", .T = StreamId },
    .{ .name = "sink", .T = Namespace(.write) },
    .{ .name = "blob", .T = Tail(32) },
});

fn sampleValue() Sample.Value {
    return .{
        .id = 7,
        .ttl_ms = 5000,
        .color = .green,
        .active = true,
        .score = 1.5,
        .items = undefined,
        .limit = 100,
        .since = .{ .ms = 10, .seq = 2 },
        .sink = "orders",
        .blob = "tail-bytes",
    };
}

/// A valid encoding of `sampleValue()` with two items.
fn sampleBytes(buf: []u8) ![]const u8 {
    var w: Writer = .{ .buf = buf };
    const v = sampleValue();
    try Int(u64).encode(&w, v.id);
    try Optional(DurationMs(86_400_000)).encode(&w, v.ttl_ms);
    try Enum(Color).encode(&w, v.color);
    try Bool.encode(&w, v.active);
    try Float(false).encode(&w, v.score);
    try List(Item, 4).encodeSlice(&w, &.{ .{ .name = "a", .weight = 1 }, .{ .name = "bb", .weight = 1000 } });
    try Defaulted(Int(u32), 100).encode(&w, v.limit);
    try StreamId.encode(&w, v.since);
    try Namespace(.write).encode(&w, v.sink);
    try Tail(32).encode(&w, v.blob);
    return w.written();
}

test "schema: a record decodes field by field and re-encodes to the same bytes" {
    var buf: [256]u8 = undefined;
    const bytes = try sampleBytes(&buf);
    var diag: Diagnostic = .{};
    const v = try Sample.decodeAll(bytes, &diag);
    try testing.expectEqual(@as(u64, 7), v.id);
    try testing.expectEqual(@as(?u64, 5000), v.ttl_ms);
    try testing.expectEqual(Color.green, v.color);
    try testing.expect(v.active);
    try testing.expectEqualStrings("orders", v.sink);
    try testing.expectEqualStrings("tail-bytes", v.blob);
    var it = v.items.iterator();
    try testing.expectEqualStrings("a", (try it.next()).?.name);
    try testing.expectEqual(@as(u32, 1000), (try it.next()).?.weight);
    try testing.expectEqual(@as(?Item.Value, null), try it.next());

    var out: [256]u8 = undefined;
    try testing.expectEqualSlices(u8, bytes, try Sample.encodeInto(&out, v));
}

test "schema: every refusal names its field and its reason" {
    const Case = struct { mutate: *const fn ([]u8) []u8, reason: Reason, field: []const u8 };
    const cases = [_]Case{
        // Cut short anywhere: truncated (inside the tail it's just a shorter tail, so cut earlier).
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                return b[0..5];
            }
        }.f, .reason = .malformed, .field = "id" },
        // ttl past its bound.
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                std.mem.writeInt(u64, b[9..17], 86_400_001, .little);
                return b;
            }
        }.f, .reason = .out_of_range, .field = "ttl_ms" },
        // Unknown enum value.
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                b[17] = 9;
                return b;
            }
        }.f, .reason = .malformed, .field = "color" },
        // Bool that's neither 0 nor 1.
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                b[18] = 2;
                return b;
            }
        }.f, .reason = .malformed, .field = "active" },
        // NaN score.
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                std.mem.writeInt(u64, b[19..27], @bitCast(std.math.nan(f64)), .little);
                return b;
            }
        }.f, .reason = .out_of_range, .field = "score" },
        // More items than the list allows.
        .{ .mutate = struct {
            fn f(b: []u8) []u8 {
                b[27] = 5;
                return b;
            }
        }.f, .reason = .field_too_long, .field = "items" },
    };
    for (cases) |c| {
        var buf: [256]u8 = undefined;
        const bytes = try sampleBytes(&buf);
        var copy: [256]u8 = undefined;
        @memcpy(copy[0..bytes.len], bytes);
        var diag: Diagnostic = .{};
        try testing.expectError(error.Refused, Sample.decodeAll(c.mutate(copy[0..bytes.len]), &diag));
        try testing.expectEqual(c.reason, diag.reason);
        try testing.expectEqualStrings(c.field, diag.field);
    }
}

test "schema: a string past its bound, invalid UTF-8 and trailing bytes are refused" {
    const S = Record(&.{.{ .name = "who", .T = String(4) }});
    var diag: Diagnostic = .{};
    try testing.expectError(error.Refused, S.decodeAll(&.{ 5, 'a', 'b', 'c', 'd', 'e' }, &diag));
    try testing.expectEqual(Reason.field_too_long, diag.reason);
    try testing.expectEqual(@as(u64, 4), diag.bound);
    try testing.expectError(error.Refused, S.decodeAll(&.{ 2, 0xc3, 0x28 }, &diag));
    try testing.expectEqual(Reason.malformed, diag.reason);
    try testing.expectError(error.Refused, S.decodeAll(&.{ 1, 'a', 'x' }, &diag));
    try testing.expectEqual(Reason.malformed, diag.reason);
    try testing.expectEqualStrings("", diag.field); // trailing bytes belong to no field
    const ok = try S.decodeAll(&.{ 4, 'a', 'b', 'c', 'd' }, &diag);
    try testing.expectEqualStrings("abcd", ok.who);
}

test "schema: a list count is checked against the bytes left before any element is read" {
    const L = Record(&.{.{ .name = "ids", .T = List(Int(u64), 60000) }});
    var diag: Diagnostic = .{};
    // Claims 60000 eight-byte ids in an 8-byte frame.
    var frame: [2 + 8]u8 = undefined;
    std.mem.writeInt(u16, frame[0..2], 60000, .little);
    @memset(frame[2..], 0);
    try testing.expectError(error.Refused, L.decodeAll(&frame, &diag));
    try testing.expectEqual(Reason.malformed, diag.reason);
    try testing.expectEqual(@as(u64, 60000), diag.bound);
}

test "schema: an optional and a defaulted field read their presence byte strictly" {
    const O = Record(&.{
        .{ .name = "a", .T = Optional(Int(u8)) },
        .{ .name = "b", .T = Defaulted(Int(u8), 9) },
    });
    var diag: Diagnostic = .{};
    const v = try O.decodeAll(&.{ 0, 0 }, &diag);
    try testing.expectEqual(@as(?u8, null), v.a);
    try testing.expectEqual(@as(u8, 9), v.b);
    try testing.expectError(error.Refused, O.decodeAll(&.{ 2, 0 }, &diag));
    try testing.expectEqualStrings("a", diag.field);
}

test "schema: worst-case sizes are computed from the types" {
    try testing.expectEqual(@as(usize, 1 + 255), String(255).max_size);
    try testing.expectEqual(@as(usize, 2 + 256), String(256).max_size);
    try testing.expectEqual(@as(usize, 1 + 4 * (1 + 8 + 4)), List(Item, 4).max_size);
    try testing.expectEqual(Item.min_size, @as(usize, 1 + 4));
    comptime fitsIn(Sample, 1024, "the sample schema");
}

test "schema: encoding refuses a value its type can't carry" {
    var buf: [16]u8 = undefined;
    var w: Writer = .{ .buf = &buf };
    try testing.expectError(error.TooLong, String(2).encode(&w, "abc"));
    try testing.expectError(error.OutOfRange, Range(u32, 1, 10).encode(&w, 11));
    try testing.expectError(error.OutOfRange, Float(false).encode(&w, std.math.inf(f64)));
    var tiny: [1]u8 = undefined;
    var t: Writer = .{ .buf = &tiny };
    try testing.expectError(error.NoSpaceLeft, Int(u32).encode(&t, 1));
}

fn fuzzDecode(_: void, smith: *testing.Smith) !void {
    var input: [512]u8 = undefined;
    const len = smith.slice(&input);
    const bytes = input[0..len];
    var diag: Diagnostic = .{};
    // Any input either decodes or is refused with a reason; it never panics.
    const v = Sample.decodeAll(bytes, &diag) catch return;
    // What decodes re-encodes to bytes that decode to the same value: the
    // encoding of a decoded value is a fixed point (a defaulted field may
    // gain its presence byte, so it needn't equal the input).
    var once: [512]u8 = undefined;
    const b1 = try Sample.encodeInto(&once, v);
    const v2 = try Sample.decodeAll(b1, &diag);
    var twice: [512]u8 = undefined;
    try testing.expectEqualSlices(u8, b1, try Sample.encodeInto(&twice, v2));
}

test "schema: fuzz: any bytes decode or are refused, and what decodes re-encodes to an equal value" {
    var buf: [256]u8 = undefined;
    const valid = try sampleBytes(&buf);
    try testing.fuzz({}, fuzzDecode, .{ .corpus = &.{ valid, "", &.{ 7, 0, 0, 0, 0, 0, 0, 0, 1 } } });
}

test "schema: an absent defaulted field decodes to its default and re-encodes as an equal value" {
    var diag: Diagnostic = .{};
    const D = Record(&.{.{ .name = "limit", .T = Defaulted(Int(u32), 100) }});
    const v = try D.decodeAll(&.{0}, &diag);
    try testing.expectEqual(@as(u32, 100), v.limit);
    var buf: [8]u8 = undefined;
    const again = try D.decodeAll(try D.encodeInto(&buf, v), &diag);
    try testing.expectEqual(v.limit, again.limit);
}

test "schema: a range refuses below its min and a tail past its max" {
    var diag: Diagnostic = .{};
    const R = Record(&.{.{ .name = "n", .T = Range(i64, -5, 5) }});
    var b: [8]u8 = undefined;
    std.mem.writeInt(i64, &b, -6, .little);
    try testing.expectError(error.Refused, R.decodeAll(&b, &diag));
    try testing.expectEqual(Reason.out_of_range, diag.reason);
    try testing.expectEqual(@as(u64, 0), diag.bound);
    std.mem.writeInt(i64, &b, -5, .little);
    try testing.expectEqual(@as(i64, -5), (try R.decodeAll(&b, &diag)).n);
    const T = Record(&.{.{ .name = "rest", .T = Tail(3) }});
    try testing.expectError(error.Refused, T.decodeAll("abcd", &diag));
    try testing.expectEqual(Reason.field_too_long, diag.reason);
    try testing.expectEqualStrings("abc", (try T.decodeAll("abc", &diag)).rest);
}

test "schema: parser-bound bytes round-trip and keep their parser's name" {
    const P = Record(&.{.{ .name = "query", .T = Parsed("floql", 64) }});
    try testing.expectEqualStrings("floql", P.field_list[0].T.parser_name);
    var buf: [80]u8 = undefined;
    var diag: Diagnostic = .{};
    const v = try P.decodeAll(try P.encodeInto(&buf, .{ .query = "avg(cpu)" }), &diag);
    try testing.expectEqualStrings("avg(cpu)", v.query);
}

test "schema: the rules refuse a tail anywhere but last, a zero-size or tail list element, and an oversized schema" {
    try testing.expect(comptime recordRule(&.{ .{ .name = "t", .T = Optional(Tail(8)) }, .{ .name = "a", .T = Int(u8) } }) != null);
    try testing.expect(comptime recordRule(&.{ .{ .name = "t", .T = Defaulted(Bool, false) }, .{ .name = "a", .T = Optional(Tail(8)) } }) == null);
    const Nested = Record(&.{.{ .name = "rest", .T = Tail(8) }});
    try testing.expect(comptime recordRule(&.{ .{ .name = "n", .T = Nested }, .{ .name = "a", .T = Int(u8) } }) != null);
    try testing.expect(comptime listRule(Tail(4)) != null);
    try testing.expect(comptime listRule(Nested) != null);
    try testing.expect(comptime listRule(Int(u8)) == null);
    try testing.expect(comptime listRule(Record(&.{})) != null);
    try testing.expect(comptime recordRule(&.{ .{ .name = "a", .T = Bool }, .{ .name = "a", .T = Bool } }) != null);
    try testing.expect(comptime recordRule(&.{.{ .name = "Bad", .T = Bool }}) != null);
    try testing.expect(comptime fitsRule(String(100), 101, "s") == null);
    try testing.expect(comptime fitsRule(String(100), 100, "s") != null);
}
