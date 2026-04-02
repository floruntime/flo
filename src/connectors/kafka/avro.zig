//! Avro Binary Decoder → JSON
//!
//! Decodes Avro binary-encoded data into JSON using a schema from the
//! Schema Registry. Supports the Confluent wire format where the first
//! 5 bytes are [0x00][schema_id: i32 BE].
//!
//! Supported Avro types:
//!   null, boolean, int, long, float, double, string, bytes,
//!   record, array, map, enum, union, fixed
//!
//! Reference: Apache Avro 1.11.x Binary Encoding Specification

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const schema_registry = @import("schema_registry.zig");

// =============================================================================
// Avro Schema Representation (parsed from JSON)
// =============================================================================

pub const AvroType = union(enum) {
    null_type,
    boolean,
    int,
    long,
    float,
    double,
    string,
    bytes,
    record: *RecordSchema,
    @"enum": *EnumSchema,
    array: *ArraySchema,
    map: *MapSchema,
    @"union": *UnionSchema,
    fixed: *FixedSchema,
};

pub const RecordField = struct {
    name: []const u8,
    field_type: AvroType,
};

pub const RecordSchema = struct {
    name: []const u8,
    fields: []RecordField,
};

pub const EnumSchema = struct {
    name: []const u8,
    symbols: []const []const u8,
};

pub const ArraySchema = struct {
    items: AvroType,
};

pub const MapSchema = struct {
    values: AvroType,
};

pub const UnionSchema = struct {
    branches: []AvroType,
};

pub const FixedSchema = struct {
    name: []const u8,
    size: usize,
};

// =============================================================================
// Schema Parser (JSON → AvroType)
// =============================================================================

pub const SchemaParser = struct {
    /// Parent allocator — used for HashMap and temporary JSON parsing
    allocator: Allocator,
    /// Arena allocator — owns all schema objects (AvroType trees).
    /// IMPORTANT: Do NOT call arena.allocator() in init() — the struct gets
    /// moved after init returns, invalidating the pointer. Call it only in
    /// methods where `self: *SchemaParser` is a stable pointer.
    arena: std.heap.ArenaAllocator,
    /// Named types for resolving references (uses parent allocator)
    named_types: std.StringHashMap(AvroType),

    pub fn init(allocator: Allocator) SchemaParser {
        return .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .named_types = std.StringHashMap(AvroType).init(allocator),
        };
    }

    pub fn deinit(self: *SchemaParser) void {
        self.named_types.deinit();
        self.arena.deinit();
    }

    /// Parse an Avro schema from its JSON representation.
    pub fn parse(self: *SchemaParser, schema_json: []const u8) !AvroType {
        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, schema_json, .{}) catch
            return error.InvalidAvroSchema;
        defer parsed.deinit();
        return self.parseValue(parsed.value);
    }

    fn parseValue(self: *SchemaParser, value: std.json.Value) error{ InvalidAvroSchema, OutOfMemory }!AvroType {
        return switch (value) {
            .string => |s| self.parsePrimitive(s),
            .object => |obj| self.parseComplex(obj),
            .array => |arr| self.parseUnion(arr),
            else => error.InvalidAvroSchema,
        };
    }

    fn parsePrimitive(self: *SchemaParser, name: []const u8) !AvroType {
        if (std.mem.eql(u8, name, "null")) return .null_type;
        if (std.mem.eql(u8, name, "boolean")) return .boolean;
        if (std.mem.eql(u8, name, "int")) return .int;
        if (std.mem.eql(u8, name, "long")) return .long;
        if (std.mem.eql(u8, name, "float")) return .float;
        if (std.mem.eql(u8, name, "double")) return .double;
        if (std.mem.eql(u8, name, "string")) return .string;
        if (std.mem.eql(u8, name, "bytes")) return .bytes;

        // Named type reference
        if (self.named_types.get(name)) |t| return t;

        return error.InvalidAvroSchema;
    }

    fn parseComplex(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const type_str = (obj.get("type") orelse return error.InvalidAvroSchema).string;

        if (std.mem.eql(u8, type_str, "record")) {
            return self.parseRecord(obj);
        } else if (std.mem.eql(u8, type_str, "enum")) {
            return self.parseEnum(obj);
        } else if (std.mem.eql(u8, type_str, "array")) {
            return self.parseArray(obj);
        } else if (std.mem.eql(u8, type_str, "map")) {
            return self.parseMap(obj);
        } else if (std.mem.eql(u8, type_str, "fixed")) {
            return self.parseFixed(obj);
        } else {
            // Could be a primitive wrapped in {"type": "string"} etc
            return self.parsePrimitive(type_str);
        }
    }

    fn parseRecord(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const alloc = self.arena.allocator();
        const name_val = obj.get("name") orelse return error.InvalidAvroSchema;
        const name = switch (name_val) {
            .string => |s| s,
            else => return error.InvalidAvroSchema,
        };
        const fields_val = obj.get("fields") orelse return error.InvalidAvroSchema;
        const fields_arr = switch (fields_val) {
            .array => |a| a,
            else => return error.InvalidAvroSchema,
        };

        const record = try alloc.create(RecordSchema);
        const name_d = try alloc.dupe(u8, name);
        const fields = try alloc.alloc(RecordField, fields_arr.items.len);

        // Register name before parsing fields (allows recursive types)
        record.* = .{ .name = name_d, .fields = fields };
        try self.named_types.put(name_d, .{ .record = record });

        for (fields_arr.items, 0..) |field_val, i| {
            const field_obj = switch (field_val) {
                .object => |o| o,
                else => return error.InvalidAvroSchema,
            };
            const fn_val = field_obj.get("name") orelse return error.InvalidAvroSchema;
            const field_name = switch (fn_val) {
                .string => |s| s,
                else => return error.InvalidAvroSchema,
            };
            const ft_val = field_obj.get("type") orelse return error.InvalidAvroSchema;
            const field_type = try self.parseValue(ft_val);
            fields[i] = .{
                .name = try alloc.dupe(u8, field_name),
                .field_type = field_type,
            };
        }

        return .{ .record = record };
    }

    fn parseEnum(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const alloc = self.arena.allocator();
        const name_val = obj.get("name") orelse return error.InvalidAvroSchema;
        const name = switch (name_val) {
            .string => |s| s,
            else => return error.InvalidAvroSchema,
        };
        const symbols_val = obj.get("symbols") orelse return error.InvalidAvroSchema;
        const symbols_arr = switch (symbols_val) {
            .array => |a| a,
            else => return error.InvalidAvroSchema,
        };

        const enum_schema = try alloc.create(EnumSchema);
        const name_d = try alloc.dupe(u8, name);
        const symbols = try alloc.alloc([]const u8, symbols_arr.items.len);

        for (symbols_arr.items, 0..) |sym, i| {
            symbols[i] = switch (sym) {
                .string => |s| try alloc.dupe(u8, s),
                else => return error.InvalidAvroSchema,
            };
        }

        enum_schema.* = .{ .name = name_d, .symbols = symbols };
        try self.named_types.put(name_d, .{ .@"enum" = enum_schema });
        return .{ .@"enum" = enum_schema };
    }

    fn parseArray(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const alloc = self.arena.allocator();
        const items_val = obj.get("items") orelse return error.InvalidAvroSchema;
        const items_type = try self.parseValue(items_val);

        const array_schema = try alloc.create(ArraySchema);
        array_schema.* = .{ .items = items_type };
        return .{ .array = array_schema };
    }

    fn parseMap(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const alloc = self.arena.allocator();
        const values_val = obj.get("values") orelse return error.InvalidAvroSchema;
        const values_type = try self.parseValue(values_val);

        const map_schema = try alloc.create(MapSchema);
        map_schema.* = .{ .values = values_type };
        return .{ .map = map_schema };
    }

    fn parseUnion(self: *SchemaParser, arr: std.json.Array) !AvroType {
        const alloc = self.arena.allocator();
        const union_schema = try alloc.create(UnionSchema);
        const branches = try alloc.alloc(AvroType, arr.items.len);

        for (arr.items, 0..) |branch, i| {
            branches[i] = try self.parseValue(branch);
        }

        union_schema.* = .{ .branches = branches };
        return .{ .@"union" = union_schema };
    }

    fn parseFixed(self: *SchemaParser, obj: std.json.ObjectMap) !AvroType {
        const alloc = self.arena.allocator();
        const name_val = obj.get("name") orelse return error.InvalidAvroSchema;
        const name = switch (name_val) {
            .string => |s| s,
            else => return error.InvalidAvroSchema,
        };
        const size_val = obj.get("size") orelse return error.InvalidAvroSchema;
        const size: usize = switch (size_val) {
            .integer => |i| if (i >= 0) @intCast(i) else return error.InvalidAvroSchema,
            else => return error.InvalidAvroSchema,
        };

        const fixed_schema = try alloc.create(FixedSchema);
        const name_d = try alloc.dupe(u8, name);
        fixed_schema.* = .{ .name = name_d, .size = size };
        try self.named_types.put(name_d, .{ .fixed = fixed_schema });
        return .{ .fixed = fixed_schema };
    }
};

// =============================================================================
// Avro Binary Decoder → JSON
// =============================================================================

pub const AvroDecoder = struct {
    allocator: Allocator,

    pub fn init(allocator: Allocator) AvroDecoder {
        return .{ .allocator = allocator };
    }

    /// Decode Avro binary data to JSON string using the given schema.
    pub fn decode(self: *AvroDecoder, data: []const u8, schema: AvroType) ![]const u8 {
        var output: std.ArrayList(u8) = .{};
        errdefer output.deinit(self.allocator);

        var pos: usize = 0;
        try self.decodeValue(data, &pos, schema, &output);

        return output.toOwnedSlice(self.allocator);
    }

    fn decodeValue(
        self: *AvroDecoder,
        data: []const u8,
        pos: *usize,
        schema: AvroType,
        output: *std.ArrayList(u8),
    ) !void {
        switch (schema) {
            .null_type => {
                try output.appendSlice(self.allocator, "null");
            },
            .boolean => {
                if (pos.* >= data.len) return error.AvroDecodeFailed;
                const b = data[pos.*];
                pos.* += 1;
                try output.appendSlice(self.allocator, if (b != 0) "true" else "false");
            },
            .int => {
                const v = try readZigzagInt(data, pos);
                try appendInt(self.allocator, output, v);
            },
            .long => {
                const v = try readZigzagLong(data, pos);
                try appendLong(self.allocator, output, v);
            },
            .float => {
                if (pos.* + 4 > data.len) return error.AvroDecodeFailed;
                const bits = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4;
                const f: f32 = @bitCast(bits);
                var buf: [32]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{f}) catch "0";
                try output.appendSlice(self.allocator, s);
            },
            .double => {
                if (pos.* + 8 > data.len) return error.AvroDecodeFailed;
                const bits = std.mem.readInt(u64, data[pos.*..][0..8], .little);
                pos.* += 8;
                const d: f64 = @bitCast(bits);
                var buf: [32]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{d}) catch "0";
                try output.appendSlice(self.allocator, s);
            },
            .string => {
                const len = try readZigzagLong(data, pos);
                if (len < 0) return error.AvroDecodeFailed;
                const str_len: usize = @intCast(len);
                if (pos.* + str_len > data.len) return error.AvroDecodeFailed;
                const str = data[pos.*..][0..str_len];
                pos.* += str_len;
                try appendJsonString(self.allocator, output, str);
            },
            .bytes => {
                const len = try readZigzagLong(data, pos);
                if (len < 0) return error.AvroDecodeFailed;
                const byte_len: usize = @intCast(len);
                if (pos.* + byte_len > data.len) return error.AvroDecodeFailed;
                // Encode bytes as base64 JSON string
                const bytes = data[pos.*..][0..byte_len];
                pos.* += byte_len;
                try output.append(self.allocator, '"');
                const encoded_len = std.base64.standard.Encoder.calcSize(bytes.len);
                const start = output.items.len;
                try output.resize(self.allocator, start + encoded_len);
                _ = std.base64.standard.Encoder.encode(output.items[start..], bytes);
                try output.append(self.allocator, '"');
            },
            .record => |rec| {
                try output.append(self.allocator, '{');
                for (rec.fields, 0..) |field, i| {
                    if (i > 0) try output.append(self.allocator, ',');
                    try appendJsonString(self.allocator, output, field.name);
                    try output.append(self.allocator, ':');
                    try self.decodeValue(data, pos, field.field_type, output);
                }
                try output.append(self.allocator, '}');
            },
            .@"enum" => |e| {
                const idx = try readZigzagInt(data, pos);
                if (idx < 0 or idx >= @as(i32, @intCast(e.symbols.len))) return error.AvroDecodeFailed;
                try appendJsonString(self.allocator, output, e.symbols[@intCast(idx)]);
            },
            .array => |arr| {
                try output.append(self.allocator, '[');
                var first = true;
                while (true) {
                    const count = try readZigzagLong(data, pos);
                    if (count == 0) break;
                    const abs_count: usize = if (count < 0) blk: {
                        // Negative count means next long is byte size (skip it)
                        _ = try readZigzagLong(data, pos);
                        break :blk @intCast(-count);
                    } else @intCast(count);

                    for (0..abs_count) |_| {
                        if (!first) try output.append(self.allocator, ',');
                        first = false;
                        try self.decodeValue(data, pos, arr.items, output);
                    }
                }
                try output.append(self.allocator, ']');
            },
            .map => |m| {
                try output.append(self.allocator, '{');
                var first = true;
                while (true) {
                    const count = try readZigzagLong(data, pos);
                    if (count == 0) break;
                    const abs_count: usize = if (count < 0) blk: {
                        _ = try readZigzagLong(data, pos);
                        break :blk @intCast(-count);
                    } else @intCast(count);

                    for (0..abs_count) |_| {
                        if (!first) try output.append(self.allocator, ',');
                        first = false;
                        // Key is always string in Avro maps
                        const key_len = try readZigzagLong(data, pos);
                        if (key_len < 0) return error.AvroDecodeFailed;
                        const kl: usize = @intCast(key_len);
                        if (pos.* + kl > data.len) return error.AvroDecodeFailed;
                        try appendJsonString(self.allocator, output, data[pos.*..][0..kl]);
                        pos.* += kl;
                        try output.append(self.allocator, ':');
                        try self.decodeValue(data, pos, m.values, output);
                    }
                }
                try output.append(self.allocator, '}');
            },
            .@"union" => |u| {
                const branch_idx = try readZigzagLong(data, pos);
                if (branch_idx < 0 or branch_idx >= @as(i64, @intCast(u.branches.len)))
                    return error.AvroDecodeFailed;
                const branch = u.branches[@intCast(branch_idx)];
                // For nullable unions ["null", "T"], just output the value directly
                try self.decodeValue(data, pos, branch, output);
            },
            .fixed => |f| {
                if (pos.* + f.size > data.len) return error.AvroDecodeFailed;
                const bytes = data[pos.*..][0..f.size];
                pos.* += f.size;
                // Encode as base64
                try output.append(self.allocator, '"');
                const encoded_len = std.base64.standard.Encoder.calcSize(bytes.len);
                const start = output.items.len;
                try output.resize(self.allocator, start + encoded_len);
                _ = std.base64.standard.Encoder.encode(output.items[start..], bytes);
                try output.append(self.allocator, '"');
            },
        }
    }
};

// =============================================================================
// Avro Encoding Helpers
// =============================================================================

/// Read a zigzag-encoded varint (int32).
fn readZigzagInt(data: []const u8, pos: *usize) !i32 {
    const v = try readZigzagLong(data, pos);
    if (v < std.math.minInt(i32) or v > std.math.maxInt(i32)) return error.AvroDecodeFailed;
    return @intCast(v);
}

/// Read a zigzag-encoded varlong (int64).
fn readZigzagLong(data: []const u8, pos: *usize) !i64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len) return error.AvroDecodeFailed;
        const b = data[pos.*];
        pos.* += 1;
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) break;
        shift = std.math.add(u6, shift, 7) catch return error.AvroDecodeFailed;
    }
    // Zigzag decode: (n >>> 1) ^ -(n & 1)
    const signed: i64 = @bitCast(result);
    return @as(i64, @intCast(@as(u64, @bitCast(signed)) >> 1)) ^ -(signed & 1);
}

fn appendInt(allocator: Allocator, output: *std.ArrayList(u8), v: i32) !void {
    var buf: [12]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable;
    try output.appendSlice(allocator, s);
}

fn appendLong(allocator: Allocator, output: *std.ArrayList(u8), v: i64) !void {
    var buf: [21]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable;
    try output.appendSlice(allocator, s);
}

/// Write a JSON-escaped string (with surrounding quotes).
fn appendJsonString(allocator: Allocator, output: *std.ArrayList(u8), str: []const u8) !void {
    try output.append(allocator, '"');
    for (str) |c| {
        switch (c) {
            '"' => try output.appendSlice(allocator, "\\\""),
            '\\' => try output.appendSlice(allocator, "\\\\"),
            '\n' => try output.appendSlice(allocator, "\\n"),
            '\r' => try output.appendSlice(allocator, "\\r"),
            '\t' => try output.appendSlice(allocator, "\\t"),
            else => {
                if (c < 0x20) {
                    var esc: [6]u8 = undefined;
                    _ = std.fmt.bufPrint(&esc, "\\u{x:0>4}", .{c}) catch unreachable;
                    try output.appendSlice(allocator, &esc);
                } else {
                    try output.append(allocator, c);
                }
            },
        }
    }
    try output.append(allocator, '"');
}

// =============================================================================
// Tests
// =============================================================================

test "readZigzagLong basic values" {
    // 0 → encoded as 0x00
    {
        const data = [_]u8{0x00};
        var pos: usize = 0;
        try std.testing.expectEqual(@as(i64, 0), try readZigzagLong(&data, &pos));
    }
    // -1 → encoded as 0x01
    {
        const data = [_]u8{0x01};
        var pos: usize = 0;
        try std.testing.expectEqual(@as(i64, -1), try readZigzagLong(&data, &pos));
    }
    // 1 → encoded as 0x02
    {
        const data = [_]u8{0x02};
        var pos: usize = 0;
        try std.testing.expectEqual(@as(i64, 1), try readZigzagLong(&data, &pos));
    }
    // 64 → zigzag = 128 → 0x80 0x01
    {
        const data = [_]u8{ 0x80, 0x01 };
        var pos: usize = 0;
        try std.testing.expectEqual(@as(i64, 64), try readZigzagLong(&data, &pos));
    }
}

test "decode Avro null" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);
    const result = try decoder.decode(&.{}, .null_type);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("null", result);
}

test "decode Avro boolean" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);
    {
        const result = try decoder.decode(&[_]u8{0x01}, .boolean);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("true", result);
    }
    {
        const result = try decoder.decode(&[_]u8{0x00}, .boolean);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("false", result);
    }
}

test "decode Avro int" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);
    // zigzag(42) = 84 = 0x54
    const result = try decoder.decode(&[_]u8{0x54}, .int);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("42", result);
}

test "decode Avro string" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);
    // String: length=5 (zigzag=10=0x0a) + "hello"
    const data = [_]u8{ 0x0a, 'h', 'e', 'l', 'l', 'o' };
    const result = try decoder.decode(&data, .string);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("\"hello\"", result);
}

test "decode Avro record" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);

    // Schema: {"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}
    var fields = [_]RecordField{
        .{ .name = "name", .field_type = .string },
        .{ .name = "age", .field_type = .int },
    };
    var record = RecordSchema{ .name = "User", .fields = &fields };

    // Data: name="Alice" (len=5, zigzag=10), age=30 (zigzag=60=0x3c)
    const data = [_]u8{ 0x0a, 'A', 'l', 'i', 'c', 'e', 0x3c };
    const result = try decoder.decode(&data, .{ .record = &record });
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"name\":\"Alice\",\"age\":30}", result);
}

test "decode Avro union (nullable string)" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);

    // Union: ["null", "string"]
    var branches = [_]AvroType{ .null_type, .string };
    var union_schema = UnionSchema{ .branches = &branches };

    // Branch index 1 (string) + string "hi" (len=2, zigzag=4)
    const data = [_]u8{ 0x02, 0x04, 'h', 'i' };
    const result = try decoder.decode(&data, .{ .@"union" = &union_schema });
    defer allocator.free(result);
    try std.testing.expectEqualStrings("\"hi\"", result);
}

test "decode Avro union (null branch)" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);

    var branches = [_]AvroType{ .null_type, .string };
    var union_schema = UnionSchema{ .branches = &branches };

    // Branch index 0 (null)
    const data = [_]u8{0x00};
    const result = try decoder.decode(&data, .{ .@"union" = &union_schema });
    defer allocator.free(result);
    try std.testing.expectEqualStrings("null", result);
}

test "decode Avro array of ints" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);

    var array_schema = ArraySchema{ .items = .int };

    // Array: block count=3 (zigzag=6), then [1,2,3] (zigzag: 2,4,6), then 0 terminator
    const data = [_]u8{ 0x06, 0x02, 0x04, 0x06, 0x00 };
    const result = try decoder.decode(&data, .{ .array = &array_schema });
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[1,2,3]", result);
}

test "decode Avro enum" {
    const allocator = std.testing.allocator;
    var decoder = AvroDecoder.init(allocator);

    const symbols = [_][]const u8{ "RED", "GREEN", "BLUE" };
    var enum_schema = EnumSchema{ .name = "Color", .symbols = &symbols };

    // Index 1 = GREEN (zigzag: 2)
    const data = [_]u8{0x02};
    const result = try decoder.decode(&data, .{ .@"enum" = &enum_schema });
    defer allocator.free(result);
    try std.testing.expectEqualStrings("\"GREEN\"", result);
}

test "SchemaParser parse primitive" {
    const allocator = std.testing.allocator;
    var parser = SchemaParser.init(allocator);
    defer parser.deinit();

    const schema = try parser.parse("\"string\"");
    try std.testing.expect(schema == .string);
}

test "SchemaParser parse record" {
    const allocator = std.testing.allocator;
    var parser = SchemaParser.init(allocator);
    defer parser.deinit();

    const schema_json =
        \\{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}
    ;
    const schema = try parser.parse(schema_json);
    switch (schema) {
        .record => |r| {
            try std.testing.expectEqualStrings("User", r.name);
            try std.testing.expectEqual(@as(usize, 2), r.fields.len);
            try std.testing.expectEqualStrings("name", r.fields[0].name);
            try std.testing.expect(r.fields[0].field_type == .string);
            try std.testing.expectEqualStrings("age", r.fields[1].name);
            try std.testing.expect(r.fields[1].field_type == .int);
        },
        else => return error.InvalidAvroSchema,
    }
}

test "SchemaParser parse union" {
    const allocator = std.testing.allocator;
    var parser = SchemaParser.init(allocator);
    defer parser.deinit();

    const schema = try parser.parse("[\"null\",\"string\"]");
    switch (schema) {
        .@"union" => |u| {
            try std.testing.expectEqual(@as(usize, 2), u.branches.len);
            try std.testing.expect(u.branches[0] == .null_type);
            try std.testing.expect(u.branches[1] == .string);
        },
        else => return error.InvalidAvroSchema,
    }
}
