//! JSON Key-By Operator (Declarative)
//!
//! Re-keys records by extracting a field from the JSON-encoded value
//! using JSONPath-style expressions (e.g., `$.user_id`, `$.metadata.region`).
//!
//! Unlike KeyByOperator (which takes a Zig function pointer), this
//! operator can be configured from YAML with a `key_expression` property.
//!
//! If the value is not valid JSON or the field doesn't exist, the
//! record passes through with its original key unchanged. Non-string
//! JSON values (integers, booleans) are stringified as keys.
//!
//! key_expression format (JSONPath-style):
//!   - `"$.user_id"`           — top-level field
//!   - `"$.metadata.region"`   — nested field (dot-separated path)
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: keyby
//!       name: by-user
//!       key_expression: "$.user_id"
//!   ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const noOpSnapshot = @import("../operator.zig").noOpSnapshot;
const noOpRestore = @import("../operator.zig").noOpRestore;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;

pub const JsonKeyByOperator = struct {
    name: []const u8,
    /// The raw key_expression from YAML (e.g., "$.user_id")
    key_expression: []const u8,
    /// Pre-split field path segments (stripped of "$." prefix)
    segments: []const []const u8,
    /// Scratch buffer for stringifying non-string key values
    scratch: [64]u8 = undefined,

    const Self = @This();

    /// Create a JSON key-by operator.
    /// `key_expression` is the JSONPath expression from YAML config (e.g., "$.user_id").
    /// Also accepts plain field names without "$." prefix for convenience.
    /// Dupes all borrowed strings — safe to free the source after init.
    pub fn init(allocator: Allocator, name: []const u8, key_expression: []const u8) !Self {
        // Strip "$." prefix if present
        const field_path = if (std.mem.startsWith(u8, key_expression, "$."))
            key_expression[2..]
        else
            key_expression;

        // Pre-split dotted path for efficient evaluation (owned copies)
        var seg_list: std.ArrayListUnmanaged([]const u8) = .{};
        var iter = std.mem.splitScalar(u8, field_path, '.');
        while (iter.next()) |seg| {
            if (seg.len > 0) try seg_list.append(allocator, try allocator.dupe(u8, seg));
        }
        return .{
            .name = try allocator.dupe(u8, name),
            .key_expression = try allocator.dupe(u8, key_expression),
            .segments = try seg_list.toOwnedSlice(allocator),
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        for (self.segments) |seg| allocator.free(seg);
        allocator.free(self.segments);
        allocator.free(self.key_expression);
        allocator.free(self.name);
    }

    /// Return an Operator interface backed by this JsonKeyByOperator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = close,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Parse JSON value to extract the key field
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            std.heap.page_allocator,
            rec.value,
            .{},
        ) catch {
            // Not valid JSON — pass through with original key
            try ctx.emitWithKey(rec.key, rec.value, rec.event_time_ms);
            return;
        };
        defer parsed.deinit();

        // Walk segments to find the target field
        var current = parsed.value;
        for (self.segments) |seg| {
            if (current != .object) {
                try ctx.emitWithKey(rec.key, rec.value, rec.event_time_ms);
                return;
            }
            current = current.object.get(seg) orelse {
                try ctx.emitWithKey(rec.key, rec.value, rec.event_time_ms);
                return;
            };
        }

        // Extract key value — must dupe strings extracted from parsed JSON
        // because parsed.deinit() will free them before the next operator runs.
        switch (current) {
            .string => |s| {
                const duped = try ctx.allocator.dupe(u8, s);
                try ctx.emitWithKey(duped, rec.value, rec.event_time_ms);
            },
            .integer => |n| {
                // Stringify integer value as key
                const formatted = std.fmt.bufPrint(&self.scratch, "{d}", .{n}) catch {
                    try ctx.emitWithKey(rec.key, rec.value, rec.event_time_ms);
                    return;
                };
                const duped = try ctx.allocator.dupe(u8, formatted);
                try ctx.emitWithKey(duped, rec.value, rec.event_time_ms);
            },
            .bool => |b| try ctx.emitWithKey(if (b) "true" else "false", rec.value, rec.event_time_ms),
            else => try ctx.emitWithKey(rec.key, rec.value, rec.event_time_ms),
        }
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {}

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}

    // =========================================================================
    // JSON field extraction (for testing — allocates a copy of the result)
    // =========================================================================

    /// Extract a JSON field value as a string. Caller owns the returned memory.
    /// Returns null if parsing fails or field doesn't exist.
    /// Non-string values (integers, booleans) are stringified.
    pub fn extractKeyAlloc(self: *const Self, allocator: Allocator, value: []const u8) ?[]const u8 {
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            allocator,
            value,
            .{},
        ) catch return null;
        defer parsed.deinit();

        var current = parsed.value;
        for (self.segments) |seg| {
            if (current != .object) return null;
            current = current.object.get(seg) orelse return null;
        }

        return switch (current) {
            .string => |s| allocator.dupe(u8, s) catch null,
            .integer => |n| std.fmt.allocPrint(allocator, "{d}", .{n}) catch null,
            .bool => |b| allocator.dupe(u8, if (b) "true" else "false") catch null,
            else => null,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "JsonKeyByOperator — getName" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "by-user", "$.user_id");
    defer op.deinit(std.testing.allocator);

    const iface = op.operator();
    try std.testing.expectEqualStrings("by-user", iface.getName());
}

test "JsonKeyByOperator — extractKey with $.field" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.user_id");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"user_id\":\"alice\",\"amount\":42}");
    defer if (key) |k| std.testing.allocator.free(k);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("alice", key.?);
}

test "JsonKeyByOperator — extractKey nested $.path.field" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.metadata.region");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"metadata\":{\"region\":\"eu-west\"}}");
    defer if (key) |k| std.testing.allocator.free(k);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("eu-west", key.?);
}

test "JsonKeyByOperator — extractKey integer value stringified" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.count");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"count\":42}");
    defer if (key) |k| std.testing.allocator.free(k);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("42", key.?);
}

test "JsonKeyByOperator — extractKey boolean value" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.active");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"active\":true}");
    defer if (key) |k| std.testing.allocator.free(k);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("true", key.?);
}

test "JsonKeyByOperator — extractKey missing field" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.missing_field");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"user_id\":\"alice\"}");
    try std.testing.expect(key == null);
}

test "JsonKeyByOperator — extractKey invalid JSON" {
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "$.field");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "not-json");
    try std.testing.expect(key == null);
}

test "JsonKeyByOperator — plain field name without $. prefix" {
    // Backward compatibility: "user_id" works same as "$.user_id"
    var op = try JsonKeyByOperator.init(std.testing.allocator, "test", "user_id");
    defer op.deinit(std.testing.allocator);

    const key = op.extractKeyAlloc(std.testing.allocator, "{\"user_id\":\"bob\"}");
    defer if (key) |k| std.testing.allocator.free(k);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("bob", key.?);
}
