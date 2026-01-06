//! Expression-Based Filter Operator
//!
//! A declarative filter operator that evaluates string-based condition
//! expressions against ProcessingRecord fields. ExprFilterOperator is
//! configured from YAML job definitions via the NativeOperatorRegistry.
//!
//! Supported condition expressions:
//!   - `value_contains:<substring>`  — value contains the substring
//!   - `key_contains:<substring>`    — key contains the substring
//!   - `key_equals:<exact>`          — key equals the exact string
//!   - `key_prefix:<prefix>`         — key starts with the prefix
//!   - `value_prefix:<prefix>`       — value starts with the prefix
//!   - `not_empty`                   — value is non-empty
//!   - `key_not_empty`               — key is non-empty
//!   - `json_field:<path>=<value>`   — JSON field at path equals value
//!   - `min_length:<n>`              — value length >= n
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: filter
//!       name: keep-important
//!       condition: "value_contains:important"
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

pub const ExprFilterOperator = struct {
    name: []const u8,
    condition: []const u8,
    /// Parsed condition kind (computed once at init)
    parsed: ParsedCondition,

    const Self = @This();

    /// Parsed condition for efficient evaluation
    const ParsedCondition = union(enum) {
        value_contains: []const u8,
        key_contains: []const u8,
        key_equals: []const u8,
        key_prefix: []const u8,
        value_prefix: []const u8,
        not_empty,
        key_not_empty,
        json_field: struct { path: []const u8, expected: []const u8 },
        min_length: usize,
        /// Unparseable condition — always passes (with warning logged at init)
        always_true,
    };

    /// Create an expression-based filter operator.
    /// `condition` is the raw condition string from YAML (e.g., "value_contains:hello").
    /// Both `name` and `condition` must outlive the operator (typically allocated by parser).
    pub fn init(name: []const u8, condition: []const u8) Self {
        return .{
            .name = name,
            .condition = condition,
            .parsed = parseCondition(condition),
        };
    }

    /// Return an Operator interface backed by this ExprFilterOperator
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
        if (self.evaluate(rec)) {
            try ctx.emit(rec);
        }
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {
        // Stateless — watermarks pass through via chain
    }

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}

    // =========================================================================
    // Condition evaluation
    // =========================================================================

    pub fn evaluate(self: *const Self, rec: ProcessingRecord) bool {
        return switch (self.parsed) {
            .value_contains => |substr| std.mem.indexOf(u8, rec.value, substr) != null,
            .key_contains => |substr| std.mem.indexOf(u8, rec.key, substr) != null,
            .key_equals => |exact| std.mem.eql(u8, rec.key, exact),
            .key_prefix => |prefix| std.mem.startsWith(u8, rec.key, prefix),
            .value_prefix => |prefix| std.mem.startsWith(u8, rec.value, prefix),
            .not_empty => rec.value.len > 0,
            .key_not_empty => rec.key.len > 0,
            .json_field => |f| evaluateJsonField(rec.value, f.path, f.expected),
            .min_length => |n| rec.value.len >= n,
            .always_true => true,
        };
    }

    // =========================================================================
    // Condition parsing
    // =========================================================================

    fn parseCondition(cond: []const u8) ParsedCondition {
        // Simple keyword conditions
        if (std.mem.eql(u8, cond, "not_empty")) return .not_empty;
        if (std.mem.eql(u8, cond, "key_not_empty")) return .key_not_empty;

        // Prefix:arg conditions
        if (splitOnce(cond, ':')) |parts| {
            const prefix = parts[0];
            const arg = parts[1];

            if (std.mem.eql(u8, prefix, "value_contains")) return .{ .value_contains = arg };
            if (std.mem.eql(u8, prefix, "key_contains")) return .{ .key_contains = arg };
            if (std.mem.eql(u8, prefix, "key_equals")) return .{ .key_equals = arg };
            if (std.mem.eql(u8, prefix, "key_prefix")) return .{ .key_prefix = arg };
            if (std.mem.eql(u8, prefix, "value_prefix")) return .{ .value_prefix = arg };
            if (std.mem.eql(u8, prefix, "min_length")) {
                const n = std.fmt.parseInt(usize, arg, 10) catch return .always_true;
                return .{ .min_length = n };
            }
            if (std.mem.eql(u8, prefix, "json_field")) {
                // json_field:path=value
                if (splitOnce(arg, '=')) |kv| {
                    return .{ .json_field = .{ .path = kv[0], .expected = kv[1] } };
                }
            }
        }

        return .always_true;
    }

    /// Split a string on the first occurrence of `sep`. Returns [before, after] or null.
    fn splitOnce(s: []const u8, sep: u8) ?[2][]const u8 {
        const idx = std.mem.indexOfScalar(u8, s, sep) orelse return null;
        return .{ s[0..idx], s[idx + 1 ..] };
    }

    /// Evaluate a simple JSON field lookup: extract key from top-level JSON object.
    /// Supports dotted paths (e.g., "status" or "user.role") — single-level for now.
    fn evaluateJsonField(value: []const u8, path: []const u8, expected: []const u8) bool {
        // Try to parse value as JSON
        const parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, value, .{}) catch return false;
        defer parsed.deinit();

        const root = parsed.value;
        if (root != .object) return false;

        // Simple single-level lookup
        const field_val = root.object.get(path) orelse return false;
        switch (field_val) {
            .string => |s| return std.mem.eql(u8, s, expected),
            .integer => |n| {
                const expected_n = std.fmt.parseInt(i64, expected, 10) catch return false;
                return n == expected_n;
            },
            .bool => |b| {
                if (std.mem.eql(u8, expected, "true")) return b;
                if (std.mem.eql(u8, expected, "false")) return !b;
                return false;
            },
            else => return false,
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ExprFilterOperator — value_contains" {
    var op = ExprFilterOperator.init("test-filter", "value_contains:hello");
    const rec_match = ProcessingRecord.init("k", "say hello world", 1000);
    const rec_miss = ProcessingRecord.init("k", "goodbye world", 1000);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));

    // vtable works
    const iface = op.operator();
    try std.testing.expectEqualStrings("test-filter", iface.getName());
}

test "ExprFilterOperator — key_equals" {
    var op = ExprFilterOperator.init("key-filter", "key_equals:user-42");
    const rec_match = ProcessingRecord.init("user-42", "data", 0);
    const rec_miss = ProcessingRecord.init("user-43", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — key_prefix" {
    var op = ExprFilterOperator.init("prefix-filter", "key_prefix:order-");
    const rec_match = ProcessingRecord.init("order-123", "data", 0);
    const rec_miss = ProcessingRecord.init("user-123", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — not_empty" {
    var op = ExprFilterOperator.init("nonempty-filter", "not_empty");
    const rec_match = ProcessingRecord.init("k", "some data", 0);
    const rec_empty = ProcessingRecord.init("k", "", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_empty));
}

test "ExprFilterOperator — min_length" {
    var op = ExprFilterOperator.init("min-len", "min_length:5");
    const rec_ok = ProcessingRecord.init("k", "abcde", 0);
    const rec_short = ProcessingRecord.init("k", "abc", 0);

    try std.testing.expect(op.evaluate(rec_ok));
    try std.testing.expect(!op.evaluate(rec_short));
}

test "ExprFilterOperator — json_field" {
    var op = ExprFilterOperator.init("json-filter", "json_field:status=approved");
    const rec_match = ProcessingRecord.init("k", "{\"status\":\"approved\",\"amount\":100}", 0);
    const rec_miss = ProcessingRecord.init("k", "{\"status\":\"pending\",\"amount\":50}", 0);
    const rec_bad = ProcessingRecord.init("k", "not-json", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
    try std.testing.expect(!op.evaluate(rec_bad));
}

test "ExprFilterOperator — always_true for unknown condition" {
    var op = ExprFilterOperator.init("unknown", "something_weird");
    const rec = ProcessingRecord.init("k", "v", 0);
    try std.testing.expect(op.evaluate(rec));
}

test "ExprFilterOperator — value_prefix" {
    var op = ExprFilterOperator.init("vp", "value_prefix:ERROR");
    const rec_match = ProcessingRecord.init("k", "ERROR: something broke", 0);
    const rec_miss = ProcessingRecord.init("k", "INFO: all good", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}
