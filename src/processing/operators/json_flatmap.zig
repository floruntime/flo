//! JSON FlatMap Operator (Declarative)
//!
//! A declarative 1:N transformation operator that explodes JSON arrays
//! into individual records — configured entirely from YAML.
//!
//! Given a JSONPath to an array field, each element of the array becomes
//! a separate output record. Non-array values or missing fields result
//! in zero output (the record is dropped).
//!
//! Optional `element_key` config extracts a key from each array element,
//! enabling downstream keyed operations (aggregate, reduce) on the
//! exploded records.
//!
//! YAML examples:
//!   ```yaml
//!   # Explode items array — each element becomes a record
//!   - type: flatmap
//!     name: explode-items
//!     array_field: "$.items"
//!
//!   # Explode with key extraction from each element
//!   - type: flatmap
//!     name: explode-events
//!     array_field: "$.events"
//!     element_key: "$.type"
//!
//!   # Nested array
//!   - type: flatmap
//!     name: explode-nested
//!     array_field: "$.data.records"
//!   ```

const std = @import("std");
const json_out = @import("json_out.zig");
const Allocator = std.mem.Allocator;
const path_segments = @import("path_segments.zig");
const Operator = @import("../operator.zig").Operator;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const log = @import("stdx").log;

pub const JsonFlatMapOperator = struct {
    name: []const u8,
    allocator: Allocator,

    /// Pre-split JSONPath segments for the array field
    array_segments: []const []const u8,

    /// Optional: pre-split JSONPath segments for key extraction from elements
    key_segments: ?[]const []const u8,

    const Self = @This();

    // =========================================================================
    // Lifecycle
    // =========================================================================

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        array_field: []const u8,
        element_key: ?[]const u8,
    ) !Self {
        const array_segs = try path_segments.splitOwned(allocator, array_field, .keep);
        errdefer path_segments.free(allocator, array_segs);
        const key_segs: ?[]const []const u8 = if (element_key) |k|
            try path_segments.splitOwned(allocator, k, .keep)
        else
            null;
        errdefer if (key_segs) |segs| path_segments.free(allocator, segs);

        return .{
            .name = try allocator.dupe(u8, name),
            .allocator = allocator,
            .array_segments = array_segs,
            .key_segments = key_segs,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.array_segments) |seg| self.allocator.free(seg);
        self.allocator.free(self.array_segments);
        if (self.key_segments) |segs| {
            for (segs) |seg| self.allocator.free(seg);
            self.allocator.free(segs);
        }
        self.allocator.free(self.name);
    }

    /// Return an Operator interface backed by this flatmap operator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    // =========================================================================
    // Vtable
    // =========================================================================

    const vtable = Operator.VTable{
        .processElement = processElement,
        .getName = getName,
        .close = closeFn,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Parse input JSON
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            rec.value,
            .{},
        ) catch return; // Skip non-JSON
        defer parsed.deinit();

        // Navigate to the array field
        const array_val = navigateJson(parsed.value, self.array_segments);
        const items = switch (array_val) {
            .array => |arr| arr.items,
            else => return, // Not an array — drop record
        };

        // Emit one record per array element
        for (items) |element| {
            // Serialize element to JSON string
            const elem_str = try json_out.valueAlloc(self.allocator, element);
            errdefer self.allocator.free(elem_str);

            // Extract key from element if configured, otherwise use original key
            const key = if (self.key_segments) |key_segs| blk: {
                const key_val = navigateJson(element, key_segs);
                break :blk try extractStringValue(self.allocator, key_val) orelse
                    try self.allocator.dupe(u8, rec.key);
            } else try self.allocator.dupe(u8, rec.key);

            try ctx.emit(.{
                .key = key,
                .value = elem_str,
                .event_time_ms = rec.event_time_ms,
                .source = rec.source,
                .headers = &.{},
                .owns_memory = true,
            });
        }
    }

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    fn splitJsonPath(allocator: Allocator, expr: []const u8) ![]const []const u8 {
        const path = if (std.mem.startsWith(u8, expr, "$."))
            expr[2..]
        else
            expr;

        var seg_list: std.ArrayListUnmanaged([]const u8) = .empty;
        var iter = std.mem.splitScalar(u8, path, '.');
        while (iter.next()) |seg| {
            try seg_list.append(allocator, seg);
        }
        return try seg_list.toOwnedSlice(allocator);
    }

    /// Like splitJsonPath but dupes each segment (owned copies).
    fn navigateJson(root: std.json.Value, segments: []const []const u8) std.json.Value {
        var current = root;
        for (segments) |seg| {
            switch (current) {
                .object => |obj| {
                    if (obj.get(seg)) |child| {
                        current = child;
                    } else {
                        return .null;
                    }
                },
                else => return .null,
            }
        }
        return current;
    }

    fn extractStringValue(allocator: Allocator, value: std.json.Value) !?[]u8 {
        return switch (value) {
            .string => |s| try allocator.dupe(u8, s),
            .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
            else => null,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

const OutputCollector = @import("../collector.zig").OutputCollector;
const OperatorMetrics = @import("../context.zig").OperatorMetrics;

test "JsonFlatMapOperator — getName" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "explode", "$.items", null);
    defer fm.deinit();

    const op = fm.operator();
    try std.testing.expectEqualStrings("explode", op.getName());
}

test "JsonFlatMapOperator — explode simple array" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "explode", "$.items", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"items\":[1,2,3]}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 3), collector.count());
    const out = collector.drain();
    try std.testing.expectEqualStrings("1", out[0].value);
    try std.testing.expectEqualStrings("2", out[1].value);
    try std.testing.expectEqualStrings("3", out[2].value);
    // Key preserved from input
    try std.testing.expectEqualStrings("k1", out[0].key);
    try std.testing.expectEqual(@as(i64, 100), out[0].event_time_ms);

    collector.clear();
}

test "JsonFlatMapOperator — explode object array" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "explode", "$.events", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"events\":[{\"type\":\"click\"},{\"type\":\"view\"}]}", 200), &ctx);

    try std.testing.expectEqual(@as(usize, 2), collector.count());
    const out = collector.drain();

    // Each element should be a valid JSON object
    const parsed0 = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed0.deinit();
    try std.testing.expectEqualStrings("click", parsed0.value.object.get("type").?.string);

    const parsed1 = try std.json.parseFromSlice(std.json.Value, allocator, out[1].value, .{});
    defer parsed1.deinit();
    try std.testing.expectEqualStrings("view", parsed1.value.object.get("type").?.string);

    collector.clear();
}

test "JsonFlatMapOperator — with element_key extraction" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "keyed-explode", "$.items", "$.id");
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("batch-1", "{\"items\":[{\"id\":\"a\",\"v\":1},{\"id\":\"b\",\"v\":2}]}", 300), &ctx);

    try std.testing.expectEqual(@as(usize, 2), collector.count());
    const out = collector.drain();

    // Keys should be extracted from elements
    try std.testing.expectEqualStrings("a", out[0].key);
    try std.testing.expectEqualStrings("b", out[1].key);

    collector.clear();
}

test "JsonFlatMapOperator — nested array field" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "nested", "$.data.records", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"data\":{\"records\":[10,20]}}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 2), collector.count());
    const out = collector.drain();
    try std.testing.expectEqualStrings("10", out[0].value);
    try std.testing.expectEqualStrings("20", out[1].value);

    collector.clear();
}

test "JsonFlatMapOperator — missing array field produces zero records" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "strict", "$.items", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"other\":1}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonFlatMapOperator — non-array field produces zero records" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "type-check", "$.items", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"items\":\"not-an-array\"}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonFlatMapOperator — invalid JSON skipped" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "validate", "$.items", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "not-json", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonFlatMapOperator — empty array produces zero records" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "empty", "$.items", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"items\":[]}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonFlatMapOperator — string array elements" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "strings", "$.tags", null);
    defer fm.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };

    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"tags\":[\"hello\",\"world\"]}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 2), collector.count());
    const out = collector.drain();
    try std.testing.expectEqualStrings("\"hello\"", out[0].value);
    try std.testing.expectEqualStrings("\"world\"", out[1].value);

    collector.clear();
}

test "JsonFlatMapOperator — each element is written as JSON" {
    const allocator = std.testing.allocator;
    var fm = try JsonFlatMapOperator.init(allocator, "explode", "$.items", null);
    defer fm.deinit();
    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .operator_name = "test-flatmap",
    };
    const op = fm.operator();
    try op.processElement(ProcessingRecord.init("k1",
        \\{"items":["a\"b\\c",{"k\"q":"v\""}]}
    , 0), &ctx);
    const out = collector.drain();
    try std.testing.expectEqual(@as(usize, 2), out.len);
    const s = try std.json.parseFromSlice([]const u8, allocator, out[0].value, .{});
    defer s.deinit();
    try std.testing.expectEqualStrings("a\"b\\c", s.value);
    const o = try std.json.parseFromSlice(std.json.Value, allocator, out[1].value, .{});
    defer o.deinit();
    try std.testing.expectEqualStrings("v\"", o.value.object.get("k\"q").?.string);
}
