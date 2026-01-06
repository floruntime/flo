//! JSON Map Operator (Declarative)
//!
//! A declarative 1:1 transformation operator that restructures JSON
//! records by projecting, renaming, and adding fields — all configured
//! from YAML without WASM or function pointers.
//!
//! Each YAML config entry defines an output field:
//!   - Values starting with `$.` are JSONPath extractions from the input
//!   - Other values are treated as string constants
//!
//! Output is always a JSON object with the specified fields.
//!
//! YAML examples:
//!   ```yaml
//!   # Extract and rename fields
//!   - type: map
//!     name: extract-user
//!     user_id: "$.data.user_id"
//!     amount: "$.transaction.amount"
//!     currency: "$.transaction.currency"
//!
//!   # Mix extraction with constants
//!   - type: map
//!     name: tag-source
//!     source: "payment-service"
//!     user: "$.user_id"
//!     total: "$.amount"
//!
//!   # Flatten nested structure
//!   - type: map
//!     name: flatten
//!     id: "$.metadata.id"
//!     region: "$.metadata.region"
//!     value: "$.payload.value"
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
const log = @import("stdx").log;

pub const JsonMapOperator = struct {
    name: []const u8,
    allocator: Allocator,

    /// Mapping entries: each defines one output field.
    mappings: []const Mapping,

    pub const Mapping = struct {
        /// Output field name
        output_field: []const u8,
        /// Either a JSONPath (segments) or a constant string
        source: Source,

        pub const Source = union(enum) {
            /// JSONPath extraction — pre-split path segments
            json_path: []const []const u8,
            /// Constant string value
            constant: []const u8,
        };
    };

    const Self = @This();

    // =========================================================================
    // Lifecycle
    // =========================================================================

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        config_entries: []const ConfigEntry,
    ) !Self {
        var mappings_list: std.ArrayListUnmanaged(Mapping) = .{};
        errdefer {
            for (mappings_list.items) |m| {
                switch (m.source) {
                    .json_path => |segs| allocator.free(segs),
                    .constant => {},
                }
            }
            mappings_list.deinit(allocator);
        }

        for (config_entries) |entry| {
            const source: Mapping.Source = if (std.mem.startsWith(u8, entry.value, "$.")) blk: {
                // JSONPath extraction — split into segments
                const field_path = entry.value[2..];
                var seg_list: std.ArrayListUnmanaged([]const u8) = .{};
                var iter = std.mem.splitScalar(u8, field_path, '.');
                while (iter.next()) |seg| {
                    try seg_list.append(allocator, seg);
                }
                break :blk .{ .json_path = try seg_list.toOwnedSlice(allocator) };
            } else .{ .constant = entry.value };

            try mappings_list.append(allocator, .{
                .output_field = entry.key,
                .source = source,
            });
        }

        return .{
            .name = name,
            .allocator = allocator,
            .mappings = try mappings_list.toOwnedSlice(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.mappings) |m| {
            switch (m.source) {
                .json_path => |segs| self.allocator.free(segs),
                .constant => {},
            }
        }
        self.allocator.free(self.mappings);
    }

    /// Return an Operator interface backed by this map operator
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
        .processWatermark = processWatermark,
        .getName = getName,
        .close = closeFn,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.mappings.len == 0) {
            // No mappings — pass through unchanged
            try ctx.emit(rec);
            return;
        }

        // Parse input JSON (lazy — only if we have JSONPath mappings)
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            rec.value,
            .{},
        ) catch {
            // If input isn't valid JSON, skip this record
            return;
        };
        defer parsed.deinit();

        // Build output JSON object
        var output_buf: std.ArrayListUnmanaged(u8) = .{};
        defer output_buf.deinit(self.allocator);

        try output_buf.append(self.allocator, '{');
        var first = true;

        for (self.mappings) |mapping| {
            if (!first) {
                try output_buf.append(self.allocator, ',');
            }
            first = false;

            // Write field name
            try output_buf.append(self.allocator, '"');
            try output_buf.appendSlice(self.allocator, mapping.output_field);
            try output_buf.appendSlice(self.allocator, "\":");

            switch (mapping.source) {
                .constant => |val| {
                    // Emit as JSON string
                    try output_buf.append(self.allocator, '"');
                    try output_buf.appendSlice(self.allocator, val);
                    try output_buf.append(self.allocator, '"');
                },
                .json_path => |segments| {
                    // Navigate JSON tree
                    const extracted = navigateJson(parsed.value, segments);
                    try writeJsonValue(&output_buf, self.allocator, extracted);
                },
            }
        }

        try output_buf.append(self.allocator, '}');

        // Emit as owned record
        const output = try self.allocator.dupe(u8, output_buf.items);
        errdefer self.allocator.free(output);

        const key_dupe = try self.allocator.dupe(u8, rec.key);

        try ctx.emit(.{
            .key = key_dupe,
            .value = output,
            .event_time_ms = rec.event_time_ms,
            .source = rec.source,
            .headers = &.{},
            .owns_memory = true,
        });
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {}

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

    fn writeJsonValue(buf: *std.ArrayListUnmanaged(u8), allocator: Allocator, value: std.json.Value) !void {
        switch (value) {
            .null => try buf.appendSlice(allocator, "null"),
            .bool => |b| try buf.appendSlice(allocator, if (b) "true" else "false"),
            .integer => |i| {
                const formatted = try std.fmt.allocPrint(allocator, "{d}", .{i});
                defer allocator.free(formatted);
                try buf.appendSlice(allocator, formatted);
            },
            .float => |f| {
                const formatted = try std.fmt.allocPrint(allocator, "{d}", .{f});
                defer allocator.free(formatted);
                try buf.appendSlice(allocator, formatted);
            },
            .string => |s| {
                try buf.append(allocator, '"');
                try buf.appendSlice(allocator, s);
                try buf.append(allocator, '"');
            },
            .array => |arr| {
                try buf.append(allocator, '[');
                for (arr.items, 0..) |item, idx| {
                    if (idx > 0) try buf.append(allocator, ',');
                    try writeJsonValue(buf, allocator, item);
                }
                try buf.append(allocator, ']');
            },
            .object => |obj| {
                try buf.append(allocator, '{');
                var first = true;
                var it = obj.iterator();
                while (it.next()) |entry| {
                    if (!first) try buf.append(allocator, ',');
                    first = false;
                    try buf.append(allocator, '"');
                    try buf.appendSlice(allocator, entry.key_ptr.*);
                    try buf.appendSlice(allocator, "\":");
                    try writeJsonValue(buf, allocator, entry.value_ptr.*);
                }
                try buf.append(allocator, '}');
            },
            .number_string => |s| {
                try buf.appendSlice(allocator, s);
            },
        }
    }

};

// =============================================================================
// Tests
// =============================================================================

const OutputCollector = @import("../collector.zig").OutputCollector;
const OperatorMetrics = @import("../context.zig").OperatorMetrics;
const ConfigEntry = @import("../definition.zig").OperatorSpec.ConfigEntry;

test "JsonMapOperator — getName" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "user", .value = "$.user_id" },
    };
    var mapper = try JsonMapOperator.init(allocator, "extract", &entries);
    defer mapper.deinit();

    const op = mapper.operator();
    try std.testing.expectEqualStrings("extract", op.getName());
}

test "JsonMapOperator — extract single field" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "user", .value = "$.user_id" },
    };
    var mapper = try JsonMapOperator.init(allocator, "extract", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"user_id\":\"alice\",\"age\":30}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();
    // Output should be {"user":"alice"}
    try std.testing.expectEqualStrings("{\"user\":\"alice\"}", out[0].value);
    try std.testing.expectEqualStrings("k1", out[0].key);
    try std.testing.expectEqual(@as(i64, 100), out[0].event_time_ms);

    collector.clear();
}

test "JsonMapOperator — multiple fields with rename" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "id", .value = "$.user_id" },
        .{ .key = "total", .value = "$.amount" },
    };
    var mapper = try JsonMapOperator.init(allocator, "project", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"user_id\":\"bob\",\"amount\":42.5}", 200), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();

    // Parse output to verify fields
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed.deinit();

    const id_val = parsed.value.object.get("id").?;
    try std.testing.expectEqualStrings("bob", id_val.string);

    const total_val = parsed.value.object.get("total").?;
    try std.testing.expectEqual(@as(f64, 42.5), total_val.float);

    collector.clear();
}

test "JsonMapOperator — constant values" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "source", .value = "payment-service" },
        .{ .key = "user", .value = "$.user_id" },
    };
    var mapper = try JsonMapOperator.init(allocator, "tag", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"user_id\":\"charlie\"}", 300), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("payment-service", parsed.value.object.get("source").?.string);
    try std.testing.expectEqualStrings("charlie", parsed.value.object.get("user").?.string);

    collector.clear();
}

test "JsonMapOperator — nested field extraction" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "region", .value = "$.metadata.region" },
        .{ .key = "value", .value = "$.payload.value" },
    };
    var mapper = try JsonMapOperator.init(allocator, "flatten", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"metadata\":{\"region\":\"us-east\"},\"payload\":{\"value\":99}}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("us-east", parsed.value.object.get("region").?.string);
    try std.testing.expectEqual(@as(i64, 99), parsed.value.object.get("value").?.integer);

    collector.clear();
}

test "JsonMapOperator — missing field becomes null" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "name", .value = "$.name" },
        .{ .key = "missing", .value = "$.nonexistent" },
    };
    var mapper = try JsonMapOperator.init(allocator, "nullable", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"name\":\"Alice\"}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.object.get("name").?.string);
    try std.testing.expect(parsed.value.object.get("missing").? == .null);

    collector.clear();
}

test "JsonMapOperator — invalid JSON input skipped" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "x", .value = "$.field" },
    };
    var mapper = try JsonMapOperator.init(allocator, "strict", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "not-json", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonMapOperator — preserves nested objects/arrays" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{
        .{ .key = "tags", .value = "$.tags" },
        .{ .key = "meta", .value = "$.metadata" },
    };
    var mapper = try JsonMapOperator.init(allocator, "preserve", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "{\"tags\":[\"a\",\"b\"],\"metadata\":{\"v\":1}}", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();

    // Verify the output is valid JSON with nested structures
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out[0].value, .{});
    defer parsed.deinit();

    const tags = parsed.value.object.get("tags").?.array;
    try std.testing.expectEqual(@as(usize, 2), tags.items.len);
    try std.testing.expectEqualStrings("a", tags.items[0].string);

    const meta = parsed.value.object.get("meta").?.object;
    try std.testing.expectEqual(@as(i64, 1), meta.get("v").?.integer);

    collector.clear();
}

test "JsonMapOperator — no mappings passes through" {
    const allocator = std.testing.allocator;
    const entries = [_]ConfigEntry{};
    var mapper = try JsonMapOperator.init(allocator, "noop", &entries);
    defer mapper.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-map",
    };

    const op = mapper.operator();
    try op.processElement(ProcessingRecord.init("k1", "raw-data", 100), &ctx);

    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();
    try std.testing.expectEqualStrings("raw-data", out[0].value);

    collector.clear();
}
