//! Native Operator Registry
//!
//! Maps operator type names from YAML (e.g., "filter", "passthrough", "keyby")
//! to factory functions that create concrete Operator instances.
//!
//! This is the core piece that enables declarative operator configuration:
//! instead of requiring WASM modules, users can specify built-in operators
//! by type name with config properties in their YAML job definitions.
//!
//! Supported type names:
//!   - `filter`      — ExprFilterOperator (requires `condition` config)
//!   - `passthrough`  — PassthroughOperator (no config needed)
//!   - `keyby`        — JsonKeyByOperator (requires `key_expression` config)
//!   - `aggregate`    — JsonAggregateOperator (requires `function` config)
//!   - `map`          — JsonMapOperator (config entries map output_field→JSONPath/constant)
//!   - `flatmap`      — JsonFlatMapOperator (requires `array_field` config)
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: filter
//!       name: keep-important
//!       condition: "value_contains:important"
//!     - type: keyby
//!       name: by-user
//!       key_expression: "$.user_id"
//!     - type: aggregate
//!       name: hourly-sum
//!       function: sum
//!       field: "$.amount"
//!       window: tumbling
//!       window_size: 3600
//!     - type: passthrough
//!       name: debug-tap
//!     - type: map
//!       name: extract-fields
//!       user_id: "$.data.user_id"
//!       amount: "$.transaction.amount"
//!     - type: flatmap
//!       name: explode-items
//!       array_field: "$.items"
//!   ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const OperatorSpec = @import("../definition.zig").OperatorSpec;
const ExprFilterOperator = @import("expr_filter.zig").ExprFilterOperator;
const PassthroughOperator = @import("passthrough.zig").PassthroughOperator;
const JsonKeyByOperator = @import("json_keyby.zig").JsonKeyByOperator;
const JsonAggregateOperator = @import("json_aggregate.zig").JsonAggregateOperator;
const JsonMapOperator = @import("json_map.zig").JsonMapOperator;
const JsonFlatMapOperator = @import("json_flatmap.zig").JsonFlatMapOperator;
const log = @import("stdx").log;

/// Result of a native operator creation attempt.
pub const CreateResult = struct {
    /// The Operator vtable interface.
    op: Operator,
    /// The backing storage pointer (for cleanup tracking).
    /// The handler is responsible for destroy()ing this when the job stops.
    backing: Backing,

    pub const Backing = union(enum) {
        expr_filter: *ExprFilterOperator,
        passthrough: *PassthroughOperator,
        json_keyby: *JsonKeyByOperator,
        json_aggregate: *JsonAggregateOperator,
        json_map: *JsonMapOperator,
        json_flatmap: *JsonFlatMapOperator,
    };

    /// Clean up the backing operator storage.
    pub fn deinit(self: *const CreateResult, allocator: Allocator) void {
        switch (self.backing) {
            .json_keyby => |ptr| {
                ptr.deinit(allocator);
                allocator.destroy(ptr);
            },
            .json_aggregate => |ptr| {
                ptr.deinit();
                allocator.destroy(ptr);
            },
            .json_map => |ptr| {
                ptr.deinit();
                allocator.destroy(ptr);
            },
            .json_flatmap => |ptr| {
                ptr.deinit();
                allocator.destroy(ptr);
            },
            .expr_filter => |ptr| allocator.destroy(ptr),
            .passthrough => |ptr| allocator.destroy(ptr),
        }
    }
};

/// Error type for registry creation failures.
pub const CreateError = error{
    UnknownOperatorType,
    MissingConfig,
    OutOfMemory,
};

/// Check whether a type name is a known native operator type.
pub fn isNativeType(type_name: []const u8) bool {
    return std.mem.eql(u8, type_name, "filter") or
        std.mem.eql(u8, type_name, "passthrough") or
        std.mem.eql(u8, type_name, "keyby") or
        std.mem.eql(u8, type_name, "aggregate") or
        std.mem.eql(u8, type_name, "map") or
        std.mem.eql(u8, type_name, "flatmap");
}

/// Create a native operator from an OperatorSpec.
///
/// The caller owns the returned operator and must call `result.deinit(allocator)`
/// when the operator is no longer needed.
///
/// Returns `CreateError.UnknownOperatorType` if the type name is not recognized.
/// Returns `CreateError.MissingConfig` if required config (e.g., "condition" for filter) is absent.
pub fn create(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    if (std.mem.eql(u8, spec.type_name, "filter")) {
        return createExprFilter(allocator, spec);
    } else if (std.mem.eql(u8, spec.type_name, "passthrough")) {
        return createPassthrough(allocator, spec);
    } else if (std.mem.eql(u8, spec.type_name, "keyby")) {
        return createJsonKeyBy(allocator, spec);
    } else if (std.mem.eql(u8, spec.type_name, "aggregate")) {
        return createJsonAggregate(allocator, spec);
    } else if (std.mem.eql(u8, spec.type_name, "map")) {
        return createJsonMap(allocator, spec);
    } else if (std.mem.eql(u8, spec.type_name, "flatmap")) {
        return createJsonFlatMap(allocator, spec);
    }

    return CreateError.UnknownOperatorType;
}

// =============================================================================
// Factory functions
// =============================================================================

fn createExprFilter(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const condition = spec.getConfig("condition") orelse {
        log.err("Native operator '{s}' (type=filter) missing required 'condition' config", .{spec.name});
        return CreateError.MissingConfig;
    };

    const ptr = allocator.create(ExprFilterOperator) catch return CreateError.OutOfMemory;
    ptr.* = ExprFilterOperator.init(spec.name, condition);

    return .{
        .op = ptr.operator(),
        .backing = .{ .expr_filter = ptr },
    };
}

fn createPassthrough(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const ptr = allocator.create(PassthroughOperator) catch return CreateError.OutOfMemory;
    ptr.* = PassthroughOperator.init(spec.name);

    return .{
        .op = ptr.operator(),
        .backing = .{ .passthrough = ptr },
    };
}

fn createJsonKeyBy(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const key_expression = spec.getConfig("key_expression") orelse {
        log.err("Native operator '{s}' (type=keyby) missing required 'key_expression' config (e.g., \"$.user_id\")", .{spec.name});
        return CreateError.MissingConfig;
    };

    const ptr = allocator.create(JsonKeyByOperator) catch return CreateError.OutOfMemory;
    ptr.* = JsonKeyByOperator.init(allocator, spec.name, key_expression) catch return CreateError.OutOfMemory;

    return .{
        .op = ptr.operator(),
        .backing = .{ .json_keyby = ptr },
    };
}

fn createJsonAggregate(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const func_str = spec.getConfig("function") orelse {
        log.err("Native operator '{s}' (type=aggregate) missing required 'function' config (sum|count|avg|min|max)", .{spec.name});
        return CreateError.MissingConfig;
    };

    const function = JsonAggregateOperator.AggFunction.fromString(func_str) orelse {
        log.err("Native operator '{s}' (type=aggregate) unknown function '{s}' — expected sum|count|avg|min|max", .{ spec.name, func_str });
        return CreateError.MissingConfig;
    };

    // field is required for sum/avg/min/max, optional for count
    const field_expr: ?[]const u8 = spec.getConfig("field");
    if (field_expr == null and function != .count) {
        log.err("Native operator '{s}' (type=aggregate, function={s}) missing required 'field' config (e.g., \"$.amount\")", .{ spec.name, func_str });
        return CreateError.MissingConfig;
    }

    // Parse window configuration
    const window: JsonAggregateOperator.WindowConfig = blk: {
        const window_type = spec.getConfig("window") orelse break :blk .none;
        const window_size_str = spec.getConfig("window_size") orelse {
            log.err("Native operator '{s}' (type=aggregate) has 'window' but missing 'window_size'", .{spec.name});
            return CreateError.MissingConfig;
        };

        if (std.mem.eql(u8, window_type, "tumbling")) {
            const seconds = std.fmt.parseInt(i64, window_size_str, 10) catch {
                log.err("Native operator '{s}' (type=aggregate) invalid window_size '{s}' — expected integer seconds", .{ spec.name, window_size_str });
                return CreateError.MissingConfig;
            };
            break :blk .{ .tumbling_time = seconds * 1000 }; // Convert seconds → ms
        } else if (std.mem.eql(u8, window_type, "count")) {
            const count = std.fmt.parseInt(u64, window_size_str, 10) catch {
                log.err("Native operator '{s}' (type=aggregate) invalid window_size '{s}' — expected integer count", .{ spec.name, window_size_str });
                return CreateError.MissingConfig;
            };
            break :blk .{ .tumbling_count = count };
        } else {
            log.err("Native operator '{s}' (type=aggregate) unknown window type '{s}' — expected tumbling|count", .{ spec.name, window_type });
            return CreateError.MissingConfig;
        }
    };

    const ptr = allocator.create(JsonAggregateOperator) catch return CreateError.OutOfMemory;
    ptr.* = JsonAggregateOperator.init(allocator, spec.name, function, field_expr, window) catch return CreateError.OutOfMemory;

    return .{
        .op = ptr.operator(),
        .backing = .{ .json_aggregate = ptr },
    };
}

fn createJsonMap(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const config = spec.config orelse {
        // No config entries → map with no mappings (passthrough)
        const ptr = allocator.create(JsonMapOperator) catch return CreateError.OutOfMemory;
        ptr.* = JsonMapOperator.init(allocator, spec.name, &.{}) catch return CreateError.OutOfMemory;
        return .{
            .op = ptr.operator(),
            .backing = .{ .json_map = ptr },
        };
    };

    const ptr = allocator.create(JsonMapOperator) catch return CreateError.OutOfMemory;
    ptr.* = JsonMapOperator.init(allocator, spec.name, config) catch return CreateError.OutOfMemory;

    return .{
        .op = ptr.operator(),
        .backing = .{ .json_map = ptr },
    };
}

fn createJsonFlatMap(allocator: Allocator, spec: *const OperatorSpec) CreateError!CreateResult {
    const array_field = spec.getConfig("array_field") orelse {
        log.err("Native operator '{s}' (type=flatmap) missing required 'array_field' config (e.g., \"$.items\")", .{spec.name});
        return CreateError.MissingConfig;
    };

    const element_key = spec.getConfig("element_key");

    const ptr = allocator.create(JsonFlatMapOperator) catch return CreateError.OutOfMemory;
    ptr.* = JsonFlatMapOperator.init(allocator, spec.name, array_field, element_key) catch return CreateError.OutOfMemory;

    return .{
        .op = ptr.operator(),
        .backing = .{ .json_flatmap = ptr },
    };
}

// =============================================================================
// Tests
// =============================================================================

test "NativeOperatorRegistry — create filter" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "condition", .value = "value_contains:hello" },
    };
    const spec = OperatorSpec{
        .type_name = "filter",
        .name = "test-filter",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("test-filter", result.op.getName());
}

test "NativeOperatorRegistry — create passthrough" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "passthrough",
        .name = "debug-tap",
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("debug-tap", result.op.getName());
}

test "NativeOperatorRegistry — create keyby" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "key_expression", .value = "$.user_id" },
    };
    const spec = OperatorSpec{
        .type_name = "keyby",
        .name = "by-user",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("by-user", result.op.getName());
}

test "NativeOperatorRegistry — unknown type" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "nonexistent",
        .name = "test",
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.UnknownOperatorType, result);
}

test "NativeOperatorRegistry — filter missing condition" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "filter",
        .name = "no-condition",
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.MissingConfig, result);
}

test "NativeOperatorRegistry — keyby missing key_expression" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "keyby",
        .name = "no-expression",
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.MissingConfig, result);
}

test "NativeOperatorRegistry — isNativeType" {
    try std.testing.expect(isNativeType("filter"));
    try std.testing.expect(isNativeType("passthrough"));
    try std.testing.expect(isNativeType("keyby"));
    try std.testing.expect(isNativeType("aggregate"));
    try std.testing.expect(isNativeType("map"));
    try std.testing.expect(isNativeType("flatmap"));
    try std.testing.expect(!isNativeType("wasm"));
    try std.testing.expect(!isNativeType("unknown"));
}

test "NativeOperatorRegistry — create aggregate sum" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "function", .value = "sum" },
        .{ .key = "field", .value = "$.amount" },
    };
    const spec = OperatorSpec{
        .type_name = "aggregate",
        .name = "total-amount",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("total-amount", result.op.getName());
}

test "NativeOperatorRegistry — create aggregate count (no field)" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "function", .value = "count" },
    };
    const spec = OperatorSpec{
        .type_name = "aggregate",
        .name = "event-counter",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("event-counter", result.op.getName());
}

test "NativeOperatorRegistry — create aggregate with tumbling window" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "function", .value = "avg" },
        .{ .key = "field", .value = "$.latency" },
        .{ .key = "window", .value = "tumbling" },
        .{ .key = "window_size", .value = "60" },
    };
    const spec = OperatorSpec{
        .type_name = "aggregate",
        .name = "avg-latency",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("avg-latency", result.op.getName());
}

test "NativeOperatorRegistry — aggregate missing function" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "aggregate",
        .name = "bad-agg",
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.MissingConfig, result);
}

test "NativeOperatorRegistry — aggregate sum missing field" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "function", .value = "sum" },
    };
    const spec = OperatorSpec{
        .type_name = "aggregate",
        .name = "bad-sum",
        .config = &config_entries,
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.MissingConfig, result);
}

test "NativeOperatorRegistry — create map with field projections" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "user", .value = "$.user_id" },
        .{ .key = "source", .value = "payments" },
    };
    const spec = OperatorSpec{
        .type_name = "map",
        .name = "extract-user",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("extract-user", result.op.getName());
}

test "NativeOperatorRegistry — create map with no config" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "map",
        .name = "empty-map",
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("empty-map", result.op.getName());
}

test "NativeOperatorRegistry — create flatmap" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "array_field", .value = "$.items" },
    };
    const spec = OperatorSpec{
        .type_name = "flatmap",
        .name = "explode-items",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("explode-items", result.op.getName());
}

test "NativeOperatorRegistry — create flatmap with element_key" {
    const allocator = std.testing.allocator;

    const config_entries = [_]OperatorSpec.ConfigEntry{
        .{ .key = "array_field", .value = "$.events" },
        .{ .key = "element_key", .value = "$.type" },
    };
    const spec = OperatorSpec{
        .type_name = "flatmap",
        .name = "keyed-explode",
        .config = &config_entries,
    };

    const result = try create(allocator, &spec);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("keyed-explode", result.op.getName());
}

test "NativeOperatorRegistry — flatmap missing array_field" {
    const allocator = std.testing.allocator;

    const spec = OperatorSpec{
        .type_name = "flatmap",
        .name = "bad-flatmap",
    };

    const result = create(allocator, &spec);
    try std.testing.expectError(CreateError.MissingConfig, result);
}
