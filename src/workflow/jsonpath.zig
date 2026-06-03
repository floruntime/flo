//! JSONPath Resolver for Workflow Step Inputs
//!
//! Resolves expressions like:
//! - `$.input` - Raw workflow input
//! - `$.input.customer_id` - Nested field from workflow input
//! - `$.steps.{name}.output` - Previous step's output
//! - `$.steps.{name}.output.email` - Nested field from step output
//! - `$.flo.timestamp` - Current epoch timestamp
//! - `$.flo.run_id` - Current run identifier
//!
//! # Usage
//!
//! ```zig
//! var resolver = PathResolver.init(allocator, input, step_outputs, run_id);
//! const value = try resolver.resolve("$.steps.enrich_company.output.email");
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const log = @import("stdx").log;

const types = @import("types.zig");
const StepOutputMap = types.StepOutputMap;
const StepOutput = types.StepOutput;

// =============================================================================
// Path Resolver
// =============================================================================

pub const PathResolver = struct {
    allocator: Allocator,
    /// Workflow input JSON (for $.input resolution)
    input: ?[]const u8,
    /// Step outputs (for $.steps.{name}.output resolution)
    step_outputs: ?*const StepOutputMap,
    /// Current run ID (for $.flo.run_id)
    run_id: []const u8,
    /// Current timestamp in ms (for $.flo.timestamp)
    timestamp_ms: i64,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        input: ?[]const u8,
        step_outputs: ?*const StepOutputMap,
        run_id: []const u8,
    ) Self {
        return .{
            .allocator = allocator,
            .input = input,
            .step_outputs = step_outputs,
            .run_id = run_id,
            .timestamp_ms = @import("stdx").time.milliTimestamp(),
        };
    }

    /// Resolve a JSONPath expression to a value
    /// Returns the extracted JSON value as a string, or null for missing paths
    pub fn resolve(self: *Self, path: []const u8) !?[]u8 {
        // Must start with $
        if (path.len == 0 or path[0] != '$') return error.InvalidPath;

        // Just "$" returns everything (not supported in our minimal impl)
        if (path.len == 1) return error.InvalidPath;

        // Must have "." after "$"
        if (path[1] != '.') return error.InvalidPath;

        const rest = path[2..]; // Skip "$."

        // Dispatch based on prefix
        if (mem.startsWith(u8, rest, "input")) {
            return self.resolveInputPath(rest);
        } else if (mem.startsWith(u8, rest, "steps.")) {
            return self.resolveStepPath(rest[6..]); // Skip "steps."
        } else if (mem.startsWith(u8, rest, "flo.")) {
            return self.resolveFloPath(rest[4..]); // Skip "flo."
        }

        return error.UnknownPathPrefix;
    }

    /// Resolve $.input or $.input.field.subfield
    fn resolveInputPath(self: *Self, path: []const u8) !?[]u8 {
        const input_json = self.input orelse return null;

        // Just $.input - return the whole input
        if (mem.eql(u8, path, "input")) {
            return try self.allocator.dupe(u8, input_json);
        }

        // $.input.something - extract nested field
        if (path.len > 6 and mem.startsWith(u8, path, "input.")) {
            const field_path = path[6..]; // Skip "input."
            return self.extractJsonField(input_json, field_path);
        }

        return error.InvalidPath;
    }

    /// Resolve $.steps.{name}.output or $.steps.{name}.output.field
    fn resolveStepPath(self: *Self, path: []const u8) !?[]u8 {
        const outputs = self.step_outputs orelse return null;

        // Find step name (until next '.')
        const dot_pos = mem.indexOf(u8, path, ".") orelse return error.InvalidPath;
        const step_name = path[0..dot_pos];
        const rest = path[dot_pos + 1 ..]; // After the step name

        // Get step output
        const step = outputs.get(step_name) orelse return null;

        // $.steps.{name}.output - return whole output
        if (mem.eql(u8, rest, "output")) {
            return try self.allocator.dupe(u8, step.output);
        }

        // $.steps.{name}.outcome - return outcome as a JSON string (quoted),
        // so the mapper's JSON re-parse accepts it. A bare word like `success`
        // is not valid JSON and would otherwise fall back to the literal path.
        if (mem.eql(u8, rest, "outcome")) {
            const result = try self.allocator.alloc(u8, step.outcome.len + 2);
            result[0] = '"';
            @memcpy(result[1 .. result.len - 1], step.outcome);
            result[result.len - 1] = '"';
            return result;
        }

        // $.steps.{name}.output.field - extract nested field
        if (mem.startsWith(u8, rest, "output.")) {
            const field_path = rest[7..]; // Skip "output."
            return self.extractJsonField(step.output, field_path);
        }

        return error.InvalidPath;
    }

    /// Resolve $.flo.* meta fields
    fn resolveFloPath(self: *Self, path: []const u8) !?[]u8 {
        if (mem.eql(u8, path, "timestamp")) {
            // Return timestamp as JSON number
            var buf: [24]u8 = undefined;
            const formatted = std.fmt.bufPrint(&buf, "{d}", .{self.timestamp_ms}) catch return error.InvalidTimestamp;
            return try self.allocator.dupe(u8, formatted);
        }

        if (mem.eql(u8, path, "run_id")) {
            // Return run_id as JSON string (quoted)
            const result = try self.allocator.alloc(u8, self.run_id.len + 2);
            result[0] = '"';
            @memcpy(result[1 .. result.len - 1], self.run_id);
            result[result.len - 1] = '"';
            return result;
        }

        return error.UnknownFloField;
    }

    /// Extract a nested field from JSON
    /// Supports dot-notation: "company.email" extracts obj.company.email
    fn extractJsonField(self: *Self, json: []const u8, field_path: []const u8) !?[]u8 {
        // Parse the JSON
        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, json, .{}) catch {
            return error.InvalidJson;
        };
        defer parsed.deinit();

        // Navigate through the path
        var current = parsed.value;
        var remaining = field_path;

        while (remaining.len > 0) {
            // Get next field name
            const dot_pos = mem.indexOf(u8, remaining, ".");
            const field_name = if (dot_pos) |pos| remaining[0..pos] else remaining;
            remaining = if (dot_pos) |pos| remaining[pos + 1 ..] else "";

            // Navigate into object
            switch (current) {
                .object => |obj| {
                    if (obj.get(field_name)) |val| {
                        current = val;
                    } else {
                        return null; // Field not found
                    }
                },
                else => return null, // Not an object, can't navigate
            }
        }

        // Stringify the result
        return try std.json.Stringify.valueAlloc(self.allocator, current, .{});
    }
};

// =============================================================================
// Helpers
// =============================================================================

/// Resolve JSONPath references in a step input JSON
/// String values starting with "$." are treated as path references
/// Example: {"domain": "$.input.company_domain"} → {"domain": "acme.com"}
pub fn resolveInput(
    allocator: Allocator,
    input_json: []const u8,
    workflow_input: ?[]const u8,
    step_outputs: ?*const StepOutputMap,
    run_id: []const u8,
) ![]u8 {
    // Fast path: no path references
    if (mem.indexOf(u8, input_json, "$.") == null) {
        return try allocator.dupe(u8, input_json);
    }

    var resolver = PathResolver.init(allocator, workflow_input, step_outputs, run_id);

    // Parse input JSON
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, input_json, .{}) catch |err| {
        log.warn("Failed to parse step input JSON: {}", .{err});
        return try allocator.dupe(u8, input_json);
    };
    defer parsed.deinit();

    // Resolve path references in the JSON tree
    const resolved = try resolveJsonValue(allocator, &resolver, parsed.value);
    defer freeJsonValue(allocator, resolved);

    // Stringify back to JSON
    return std.json.Stringify.valueAlloc(allocator, resolved, .{});
}

/// Recursively resolve path references in JSON values.
/// Returns a fully-owned JSON tree that must be freed with freeJsonValue.
fn resolveJsonValue(allocator: Allocator, resolver: *PathResolver, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .string => |s| {
            // Check if string is a path reference
            if (s.len > 2 and s[0] == '$' and s[1] == '.') {
                // Resolve the path
                if (try resolver.resolve(s)) |resolved| {
                    defer allocator.free(resolved);
                    // Parse the resolved value as JSON
                    const parsed = std.json.parseFromSlice(std.json.Value, allocator, resolved, .{}) catch {
                        // If not valid JSON, keep as owned copy of original string
                        return .{ .string = try allocator.dupe(u8, s) };
                    };
                    defer parsed.deinit();
                    // Clone the parsed value since we're deferring deinit
                    return cloneJsonValue(allocator, parsed.value);
                } else {
                    // Path not found, return null
                    return .null;
                }
            }
            // Not a path reference — return owned copy
            return .{ .string = try allocator.dupe(u8, s) };
        },
        .object => |obj| {
            var new_obj: std.json.ObjectMap = .empty;
            var iter = obj.iterator();
            while (iter.next()) |entry| {
                const owned_key = try allocator.dupe(u8, entry.key_ptr.*);
                const resolved_val = try resolveJsonValue(allocator, resolver, entry.value_ptr.*);
                try new_obj.put(allocator, owned_key, resolved_val);
            }
            return .{ .object = new_obj };
        },
        .array => |arr| {
            var new_arr = try std.json.Array.initCapacity(allocator, arr.items.len);
            for (arr.items) |item| {
                const resolved_item = try resolveJsonValue(allocator, resolver, item);
                try new_arr.append(resolved_item);
            }
            return .{ .array = new_arr };
        },
        else => value, // Primitives (bool, integer, float, null) are value types — no alloc needed
    };
}

/// Deep clone a JSON value
fn cloneJsonValue(allocator: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .string => |s| .{ .string = try allocator.dupe(u8, s) },
        .object => |obj| {
            var new_obj: std.json.ObjectMap = .empty;
            var iter = obj.iterator();
            while (iter.next()) |entry| {
                const cloned_key = try allocator.dupe(u8, entry.key_ptr.*);
                const cloned_val = try cloneJsonValue(allocator, entry.value_ptr.*);
                try new_obj.put(allocator, cloned_key, cloned_val);
            }
            return .{ .object = new_obj };
        },
        .array => |arr| {
            var new_arr = try std.json.Array.initCapacity(allocator, arr.items.len);
            for (arr.items) |item| {
                try new_arr.append(try cloneJsonValue(allocator, item));
            }
            return .{ .array = new_arr };
        },
        else => value,
    };
}

/// Free a dynamically-constructed JSON value tree (inverse of cloneJsonValue).
/// Only frees values that were allocated by resolveJsonValue/cloneJsonValue,
/// not values borrowed from a parsed tree.
fn freeJsonValue(allocator: Allocator, value: std.json.Value) void {
    switch (value) {
        .string => |s| allocator.free(s),
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                freeJsonValue(allocator, entry.value_ptr.*);
            }
            // ObjectMap = StringArrayHashMap(Value) (unmanaged in 0.16 — deinit takes allocator)
            var mut_obj = obj;
            mut_obj.deinit(allocator);
        },
        .array => |arr| {
            for (arr.items) |item| {
                freeJsonValue(allocator, item);
            }
            // Array = array_list.Managed(Value) (deinit takes no allocator)
            var mut_arr = arr;
            mut_arr.deinit();
        },
        else => {},
    }
}

// =============================================================================
// Tests
// =============================================================================

test "PathResolver: resolve $.input" {
    const allocator = std.testing.allocator;
    var resolver = PathResolver.init(
        allocator,
        \\{"customer_id": "cust_123", "amount": 100}
    ,
        null,
        "run_abc",
    );

    const result = try resolver.resolve("$.input");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings(
        \\{"customer_id": "cust_123", "amount": 100}
    , result.?);
}

test "PathResolver: resolve $.input.customer_id" {
    const allocator = std.testing.allocator;
    var resolver = PathResolver.init(
        allocator,
        \\{"customer_id": "cust_123", "amount": 100}
    ,
        null,
        "run_abc",
    );

    const result = try resolver.resolve("$.input.customer_id");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("\"cust_123\"", result.?);
}

test "PathResolver: resolve $.flo.run_id" {
    const allocator = std.testing.allocator;
    var resolver = PathResolver.init(allocator, null, null, "run_xyz");

    const result = try resolver.resolve("$.flo.run_id");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("\"run_xyz\"", result.?);
}

test "PathResolver: resolve $.flo.timestamp" {
    const allocator = std.testing.allocator;
    var resolver = PathResolver.init(allocator, null, null, "run_xyz");

    const result = try resolver.resolve("$.flo.timestamp");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    // Timestamp should be a number (no quotes)
    try std.testing.expect(result.?[0] != '"');
}

test "PathResolver: resolve $.steps.step_name.output" {
    const allocator = std.testing.allocator;

    // Build step outputs
    var outputs = StepOutputMap.init();
    defer outputs.deinit(allocator);

    try outputs.put(allocator, "validate",
        \\{"valid": true, "score": 95}
    , "success");

    var resolver = PathResolver.init(allocator, null, &outputs, "run_test");

    const result = try resolver.resolve("$.steps.validate.output");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings(
        \\{"valid": true, "score": 95}
    , result.?);
}

test "PathResolver: resolve $.steps.step_name.output.field" {
    const allocator = std.testing.allocator;

    var outputs = StepOutputMap.init();
    defer outputs.deinit(allocator);

    try outputs.put(allocator, "enrich",
        \\{"email": "test@example.com", "verified": true}
    , "success");

    var resolver = PathResolver.init(allocator, null, &outputs, "run_test");

    const result = try resolver.resolve("$.steps.enrich.output.email");
    defer if (result) |r| allocator.free(r);

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("\"test@example.com\"", result.?);
}

test "PathResolver: resolveInput with path references" {
    const allocator = std.testing.allocator;

    var outputs = StepOutputMap.init();
    defer outputs.deinit(allocator);

    try outputs.put(allocator, "step1",
        \\{"value": 42, "name": "test"}
    , "success");

    const input =
        \\{"step1_val": "$.steps.step1.output.value", "run": "$.flo.run_id"}
    ;

    const result = try resolveInput(allocator, input, null, &outputs, "run_123");
    defer allocator.free(result);

    // The resolved JSON should have the values replaced
    try std.testing.expectEqualStrings(
        \\{"step1_val":42,"run":"run_123"}
    , result);
}

test "PathResolver: composed workflow output mapping" {
    const allocator = std.testing.allocator;

    var outputs = StepOutputMap.init();
    defer outputs.deinit(allocator);

    try outputs.put(allocator, "validate_order",
        \\{"valid": true, "orderId": "ORD-123"}
    , "success");
    try outputs.put(allocator, "charge_payment",
        \\{"charged": true, "orderId": "ORD-123", "amount": 99.99}
    , "success");
    try outputs.put(allocator, "ship_order",
        \\{"shipped": true, "orderId": "ORD-123", "trackingId": "TRK-42"}
    , "success");

    const workflow_input =
        \\{"orderId": "ORD-123", "amount": 99.99}
    ;
    const output_mapping =
        \\{"order_id": "$.input.orderId", "tracking_id": "$.steps.ship_order.output.trackingId", "amount_charged": "$.steps.charge_payment.output.amount", "run_id": "$.flo.run_id"}
    ;

    const result = try resolveInput(allocator, output_mapping, workflow_input, &outputs, "wfr-ABC123");
    defer allocator.free(result);

    // Verify all fields resolved correctly
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, result, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;

    try std.testing.expectEqualStrings("ORD-123", obj.get("order_id").?.string);
    try std.testing.expectEqualStrings("TRK-42", obj.get("tracking_id").?.string);
    try std.testing.expect(obj.get("amount_charged").?.float == 99.99);
    try std.testing.expectEqualStrings("wfr-ABC123", obj.get("run_id").?.string);
}

test "PathResolver: direct step output passthrough" {
    const allocator = std.testing.allocator;

    var outputs = StepOutputMap.init();
    defer outputs.deinit(allocator);

    // Step output could be any raw bytes (JSON, binary, etc.)
    try outputs.put(allocator, "ship_order",
        \\{"shipped": true, "trackingId": "TRK-42", "binaryRef": "s3://bucket/audio.wav"}
    , "success");

    var resolver = PathResolver.init(allocator, null, &outputs, "wfr-test");

    // Direct passthrough — resolves the whole step output
    const result = try resolver.resolve("$.steps.ship_order.output");
    defer allocator.free(result.?);
    try std.testing.expectEqualStrings(
        \\{"shipped": true, "trackingId": "TRK-42", "binaryRef": "s3://bucket/audio.wav"}
    , result.?);
}
