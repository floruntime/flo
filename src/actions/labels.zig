//! Label Matching for Worker Selection
//!
//! Labels are flat JSON objects attached to workers at registration.
//! When an action is invoked with `required_labels`, only workers whose
//! labels satisfy all required key-value pairs can pick up the task.
//!
//! # Matching Semantics (v1 - Exact Subset)
//!
//! All keys in `required_labels` must exist in `worker_labels` with equal values.
//! Extra keys on the worker are ignored.
//!
//! ```
//! Worker:   {"gpu": true, "vram_gb": 24, "region": "us-east"}
//! Required: {"gpu": true}              → MATCH
//! Required: {"gpu": true, "vram_gb": 24} → MATCH
//! Required: {"gpu": false}             → NO MATCH (value differs)
//! Required: {"ssd": true}              → NO MATCH (key missing)
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Check if worker labels satisfy all required label constraints.
///
/// Returns true if every key in `required_json` exists in `worker_json`
/// with an equal value. Returns false if either string is invalid JSON,
/// not an object, or any required key is missing/mismatched.
///
/// If `worker_labels` is null, returns false (worker has no labels).
pub fn matchLabels(allocator: Allocator, required_json: []const u8, worker_labels: ?[]const u8) bool {
    const worker_json = worker_labels orelse return false;
    if (required_json.len == 0) return true;
    if (worker_json.len == 0) return false;

    const required = std.json.parseFromSlice(std.json.Value, allocator, required_json, .{}) catch return false;
    defer required.deinit();

    const worker = std.json.parseFromSlice(std.json.Value, allocator, worker_json, .{}) catch return false;
    defer worker.deinit();

    // Both must be objects
    const req_obj = switch (required.value) {
        .object => |o| o,
        else => return false,
    };
    const worker_obj = switch (worker.value) {
        .object => |o| o,
        else => return false,
    };

    // Every key in required must exist in worker with the same value
    var it = req_obj.iterator();
    while (it.next()) |entry| {
        const worker_val = worker_obj.get(entry.key_ptr.*) orelse return false;
        if (!jsonValueEqual(entry.value_ptr.*, worker_val)) return false;
    }

    return true;
}

/// Compare two JSON values for equality (flat values only in v1).
fn jsonValueEqual(a: std.json.Value, b: std.json.Value) bool {
    const Tag = std.meta.Tag(@TypeOf(a));
    const a_tag: Tag = a;
    const b_tag: Tag = b;
    if (a_tag != b_tag) return false;

    return switch (a) {
        .null => true, // both null
        .bool => |av| av == b.bool,
        .integer => |av| av == b.integer,
        .float => |av| av == b.float,
        .string => |av| std.mem.eql(u8, av, b.string),
        .number_string => |av| std.mem.eql(u8, av, b.number_string),
        // arrays/objects not compared in v1 label matching
        .array, .object => false,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "matchLabels - exact match" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator, "{\"gpu\":true}", "{\"gpu\":true}"));
}

test "matchLabels - subset match" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator,
        \\{"gpu":true}
    ,
        \\{"gpu":true,"vram_gb":24,"region":"us-east"}
    ));
}

test "matchLabels - multi-key required" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator,
        \\{"gpu":true,"vram_gb":24}
    ,
        \\{"gpu":true,"vram_gb":24,"region":"us-east"}
    ));
}

test "matchLabels - value mismatch" {
    const allocator = std.testing.allocator;
    try std.testing.expect(!matchLabels(allocator,
        \\{"gpu":false}
    ,
        \\{"gpu":true}
    ));
}

test "matchLabels - missing key" {
    const allocator = std.testing.allocator;
    try std.testing.expect(!matchLabels(allocator,
        \\{"ssd":true}
    ,
        \\{"gpu":true}
    ));
}

test "matchLabels - null worker labels" {
    const allocator = std.testing.allocator;
    try std.testing.expect(!matchLabels(allocator, "{\"gpu\":true}", null));
}

test "matchLabels - empty required" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator, "", "{\"gpu\":true}"));
}

test "matchLabels - string values" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator,
        \\{"region":"us-east"}
    ,
        \\{"region":"us-east","gpu":true}
    ));
    try std.testing.expect(!matchLabels(allocator,
        \\{"region":"eu-west"}
    ,
        \\{"region":"us-east","gpu":true}
    ));
}

test "matchLabels - integer values" {
    const allocator = std.testing.allocator;
    try std.testing.expect(matchLabels(allocator,
        \\{"vram_gb":24}
    ,
        \\{"vram_gb":24,"gpu":true}
    ));
    try std.testing.expect(!matchLabels(allocator,
        \\{"vram_gb":48}
    ,
        \\{"vram_gb":24,"gpu":true}
    ));
}

test "matchLabels - invalid json" {
    const allocator = std.testing.allocator;
    try std.testing.expect(!matchLabels(allocator, "not json", "{\"gpu\":true}"));
    try std.testing.expect(!matchLabels(allocator, "{\"gpu\":true}", "not json"));
}
