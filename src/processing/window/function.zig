//! Window Functions
//!
//! Defines the computation applied when a window fires.
//! Three variants (matching Flink):
//!   - ReduceWindowFn: incremental reduce (O(1) state per window)
//!   - AggregateWindowFn: incremental accumulator (O(1) state per window)
//!   - ProcessWindowFn: full window contents available (O(N) state per window)
//!
//! Prefer ReduceWindowFn/AggregateWindowFn for efficiency — they maintain
//! a single accumulator value rather than buffering all records.

const std = @import("std");
const TimeWindow = @import("assigner.zig").TimeWindow;

// =============================================================================
// WindowFunction Types
// =============================================================================

/// Reduce-style window function: combine two values into one.
/// Used for incremental aggregation — only one accumulated value stored.
pub const ReduceWindowFn = *const fn (a: []const u8, b: []const u8) []const u8;

/// Aggregate-style window function: createAccumulator/add/getResult.
/// More flexible than reduce — accumulator can be different type from elements.
pub const AggregateWindowFns = struct {
    createAccumulator: *const fn () []const u8,
    add: *const fn (accumulator: []const u8, value: []const u8) []const u8,
    getResult: *const fn (accumulator: []const u8) []const u8,
};

/// Callback for when a window fires.
/// Called with the key, window, and computed result.
pub const WindowResultFn = *const fn (key: []const u8, window: TimeWindow, result: []const u8) void;

/// The type of window function to apply
pub const WindowFunction = union(enum) {
    /// Incremental reduce: combine pairs of values
    reduce: ReduceWindowFn,
    /// Incremental aggregate: custom accumulator
    aggregate: AggregateWindowFns,
};

// =============================================================================
// Tests
// =============================================================================

fn testSum(a: []const u8, b: []const u8) []const u8 {
    // Just return b for simplicity — real impl would parse and add
    _ = a;
    return b;
}

test "WindowFunction reduce type" {
    const wf = WindowFunction{ .reduce = &testSum };
    switch (wf) {
        .reduce => |f| {
            const result = f("1", "2");
            try std.testing.expectEqualStrings("2", result);
        },
        else => unreachable,
    }
}

fn testCreateAcc() []const u8 {
    return "0";
}
fn testAdd(_: []const u8, v: []const u8) []const u8 {
    return v;
}
fn testGetResult(acc: []const u8) []const u8 {
    return acc;
}

test "WindowFunction aggregate type" {
    const wf = WindowFunction{
        .aggregate = .{
            .createAccumulator = &testCreateAcc,
            .add = &testAdd,
            .getResult = &testGetResult,
        },
    };
    switch (wf) {
        .aggregate => |fns| {
            const acc = fns.createAccumulator();
            try std.testing.expectEqualStrings("0", acc);
            const new_acc = fns.add(acc, "42");
            const result = fns.getResult(new_acc);
            try std.testing.expectEqualStrings("42", result);
        },
        else => unreachable,
    }
}
