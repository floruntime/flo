//! Watermark Tracker
//!
//! Tracks watermarks across multiple input channels for an operator.
//! The effective watermark is the MINIMUM across all inputs, ensuring
//! we never advance past events that may still arrive on slower inputs.
//!
//! This implements Flink's multi-input watermark alignment:
//!   effective_watermark = min(input_0.watermark, input_1.watermark, ...)
//!
//! Each input channel is identified by a u32 index.

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const Watermark = record_mod.Watermark;

// =============================================================================
// WatermarkTracker
// =============================================================================

/// Tracks watermarks from multiple input streams and computes the
/// minimum (aligned) watermark across all of them.
///
/// Usage:
///   var tracker = WatermarkTracker.init(allocator, 3); // 3 inputs
///   tracker.updateInput(0, Watermark.init(5000));
///   tracker.updateInput(1, Watermark.init(3000));
///   // aligned watermark = 3000 (minimum)
pub const WatermarkTracker = struct {
    /// Per-input watermarks
    input_watermarks: []i64,
    allocator: Allocator,
    /// Number of inputs
    num_inputs: u32,
    /// Current aligned (minimum) watermark
    aligned_watermark_ms: i64,

    const Self = @This();

    pub fn init(allocator: Allocator, num_inputs: u32) !Self {
        const wms = try allocator.alloc(i64, num_inputs);
        @memset(wms, std.math.minInt(i64));
        return .{
            .input_watermarks = wms,
            .allocator = allocator,
            .num_inputs = num_inputs,
            .aligned_watermark_ms = std.math.minInt(i64),
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.input_watermarks);
    }

    /// Update the watermark for a specific input channel.
    /// Returns the new aligned watermark if it advanced, null otherwise.
    pub fn updateInput(self: *Self, input_index: u32, watermark: Watermark) ?Watermark {
        if (input_index >= self.num_inputs) return null;

        self.input_watermarks[input_index] = watermark.timestamp_ms;

        // Recompute minimum across all inputs
        var min_wm: i64 = std.math.maxInt(i64);
        for (self.input_watermarks) |wm| {
            if (wm < min_wm) min_wm = wm;
        }

        if (min_wm > self.aligned_watermark_ms) {
            self.aligned_watermark_ms = min_wm;
            return Watermark.init(min_wm);
        }
        return null;
    }

    /// Get the current aligned watermark (min across all inputs)
    pub fn alignedWatermark(self: *const Self) i64 {
        return self.aligned_watermark_ms;
    }

    /// Get the watermark for a specific input
    pub fn inputWatermark(self: *const Self, input_index: u32) i64 {
        if (input_index >= self.num_inputs) return std.math.minInt(i64);
        return self.input_watermarks[input_index];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "WatermarkTracker single input" {
    const allocator = std.testing.allocator;
    var tracker = try WatermarkTracker.init(allocator, 1);
    defer tracker.deinit();

    const wm = tracker.updateInput(0, Watermark.init(5000));
    try std.testing.expect(wm != null);
    try std.testing.expectEqual(@as(i64, 5000), wm.?.timestamp_ms);
    try std.testing.expectEqual(@as(i64, 5000), tracker.alignedWatermark());
}

test "WatermarkTracker multi-input alignment" {
    const allocator = std.testing.allocator;
    var tracker = try WatermarkTracker.init(allocator, 3);
    defer tracker.deinit();

    // Input 0 advances to 5000
    const wm1 = tracker.updateInput(0, Watermark.init(5000));
    try std.testing.expectEqual(@as(?Watermark, null), wm1); // min is still minInt (inputs 1,2)

    // Input 1 advances to 3000
    const wm2 = tracker.updateInput(1, Watermark.init(3000));
    try std.testing.expectEqual(@as(?Watermark, null), wm2); // input 2 still at minInt

    // Input 2 advances to 4000 → aligned = min(5000, 3000, 4000) = 3000
    const wm3 = tracker.updateInput(2, Watermark.init(4000));
    try std.testing.expect(wm3 != null);
    try std.testing.expectEqual(@as(i64, 3000), wm3.?.timestamp_ms);

    // Input 1 advances to 6000 → aligned = min(5000, 6000, 4000) = 4000
    const wm4 = tracker.updateInput(1, Watermark.init(6000));
    try std.testing.expect(wm4 != null);
    try std.testing.expectEqual(@as(i64, 4000), wm4.?.timestamp_ms);
}

test "WatermarkTracker watermark never goes backward" {
    const allocator = std.testing.allocator;
    var tracker = try WatermarkTracker.init(allocator, 1);
    defer tracker.deinit();

    _ = tracker.updateInput(0, Watermark.init(5000));
    try std.testing.expectEqual(@as(i64, 5000), tracker.alignedWatermark());

    // Even if input reports lower watermark, aligned shouldn't decrease
    const wm = tracker.updateInput(0, Watermark.init(3000));
    try std.testing.expectEqual(@as(?Watermark, null), wm); // no advance
    try std.testing.expectEqual(@as(i64, 5000), tracker.alignedWatermark());
}

test "WatermarkTracker invalid input index" {
    const allocator = std.testing.allocator;
    var tracker = try WatermarkTracker.init(allocator, 2);
    defer tracker.deinit();

    const wm = tracker.updateInput(99, Watermark.init(1000));
    try std.testing.expectEqual(@as(?Watermark, null), wm);
}
