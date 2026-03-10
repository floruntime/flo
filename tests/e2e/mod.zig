//! E2E Test Suite Entry Point
//!
//! This module imports all e2e tests for the build system.
//! Run with: zig build test-e2e

const std = @import("std");

// Feature tests
pub const kv_test = @import("kv_test.zig");
pub const http_test = @import("http_test.zig");
pub const stream_test = @import("stream_test.zig");
pub const queue_test = @import("queue_test.zig");
pub const namespace_test = @import("namespace_test.zig");
pub const action_test = @import("action_test.zig");
pub const worker_test = @import("worker_test.zig");
pub const workflow_test = @import("workflow_test.zig");
pub const processing_test = @import("processing_test.zig");
pub const ts_test = @import("ts_test.zig");

// Future test modules:
// pub const cluster_test = @import("cluster_test.zig");

test {
    // Import all test namespaces
    std.testing.refAllDecls(@This());
}
