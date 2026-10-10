//! E2E Test Suite Entry Point
//!
//! This module imports all e2e tests for the build system.
//! Run with: zig build test-e2e

const std = @import("std");

// Feature tests
pub const harness_test = @import("harness_test.zig");
pub const kv_test = @import("kv_test.zig");
pub const kv_key_nul_test = @import("kv_key_nul_test.zig");
pub const http_test = @import("http_test.zig");
pub const cluster_test = @import("cluster_test.zig");
pub const cluster_peering_test = @import("cluster_peering_test.zig");
pub const cluster_secret_test = @import("cluster_secret_test.zig");
pub const stream_test = @import("stream_test.zig");
pub const stream_recovery_test = @import("stream_recovery_test.zig");
pub const stream_namespace_test = @import("stream_namespace_test.zig");
pub const queue_test = @import("queue_test.zig");
pub const namespace_test = @import("namespace_test.zig");
pub const namespace_names_test = @import("namespace_names_test.zig");
pub const namespace_scoping_test = @import("namespace_scoping_test.zig");
pub const action_test = @import("action_test.zig");
pub const worker_test = @import("worker_test.zig");
pub const workflow_test = @import("workflow_test.zig");
pub const large_request_test = @import("large_request_test.zig");
pub const time_bounds_test = @import("time_bounds_test.zig");
pub const config_test = @import("config_test.zig");
pub const data_dir_lock_test = @import("data_dir_lock_test.zig");
pub const request_bounds_test = @import("request_bounds_test.zig");
pub const request_decode_test = @import("request_decode_test.zig");
pub const definition_bounds_test = @import("definition_bounds_test.zig");
pub const definition_keys_test = @import("definition_keys_test.zig");
pub const force_members_test = @import("force_members_test.zig");
pub const wipe_rejoin_test = @import("wipe_rejoin_test.zig");
pub const membership_test = @import("membership_test.zig");
pub const processing_test = @import("processing_test.zig");
pub const ts_test = @import("ts_test.zig");
pub const dual_connection_test = @import("dual_connection_test.zig");
pub const metrics_test = @import("metrics_test.zig");
pub const metrics_values_test = @import("metrics_values_test.zig");
pub const dashboard_streams_test = @import("dashboard_streams_test.zig");
pub const dashboard_queues_test = @import("dashboard_queues_test.zig");
pub const dashboard_actions_test = @import("dashboard_actions_test.zig");
pub const dashboard_processing_test = @import("dashboard_processing_test.zig");
pub const dashboard_workflows_test = @import("dashboard_workflows_test.zig");
pub const dashboard_floql_test = @import("dashboard_floql_test.zig");
pub const dashboard_request_checks_test = @import("dashboard_request_checks_test.zig");
pub const server_flags_test = @import("server_flags_test.zig");

// Future test modules:
// pub const cluster_test = @import("cluster_test.zig");

test {
    // Import all test namespaces
    std.testing.refAllDecls(@This());
}
