//! Integration Test Suite — New Internals
//!
//! Tests that exercise the full stack of new architecture components
//! in realistic scenarios, verifying correct interaction between modules.
//!
//! ## Coverage
//!
//! - Reactor stress: high fd count, rapid add/remove cycles
//! - Inbox throughput: MPSC ring under contention
//! - UAL consistency: append, read, ring wrap-around
//! - KV projection: concurrent operations, TTL, scan consistency
//! - Cold tier: UAL segment archival, on-demand retrieval, manifest persistence
//! - Router: partition distribution uniformity
//! - Shard walker: cross-shard scan iteration

const std = @import("std");
const testing = std.testing;

// Import test modules — each file's `test` blocks are auto-discovered.
test {
    _ = @import("test_inbox.zig");
    _ = @import("test_ual.zig");
    _ = @import("test_kv_projection.zig");
    _ = @import("test_router.zig");
    _ = @import("test_reactor.zig");
    _ = @import("test_cold_tier.zig");
    _ = @import("test_raft_failover.zig");
}
