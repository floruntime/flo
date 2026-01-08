//! Legacy engine interfaces — stub for ADAPT file compatibility
//!
//! This file provides types that ADAPT-layer config files depend on.
//! In the rewritten architecture, durability is controlled per-partition
//! via the UAL configuration. This stub preserves the config contract.

const std = @import("std");

/// Storage durability level
pub const Durability = enum(u8) {
    /// Wait for fdatasync before returning
    sync = 0,
    /// Return after WAL append (async flush) — DEFAULT
    async_flush = 1,
    /// No persistence (for caching use cases)
    ephemeral = 2,

    /// Parse durability from string (for CLI/config parsing)
    pub fn fromString(s: []const u8) Durability {
        if (std.mem.eql(u8, s, "sync")) return .sync;
        if (std.mem.eql(u8, s, "ephemeral")) return .ephemeral;
        return .async_flush; // default
    }
};
