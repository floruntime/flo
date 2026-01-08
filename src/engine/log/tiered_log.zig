//! Legacy tiered log config — stub for ADAPT file compatibility
//!
//! Provides the Config struct that TieredLogConfig.toInternalConfig() returns.
//! In the rewritten architecture, these settings map to UAL segment parameters.

/// Internal configuration for tiered log storage
pub const Config = struct {
    buffer_capacity: usize = 64 * 1024 * 1024,
    max_hot_entries: usize = 10_000,
    hot_window_seconds: u64 = 300,
    segment_dir: []const u8 = "data/raft",
    max_local_segments: usize = 100,
    enable_wal_truncation: bool = true,
};
