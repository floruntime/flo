//! Legacy tiered log config — stub for ADAPT file compatibility
//!
//! In the rewritten architecture, these settings map to UAL segment parameters.
//! Field names updated to match the unified [storage] config layout.

/// Internal configuration for tiered log storage
pub const Config = struct {
    hot_buffer_capacity: usize = 64 * 1024 * 1024,
    max_hot_entries: usize = 0,
    hot_flush_seconds: u64 = 0,
    segment_dir: []const u8 = "data/raft",
    max_local_segments: usize = 100,
    enable_wal_truncation: bool = true,
};
