//! Tiered Log Configuration
//!
//! Configuration for the TieredRaftLog which provides the "Log is Data"
//! architecture with hot (RAM) → warm (disk segments) → cold (archive) tiers.

/// Configuration for tiered log storage
pub const TieredLogConfig = struct {
    /// RAM buffer capacity in bytes (default: 64MB)
    /// This is the ring buffer for hot tier entries
    buffer_capacity: usize = 64 * 1024 * 1024,

    /// Maximum entries in hot tier before auto-flush to warm tier (default: 10,000)
    /// When exceeded, older entries are flushed to disk segments
    max_hot_entries: usize = 10_000,

    /// Maximum time (in seconds) entries stay in hot tier before flush
    /// 0 = disable time-based flush (default: 300 = 5 minutes)
    hot_window_seconds: u64 = 300,

    /// Maximum number of local segments before archival to cold tier
    /// When exceeded, oldest segments are archived (default: 100)
    max_local_segments: usize = 100,

    /// Enable WAL truncation after segment flush (default: true)
    /// When true, WAL entries are removed after being safely in segments
    enable_wal_truncation: bool = true,

    /// Convert to TieredRaftLog.Config for internal use
    pub fn toInternalConfig(self: TieredLogConfig, segment_dir: []const u8) @import("../engine/log/tiered_log.zig").Config {
        return .{
            .buffer_capacity = self.buffer_capacity,
            .max_hot_entries = self.max_hot_entries,
            .hot_window_seconds = self.hot_window_seconds,
            .segment_dir = segment_dir,
            .max_local_segments = self.max_local_segments,
            .enable_wal_truncation = self.enable_wal_truncation,
        };
    }
};
