//! Storage Tier Configuration
//!
//! Unified configuration for the "Log is Data" architecture with
//! hot (RAM ring buffer) → warm (disk segments) → cold (archive) tiers.
//! All settings live under the [storage] TOML section.

/// Configuration for tiered storage (parsed from [storage] section)
pub const TieredLogConfig = struct {
    /// Hot tier RAM buffer capacity in bytes (default: 64MB).
    /// This is the mmap'd ring buffer for hot tier entries per partition.
    hot_buffer_capacity: usize = 64 * 1024 * 1024,

    /// Maximum entries in hot tier before eviction to warm tier.
    /// 0 = rely on buffer capacity only (default).
    /// Non-zero values useful for testing deterministic spill behavior.
    max_hot_entries: usize = 0,

    /// Maximum seconds entries stay in hot tier before flush to warm.
    /// 0 = disable time-based flush (default — timer not yet implemented).
    /// Recommended production value: 300 (5 minutes).
    hot_flush_seconds: u64 = 0,

    /// Maximum local disk segments before archival to cold tier (default: 100).
    max_local_segments: usize = 100,

    /// Enable WAL truncation after segment flush (default: true).
    enable_wal_truncation: bool = true,
};
