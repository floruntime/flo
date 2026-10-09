//! Per-namespace settings: defaults and ceilings an admin sets with
//! `namespace config set`, and their TLV form.

const std = @import("std");

/// Namespace configuration — registry entry + admin-configurable settings.
///
/// Registry fields (name, partition_count, etc.) are set at creation time.
/// Settings fields (nullable) are admin-settable via `namespace_config_set`.
/// Null settings mean "use system default / unlimited (memory controller decides)".
/// Settings are both defaults AND ceilings: per-resource overrides can only
/// be MORE restrictive than these.
pub const NamespaceConfig = struct {
    // ── Registry fields (set at creation) ───────────────────────────────

    /// Namespace name ("" for update-only instances)
    name: []const u8 = "",
    /// Number of partitions for this namespace
    partition_count: u16 = 0,
    /// Replication factor
    replication_factor: u8 = 0,
    /// Creation timestamp (nanoseconds)
    created_at_ns: u64 = 0,
    /// Whether the namespace is being deleted (tombstone)
    deleted: bool = false,

    // ── Admin-configurable settings ─────────────────────────────────────

    /// Max version entries kept in KV projection memory per key.
    /// null = unlimited (memory controller is the backstop).
    kv_max_hot_versions: ?u32 = null,
    /// Auto-expire historical versions older than this (seconds).
    /// null = no TTL on versions.
    kv_version_ttl_s: ?u64 = null,
    /// Max stream size in bytes per stream.
    /// null = unlimited.
    stream_retention_bytes: ?u64 = null,
    /// Max age of stream messages (seconds).
    /// null = no time-based retention.
    stream_retention_s: ?u64 = null,
    /// Dead-letter queue capacity per queue.
    /// null = system default (1000).
    queue_max_dlq_size: ?u32 = null,
    /// Max lease hold time for queue messages (seconds).
    /// null = system default (30s).
    queue_max_lease_s: ?u32 = null,
    /// Memory budget for this namespace (bytes).
    /// null = fair share of shard budget.
    memory_budget_bytes: ?u64 = null,

    // ── Settings TLV serialization ──────────────────────────────────────

    /// Setting tags for TLV serialization of configurable fields.
    pub const SettingsTag = enum(u8) {
        end = 0,
        kv_max_hot_versions = 1,
        kv_version_ttl_s = 2,
        stream_retention_bytes = 3,
        stream_retention_s = 4,
        queue_max_dlq_size = 5,
        queue_max_lease_s = 6,
        memory_budget_bytes = 7,
    };

    /// Maximum serialized size of settings TLV: 1 (count) + 7 * (1 tag + 8 max value) = 64 bytes
    pub const MAX_SETTINGS_SIZE: usize = 1 + 7 * 9;

    /// Returns true if all configurable settings are null (no overrides).
    pub fn settingsEmpty(self: NamespaceConfig) bool {
        return self.kv_max_hot_versions == null and
            self.kv_version_ttl_s == null and
            self.stream_retention_bytes == null and
            self.stream_retention_s == null and
            self.queue_max_dlq_size == null and
            self.queue_max_lease_s == null and
            self.memory_budget_bytes == null;
    }

    /// Merge configurable settings from `other` into self.
    /// Non-null fields in `other` overwrite self.
    pub fn mergeSettings(self: *NamespaceConfig, other: NamespaceConfig) void {
        if (other.kv_max_hot_versions) |v| self.kv_max_hot_versions = v;
        if (other.kv_version_ttl_s) |v| self.kv_version_ttl_s = v;
        if (other.stream_retention_bytes) |v| self.stream_retention_bytes = v;
        if (other.stream_retention_s) |v| self.stream_retention_s = v;
        if (other.queue_max_dlq_size) |v| self.queue_max_dlq_size = v;
        if (other.queue_max_lease_s) |v| self.queue_max_lease_s = v;
        if (other.memory_budget_bytes) |v| self.memory_budget_bytes = v;
    }

    /// Serialize configurable settings to TLV format: [count:u8] ([tag:u8][value:u32/u64])*
    /// Returns number of bytes written.
    pub fn serializeSettings(self: NamespaceConfig, buf: []u8) usize {
        var count: u8 = 0;
        var pos: usize = 1; // reserve byte 0 for count

        if (self.kv_max_hot_versions) |v| {
            buf[pos] = @intFromEnum(SettingsTag.kv_max_hot_versions);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.kv_version_ttl_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.kv_version_ttl_s);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.stream_retention_bytes) |v| {
            buf[pos] = @intFromEnum(SettingsTag.stream_retention_bytes);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.stream_retention_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.stream_retention_s);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.queue_max_dlq_size) |v| {
            buf[pos] = @intFromEnum(SettingsTag.queue_max_dlq_size);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.queue_max_lease_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.queue_max_lease_s);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.memory_budget_bytes) |v| {
            buf[pos] = @intFromEnum(SettingsTag.memory_budget_bytes);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }

        buf[0] = count;
        return pos;
    }

    /// Deserialize configurable settings from TLV format.
    /// Returns a NamespaceConfig with only settings populated and bytes consumed.
    pub fn deserializeSettings(data: []const u8) struct { config: NamespaceConfig, consumed: usize } {
        var s: NamespaceConfig = .{};
        if (data.len == 0) return .{ .config = s, .consumed = 0 };

        const count = data[0];
        var pos: usize = 1;

        for (0..count) |_| {
            if (pos >= data.len) break;
            const tag = std.enums.fromInt(SettingsTag, data[pos]) orelse break;
            pos += 1;
            switch (tag) {
                .kv_max_hot_versions => {
                    if (pos + 4 > data.len) break;
                    s.kv_max_hot_versions = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .kv_version_ttl_s => {
                    if (pos + 8 > data.len) break;
                    s.kv_version_ttl_s = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .stream_retention_bytes => {
                    if (pos + 8 > data.len) break;
                    s.stream_retention_bytes = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .stream_retention_s => {
                    if (pos + 8 > data.len) break;
                    s.stream_retention_s = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .queue_max_dlq_size => {
                    if (pos + 4 > data.len) break;
                    s.queue_max_dlq_size = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .queue_max_lease_s => {
                    if (pos + 4 > data.len) break;
                    s.queue_max_lease_s = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .memory_budget_bytes => {
                    if (pos + 8 > data.len) break;
                    s.memory_budget_bytes = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .end => break,
            }
        }

        return .{ .config = s, .consumed = pos };
    }
};

const testing = std.testing;

test "NamespaceConfig settings serialize/deserialize roundtrip" {
    const original = NamespaceConfig{
        .kv_max_hot_versions = 50,
        .kv_version_ttl_s = 3600,
        .stream_retention_bytes = 1_073_741_824,
        .stream_retention_s = 86400,
        .queue_max_dlq_size = 500,
        .queue_max_lease_s = 60,
        .memory_budget_bytes = 2_147_483_648,
    };

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);
    try testing.expect(len > 0);

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expectEqual(original.kv_max_hot_versions, result.config.kv_max_hot_versions);
    try testing.expectEqual(original.kv_version_ttl_s, result.config.kv_version_ttl_s);
    try testing.expectEqual(original.stream_retention_bytes, result.config.stream_retention_bytes);
    try testing.expectEqual(original.stream_retention_s, result.config.stream_retention_s);
    try testing.expectEqual(original.queue_max_dlq_size, result.config.queue_max_dlq_size);
    try testing.expectEqual(original.queue_max_lease_s, result.config.queue_max_lease_s);
    try testing.expectEqual(original.memory_budget_bytes, result.config.memory_budget_bytes);
    try testing.expectEqual(len, result.consumed);
}

test "NamespaceConfig settings serialize/deserialize partial" {
    const original = NamespaceConfig{
        .kv_max_hot_versions = 100,
        .stream_retention_s = 7200,
    };

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expectEqual(@as(?u32, 100), result.config.kv_max_hot_versions);
    try testing.expectEqual(@as(?u64, 7200), result.config.stream_retention_s);
    try testing.expectEqual(@as(?u64, null), result.config.kv_version_ttl_s);
    try testing.expectEqual(@as(?u64, null), result.config.stream_retention_bytes);
    try testing.expectEqual(@as(?u32, null), result.config.queue_max_dlq_size);
    try testing.expectEqual(@as(?u32, null), result.config.queue_max_lease_s);
    try testing.expectEqual(@as(?u64, null), result.config.memory_budget_bytes);
}

test "NamespaceConfig settings empty roundtrip" {
    const original = NamespaceConfig{};
    try testing.expect(original.settingsEmpty());

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);
    try testing.expectEqual(@as(usize, 1), len); // just the count byte = 0

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expect(result.config.settingsEmpty());
}

test "NamespaceConfig settings merge" {
    var base = NamespaceConfig{
        .kv_max_hot_versions = 50,
        .stream_retention_s = 3600,
    };

    const update = NamespaceConfig{
        .kv_max_hot_versions = 100,
        .queue_max_dlq_size = 200,
    };

    base.mergeSettings(update);
    try testing.expectEqual(@as(?u32, 100), base.kv_max_hot_versions); // overwritten
    try testing.expectEqual(@as(?u64, 3600), base.stream_retention_s); // preserved
    try testing.expectEqual(@as(?u32, 200), base.queue_max_dlq_size); // newly set
    try testing.expectEqual(@as(?u64, null), base.kv_version_ttl_s); // still null
}
