//! Stream Projection — offset tracking + consumer groups.
//!
//! A stream is an append-only log of records. The StreamProjection tracks:
//!   - High water mark (HWM): the latest appended offset
//!   - Consumer groups: named groups of consumers with offset tracking
//!
//! Records themselves are NOT stored here — they live in the UAL.
//! The projection only maintains offsets and consumer group state.
//!
//! Data flow:
//!   stream_append → increment HWM, record UAL index for offset
//!   commitOffset  → update consumer group's committed offset
//!   stream_trim   → advance trim offset (records below are GC'd)
//!
//! Consumer group lifecycle:
//!   create → join member → read (tracked per-member) → commit → leave → delete

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Group Member
// ═══════════════════════════════════════════════════════════════════════════════

pub const MemberState = enum(u8) {
    active,
    leaving,
};

pub const Member = struct {
    /// Member identifier (e.g., consumer instance ID).
    id: []const u8,
    /// This member's last committed offset.
    committed_offset: u64,
    /// Timestamp when joined.
    joined_at_ns: u64,
    /// Current state.
    state: MemberState,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Group
// ═══════════════════════════════════════════════════════════════════════════════

pub const ConsumerGroup = struct {
    /// Group name (owned).
    name: []const u8,
    /// Members by ID.
    members: std.StringHashMap(Member),
    /// Group-level committed offset (min of all member offsets, or explicit).
    committed_offset: u64,
    /// When the group was created.
    created_at_ns: u64,
    /// Allocator for member management.
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, created_at_ns: u64) !ConsumerGroup {
        const owned_name = try allocator.dupe(u8, name);
        return .{
            .name = owned_name,
            .members = std.StringHashMap(Member).init(allocator),
            .committed_offset = 0,
            .created_at_ns = created_at_ns,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConsumerGroup) void {
        // Free member IDs
        var it = self.members.iterator();
        while (it.next()) |kv| {
            self.allocator.free(@constCast(kv.value_ptr.id));
        }
        self.members.deinit();
        self.allocator.free(@constCast(self.name));
    }

    /// Add a member to the group. Returns true if new, false if already exists.
    pub fn join(self: *ConsumerGroup, member_id: []const u8, now_ns: u64) !bool {
        const gop = try self.members.getOrPut(member_id);
        if (gop.found_existing) return false;

        const owned_id = try self.allocator.dupe(u8, member_id);
        gop.key_ptr.* = owned_id;
        gop.value_ptr.* = .{
            .id = owned_id,
            .committed_offset = 0,
            .joined_at_ns = now_ns,
            .state = .active,
        };
        return true;
    }

    /// Remove a member from the group. Returns true if removed.
    pub fn leave(self: *ConsumerGroup, member_id: []const u8) bool {
        if (self.members.fetchRemove(member_id)) |kv| {
            self.allocator.free(@constCast(kv.value.id));
            return true;
        }
        return false;
    }

    /// Commit offset for a specific member.
    pub fn commitMemberOffset(self: *ConsumerGroup, member_id: []const u8, offset: u64) bool {
        if (self.members.getPtr(member_id)) |member| {
            if (offset > member.committed_offset) {
                member.committed_offset = offset;
            }
            return true;
        }
        return false;
    }

    /// Commit offset at the group level (not per-member).
    pub fn commitGroupOffset(self: *ConsumerGroup, offset: u64) void {
        if (offset > self.committed_offset) {
            self.committed_offset = offset;
        }
    }

    /// Get the minimum committed offset across all active members.
    /// Returns group-level committed_offset if no members.
    pub fn minCommittedOffset(self: *const ConsumerGroup) u64 {
        var min: u64 = std.math.maxInt(u64);
        var has_any = false;
        var it = self.members.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.state == .active) {
                min = @min(min, kv.value_ptr.committed_offset);
                has_any = true;
            }
        }
        return if (has_any) min else self.committed_offset;
    }

    pub fn memberCount(self: *const ConsumerGroup) usize {
        return self.members.count();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Offset Entry — maps stream offset → UAL index
// ═══════════════════════════════════════════════════════════════════════════════

pub const OffsetEntry = struct {
    /// UAL index where this record's data lives.
    ual_index: u64,
    /// Timestamp when appended.
    timestamp_ns: u64,
    /// Hash of the stream name this entry belongs to (for per-stream filtering).
    stream_name_hash: u64,
    /// User partition index (for multi-partition streams).
    partition_index: u32 = 0,
};

/// Stream metadata — partition count and other per-stream config.
pub const StreamMetadata = struct {
    partition_count: u32 = 1,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Stream Projection
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamProjection = struct {
    allocator: Allocator,

    /// Offset → UAL index mapping. Offsets are 1-based.
    offsets: std.AutoHashMap(u64, OffsetEntry),

    /// Consumer groups by name.
    groups: std.StringHashMap(ConsumerGroup),

    /// Registered stream names (for listing).
    stream_names: std.StringHashMap(void),

    /// Stream metadata (partition count, etc.).
    stream_metadata: std.StringHashMap(StreamMetadata),

    /// High water mark — the latest appended offset.
    hwm: u64,

    /// Trim offset — records at or below this are considered trimmed.
    trim_offset: u64,

    /// Last applied UAL index.
    applied_index: u64,

    /// Stats.
    stats: Stats,

    pub const Stats = struct {
        appended: u64 = 0,
        reads: u64 = 0,
        commits: u64 = 0,
        groups_created: u64 = 0,
        groups_deleted: u64 = 0,
        trimmed: u64 = 0,
    };

    pub fn init(allocator: Allocator) StreamProjection {
        return .{
            .allocator = allocator,
            .offsets = std.AutoHashMap(u64, OffsetEntry).init(allocator),
            .groups = std.StringHashMap(ConsumerGroup).init(allocator),
            .stream_names = std.StringHashMap(void).init(allocator),
            .stream_metadata = std.StringHashMap(StreamMetadata).init(allocator),
            .hwm = 0,
            .trim_offset = 0,
            .applied_index = 0,
            .stats = .{},
        };
    }

    pub fn deinit(self: *StreamProjection) void {
        // Free consumer groups
        var git = self.groups.iterator();
        while (git.next()) |kv| {
            kv.value_ptr.deinit();
        }
        self.groups.deinit();
        self.offsets.deinit();
        // Free stream name keys
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_names.deinit();
        // Free stream metadata keys
        var mit = self.stream_metadata.keyIterator();
        while (mit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_metadata.deinit();
    }

    /// Reset the stream projection to empty state.
    /// Used during namespace force-delete to clear all stream data.
    pub fn reset(self: *StreamProjection) void {
        // Free consumer groups
        var git = self.groups.iterator();
        while (git.next()) |kv| {
            kv.value_ptr.deinit();
        }
        self.groups.clearAndFree();
        self.offsets.clearAndFree();
        // Free stream name keys
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_names.clearAndFree();
        // Free stream metadata keys
        var mit = self.stream_metadata.keyIterator();
        while (mit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_metadata.clearAndFree();
        self.hwm = 0;
        self.trim_offset = 0;
        self.applied_index = 0;
        self.stats = .{};
    }

    // ─── Core operations ───────────────────────────────────────────────────

    /// Append a record to the stream. Returns the assigned offset.
    /// The record payload is stored in the UAL at `ual_index`.
    pub fn append(self: *StreamProjection, ual_index: u64, timestamp_ns: u64, stream_name_hash: u64, partition_index: u32) !u64 {
        self.hwm += 1;
        const offset = self.hwm;

        try self.offsets.put(offset, .{
            .ual_index = ual_index,
            .timestamp_ns = timestamp_ns,
            .stream_name_hash = stream_name_hash,
            .partition_index = partition_index,
        });

        self.stats.appended += 1;
        return offset;
    }

    /// Read the UAL index for a given stream offset.
    /// Returns null if the offset doesn't exist or has been trimmed.
    pub fn read(self: *StreamProjection, offset: u64) ?OffsetEntry {
        if (offset <= self.trim_offset) return null;
        self.stats.reads += 1;
        return self.offsets.get(offset);
    }

    /// Read a range of offsets [from_offset, to_offset] inclusive.
    /// Returns entries that exist and are not trimmed.
    pub fn readRange(self: *StreamProjection, from_offset: u64, to_offset: u64, buf: []OffsetEntry) usize {
        const start = @max(from_offset, self.trim_offset + 1);
        if (start > to_offset) return 0;
        if (start > self.hwm) return 0;

        const end = @min(to_offset, self.hwm);
        var count: usize = 0;

        var offset = start;
        while (offset <= end and count < buf.len) : (offset += 1) {
            if (self.offsets.get(offset)) |entry| {
                buf[count] = entry;
                count += 1;
            }
        }

        self.stats.reads += count;
        return count;
    }

    /// Read a range of offsets filtered by stream name hash.
    /// Only returns entries belonging to the specified stream.
    pub fn readRangeForStream(self: *StreamProjection, from_offset: u64, to_offset: u64, stream_name_hash: u64, buf: []OffsetEntry) usize {
        const start = @max(from_offset, self.trim_offset + 1);
        if (start > to_offset) return 0;
        if (start > self.hwm) return 0;

        const end = @min(to_offset, self.hwm);
        var count: usize = 0;

        var offset = start;
        while (offset <= end and count < buf.len) : (offset += 1) {
            if (self.offsets.get(offset)) |entry| {
                if (entry.stream_name_hash == stream_name_hash) {
                    buf[count] = entry;
                    count += 1;
                }
            }
        }

        self.stats.reads += count;
        return count;
    }

    /// Read a range of offsets filtered by stream name hash AND partition index.
    /// For multi-partition reads with explicit --partition flag.
    pub fn readRangeForStreamPartition(self: *StreamProjection, from_offset: u64, to_offset: u64, stream_name_hash: u64, partition_index: u32, buf: []OffsetEntry) usize {
        const start = @max(from_offset, self.trim_offset + 1);
        if (start > to_offset) return 0;
        if (start > self.hwm) return 0;

        const end = @min(to_offset, self.hwm);
        var count: usize = 0;

        var offset = start;
        while (offset <= end and count < buf.len) : (offset += 1) {
            if (self.offsets.get(offset)) |entry| {
                if (entry.stream_name_hash == stream_name_hash and entry.partition_index == partition_index) {
                    buf[count] = entry;
                    count += 1;
                }
            }
        }

        self.stats.reads += count;
        return count;
    }

    /// Get the current high water mark (latest offset).
    pub fn highWaterMark(self: *const StreamProjection) u64 {
        return self.hwm;
    }

    /// Trim records up to and including the given offset.
    /// Removes offset entries from the mapping.
    pub fn trim(self: *StreamProjection, up_to_offset: u64) u64 {
        if (up_to_offset <= self.trim_offset) return 0;

        var trimmed: u64 = 0;
        var offset = self.trim_offset + 1;
        while (offset <= up_to_offset) : (offset += 1) {
            if (self.offsets.remove(offset)) {
                trimmed += 1;
            }
        }

        self.trim_offset = up_to_offset;
        self.stats.trimmed += trimmed;
        return trimmed;
    }

    // ─── Consumer Group operations ─────────────────────────────────────────

    /// Create a consumer group. Returns error.AlreadyExists if name taken.
    pub fn createGroup(self: *StreamProjection, name: []const u8, now_ns: u64) !void {
        const gop = try self.groups.getOrPut(name);
        if (gop.found_existing) return error.AlreadyExists;

        var group = try ConsumerGroup.init(self.allocator, name, now_ns);
        gop.key_ptr.* = group.name;
        gop.value_ptr.* = group;
        _ = &group;
        self.stats.groups_created += 1;
    }

    /// Delete a consumer group.
    pub fn deleteGroup(self: *StreamProjection, name: []const u8) bool {
        if (self.groups.fetchRemove(name)) |kv| {
            var group = kv.value;
            group.deinit();
            self.stats.groups_deleted += 1;
            return true;
        }
        return false;
    }

    /// Get a consumer group by name.
    pub fn getGroup(self: *StreamProjection, name: []const u8) ?*ConsumerGroup {
        return self.groups.getPtr(name);
    }

    /// Join a member to a consumer group.
    pub fn joinGroup(self: *StreamProjection, group_name: []const u8, member_id: []const u8, now_ns: u64) !bool {
        if (self.groups.getPtr(group_name)) |group| {
            return try group.join(member_id, now_ns);
        }
        return error.GroupNotFound;
    }

    /// Remove a member from a consumer group.
    pub fn leaveGroup(self: *StreamProjection, group_name: []const u8, member_id: []const u8) !bool {
        if (self.groups.getPtr(group_name)) |group| {
            return group.leave(member_id);
        }
        return error.GroupNotFound;
    }

    /// Commit offset for a consumer group (group-level).
    pub fn commitOffset(self: *StreamProjection, group_name: []const u8, offset: u64) !void {
        if (self.groups.getPtr(group_name)) |group| {
            group.commitGroupOffset(offset);
            self.stats.commits += 1;
            return;
        }
        return error.GroupNotFound;
    }

    /// Commit offset for a specific member in a group.
    pub fn commitMemberOffset(self: *StreamProjection, group_name: []const u8, member_id: []const u8, offset: u64) !void {
        if (self.groups.getPtr(group_name)) |group| {
            if (group.commitMemberOffset(member_id, offset)) {
                self.stats.commits += 1;
                return;
            }
            return error.MemberNotFound;
        }
        return error.GroupNotFound;
    }

    /// Count of consumer groups.
    pub fn groupCount(self: *const StreamProjection) usize {
        return self.groups.count();
    }

    /// Total offsets currently tracked (not trimmed).
    pub fn trackedOffsets(self: *const StreamProjection) usize {
        return self.offsets.count();
    }

    // ─── Stream Name Registry ──────────────────────────────────────────────

    /// Register a stream name. No-op if already registered.
    pub fn registerStream(self: *StreamProjection, name: []const u8) !void {
        if (name.len == 0) return;
        const gop = try self.stream_names.getOrPut(name);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, name);
            gop.key_ptr.* = owned;
        }
    }

    /// Register (or update) partition metadata for a stream.
    pub fn registerStreamMetadata(self: *StreamProjection, name: []const u8, partition_count: u32) !void {
        if (name.len == 0) return;
        const gop = try self.stream_metadata.getOrPut(name);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, name);
            gop.key_ptr.* = owned;
        }
        gop.value_ptr.* = .{ .partition_count = @max(1, partition_count) };
    }

    /// Get the partition count for a stream (defaults to 1).
    pub fn getPartitionCount(self: *const StreamProjection, name: []const u8) u32 {
        if (self.stream_metadata.get(name)) |meta| return meta.partition_count;
        return 1;
    }

    /// Scan registered stream names into a buffer. Returns the count written.
    /// Returns borrowed references into the HashMap (zero-copy, valid until mutation).
    pub fn scanStreamNames(self: *const StreamProjection, buf: [][]const u8) usize {
        var count: usize = 0;
        var it = self.stream_names.keyIterator();
        while (it.next()) |key| {
            if (count >= buf.len) break;
            buf[count] = key.*;
            count += 1;
        }
        return count;
    }

    /// Number of registered stream names.
    pub fn streamCount(self: *const StreamProjection) usize {
        return self.stream_names.count();
    }

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *StreamProjection, ual_entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(ual_entry.header.entry_type);

        switch (entry_type) {
            .stream_append => {
                // Extract stream name hash from command payload, incorporating
                // namespace_hash as the Wyhash seed for namespace isolation.
                // This matches the live path: router.nameHash(ns_hash, key).
                const name_hash = if (CommandPayload.deserialize(ual_entry.payload)) |cmd|
                    std.hash.Wyhash.hash(@as(u64, cmd.namespace_hash), cmd.key)
                else
                    0;
                _ = try self.append(ual_entry.header.index, ual_entry.header.timestamp_ns, name_hash, 0);
            },
            .stream_trim => {
                // Trim offset encoded in command payload key as u64
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len >= 8) {
                        const trim_to = std.mem.readInt(u64, cmd.key[0..8], .little);
                        _ = self.trim(trim_to);
                    }
                }
            },
            .cg_create => {
                // Consumer group name in command payload key
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len > 0) {
                        self.createGroup(cmd.key, ual_entry.header.timestamp_ns) catch |err| {
                            if (err != error.AlreadyExists) return err;
                        };
                    }
                }
            },
            .cg_delete => {
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len > 0) {
                        _ = self.deleteGroup(cmd.key);
                    }
                }
            },
            .cg_commit => {
                // Key = group name, Value = offset as u64
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len > 0 and cmd.value.len >= 8) {
                        const offset = std.mem.readInt(u64, cmd.value[0..8], .little);
                        self.commitOffset(cmd.key, offset) catch |err| {
                            if (err != error.GroupNotFound) return err;
                        };
                    }
                }
            },
            else => {},
        }

        self.applied_index = ual_entry.header.index;
    }

    /// ProjectionVTable implementation.
    pub fn projectionHandle(self: *StreamProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = vtableApply,
                .memoryUsageFn = vtableMemory,
            },
        };
    }

    fn vtableApply(ctx: *anyopaque, ual_entry: *const Entry) router_mod.ApplyError!void {
        const self: *StreamProjection = @ptrCast(@alignCast(ctx));
        self.applyEntry(ual_entry) catch return error.OutOfMemory;
    }

    fn vtableMemory(ctx: *anyopaque) usize {
        const self: *StreamProjection = @ptrCast(@alignCast(ctx));
        return self.memoryUsage();
    }

    pub fn memoryUsage(self: *const StreamProjection) usize {
        var mem: usize = @sizeOf(StreamProjection);
        mem += self.offsets.count() * (@sizeOf(u64) + @sizeOf(OffsetEntry));

        var git = self.groups.iterator();
        while (git.next()) |kv| {
            mem += kv.value_ptr.name.len;
            mem += kv.value_ptr.members.count() * @sizeOf(Member);
        }

        // Stream names
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            mem += key.len;
        }

        return mem;
    }

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// Serialize the full stream projection state.
    /// Format: [hwm: u64][trim_offset: u64]
    ///   [offset_count: u32] then per offset:
    ///     [offset: u64][ual_index: u64][timestamp_ns: u64][stream_name_hash: u64][partition_index: u32]
    ///   [stream_name_count: u32] then per stream name:
    ///     [name_len: u16][name bytes]
    ///   [metadata_count: u32] then per stream metadata:
    ///     [name_len: u16][name bytes][partition_count: u32]
    ///   [group_count: u32] then per consumer group:
    ///     [name_len: u16][name bytes][committed_offset: u64][created_at_ns: u64]
    ///     [member_count: u32] then per member:
    ///       [id_len: u16][id bytes][committed_offset: u64][joined_at_ns: u64][state: u8]
    /// Caller owns returned slice.
    pub fn serialize(self: *StreamProjection, allocator: Allocator) ![]u8 {
        // Calculate total size
        var total_size: usize = 8 + 8; // hwm + trim_offset

        // Offsets: count(4) + entries(8+8+8+8+4=36 each)
        total_size += 4 + self.offsets.count() * 36;

        // Stream names: count(4) + per name(len_u16 + bytes)
        total_size += 4;
        var sn_it = self.stream_names.keyIterator();
        while (sn_it.next()) |key| {
            total_size += 2 + key.len;
        }

        // Stream metadata: count(4) + per entry(len_u16 + bytes + u32)
        total_size += 4;
        var sm_it = self.stream_metadata.iterator();
        while (sm_it.next()) |kv| {
            total_size += 2 + kv.key_ptr.len + 4;
        }

        // Groups: count(4) + per group(...)
        total_size += 4;
        var g_it = self.groups.iterator();
        while (g_it.next()) |kv| {
            const group = kv.value_ptr;
            // name_len(2) + name + committed_offset(8) + created_at_ns(8) + member_count(4)
            total_size += 2 + group.name.len + 8 + 8 + 4;
            var m_it = group.members.iterator();
            while (m_it.next()) |mkv| {
                // id_len(2) + id + committed_offset(8) + joined_at_ns(8) + state(1)
                total_size += 2 + mkv.value_ptr.id.len + 8 + 8 + 1;
            }
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // hwm + trim_offset
        std.mem.writeInt(u64, buf[offset..][0..8], self.hwm, .little);
        offset += 8;
        std.mem.writeInt(u64, buf[offset..][0..8], self.trim_offset, .little);
        offset += 8;

        // Offsets
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.offsets.count()), .little);
        offset += 4;
        var off_it = self.offsets.iterator();
        while (off_it.next()) |kv| {
            const off_key = kv.key_ptr.*;
            const entry = kv.value_ptr;
            std.mem.writeInt(u64, buf[offset..][0..8], off_key, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.ual_index, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.timestamp_ns, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.stream_name_hash, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], entry.partition_index, .little);
            offset += 4;
        }

        // Stream names
        sn_it = self.stream_names.keyIterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.stream_names.count()), .little);
        offset += 4;
        while (sn_it.next()) |key| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(key.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..key.len], key.*);
            offset += key.len;
        }

        // Stream metadata
        sm_it = self.stream_metadata.iterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.stream_metadata.count()), .little);
        offset += 4;
        while (sm_it.next()) |kv| {
            const name = kv.key_ptr.*;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(name.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..name.len], name);
            offset += name.len;
            std.mem.writeInt(u32, buf[offset..][0..4], kv.value_ptr.partition_count, .little);
            offset += 4;
        }

        // Groups
        g_it = self.groups.iterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.groups.count()), .little);
        offset += 4;
        while (g_it.next()) |kv| {
            const group = kv.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(group.name.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..group.name.len], group.name);
            offset += group.name.len;
            std.mem.writeInt(u64, buf[offset..][0..8], group.committed_offset, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], group.created_at_ns, .little);
            offset += 8;

            // Members
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(group.members.count()), .little);
            offset += 4;
            var m_it = group.members.iterator();
            while (m_it.next()) |mkv| {
                const member = mkv.value_ptr;
                std.mem.writeInt(u16, buf[offset..][0..2], @intCast(member.id.len), .little);
                offset += 2;
                @memcpy(buf[offset..][0..member.id.len], member.id);
                offset += member.id.len;
                std.mem.writeInt(u64, buf[offset..][0..8], member.committed_offset, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], member.joined_at_ns, .little);
                offset += 8;
                buf[offset] = @intFromEnum(member.state);
                offset += 1;
            }
        }

        return buf;
    }

    /// Restore stream projection state from serialized bytes.
    /// Clears all existing state before restoring.
    pub fn deserialize(self: *StreamProjection, data: []const u8) !void {
        self.reset();

        if (data.len < 16) return; // minimum: hwm + trim_offset
        var offset: usize = 0;

        // hwm + trim_offset
        self.hwm = std.mem.readInt(u64, data[offset..][0..8], .little);
        offset += 8;
        self.trim_offset = std.mem.readInt(u64, data[offset..][0..8], .little);
        offset += 8;

        // Offsets
        if (offset + 4 > data.len) return;
        const off_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var oi: u32 = 0;
        while (oi < off_count) : (oi += 1) {
            if (offset + 36 > data.len) return;
            const off_key = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const ual_index = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const stream_name_hash = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const partition_index = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            try self.offsets.put(off_key, .{
                .ual_index = ual_index,
                .timestamp_ns = timestamp_ns,
                .stream_name_hash = stream_name_hash,
                .partition_index = partition_index,
            });
        }

        // Stream names
        if (offset + 4 > data.len) return;
        const sn_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var si: u32 = 0;
        while (si < sn_count) : (si += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len > data.len) return;
            const name = try self.allocator.dupe(u8, data[offset..][0..name_len]);
            offset += name_len;
            try self.stream_names.put(name, {});
        }

        // Stream metadata
        if (offset + 4 > data.len) return;
        const sm_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var mi: u32 = 0;
        while (mi < sm_count) : (mi += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len + 4 > data.len) return;
            const name = try self.allocator.dupe(u8, data[offset..][0..name_len]);
            offset += name_len;
            const partition_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            try self.stream_metadata.put(name, .{ .partition_count = partition_count });
        }

        // Consumer groups
        if (offset + 4 > data.len) return;
        const group_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var gi: u32 = 0;
        while (gi < group_count) : (gi += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len + 20 > data.len) return;
            const group_name = data[offset..][0..name_len];
            offset += name_len;
            const committed_offset = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const created_at_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;

            var group = try ConsumerGroup.init(self.allocator, group_name, created_at_ns);
            group.committed_offset = committed_offset;

            // Members
            if (offset + 4 > data.len) {
                group.deinit();
                return;
            }
            const member_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            var mj: u32 = 0;
            while (mj < member_count) : (mj += 1) {
                if (offset + 2 > data.len) {
                    group.deinit();
                    return;
                }
                const id_len = std.mem.readInt(u16, data[offset..][0..2], .little);
                offset += 2;
                if (offset + id_len + 17 > data.len) {
                    group.deinit();
                    return;
                }
                const member_id = try self.allocator.dupe(u8, data[offset..][0..id_len]);
                offset += id_len;
                const member_committed = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
                const joined_at_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
                const state: MemberState = @enumFromInt(data[offset]);
                offset += 1;

                try group.members.put(member_id, .{
                    .id = member_id,
                    .committed_offset = member_committed,
                    .joined_at_ns = joined_at_ns,
                    .state = state,
                });
            }

            // Use group_name as key (group.name is the owned copy)
            try self.groups.put(group.name, group);
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "stream: basic append and read" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const off1 = try s.append(100, 1000, 0, 0);
    const off2 = try s.append(101, 2000, 0, 0);

    try testing.expectEqual(@as(u64, 1), off1);
    try testing.expectEqual(@as(u64, 2), off2);
    try testing.expectEqual(@as(u64, 2), s.highWaterMark());

    const e1 = s.read(1).?;
    try testing.expectEqual(@as(u64, 100), e1.ual_index);
    try testing.expectEqual(@as(u64, 1000), e1.timestamp_ns);

    const e2 = s.read(2).?;
    try testing.expectEqual(@as(u64, 101), e2.ual_index);

    try testing.expect(s.read(3) == null); // beyond HWM
}

test "stream: read range" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000, 0, 0);
    _ = try s.append(101, 2000, 0, 0);
    _ = try s.append(102, 3000, 0, 0);
    _ = try s.append(103, 4000, 0, 0);

    var buf: [10]OffsetEntry = undefined;
    const count = s.readRange(2, 3, &buf);
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expectEqual(@as(u64, 101), buf[0].ual_index);
    try testing.expectEqual(@as(u64, 102), buf[1].ual_index);
}

test "stream: trim removes old offsets" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000, 0, 0);
    _ = try s.append(101, 2000, 0, 0);
    _ = try s.append(102, 3000, 0, 0);

    const trimmed = s.trim(2);
    try testing.expectEqual(@as(u64, 2), trimmed);
    try testing.expectEqual(@as(u64, 2), s.trim_offset);

    // Offsets 1 and 2 should be gone
    try testing.expect(s.read(1) == null);
    try testing.expect(s.read(2) == null);

    // Offset 3 still readable
    try testing.expect(s.read(3) != null);
    try testing.expectEqual(@as(usize, 1), s.trackedOffsets());
}

test "stream: consumer group lifecycle" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Create group
    try s.createGroup("my-group", 1000);
    try testing.expectEqual(@as(usize, 1), s.groupCount());

    // Duplicate create returns AlreadyExists (handled gracefully)
    try testing.expectError(error.AlreadyExists, s.createGroup("my-group", 2000));

    // Join member
    const joined = try s.joinGroup("my-group", "consumer-1", 3000);
    try testing.expect(joined);

    // Duplicate join returns false
    const joined2 = try s.joinGroup("my-group", "consumer-1", 4000);
    try testing.expect(!joined2);

    // Get group
    const group = s.getGroup("my-group").?;
    try testing.expectEqual(@as(usize, 1), group.memberCount());

    // Leave
    const left = try s.leaveGroup("my-group", "consumer-1");
    try testing.expect(left);
    try testing.expectEqual(@as(usize, 0), group.memberCount());

    // Delete group
    try testing.expect(s.deleteGroup("my-group"));
    try testing.expectEqual(@as(usize, 0), s.groupCount());
}

test "stream: commit offset" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000, 0, 0);
    _ = try s.append(101, 2000, 0, 0);

    try s.createGroup("grp", 1000);
    _ = try s.joinGroup("grp", "c1", 2000);

    // Commit group-level offset
    try s.commitOffset("grp", 2);
    const group = s.getGroup("grp").?;
    try testing.expectEqual(@as(u64, 2), group.committed_offset);

    // Commit member offset
    try s.commitMemberOffset("grp", "c1", 1);

    try testing.expectEqual(@as(u64, 1), group.minCommittedOffset());
}

test "stream: commit to non-existent group" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    try testing.expectError(error.GroupNotFound, s.commitOffset("nonexistent", 1));
}

test "stream: stats tracking" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000, 0, 0);
    _ = try s.append(101, 2000, 0, 0);
    _ = s.read(1);
    _ = s.trim(1);
    try s.createGroup("g", 1000);

    try testing.expectEqual(@as(u64, 2), s.stats.appended);
    try testing.expectEqual(@as(u64, 1), s.stats.reads);
    try testing.expectEqual(@as(u64, 1), s.stats.trimmed);
    try testing.expectEqual(@as(u64, 1), s.stats.groups_created);
}

test "stream: empty read returns null" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    try testing.expect(s.read(1) == null);
    try testing.expectEqual(@as(u64, 0), s.highWaterMark());
}

test "stream: memory usage estimate" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000, 0, 0);
    try s.createGroup("g", 1000);

    try testing.expect(s.memoryUsage() > 0);
}

test "stream: projection handle with router" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Stream projection doesn't register with router currently
    // (stream_append routes to .none in the current router).
    // Test the vtable wiring directly:
    const handle = s.projectionHandle();
    try testing.expect(handle.memoryUsage() > 0);
}

test "stream: apply entry for stream_append" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const ual_entry = entry_mod.buildEntry(.stream_append, 0, 1, 1, 1000, "data");
    try s.applyEntry(&ual_entry);

    try testing.expectEqual(@as(u64, 1), s.highWaterMark());
    try testing.expectEqual(@as(u64, 1), s.stats.appended);
}

test "stream: serialize/deserialize round-trip" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Append records with different stream name hashes
    _ = try s.append(100, 1000, 42, 0);
    _ = try s.append(101, 2000, 42, 0);
    _ = try s.append(102, 3000, 99, 1);

    // Register stream names
    try s.registerStream("events");
    try s.registerStream("logs");

    // Set stream metadata
    try s.registerStreamMetadata("events", 4);

    // Create consumer group with members
    try s.createGroup("my-group", 5000);
    _ = try s.joinGroup("my-group", "consumer-1", 6000);
    _ = try s.joinGroup("my-group", "consumer-2", 7000);
    try s.commitOffset("my-group", 2);

    // Trim
    _ = s.trim(1);

    // Serialize
    const data = try s.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into fresh projection
    var s2 = StreamProjection.init(testing.allocator);
    defer s2.deinit();

    try s2.deserialize(data);

    // Verify HWM and trim offset
    try testing.expectEqual(@as(u64, 3), s2.highWaterMark());
    try testing.expectEqual(@as(u64, 1), s2.trim_offset);

    // Verify offsets (offset 1 is trimmed, so read(1) should be null)
    try testing.expect(s2.read(1) == null);
    const o2 = s2.read(2).?;
    try testing.expectEqual(@as(u64, 101), o2.ual_index);
    const o3 = s2.read(3).?;
    try testing.expectEqual(@as(u64, 102), o3.ual_index);
    try testing.expectEqual(@as(u64, 99), o3.stream_name_hash);

    // Verify stream names restored
    try testing.expect(s2.stream_names.contains("events"));
    try testing.expect(s2.stream_names.contains("logs"));
    try testing.expectEqual(@as(usize, 2), s2.streamCount());

    // Verify metadata
    const meta = s2.stream_metadata.get("events").?;
    try testing.expectEqual(@as(u32, 4), meta.partition_count);

    // Verify consumer group
    const group = s2.getGroup("my-group").?;
    try testing.expectEqual(@as(u64, 2), group.committed_offset);
    try testing.expectEqual(@as(usize, 2), group.memberCount());
}

test "stream: serialize empty projection" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const data = try s.serialize(testing.allocator);
    defer testing.allocator.free(data);

    var s2 = StreamProjection.init(testing.allocator);
    defer s2.deinit();

    try s2.deserialize(data);
    try testing.expectEqual(@as(u64, 0), s2.highWaterMark());
    try testing.expectEqual(@as(usize, 0), s2.streamCount());
}
