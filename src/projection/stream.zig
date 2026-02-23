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
        self.hwm = 0;
        self.trim_offset = 0;
        self.applied_index = 0;
        self.stats = .{};
    }

    // ─── Core operations ───────────────────────────────────────────────────

    /// Append a record to the stream. Returns the assigned offset.
    /// The record payload is stored in the UAL at `ual_index`.
    pub fn append(self: *StreamProjection, ual_index: u64, timestamp_ns: u64) !u64 {
        self.hwm += 1;
        const offset = self.hwm;

        try self.offsets.put(offset, .{
            .ual_index = ual_index,
            .timestamp_ns = timestamp_ns,
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

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *StreamProjection, ual_entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(ual_entry.header.entry_type);

        switch (entry_type) {
            .stream_append => {
                _ = try self.append(ual_entry.header.index, ual_entry.header.timestamp_ns);
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

        return mem;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "stream: basic append and read" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const off1 = try s.append(100, 1000);
    const off2 = try s.append(101, 2000);

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

    _ = try s.append(100, 1000);
    _ = try s.append(101, 2000);
    _ = try s.append(102, 3000);
    _ = try s.append(103, 4000);

    var buf: [10]OffsetEntry = undefined;
    const count = s.readRange(2, 3, &buf);
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expectEqual(@as(u64, 101), buf[0].ual_index);
    try testing.expectEqual(@as(u64, 102), buf[1].ual_index);
}

test "stream: trim removes old offsets" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.append(100, 1000);
    _ = try s.append(101, 2000);
    _ = try s.append(102, 3000);

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

    _ = try s.append(100, 1000);
    _ = try s.append(101, 2000);

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

    _ = try s.append(100, 1000);
    _ = try s.append(101, 2000);
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

    _ = try s.append(100, 1000);
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
