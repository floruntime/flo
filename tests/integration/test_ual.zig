const std = @import("std");
const testing = std.testing;
const src = @import("src");

const UAL = src.storage.ual.ual.UAL;
const entry_mod = src.storage.ual.entry;
const Entry = entry_mod.Entry;
const Header = entry_mod.Header;
const EntryType = entry_mod.EntryType;
const HEADER_SIZE = entry_mod.HEADER_SIZE;

fn makeEntry(index: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(.kv_put, 0, 1, index, @intCast(std.time.nanoTimestamp()), payload);
}

test "integration: UAL append and read back" {
    var ual = try UAL.init(testing.allocator, 64 * 1024); // 64 KB — plenty of room
    defer ual.deinit();

    // Append 100 entries with unique payloads
    var payload_buf: [100][32]u8 = undefined;
    for (0..100) |i| {
        const payload = std.fmt.bufPrint(&payload_buf[i], "entry-payload-{d:0>4}", .{i}) catch unreachable;
        const entry = makeEntry(@as(u64, @intCast(i)) + 1, payload);
        const idx = try ual.append(&entry);
        try testing.expectEqual(@as(u64, @intCast(i)) + 1, idx);
    }

    try testing.expectEqual(@as(u64, 100), ual.entry_count);
    try testing.expectEqual(@as(u64, 1), ual.min_live_index);
    try testing.expectEqual(@as(u64, 100), ual.max_index);

    // Read each entry back and verify content matches
    for (0..100) |i| {
        const index = @as(u64, @intCast(i)) + 1;
        const recovered = ual.read(index);
        try testing.expect(recovered != null);

        const entry = recovered.?;
        try testing.expectEqual(index, entry.header.index);
        try testing.expectEqual(entry_mod.ENTRY_MAGIC, entry.header.magic);
        try testing.expectEqual(@as(u8, 1), entry.header.version);
        try testing.expectEqual(@as(u8, @intFromEnum(EntryType.kv_put)), entry.header.entry_type);

        // Verify payload content
        var expected_buf: [32]u8 = undefined;
        const expected = std.fmt.bufPrint(&expected_buf, "entry-payload-{d:0>4}", .{i}) catch unreachable;
        try testing.expectEqualStrings(expected, entry.payload);
    }

    // Reading a non-existent index returns null
    try testing.expect(ual.read(0) == null);
    try testing.expect(ual.read(101) == null);
    try testing.expect(ual.read(999) == null);
}

test "integration: UAL wraps around ring" {
    // Use a small ring buffer (4096 bytes).
    // Each entry is ~40 (header) + 10 (payload) = 50 bytes.
    // 4096 / 50 ≈ 81 entries before eviction starts.
    var ual = try UAL.init(testing.allocator, 4096, 0);
    defer ual.deinit();

    const payload_text = "wrap-test!"; // 10 bytes
    const total_entries: u64 = 200; // Enough to force multiple wraps

    // Append 200 entries — this will evict older ones
    var i: u64 = 1;
    while (i <= total_entries) : (i += 1) {
        const entry = makeEntry(i, payload_text);
        _ = try ual.append(&entry);
    }

    // Verify that the ring has fewer entries than total appended
    try testing.expect(ual.entry_count < total_entries);
    try testing.expectEqual(total_entries, ual.total_appended);
    try testing.expectEqual(total_entries, ual.max_index);

    // Oldest entries should be evicted (return null)
    try testing.expect(ual.read(1) == null);
    try testing.expect(ual.read(2) == null);

    // Newest entries should still be readable
    const newest = ual.read(total_entries);
    try testing.expect(newest != null);
    try testing.expectEqual(total_entries, newest.?.header.index);
    try testing.expectEqualStrings(payload_text, newest.?.payload);

    // Verify min_live_index is greater than 1
    try testing.expect(ual.min_live_index > 1);

    // All entries from min_live_index to max_index should be readable
    i = ual.min_live_index;
    while (i <= ual.max_index) : (i += 1) {
        // Some may wrap across boundary and return null from read() (but not readCopy)
        // So we use readCopy for robustness
        var pbuf: [64]u8 = undefined;
        const entry = ual.readCopy(i, &pbuf);
        try testing.expect(entry != null);
        try testing.expectEqual(i, entry.?.header.index);
        try testing.expectEqualStrings(payload_text, entry.?.payload);
    }
}
