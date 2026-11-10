//! What a machine may read of its shard's streams while it applies an
//! entry: a stream's bounds and its records' ids. Appends, trims and
//! deletes apply in log order through the same apply as contract entries
//! (`node/shard.zig` `applyEntryCore`), so at any index every replica reads
//! the same. The projection is held through a const pointer: a machine can
//! read a stream and has no way to change one.

const std = @import("std");
const stream_mod = @import("../projection/stream.zig");

pub const StreamID = stream_mod.StreamID;
const StreamProjection = stream_mod.StreamProjection;

pub const Streams = struct {
    proj: *const StreamProjection,

    /// The stream's first record, or MIN when it holds none.
    pub fn first(self: Streams, key: stream_mod.StreamKey) StreamID {
        return self.proj.streamFirstId(key);
    }

    /// The stream's last record, or MIN when it holds none.
    pub fn last(self: Streams, key: stream_mod.StreamKey) StreamID {
        return self.proj.streamLastId(key);
    }

    /// The last record a trim removed, or MIN. Never lowered while the
    /// stream exists: every record at or before it is gone.
    pub fn removedThrough(self: Streams, key: stream_mod.StreamKey) StreamID {
        return self.proj.streamRemovedThrough(key);
    }

    /// The append that holds `record`, by its first record, and the
    /// most its records take in an answer; null when the stream no longer
    /// holds it.
    pub fn appendOf(self: Streams, key: stream_mod.StreamKey, record: StreamID) ?Append {
        const rec = self.proj.streamAppendOf(key, record) orelse return null;
        return .{ .first = rec.id, .answer_bytes = stream_mod.answerBound(rec) };
    }

    pub const Append = struct { first: StreamID, answer_bytes: usize };

    /// The ids of the records after `cursor`, oldest first, one per
    /// record, at most `out.len`.
    pub fn idsAfter(self: Streams, key: stream_mod.StreamKey, cursor: StreamID, out: []StreamID) usize {
        return self.proj.streamIdsAfter(key, cursor, out);
    }
};

comptime {
    // The read-only rule is the pointer's constness; keep it.
    std.debug.assert(@typeInfo(@FieldType(Streams, "proj")).pointer.attrs.@"const");
}

const testing = std.testing;

fn id(ts: u64, seq: u64) StreamID {
    return .{ .timestamp_ms = ts, .sequence = seq };
}

test "streams: ids after a cursor are one per record, a cursor inside an append resuming in it" {
    var proj = StreamProjection.init(testing.allocator);
    defer proj.deinit();
    const h = stream_mod.streamKey(.default, "s");
    // Two appends in one millisecond: three records, then two.
    _ = try proj.appendToStreamAt(h, 1, 0, 100, 3, 30);
    _ = try proj.appendToStreamAt(h, 2, 0, 100, 2, 20);
    const s: Streams = .{ .proj = &proj };
    var out: [8]StreamID = undefined;

    const all = s.idsAfter(h, StreamID.MIN, &out);
    try testing.expectEqual(@as(usize, 5), all);
    for (out[0..all], 0..) |got, i| try testing.expect(got.eql(id(100, i)));

    const rest = s.idsAfter(h, id(100, 1), &out);
    try testing.expectEqual(@as(usize, 3), rest);
    try testing.expect(out[0].eql(id(100, 2)));
    try testing.expect(out[2].eql(id(100, 4)));

    try testing.expectEqual(@as(usize, 2), s.idsAfter(h, StreamID.MIN, out[0..2]));
    try testing.expectEqual(@as(usize, 0), s.idsAfter(h, id(100, 4), &out));
    try testing.expectEqual(@as(usize, 0), s.idsAfter(stream_mod.streamKey(.default, "other"), StreamID.MIN, &out));
    try testing.expect(s.first(h).eql(id(100, 0)));
    try testing.expect(s.last(h).eql(id(100, 4)));
}

test "streams: each record names its append, until a trim cuts it" {
    var proj = StreamProjection.init(testing.allocator);
    defer proj.deinit();
    const h = stream_mod.streamKey(.default, "s");
    _ = try proj.appendToStreamAt(h, 1, 0, 100, 3, 30);
    _ = try proj.appendToStreamAt(h, 2, 0, 100, 2, 20);
    const s: Streams = .{ .proj = &proj };
    try testing.expect(s.appendOf(h, id(100, 1)).?.first.eql(id(100, 0)));
    try testing.expect(s.appendOf(h, id(100, 3)).?.first.eql(id(100, 3)));
    try testing.expect(s.appendOf(h, id(100, 5)) == null);
    try testing.expectEqual(stream_mod.answerBound(.{ .id = id(100, 0), .ual_index = 1, .partition_index = 0, .record_count = 3, .byte_len = 30 }), s.appendOf(h, id(100, 2)).?.answer_bytes);
    _ = proj.trimStream(h, id(100, 0));
    try testing.expect(s.appendOf(h, id(100, 1)) == null);
}

test "streams: a trim's removed-through is its last cut append's last record, and never goes back" {
    var proj = StreamProjection.init(testing.allocator);
    defer proj.deinit();
    const h = stream_mod.streamKey(.default, "s");
    _ = try proj.appendToStreamAt(h, 1, 0, 100, 3, 30);
    _ = try proj.appendToStreamAt(h, 2, 0, 200, 1, 10);
    const s: Streams = .{ .proj = &proj };
    try testing.expect(s.removedThrough(h).eql(StreamID.MIN));

    // A bound inside the first append cuts all of it.
    _ = proj.trimStream(h, id(100, 0));
    try testing.expect(s.removedThrough(h).eql(id(100, 2)));
    try testing.expect(s.first(h).eql(id(200, 0)));

    // A lower bound, and one that cuts nothing, leave it as it was.
    _ = proj.trimStream(h, id(50, 0));
    try testing.expect(s.removedThrough(h).eql(id(100, 2)));

    // Trimmed empty: no records, and everything up to the last is gone.
    _ = proj.trimStream(h, id(200, 0));
    try testing.expect(s.removedThrough(h).eql(id(200, 0)));
    try testing.expect(s.first(h).eql(StreamID.MIN));
    var out: [4]StreamID = undefined;
    try testing.expectEqual(@as(usize, 0), s.idsAfter(h, StreamID.MIN, &out));
}
