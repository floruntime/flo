//! What a machine may read of its shard's streams while it applies an
//! entry: a stream's bounds and its records' ids, from the streams
//! machine's families (`stream/machine.zig`) as committed before the entry.
//! It reads the committed store, not the entry's view, so neither the
//! entry's staged writes nor its declared keys apply to it: a machine that
//! reads a stream still declares those keys (`stream.meta`, a
//! `stream.record` range) as any read, and nothing checks the reads against
//! them. This handle has no way to write.

const std = @import("std");
const stream_mod = @import("../projection/stream.zig");
const sm = @import("../stream/machine.zig");
const view_mod = @import("view.zig");

pub const StreamID = stream_mod.StreamID;

pub const Streams = struct {
    read: view_mod.ReadView,

    fn meta(self: Streams, key: stream_mod.StreamKey) ?sm.Meta.Value {
        const m = sm.metaAt(self.read, key) orelse return null;
        return if (m.exists) m else null;
    }

    /// The stream's first record, or MIN when it holds none.
    pub fn first(self: Streams, key: stream_mod.StreamKey) StreamID {
        const m = self.meta(key) orelse return StreamID.MIN;
        var it = sm.Appends.init(self.read, key, m, StreamID.MIN);
        return if (it.next()) |rec| rec.id else StreamID.MIN;
    }

    /// The stream's last record, or MIN when it holds none.
    pub fn last(self: Streams, key: stream_mod.StreamKey) StreamID {
        const m = self.meta(key) orelse return StreamID.MIN;
        if (sm.countAbove(self.read, key, m, sm.floorOf(m)).records == 0) return StreamID.MIN;
        return sm.lastOf(m);
    }

    /// The last record a trim removed, or MIN. Never lowered while the
    /// stream exists: every record at or before it is gone.
    pub fn removedThrough(self: Streams, key: stream_mod.StreamKey) StreamID {
        const m = self.meta(key) orelse return StreamID.MIN;
        return sm.floorOf(m);
    }

    /// The append that holds `record`, by its first record, and the
    /// most its records take in an answer; null when the stream no longer
    /// holds it.
    pub fn appendOf(self: Streams, key: stream_mod.StreamKey, record: StreamID) ?Append {
        const m = self.meta(key) orelse return null;
        const rec = sm.appendOf(self.read, key, m, record) orelse return null;
        return .{ .first = rec.id, .answer_bytes = stream_mod.answerBound(rec) };
    }

    pub const Append = struct { first: StreamID, answer_bytes: usize };

    /// The ids of the records after `cursor`, oldest first, one per
    /// record, at most `out.len`.
    pub fn idsAfter(self: Streams, key: stream_mod.StreamKey, cursor: StreamID, out: []StreamID) usize {
        const m = self.meta(key) orelse return 0;
        var it = sm.Appends.init(self.read, key, m, cursor);
        var n: usize = 0;
        while (n < out.len) {
            const rec = it.next() orelse break;
            var k: u32 = 0;
            while (k < rec.record_count and n < out.len) : (k += 1) {
                const id: StreamID = .{ .timestamp_ms = rec.id.timestamp_ms, .sequence = rec.id.sequence + k };
                if (!id.greaterThan(cursor)) continue;
                out[n] = id;
                n += 1;
            }
        }
        return n;
    }
};

const testing = std.testing;

fn sid(ts: u64, seq: u64) StreamID {
    return .{ .timestamp_ms = ts, .sequence = seq };
}

test "streams: ids after a cursor are one per record, a cursor inside an append resuming in it" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    const h = stream_mod.streamKey(.default, "s");
    // Two appends in one millisecond: three records, then two.
    _ = try t.append("s", 100, 3, 4);
    _ = try t.append("s", 100, 2, 4);
    const s = t.streams();
    var out: [8]StreamID = undefined;

    const all = s.idsAfter(h, StreamID.MIN, &out);
    try testing.expectEqual(@as(usize, 5), all);
    for (out[0..all], 0..) |got, i| try testing.expect(got.eql(sid(100, i)));

    const rest = s.idsAfter(h, sid(100, 1), &out);
    try testing.expectEqual(@as(usize, 3), rest);
    try testing.expect(out[0].eql(sid(100, 2)));
    try testing.expect(out[2].eql(sid(100, 4)));

    try testing.expectEqual(@as(usize, 2), s.idsAfter(h, StreamID.MIN, out[0..2]));
    try testing.expectEqual(@as(usize, 0), s.idsAfter(h, sid(100, 4), &out));
    try testing.expectEqual(@as(usize, 0), s.idsAfter(stream_mod.streamKey(.default, "other"), StreamID.MIN, &out));
    try testing.expect(s.first(h).eql(sid(100, 0)));
    try testing.expect(s.last(h).eql(sid(100, 4)));
}

test "streams: each record names its append, until a trim cuts it" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    const h = stream_mod.streamKey(.default, "s");
    _ = try t.append("s", 100, 3, 4);
    _ = try t.append("s", 100, 2, 4);
    const s = t.streams();
    try testing.expect(s.appendOf(h, sid(100, 1)).?.first.eql(sid(100, 0)));
    try testing.expect(s.appendOf(h, sid(100, 3)).?.first.eql(sid(100, 3)));
    try testing.expect(s.appendOf(h, sid(100, 5)) == null);
    try testing.expectEqual(stream_mod.answerBound(.{ .id = sid(100, 0), .ual_index = 1, .partition_index = 0, .record_count = 3, .byte_len = 34 }), s.appendOf(h, sid(100, 2)).?.answer_bytes);
    _ = try t.trim("s", .{ .id = sid(100, 0) });
    try testing.expect(s.appendOf(h, sid(100, 1)) == null);
}

test "streams: a trim's removed-through is its last cut append's last record, and never goes back" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    const h = stream_mod.streamKey(.default, "s");
    _ = try t.append("s", 100, 3, 4);
    _ = try t.append("s", 200, 1, 4);
    const s = t.streams();
    try testing.expect(s.removedThrough(h).eql(StreamID.MIN));

    // A bound inside the first append cuts all of it.
    _ = try t.trim("s", .{ .id = sid(100, 0) });
    try testing.expect(s.removedThrough(h).eql(sid(100, 2)));
    try testing.expect(s.first(h).eql(sid(200, 0)));

    // A lower bound, and one that cuts nothing, leave it as it was.
    _ = try t.trim("s", .{ .id = sid(50, 0) });
    try testing.expect(s.removedThrough(h).eql(sid(100, 2)));

    // Trimmed empty: no records, and everything up to the last is gone.
    _ = try t.trim("s", .{ .id = sid(200, 0) });
    try testing.expect(s.removedThrough(h).eql(sid(200, 0)));
    try testing.expect(s.first(h).eql(StreamID.MIN));
    try testing.expect(s.last(h).eql(StreamID.MIN));
    var out: [4]StreamID = undefined;
    try testing.expectEqual(@as(usize, 0), s.idsAfter(h, StreamID.MIN, &out));
}
