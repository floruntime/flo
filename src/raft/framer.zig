//! Streaming framer for one peer link. Bytes arrive in whatever pieces TCP
//! delivers them; frames come out whole, or not at all. Every frame is
//! checked before it is handed on — length bound, known type, checksum,
//! and the source id the link was authenticated for; a frame that fails
//! any of these closes the link.

const std = @import("std");
const transport = @import("transport.zig");

const RaftHeader = transport.RaftHeader;
const HEADER_SIZE = transport.HEADER_SIZE;

pub const MAX_FRAME_SIZE: usize = HEADER_SIZE + transport.MAX_PAYLOAD_SIZE;

pub const Frame = struct {
    header: RaftHeader,
    msg_type: transport.MsgType,
    /// Borrowed from the framer until `advance`.
    payload: []const u8,
};

pub const Error = error{
    /// `payload_len` above what a member may send.
    Oversize,
    /// A `msg_type` this build does not speak.
    UnknownType,
    /// The checksum over header and payload does not match.
    BadCrc,
    /// `source_node` is not the id this link authenticated as.
    SourceMismatch,
};

pub const Framer = struct {
    allocator: std.mem.Allocator,
    buf: []u8,
    len: usize = 0,
    /// Size of the frame `next` last returned, taken off by `advance`.
    pending: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !Framer {
        return .{ .allocator = allocator, .buf = try allocator.alloc(u8, MAX_FRAME_SIZE) };
    }

    pub fn deinit(self: *Framer) void {
        self.allocator.free(self.buf);
        // Inert after free: `space`, `next` and `advance` on a retained
        // pointer see an empty buffer, not freed memory.
        self.buf = &.{};
        self.len = 0;
        self.pending = 0;
    }

    /// Where the next read goes. Empty only when a whole frame sits
    /// undrained: a frame always fits, so a full buffer holds one.
    pub fn space(self: *Framer) []u8 {
        return self.buf[self.len..];
    }

    pub fn commit(self: *Framer, n: usize) void {
        self.len += n;
    }

    /// The next whole frame, validated against `expected_source`, or null
    /// when more bytes are needed. Call `advance` before the next call.
    pub fn next(self: *Framer, expected_source: u32) Error!?Frame {
        std.debug.assert(self.pending == 0);
        if (self.len < HEADER_SIZE) return null;
        const hdr = RaftHeader.fromBytes(self.buf[0..HEADER_SIZE]).*;
        if (hdr.payload_len > transport.MAX_PAYLOAD_SIZE) return error.Oversize;
        const msg_type = hdr.msgType() orelse return error.UnknownType;
        if (hdr.source_node != expected_source) return error.SourceMismatch;
        const size = HEADER_SIZE + @as(usize, hdr.payload_len);
        if (self.len < size) return null;
        const payload = self.buf[HEADER_SIZE..size];
        if (transport.computeCrc(self.buf[0..HEADER_SIZE], payload) != hdr.crc32) return error.BadCrc;
        self.pending = size;
        return .{ .header = hdr, .msg_type = msg_type, .payload = payload };
    }

    /// Drop the frame `next` returned and pull what follows to the front.
    pub fn advance(self: *Framer) void {
        const size = self.pending;
        self.pending = 0;
        if (size == 0) return;
        if (size < self.len) std.mem.copyForwards(u8, self.buf, self.buf[size..self.len]);
        self.len -= size;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn frameInto(buf: []u8, msg_type: transport.MsgType, source: u32, payload: []const u8) usize {
    return transport.frameMessage(msg_type, 0, source, payload, buf);
}

test "framer: a frame split across reads comes out whole, and two in one read come out in order" {
    var f = try Framer.init(testing.allocator);
    defer f.deinit();
    var wire: [256]u8 = undefined;
    const a = frameInto(&wire, .replicate_entry, 7, "hello");
    const b = frameInto(wire[a..], .peer_info, 7, "0123456789");
    const total = a + b;

    // Three bytes at a time.
    var fed: usize = 0;
    var got: usize = 0;
    while (fed < total) {
        const n = @min(3, total - fed);
        @memcpy(f.space()[0..n], wire[fed .. fed + n]);
        f.commit(n);
        fed += n;
        while (try f.next(7)) |fr| {
            got += 1;
            if (got == 1) {
                try testing.expectEqual(transport.MsgType.replicate_entry, fr.msg_type);
                try testing.expectEqualStrings("hello", fr.payload);
            } else {
                try testing.expectEqualStrings("0123456789", fr.payload);
            }
            f.advance();
        }
    }
    try testing.expectEqual(@as(usize, 2), got);
    try testing.expectEqual(@as(usize, 0), f.len);
}

test "framer: a frame that exactly fills the buffer is delivered" {
    var f = try Framer.init(testing.allocator);
    defer f.deinit();
    const payload = try testing.allocator.alloc(u8, transport.MAX_PAYLOAD_SIZE);
    defer testing.allocator.free(payload);
    @memset(payload, 0xab);
    const n = frameInto(f.space(), .replicate_entry, 3, payload);
    try testing.expectEqual(MAX_FRAME_SIZE, n);
    f.commit(n);
    const fr = (try f.next(3)).?;
    try testing.expectEqual(transport.MAX_PAYLOAD_SIZE, fr.payload.len);
    f.advance();
    try testing.expectEqual(@as(usize, 0), f.len);
}

test "framer: oversize, unknown type, wrong source and bad checksum are refused" {
    var f = try Framer.init(testing.allocator);
    defer f.deinit();

    // Oversize is refused from the header alone, before the payload arrives.
    var hdr = RaftHeader{ .msg_type = @intFromEnum(transport.MsgType.replicate_entry), ._pad = .{ 0, 0, 0 }, .group_id = 0, .source_node = 1, .payload_len = @intCast(transport.MAX_PAYLOAD_SIZE + 1), .crc32 = 0 };
    @memcpy(f.space()[0..HEADER_SIZE], hdr.asBytes());
    f.commit(HEADER_SIZE);
    try testing.expectError(error.Oversize, f.next(1));
    f.len = 0;

    hdr.payload_len = 0;
    hdr.msg_type = 200;
    @memcpy(f.space()[0..HEADER_SIZE], hdr.asBytes());
    f.commit(HEADER_SIZE);
    try testing.expectError(error.UnknownType, f.next(1));
    f.len = 0;

    var wire: [64]u8 = undefined;
    const n = frameInto(&wire, .replicate_entry, 1, "x");
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.SourceMismatch, f.next(2));
    f.len = 0;

    wire[n - 1] ^= 0xff; // last payload byte
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.BadCrc, f.next(1));
}
