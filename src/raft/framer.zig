//! Streaming framer for one peer link. Bytes arrive in whatever pieces TCP
//! delivers them; frames come out whole, or not at all. Every frame is
//! checked before it is handed on — length bound, known type, the source id
//! the link was authenticated for, and its seal (`seal.zig`); a frame that
//! fails any of these closes the link.

const std = @import("std");
const transport = @import("transport.zig");
const seal = @import("seal.zig");

const RaftHeader = transport.RaftHeader;
const HEADER_SIZE = transport.HEADER_SIZE;

pub const MAX_FRAME_SIZE: usize = HEADER_SIZE + transport.MAX_PAYLOAD_SIZE + seal.TAG_LEN;

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
    /// The frame does not open under the link's key: altered, replayed,
    /// reordered, or a frame before it went missing.
    BadSeal,
    /// `source_node` is not the id this link authenticated as.
    SourceMismatch,
};

pub const Framer = struct {
    allocator: std.mem.Allocator,
    buf: []u8,
    len: usize = 0,
    /// Size of the frame `next` last returned, taken off by `advance`.
    pending: usize = 0,
    /// The key frames from the far side open under.
    key: seal.Key,

    pub fn init(allocator: std.mem.Allocator, key: seal.Key) !Framer {
        return .{ .allocator = allocator, .buf = try allocator.alloc(u8, MAX_FRAME_SIZE), .key = key };
    }

    pub fn deinit(self: *Framer) void {
        self.key.wipe();
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

    /// Bytes that arrived with the end of the handshake, ahead of any read.
    pub fn preload(self: *Framer, bytes: []const u8) void {
        @memcpy(self.buf[self.len..][0..bytes.len], bytes);
        self.len += bytes.len;
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
        const len: usize = hdr.payload_len;
        const size = HEADER_SIZE + len + seal.TAG_LEN;
        if (self.len < size) return null;
        self.key.open(self.buf[0..HEADER_SIZE], self.buf[HEADER_SIZE..size]) catch return error.BadSeal;
        self.pending = size;
        return .{ .header = hdr, .msg_type = msg_type, .payload = self.buf[HEADER_SIZE..][0..len] };
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

/// A key pair as the two ends of one link would hold it.
fn keys() struct { send: seal.Key, recv: seal.Key } {
    var t: seal.Transcript = .{};
    t.add("test handshake");
    const d = t.peek();
    return .{ .send = seal.Key.derive("s", &d, .dialer_to_acceptor), .recv = seal.Key.derive("s", &d, .dialer_to_acceptor) };
}

/// Frame and seal `payload` into `buf` as a sender holding `key` would.
fn frameInto(buf: []u8, key: *seal.Key, msg_type: transport.MsgType, source: u32, payload: []const u8) usize {
    var hdr = RaftHeader{ .msg_type = @intFromEnum(msg_type), ._pad = .{ 0, 0, 0 }, .group_id = 0, .source_node = source, .payload_len = @intCast(payload.len), .crc32 = 0 };
    @memcpy(buf[0..HEADER_SIZE], hdr.asBytes());
    key.seal(buf[0..HEADER_SIZE], payload, buf[HEADER_SIZE..][0 .. payload.len + seal.TAG_LEN]);
    return HEADER_SIZE + payload.len + seal.TAG_LEN;
}

test "framer: a frame split across reads comes out whole, and two in one read come out in order" {
    var k = keys();
    var f = try Framer.init(testing.allocator, k.recv);
    defer f.deinit();
    var wire: [256]u8 = undefined;
    const a = frameInto(&wire, &k.send, .append_entries, 7, "hello");
    const b = frameInto(wire[a..], &k.send, .peer_info, 7, "0123456789");
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
                try testing.expectEqual(transport.MsgType.append_entries, fr.msg_type);
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
    var k = keys();
    var f = try Framer.init(testing.allocator, k.recv);
    defer f.deinit();
    const payload = try testing.allocator.alloc(u8, transport.MAX_PAYLOAD_SIZE);
    defer testing.allocator.free(payload);
    @memset(payload, 0xab);
    const n = frameInto(f.space(), &k.send, .append_entries, 3, payload);
    try testing.expectEqual(MAX_FRAME_SIZE, n);
    f.commit(n);
    const fr = (try f.next(3)).?;
    try testing.expectEqual(transport.MAX_PAYLOAD_SIZE, fr.payload.len);
    try testing.expectEqual(@as(u8, 0xab), fr.payload[fr.payload.len - 1]);
    f.advance();
    try testing.expectEqual(@as(usize, 0), f.len);
}

test "framer: oversize, unknown type, wrong source, and a frame altered, replayed or after a gap are refused" {
    var k = keys();
    var f = try Framer.init(testing.allocator, k.recv);
    defer f.deinit();

    // Oversize is refused from the header alone, before the payload arrives.
    var hdr = RaftHeader{ .msg_type = @intFromEnum(transport.MsgType.append_entries), ._pad = .{ 0, 0, 0 }, .group_id = 0, .source_node = 1, .payload_len = @intCast(transport.MAX_PAYLOAD_SIZE + 1), .crc32 = 0 };
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
    var n = frameInto(&wire, &k.send, .append_entries, 1, "x");
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.SourceMismatch, f.next(2));
    f.len = 0;

    // Altered: the payload, or the header's group id.
    wire[HEADER_SIZE] ^= 0xff;
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.BadSeal, f.next(1));

    k = keys();
    f.key = k.recv;
    f.len = 0;
    n = frameInto(&wire, &k.send, .append_entries, 1, "x");
    wire[4] ^= 1;
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.BadSeal, f.next(1));

    // Replayed: the same frame twice.
    k = keys();
    f.key = k.recv;
    f.len = 0;
    n = frameInto(&wire, &k.send, .append_entries, 1, "x");
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    _ = (try f.next(1)).?;
    f.advance();
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.BadSeal, f.next(1));

    // After a gap: one frame never arrives.
    k = keys();
    f.key = k.recv;
    f.len = 0;
    _ = frameInto(&wire, &k.send, .append_entries, 1, "lost");
    n = frameInto(&wire, &k.send, .append_entries, 1, "x");
    @memcpy(f.space()[0..n], wire[0..n]);
    f.commit(n);
    try testing.expectError(error.BadSeal, f.next(1));
}
