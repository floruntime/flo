//! A shard's scratch buffer: one allocation, borrowed whole by one caller at
//! a time and handed back before anything else can want it.
//!
//! A large buffer declared `undefined` on the stack is filled with 0xAA on
//! every call in a safe build, so per-request stack buffers of a few hundred
//! KiB cost a memset each. A shard owns this one instead, allocated once,
//! and only its own thread uses it.
//!
//! Rule: borrow, use, release, with no call in between that could borrow
//! again. In safe builds a second borrow, or a release of anything else,
//! fails at once rather than hand out a buffer someone is still using.

const std = @import("std");

pub const Scratch = struct {
    buf: []u8,
    /// Set from borrow to release.
    held: bool = false,

    pub fn init(allocator: std.mem.Allocator, size: usize) !Scratch {
        return .{ .buf = try allocator.alloc(u8, size) };
    }

    pub fn deinit(self: *Scratch, allocator: std.mem.Allocator) void {
        std.debug.assert(!self.held);
        allocator.free(self.buf);
        self.* = undefined;
    }

    /// The first `n` bytes, until `release`. `n` is at most the capacity.
    pub fn borrow(self: *Scratch, n: usize) []u8 {
        std.debug.assert(!self.held);
        std.debug.assert(n <= self.buf.len);
        self.held = true;
        return self.buf[0..n];
    }

    pub fn release(self: *Scratch, borrowed: []const u8) void {
        std.debug.assert(self.held);
        std.debug.assert(borrowed.ptr == self.buf.ptr);
        self.held = false;
    }

    pub fn capacity(self: *const Scratch) usize {
        return self.buf.len;
    }
};

test "scratch: borrowed and released in turn, the same bytes each time" {
    var s = try Scratch.init(std.testing.allocator, 64);
    defer s.deinit(std.testing.allocator);
    const a = s.borrow(10);
    try std.testing.expectEqual(@as(usize, 10), a.len);
    a[0] = 7;
    s.release(a);
    const b = s.borrow(64);
    try std.testing.expectEqual(a.ptr, b.ptr);
    try std.testing.expectEqual(@as(u8, 7), b[0]);
    s.release(b);
    try std.testing.expect(!s.held);
}
