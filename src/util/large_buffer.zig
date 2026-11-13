//! Buffers of a mebibyte or more, mapped straight from the OS.
//!
//! In a safe build `Allocator.alloc` fills every new allocation with
//! `undefined`, written as 0xAA, so every page is touched as it is
//! allocated. A shard's log rings (80 MiB at the default sizes) were then
//! resident from the start, though an idle shard writes almost none of it.
//! Mapped here instead, a buffer's pages are touched only as they are
//! written: an idle shard holds what it uses, and a ring grows with use, up
//! to its size.
//!
//! The pages read as zeros until written, not 0xAA. Nothing reads a ring's
//! bytes before writing them: a ring is read only where an entry was
//! appended, every entry read is checked against its CRC, and nothing takes
//! 0xAA to mean "not written".
//!
//! Smaller buffers go to the caller's allocator as before, and each is freed
//! the way it was allocated, which its size decides.
//!
//! A mapped buffer bypasses the caller's allocator, so a test's leak check
//! doesn't see one that is never freed. Debug builds count the bytes mapped
//! (`mapped_bytes`), and the shard's init-and-deinit test checks they all
//! come back.

const std = @import("std");
const builtin = @import("builtin");

/// From this size up, a buffer is mapped.
pub const MIN_BYTES: usize = 1024 * 1024;

const counted = builtin.mode == .debug;

/// Bytes mapped and not yet freed, in a debug build; 0 otherwise.
pub var mapped_bytes: std.atomic.Value(usize) = .init(0);

pub fn alloc(allocator: std.mem.Allocator, comptime T: type, n: usize) error{OutOfMemory}![]T {
    const bytes = std.math.mul(usize, n, @sizeOf(T)) catch return error.OutOfMemory;
    if (bytes < MIN_BYTES) return allocator.alloc(T, n);
    return mapped(T, n);
}

pub fn free(allocator: std.mem.Allocator, comptime T: type, buf: []T) void {
    if (buf.len * @sizeOf(T) < MIN_BYTES) return allocator.free(buf);
    unmap(T, buf);
}

/// Mapped whatever its size, for a buffer a shard holds from init but may
/// never write (a request's parse buffer, the slab's chunks): its pages,
/// rounded up to whole ones, cost nothing until used. Freed by `unmap`.
pub fn mapped(comptime T: type, n: usize) error{OutOfMemory}![]T {
    const bytes = std.math.mul(usize, n, @sizeOf(T)) catch return error.OutOfMemory;
    // `rawAlloc`, not `alloc`: the interface's `alloc` is what fills.
    const ptr = std.heap.page_allocator.rawAlloc(bytes, .of(T), @returnAddress()) orelse return error.OutOfMemory;
    if (counted) _ = mapped_bytes.fetchAdd(bytes, .monotonic);
    const typed: [*]T = @ptrCast(@alignCast(ptr));
    return typed[0..n];
}

pub fn unmap(comptime T: type, buf: []T) void {
    std.heap.page_allocator.rawFree(std.mem.sliceAsBytes(buf), .of(T), @returnAddress());
    if (counted) _ = mapped_bytes.fetchSub(buf.len * @sizeOf(T), .monotonic);
}

test "large buffer: a mapped buffer reads zeros, not the safe-mode fill, and a small one is the allocator's" {
    const before = mapped_bytes.load(.monotonic);
    defer std.testing.expectEqual(before, mapped_bytes.load(.monotonic)) catch @panic("a mapped buffer was not freed");
    // A size that overflows is refused, not wrapped.
    try std.testing.expectError(error.OutOfMemory, alloc(std.testing.allocator, u64, std.math.maxInt(usize) / 4));
    const big = try alloc(std.testing.allocator, u8, MIN_BYTES);
    defer free(std.testing.allocator, u8, big);
    try std.testing.expectEqual(@as(u8, 0), big[0]);
    try std.testing.expectEqual(@as(u8, 0), big[big.len - 1]);
    big[12345] = 7;
    try std.testing.expectEqual(@as(u8, 7), big[12345]);
    // Under the testing allocator a small one is tracked: a missed free leaks.
    const small = try alloc(std.testing.allocator, u8, 4096);
    free(std.testing.allocator, u8, small);
    // Typed, aligned for its element.
    const Wide = struct { a: u64, b: u32 };
    const wide = try alloc(std.testing.allocator, Wide, MIN_BYTES / @sizeOf(Wide) + 1);
    defer free(std.testing.allocator, Wide, wide);
    try std.testing.expect(@intFromPtr(wide.ptr) % @alignOf(Wide) == 0);
}
