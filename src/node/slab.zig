//! Slab Allocator — per-shard fixed-size allocator for cross-core payloads
//!
//! Cross-shard messages carry slab-allocated payloads to avoid hot-path
//! malloc/free. Each shard owns its own SlabAllocator — no locks needed
//! for the owning shard. The receiver shard frees via `free()` after
//! processing (ownership transfers across the inbox).
//!
//! ## Size Classes
//!
//! | Class | Size  | Typical Use                       |
//! |-------|-------|-----------------------------------|
//! | 0     | 64 B  | Small KV gets, acks               |
//! | 1     | 256 B | Medium keys, short values          |
//! | 2     | 1 KB  | Typical KV puts, queue messages    |
//! | 3     | 4 KB  | Large values, batch responses      |
//! | 4     | 64 KB | Bulk transfers, scan results       |
//!
//! Allocations larger than 64 KB fall through to the backing allocator.
//!
//! ## Usage
//!
//! ```zig
//! var slab = try SlabAllocator.init(allocator);
//! defer slab.deinit();
//!
//! const ref = try slab.alloc(128);  // gets 256B slab
//! const buf = ref.slice();          // []u8 of requested size
//! @memcpy(buf, payload);
//! // ... send via inbox ...
//! slab.free(ref);                   // return to pool
//! ```

const std = @import("std");

/// Reference to a slab-allocated buffer. Carries metadata for deallocation.
pub const SlabRef = struct {
    /// Pointer to the allocated memory
    ptr: [*]u8,
    /// Requested size (may be smaller than the slab class size)
    len: usize,
    /// Index of the size class pool (or FALLBACK for oversized)
    class: u8,

    pub const FALLBACK: u8 = 0xFF;

    /// Return a slice of the requested length.
    pub fn slice(self: SlabRef) []u8 {
        return self.ptr[0..self.len];
    }

    /// Return the raw pointer as *anyopaque (for Inbox Message.payload_ptr).
    pub fn toOpaque(self: SlabRef) *anyopaque {
        return @ptrCast(self.ptr);
    }
};

/// Number of size classes
const POOL_COUNT = 5;

/// Size class definitions (bytes)
const CLASS_SIZES = [POOL_COUNT]usize{ 64, 256, 1024, 4096, 65536 };

/// Default initial slabs per class
const INITIAL_SLABS = [POOL_COUNT]usize{ 256, 128, 64, 32, 8 };

/// A free-list pool for a single size class.
const Pool = struct {
    /// Stack of free buffers
    free_list: std.ArrayListUnmanaged([*]u8),
    /// All allocated chunks (for cleanup)
    all_chunks: std.ArrayListUnmanaged([*]u8),
    /// Size of each slab in this pool
    slab_size: usize,

    fn init(allocator: std.mem.Allocator, slab_size: usize, initial_count: usize) !Pool {
        var pool = Pool{
            .free_list = .{},
            .all_chunks = .{},
            .slab_size = slab_size,
        };

        // Pre-allocate initial slabs
        try pool.free_list.ensureTotalCapacity(allocator, initial_count);
        try pool.all_chunks.ensureTotalCapacity(allocator, initial_count);

        for (0..initial_count) |_| {
            const buf = try allocator.alloc(u8, slab_size);
            const ptr: [*]u8 = buf.ptr;
            pool.free_list.appendAssumeCapacity(ptr);
            pool.all_chunks.appendAssumeCapacity(ptr);
        }

        return pool;
    }

    fn deinit(self: *Pool, allocator: std.mem.Allocator) void {
        // Free all chunks (both in-use and free)
        for (self.all_chunks.items) |chunk| {
            allocator.free(chunk[0..self.slab_size]);
        }
        self.all_chunks.deinit(allocator);
        self.free_list.deinit(allocator);
    }

    fn pop(self: *Pool) ?[*]u8 {
        if (self.free_list.items.len == 0) return null;
        return self.free_list.pop();
    }

    fn push(self: *Pool, allocator: std.mem.Allocator, ptr: [*]u8) void {
        self.free_list.append(allocator, ptr) catch {
            // If we can't grow the free list, just leak it.
            // This is extremely unlikely given we pre-allocate.
        };
    }

    /// Grow pool by allocating a new slab
    fn grow(self: *Pool, allocator: std.mem.Allocator) ![*]u8 {
        const buf = try allocator.alloc(u8, self.slab_size);
        const ptr: [*]u8 = buf.ptr;
        try self.all_chunks.append(allocator, ptr);
        return ptr;
    }
};

/// Per-shard slab allocator with fixed-size classes.
///
/// NOT thread-safe — designed for single-shard ownership.
/// Cross-shard free is safe because the receiver calls free()
/// on its own slab (the allocator that originally allocated it).
/// Actually, cross-shard payloads transfer ownership: the sender
/// allocates, the receiver frees on the *sender's* slab. Since
/// free just pushes to a free-list, and the owning shard is the
/// only one touching its own pools, this is safe as long as
/// free() is called from the owning shard's thread.
///
/// For true cross-shard free (receiver deallocates on sender's slab),
/// we'd need atomic free-lists. For now, the design assumes the
/// owning shard drains inbox and frees payloads it originally allocated.
pub const SlabAllocator = struct {
    pools: [POOL_COUNT]Pool,
    allocator: std.mem.Allocator,

    /// Track fallback allocations for proper cleanup
    fallback_allocs: std.ArrayListUnmanaged(FallbackEntry),

    const FallbackEntry = struct {
        ptr: [*]u8,
        len: usize,
    };

    pub fn init(allocator: std.mem.Allocator) !SlabAllocator {
        var pools: [POOL_COUNT]Pool = undefined;
        var initialized: usize = 0;
        errdefer {
            for (0..initialized) |i| {
                pools[i].deinit(allocator);
            }
        }

        for (0..POOL_COUNT) |i| {
            pools[i] = try Pool.init(allocator, CLASS_SIZES[i], INITIAL_SLABS[i]);
            initialized += 1;
        }

        return .{
            .pools = pools,
            .allocator = allocator,
            .fallback_allocs = .{},
        };
    }

    pub fn deinit(self: *SlabAllocator) void {
        // Free fallback allocations
        for (self.fallback_allocs.items) |entry| {
            self.allocator.free(entry.ptr[0..entry.len]);
        }
        self.fallback_allocs.deinit(self.allocator);

        // Free all pools
        for (&self.pools) |*pool| {
            pool.deinit(self.allocator);
        }
    }

    /// Allocate a buffer of at least `size` bytes.
    /// Returns a SlabRef that must be freed with `free()`.
    pub fn alloc(self: *SlabAllocator, size: usize) !SlabRef {
        // Find smallest class that fits
        for (0..POOL_COUNT) |i| {
            if (CLASS_SIZES[i] >= size) {
                const ptr = self.pools[i].pop() orelse try self.pools[i].grow(self.allocator);
                return .{
                    .ptr = ptr,
                    .len = size,
                    .class = @intCast(i),
                };
            }
        }

        // Oversized — fall through to backing allocator
        const buf = try self.allocator.alloc(u8, size);
        try self.fallback_allocs.append(self.allocator, .{
            .ptr = buf.ptr,
            .len = size,
        });
        return .{
            .ptr = buf.ptr,
            .len = size,
            .class = SlabRef.FALLBACK,
        };
    }

    /// Return a slab to its pool (or free oversized allocation).
    pub fn free(self: *SlabAllocator, ref: SlabRef) void {
        if (ref.class == SlabRef.FALLBACK) {
            // Remove from fallback tracking and free
            for (self.fallback_allocs.items, 0..) |entry, idx| {
                if (entry.ptr == ref.ptr) {
                    _ = self.fallback_allocs.swapRemove(idx);
                    break;
                }
            }
            self.allocator.free(ref.ptr[0..ref.len]);
            return;
        }

        // Return to pool
        self.pools[ref.class].push(self.allocator, ref.ptr);
    }

    /// Returns the size class used for a given allocation size.
    /// Returns null if it would use the fallback allocator.
    pub fn classForSize(size: usize) ?u8 {
        for (0..POOL_COUNT) |i| {
            if (CLASS_SIZES[i] >= size) return @intCast(i);
        }
        return null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Slab: alloc and free small" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    const ref = try slab.alloc(32);
    try std.testing.expectEqual(@as(u8, 0), ref.class); // 64B class
    try std.testing.expectEqual(@as(usize, 32), ref.len);

    // Write to the buffer
    const buf = ref.slice();
    @memset(buf, 0xAB);
    try std.testing.expectEqual(@as(u8, 0xAB), buf[0]);
    try std.testing.expectEqual(@as(u8, 0xAB), buf[31]);

    slab.free(ref);
}

test "Slab: size class selection" {
    try std.testing.expectEqual(@as(?u8, 0), SlabAllocator.classForSize(1));
    try std.testing.expectEqual(@as(?u8, 0), SlabAllocator.classForSize(64));
    try std.testing.expectEqual(@as(?u8, 1), SlabAllocator.classForSize(65));
    try std.testing.expectEqual(@as(?u8, 1), SlabAllocator.classForSize(256));
    try std.testing.expectEqual(@as(?u8, 2), SlabAllocator.classForSize(257));
    try std.testing.expectEqual(@as(?u8, 2), SlabAllocator.classForSize(1024));
    try std.testing.expectEqual(@as(?u8, 3), SlabAllocator.classForSize(1025));
    try std.testing.expectEqual(@as(?u8, 3), SlabAllocator.classForSize(4096));
    try std.testing.expectEqual(@as(?u8, 4), SlabAllocator.classForSize(4097));
    try std.testing.expectEqual(@as(?u8, 4), SlabAllocator.classForSize(65536));
    try std.testing.expectEqual(@as(?u8, null), SlabAllocator.classForSize(65537));
}

test "Slab: alloc each size class" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    const sizes = [_]usize{ 32, 128, 512, 2048, 32768 };
    var refs: [5]SlabRef = undefined;

    for (sizes, 0..) |size, i| {
        refs[i] = try slab.alloc(size);
        try std.testing.expectEqual(@as(u8, @intCast(i)), refs[i].class);
        try std.testing.expectEqual(size, refs[i].len);
    }

    // Free in reverse order
    var i: usize = 5;
    while (i > 0) {
        i -= 1;
        slab.free(refs[i]);
    }
}

test "Slab: fallback for oversized" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    const ref = try slab.alloc(100_000); // > 64KB
    try std.testing.expectEqual(SlabRef.FALLBACK, ref.class);
    try std.testing.expectEqual(@as(usize, 100_000), ref.len);

    // Write and verify
    const buf = ref.slice();
    @memset(buf, 0xCD);
    try std.testing.expectEqual(@as(u8, 0xCD), buf[99_999]);

    slab.free(ref);
}

test "Slab: pool reuse after free" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    // Alloc and free, then alloc again — should reuse
    const ref1 = try slab.alloc(100);
    const ptr1 = ref1.ptr;
    slab.free(ref1);

    const ref2 = try slab.alloc(100);
    // Same size class, should get the same buffer back (LIFO free-list)
    try std.testing.expectEqual(ptr1, ref2.ptr);
    slab.free(ref2);
}

test "Slab: pool growth" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    // Exhaust initial pool (256 slabs for 64B class) and force growth
    var refs: [260]SlabRef = undefined;
    for (0..260) |i| {
        refs[i] = try slab.alloc(64);
    }

    // Free all
    for (0..260) |i| {
        slab.free(refs[i]);
    }
}

test "Slab: opaque pointer for inbox" {
    var slab = try SlabAllocator.init(std.testing.allocator);
    defer slab.deinit();

    const ref = try slab.alloc(128);
    defer slab.free(ref);

    const opq = ref.toOpaque();
    // Round-trip through *anyopaque
    const recovered: [*]u8 = @ptrCast(@alignCast(opq));
    try std.testing.expectEqual(ref.ptr, recovered);
}
