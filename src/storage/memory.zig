//! Shard Memory Controller
//!
//! Enforces per-shard memory budgets across all components (UAL, projections,
//! I/O buffers, snapshots). Each component registers with a budget and an
//! optional eviction callback. When a component approaches its budget, the
//! controller signals eviction or applies backpressure.
//!
//! Escalation levels:
//! 1. Eviction signal — ask component to free memory
//! 2. Dynamic rebalance — borrow from reserve pool
//! 3. Client backpressure — return ShardMemoryPressure error
//! 4. Hard limit — reject immediately

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Component identifiers for memory budget tracking.
pub const ComponentId = enum(u8) {
    ual_hot = 0,
    kv_projection = 1,
    queue_projection = 2,
    ts_projection = 3,
    io_buffers = 4,
    snapshot_buffer = 5,
    warm_store = 6,

    pub const COUNT: usize = 7;
};

/// Eviction callback — asks a component to free `target_bytes`, returns actual freed.
pub const EvictFn = *const fn (ctx: *anyopaque, target_bytes: usize) usize;

/// Memory usage reporter — returns current memory usage for a component.
pub const UsageFn = *const fn (ctx: *anyopaque) usize;

/// Per-component memory allocation tracking.
pub const Allocation = struct {
    budget: usize,
    used: usize,
    high_watermark: f32, // trigger eviction above this (fraction of budget)
    low_watermark: f32, // evict down to this level (fraction of budget)
    peak_used: usize, // highest usage observed

    evict_fn: ?EvictFn,
    evict_ctx: ?*anyopaque,
    usage_fn: ?UsageFn,
    usage_ctx: ?*anyopaque,

    /// Returns the high watermark threshold in bytes.
    pub fn highThreshold(self: *const Allocation) usize {
        return @intFromFloat(@as(f32, @floatFromInt(self.budget)) * self.high_watermark);
    }

    /// Returns the low watermark threshold in bytes.
    pub fn lowThreshold(self: *const Allocation) usize {
        return @intFromFloat(@as(f32, @floatFromInt(self.budget)) * self.low_watermark);
    }

    /// Returns available bytes within budget.
    pub fn available(self: *const Allocation) usize {
        if (self.used >= self.budget) return 0;
        return self.budget - self.used;
    }

    /// Returns usage as a fraction of budget (0.0–1.0+).
    pub fn usageFraction(self: *const Allocation) f32 {
        if (self.budget == 0) return 0.0;
        return @as(f32, @floatFromInt(self.used)) / @as(f32, @floatFromInt(self.budget));
    }
};

const DEFAULT_HIGH_WATERMARK: f32 = 0.85;
const DEFAULT_LOW_WATERMARK: f32 = 0.70;

// ═══════════════════════════════════════════════════════════════════════════════
// MemoryController
// ═══════════════════════════════════════════════════════════════════════════════

pub const MemoryController = struct {
    /// Total shard memory budget (all components + reserve).
    total_budget: usize,

    /// Per-component allocations.
    allocations: [ComponentId.COUNT]Allocation,

    /// Reserve pool — available for temporary rebalancing.
    reserve_total: usize,
    reserve_used: usize,

    /// Statistics.
    total_evictions: u64,
    total_bytes_evicted: u64,
    total_reserve_borrows: u64,
    total_pressure_events: u64,

    // ── Construction ────────────────────────────────────────────────────

    /// Initialize with a total shard budget and default component allocations.
    /// Uses the default budget split from the design doc.
    pub fn init(total_budget: usize) MemoryController {
        return initWithBudgets(total_budget, defaultBudgets(total_budget));
    }

    /// Initialize with explicit per-component budgets.
    pub fn initWithBudgets(total_budget: usize, budgets: [ComponentId.COUNT]usize) MemoryController {
        var allocs: [ComponentId.COUNT]Allocation = undefined;
        var allocated: usize = 0;

        for (0..ComponentId.COUNT) |i| {
            allocs[i] = .{
                .budget = budgets[i],
                .used = 0,
                .high_watermark = DEFAULT_HIGH_WATERMARK,
                .low_watermark = DEFAULT_LOW_WATERMARK,
                .peak_used = 0,
                .evict_fn = null,
                .evict_ctx = null,
                .usage_fn = null,
                .usage_ctx = null,
            };
            allocated += budgets[i];
        }

        const reserve = if (total_budget > allocated) total_budget - allocated else 0;

        return .{
            .total_budget = total_budget,
            .allocations = allocs,
            .reserve_total = reserve,
            .reserve_used = 0,
            .total_evictions = 0,
            .total_bytes_evicted = 0,
            .total_reserve_borrows = 0,
            .total_pressure_events = 0,
        };
    }

    /// Default budget split (design doc percentages).
    pub fn defaultBudgets(total: usize) [ComponentId.COUNT]usize {
        return .{
            total / 8, // ual_hot: 12.5%
            total * 3 / 8, // kv_projection: 37.5%
            total / 16, // queue_projection: 6.25%
            total / 8, // ts_projection: 12.5%
            total / 16, // io_buffers: 6.25%
            total / 32, // snapshot_buffer: 3.125%
            total / 16, // warm_store: 6.25%
        };
    }

    // ── Registration ────────────────────────────────────────────────────

    /// Register an eviction callback for a component.
    pub fn registerEviction(self: *MemoryController, id: ComponentId, ctx: *anyopaque, evict_fn: EvictFn) void {
        self.allocations[@intFromEnum(id)].evict_fn = evict_fn;
        self.allocations[@intFromEnum(id)].evict_ctx = ctx;
    }

    /// Register a usage reporter for a component.
    pub fn registerUsage(self: *MemoryController, id: ComponentId, ctx: *anyopaque, usage_fn: UsageFn) void {
        self.allocations[@intFromEnum(id)].usage_fn = usage_fn;
        self.allocations[@intFromEnum(id)].usage_ctx = ctx;
    }

    /// Set custom watermarks for a component.
    pub fn setWatermarks(self: *MemoryController, id: ComponentId, high: f32, low: f32) void {
        self.allocations[@intFromEnum(id)].high_watermark = high;
        self.allocations[@intFromEnum(id)].low_watermark = low;
    }

    // ── Memory Operations ───────────────────────────────────────────────

    /// Request memory for a component. Returns error if budget exceeded
    /// and eviction + reserve cannot accommodate.
    pub fn requestMemory(self: *MemoryController, id: ComponentId, bytes: usize) error{ShardMemoryPressure}!void {
        const idx = @intFromEnum(id);
        var alloc = &self.allocations[idx];

        // Level 0: fits within budget
        if (alloc.used + bytes <= alloc.budget) {
            alloc.used += bytes;
            if (alloc.used > alloc.peak_used) alloc.peak_used = alloc.used;
            return;
        }

        // Level 1: trigger eviction with 10% headroom
        if (alloc.evict_fn) |evict| {
            const overshoot = (alloc.used + bytes) - alloc.budget;
            const headroom = alloc.budget / 10;
            const target = overshoot + headroom;
            const freed = evict(alloc.evict_ctx.?, target);
            if (freed >= overshoot) {
                alloc.used = if (alloc.used >= freed) alloc.used - freed else 0;
                self.total_evictions += 1;
                self.total_bytes_evicted += freed;
            }
        }

        // Check again after eviction
        if (alloc.used + bytes <= alloc.budget) {
            alloc.used += bytes;
            if (alloc.used > alloc.peak_used) alloc.peak_used = alloc.used;
            return;
        }

        // Level 2: borrow from reserve
        const needed = (alloc.used + bytes) - alloc.budget;
        const reserve_avail = self.reserve_total - self.reserve_used;
        if (needed <= reserve_avail) {
            self.reserve_used += needed;
            self.total_reserve_borrows += 1;
            alloc.used += bytes;
            if (alloc.used > alloc.peak_used) alloc.peak_used = alloc.used;
            return;
        }

        // Level 3: backpressure
        self.total_pressure_events += 1;
        return error.ShardMemoryPressure;
    }

    /// Release memory back from a component.
    pub fn releaseMemory(self: *MemoryController, id: ComponentId, bytes: usize) void {
        const idx = @intFromEnum(id);
        var alloc = &self.allocations[idx];

        const actual = @min(bytes, alloc.used);
        alloc.used -= actual;

        // Return reserve borrows if usage dropped below budget
        if (alloc.used < alloc.budget and self.reserve_used > 0) {
            const can_return = alloc.budget - alloc.used;
            const to_return = @min(can_return, self.reserve_used);
            _ = to_return;
            // Note: we don't return partial reserve here because the reserve
            // borrow tracking is aggregate. Reserve is recovered when usage
            // drops below budget naturally.
        }
    }

    /// Check if a component is under pressure (above high watermark).
    pub fn isUnderPressure(self: *const MemoryController, id: ComponentId) bool {
        const alloc = &self.allocations[@intFromEnum(id)];
        return alloc.used > alloc.highThreshold();
    }

    // ── Pressure Check (periodic task) ──────────────────────────────────

    /// Run a pressure check across all components. Triggers eviction for
    /// any component above its high watermark. Called periodically by the
    /// TaskScheduler (every 5 seconds).
    pub fn checkPressure(self: *MemoryController) void {
        for (0..ComponentId.COUNT) |i| {
            var alloc = &self.allocations[i];

            // Sync with usage reporter if registered
            if (alloc.usage_fn) |usage| {
                alloc.used = usage(alloc.usage_ctx.?);
            }

            if (alloc.used > alloc.peak_used) {
                alloc.peak_used = alloc.used;
            }

            // Trigger eviction if above high watermark
            if (alloc.used > alloc.highThreshold()) {
                if (alloc.evict_fn) |evict| {
                    const target = alloc.used - alloc.lowThreshold();
                    const freed = evict(alloc.evict_ctx.?, target);
                    if (freed > 0) {
                        alloc.used = if (alloc.used >= freed) alloc.used - freed else 0;
                        self.total_evictions += 1;
                        self.total_bytes_evicted += freed;
                    }
                }
            }
        }

        // Recalculate reserve usage
        var over_budget: usize = 0;
        for (0..ComponentId.COUNT) |i| {
            if (self.allocations[i].used > self.allocations[i].budget) {
                over_budget += self.allocations[i].used - self.allocations[i].budget;
            }
        }
        self.reserve_used = @min(over_budget, self.reserve_total);
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Get allocation info for a component.
    pub fn getAllocation(self: *const MemoryController, id: ComponentId) *const Allocation {
        return &self.allocations[@intFromEnum(id)];
    }

    /// Total memory used across all components.
    pub fn totalUsed(self: *const MemoryController) usize {
        var total: usize = 0;
        for (0..ComponentId.COUNT) |i| {
            total += self.allocations[i].used;
        }
        return total;
    }

    /// Total memory available (budget minus used, excluding reserve).
    pub fn totalAvailable(self: *const MemoryController) usize {
        var total: usize = 0;
        for (0..ComponentId.COUNT) |i| {
            total += self.allocations[i].available();
        }
        return total;
    }

    /// Reserve pool available.
    pub fn reserveAvailable(self: *const MemoryController) usize {
        return self.reserve_total - self.reserve_used;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Mock projection for testing eviction callbacks.
const MockProjection = struct {
    allocated: usize,
    evict_calls: u32,
    last_evict_target: usize,

    fn init(initial: usize) MockProjection {
        return .{
            .allocated = initial,
            .evict_calls = 0,
            .last_evict_target = 0,
        };
    }

    fn evict(ctx: *anyopaque, target_bytes: usize) usize {
        const self: *MockProjection = @ptrCast(@alignCast(ctx));
        self.evict_calls += 1;
        self.last_evict_target = target_bytes;
        // Free up to target or all allocated
        const freed = @min(target_bytes, self.allocated);
        self.allocated -= freed;
        return freed;
    }

    fn usage(ctx: *anyopaque) usize {
        const self: *const MockProjection = @ptrCast(@alignCast(ctx));
        return self.allocated;
    }
};

test "memory controller: init with defaults" {
    const total: usize = 2 * 1024 * 1024 * 1024; // 2 GB
    const mc = MemoryController.init(total);

    try testing.expectEqual(total, mc.total_budget);
    try testing.expectEqual(@as(usize, 0), mc.totalUsed());

    // Check default budgets match design doc percentages
    const ual_alloc = mc.getAllocation(.ual_hot);
    try testing.expectEqual(total / 8, ual_alloc.budget); // 12.5%

    const kv_alloc = mc.getAllocation(.kv_projection);
    try testing.expectEqual(total * 3 / 8, kv_alloc.budget); // 37.5%

    // Reserve should be non-zero (total - sum of component budgets)
    try testing.expect(mc.reserve_total > 0);
}

test "memory controller: request and release within budget" {
    var mc = MemoryController.init(1024 * 1024); // 1 MB

    const budget = mc.getAllocation(.kv_projection).budget;

    // Request half the budget
    try mc.requestMemory(.kv_projection, budget / 2);
    try testing.expectEqual(budget / 2, mc.getAllocation(.kv_projection).used);

    // Release it
    mc.releaseMemory(.kv_projection, budget / 2);
    try testing.expectEqual(@as(usize, 0), mc.getAllocation(.kv_projection).used);
}

test "memory controller: peak tracking" {
    var mc = MemoryController.init(1024 * 1024);

    const budget = mc.getAllocation(.ual_hot).budget;

    try mc.requestMemory(.ual_hot, budget / 4);
    try testing.expectEqual(budget / 4, mc.getAllocation(.ual_hot).peak_used);

    try mc.requestMemory(.ual_hot, budget / 4);
    try testing.expectEqual(budget / 2, mc.getAllocation(.ual_hot).peak_used);

    mc.releaseMemory(.ual_hot, budget / 4);
    // Peak should not decrease
    try testing.expectEqual(budget / 2, mc.getAllocation(.ual_hot).peak_used);
}

test "memory controller: eviction triggered when budget exceeded" {
    var mc = MemoryController.initWithBudgets(1000, .{ 200, 200, 200, 200, 100, 100, 0 });

    var mock = MockProjection.init(150);
    mc.registerEviction(.kv_projection, @ptrCast(&mock), MockProjection.evict);

    // Fill up the KV budget
    try mc.requestMemory(.kv_projection, 180);

    // Request more than budget — should trigger eviction
    try mc.requestMemory(.kv_projection, 50);

    try testing.expect(mock.evict_calls > 0);
    try testing.expect(mc.total_evictions > 0);
}

test "memory controller: reserve borrow when eviction insufficient" {
    // No eviction callback registered → goes straight to reserve
    var mc = MemoryController.initWithBudgets(1000, .{ 100, 100, 100, 100, 100, 100, 0 });
    // Reserve = 1000 - 600 = 400

    try testing.expectEqual(@as(usize, 400), mc.reserve_total);

    // Fill KV budget
    try mc.requestMemory(.kv_projection, 100);

    // Exceed budget by 50 — should borrow from reserve
    try mc.requestMemory(.kv_projection, 50);
    try testing.expectEqual(@as(usize, 150), mc.getAllocation(.kv_projection).used);
    try testing.expect(mc.reserve_used > 0);
    try testing.expect(mc.total_reserve_borrows > 0);
}

test "memory controller: pressure error when reserve exhausted" {
    var mc = MemoryController.initWithBudgets(200, .{ 50, 50, 50, 50, 0, 0, 0 });
    // Reserve = 200 - 200 = 0

    try testing.expectEqual(@as(usize, 0), mc.reserve_total);

    // Fill budget
    try mc.requestMemory(.kv_projection, 50);

    // Exceed — no reserve available
    const result = mc.requestMemory(.kv_projection, 10);
    try testing.expectError(error.ShardMemoryPressure, result);
    try testing.expectEqual(@as(u64, 1), mc.total_pressure_events);
}

test "memory controller: checkPressure with usage reporter" {
    var mc = MemoryController.initWithBudgets(1000, .{ 200, 200, 200, 200, 100, 100, 0 });

    var mock = MockProjection.init(180); // Just below high watermark (0.85 * 200 = 170)
    mc.registerUsage(.kv_projection, @ptrCast(&mock), MockProjection.usage);
    mc.registerEviction(.kv_projection, @ptrCast(&mock), MockProjection.evict);

    // First check: 180 > 170 (high watermark), should trigger eviction
    mc.checkPressure();

    // Usage was synced from reporter
    try testing.expect(mock.evict_calls > 0);
    try testing.expect(mc.total_evictions > 0);
}

test "memory controller: checkPressure no eviction below watermark" {
    var mc = MemoryController.initWithBudgets(1000, .{ 200, 200, 200, 200, 100, 100, 0 });

    var mock = MockProjection.init(100); // Well below high watermark (170)
    mc.registerUsage(.kv_projection, @ptrCast(&mock), MockProjection.usage);
    mc.registerEviction(.kv_projection, @ptrCast(&mock), MockProjection.evict);

    mc.checkPressure();

    try testing.expectEqual(@as(u32, 0), mock.evict_calls);
    try testing.expectEqual(@as(u64, 0), mc.total_evictions);
}

test "memory controller: watermark calculations" {
    const alloc = Allocation{
        .budget = 1000,
        .used = 0,
        .high_watermark = 0.85,
        .low_watermark = 0.70,
        .peak_used = 0,
        .evict_fn = null,
        .evict_ctx = null,
        .usage_fn = null,
        .usage_ctx = null,
    };

    try testing.expectEqual(@as(usize, 850), alloc.highThreshold());
    try testing.expectEqual(@as(usize, 700), alloc.lowThreshold());
    try testing.expectEqual(@as(usize, 1000), alloc.available());
    try testing.expectEqual(@as(f32, 0.0), alloc.usageFraction());
}

test "memory controller: custom watermarks" {
    var mc = MemoryController.initWithBudgets(1000, .{ 200, 200, 200, 200, 100, 100, 0 });

    mc.setWatermarks(.kv_projection, 0.90, 0.60);

    const alloc = mc.getAllocation(.kv_projection);
    try testing.expectEqual(@as(f32, 0.90), alloc.high_watermark);
    try testing.expectEqual(@as(f32, 0.60), alloc.low_watermark);
    try testing.expectEqual(@as(usize, 180), alloc.highThreshold()); // 0.9 * 200
    try testing.expectEqual(@as(usize, 120), alloc.lowThreshold()); // 0.6 * 200
}

test "memory controller: multiple components" {
    var mc = MemoryController.initWithBudgets(1000, .{ 200, 200, 200, 200, 100, 100, 0 });

    try mc.requestMemory(.ual_hot, 50);
    try mc.requestMemory(.kv_projection, 75);
    try mc.requestMemory(.queue_projection, 20);

    try testing.expectEqual(@as(usize, 145), mc.totalUsed());

    mc.releaseMemory(.kv_projection, 25);
    try testing.expectEqual(@as(usize, 120), mc.totalUsed());
}

test "memory controller: eviction frees exact amount" {
    var mc = MemoryController.initWithBudgets(500, .{ 100, 100, 100, 100, 50, 50, 0 });

    var mock = MockProjection.init(80);
    mc.registerEviction(.ual_hot, @ptrCast(&mock), MockProjection.evict);

    // Fill to 90
    try mc.requestMemory(.ual_hot, 90);

    // Request 20 more — exceeds budget of 100 by 10
    // Eviction should be triggered. Target = overshoot(10) + headroom(10) = 20
    try mc.requestMemory(.ual_hot, 20);

    try testing.expectEqual(@as(u32, 1), mock.evict_calls);
    // Mock freed the requested target from its internal allocation
    try testing.expect(mc.total_bytes_evicted > 0);
}

test "memory controller: release more than used clamps to zero" {
    var mc = MemoryController.initWithBudgets(500, .{ 100, 100, 100, 100, 50, 50, 0 });

    try mc.requestMemory(.io_buffers, 30);
    mc.releaseMemory(.io_buffers, 100); // Release more than allocated

    try testing.expectEqual(@as(usize, 0), mc.getAllocation(.io_buffers).used);
}
