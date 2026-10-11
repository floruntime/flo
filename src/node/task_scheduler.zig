//! TaskScheduler — cooperative periodic background tasks
//!
//! Each shard has one TaskScheduler, invoked from the Reactor loop.
//! Tasks are registered with an interval. Each `tick()` runs the due
//! tasks until the time they actually took passes the tick's budget; the
//! rest wait for the next tick. A task runs to completion: the budget
//! decides how many start, not how long one runs.
//!
//! ## Usage
//!
//! ```zig
//! var sched = TaskScheduler.init();
//! try sched.register("ttl_sweep", 1_000, ttlSweepFn, ctx);
//! // In reactor loop:
//! sched.tick(2_000_000); // 2ms total budget
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// TaskScheduler
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum number of registered tasks per shard.
pub const MAX_TASKS: usize = 32;

/// Function type for a background task.
/// Returns the number of items processed (for stats/logging).
pub const TaskFn = *const fn (ctx: *anyopaque) u64;

/// A registered periodic task.
pub const Task = struct {
    /// Human-readable name (for metrics/logging).
    name: [32]u8,
    name_len: u8,

    /// Task callback.
    callback: TaskFn,
    /// Opaque context passed to callback.
    context: *anyopaque,

    /// Interval between invocations in milliseconds.
    interval_ms: u64,

    /// When it last ran, on the monotonic clock; null until it has.
    last_run_ms: ?u64,
    /// Time it has taken, all runs together.
    elapsed_ns: u64,
    /// Total invocation count.
    run_count: u64,
    /// Total items processed (sum of callback returns).
    items_processed: u64,
    /// Whether the task is enabled.
    enabled: bool,

    pub fn nameSlice(self: *const Task) []const u8 {
        return self.name[0..self.name_len];
    }
};

pub const TaskScheduler = struct {
    tasks: [MAX_TASKS]Task,
    task_count: u8,
    total_ticks: u64,

    pub fn init() TaskScheduler {
        return .{
            .tasks = undefined,
            .task_count = 0,
            .total_ticks = 0,
        };
    }

    // No heap allocation → no deinit needed.

    /// Register a new periodic task.
    ///
    /// - `name`:        task name (max 32 bytes, truncated if longer)
    /// - `interval_ms`: how often to run (milliseconds)
    /// - `callback`:    task function
    /// - `context`:     opaque context pointer
    pub fn register(
        self: *TaskScheduler,
        name: []const u8,
        interval_ms: u64,
        callback: TaskFn,
        context: *anyopaque,
    ) error{TaskLimitReached}!void {
        if (self.task_count >= MAX_TASKS) return error.TaskLimitReached;

        var task: Task = .{
            .name = .{0} ** 32,
            .name_len = 0,
            .callback = callback,
            .context = context,
            .interval_ms = interval_ms,
            .last_run_ms = null,
            .elapsed_ns = 0,
            .run_count = 0,
            .items_processed = 0,
            .enabled = true,
        };

        const copy_len: u8 = @intCast(@min(name.len, 32));
        @memcpy(task.name[0..copy_len], name[0..copy_len]);
        task.name_len = copy_len;

        self.tasks[self.task_count] = task;
        self.task_count += 1;
    }

    /// Run the due tasks until the time they took reaches
    /// `total_budget_ns`; the rest wait for the next tick.
    ///
    /// Called from the Reactor loop on each tick. Returns the number
    /// of tasks that were executed.
    pub fn tick(self: *TaskScheduler, total_budget_ns: u64) u32 {
        const time = @import("stdx").time;
        const now_ms = time.monotonicMs();
        const start_ns = time.monotonicNs();
        var executed: u32 = 0;

        for (self.tasks[0..self.task_count]) |*task| {
            if (!task.enabled) continue;
            if (time.monotonicNs() -| start_ns >= total_budget_ns) break;
            if (task.last_run_ms) |last| {
                if (now_ms -| last < task.interval_ms) continue;
            }

            const task_start_ns = time.monotonicNs();
            const items = task.callback(task.context);
            task.elapsed_ns += time.monotonicNs() -| task_start_ns;

            task.last_run_ms = now_ms;
            task.run_count += 1;
            task.items_processed += items;
            executed += 1;
        }

        self.total_ticks += 1;
        return executed;
    }

    /// Enable or disable a task by name.
    pub fn setEnabled(self: *TaskScheduler, name: []const u8, enabled: bool) bool {
        for (self.tasks[0..self.task_count]) |*task| {
            if (std.mem.eql(u8, task.nameSlice(), name)) {
                task.enabled = enabled;
                return true;
            }
        }
        return false;
    }

    /// Get a task by name (read-only).
    pub fn getTask(self: *const TaskScheduler, name: []const u8) ?*const Task {
        for (self.tasks[0..self.task_count]) |*task| {
            if (std.mem.eql(u8, task.nameSlice(), name)) {
                return task;
            }
        }
        return null;
    }

    /// Unregister a task by name. Returns true if found and removed.
    pub fn unregister(self: *TaskScheduler, name: []const u8) bool {
        var i: u8 = 0;
        while (i < self.task_count) : (i += 1) {
            if (std.mem.eql(u8, self.tasks[i].name[0..self.tasks[i].name_len], name)) {
                // Shift remaining tasks left
                const idx: usize = i;
                if (idx + 1 < self.task_count) {
                    const count = self.task_count - i - 1;
                    const dest = self.tasks[idx + 1 .. idx + 1 + count];
                    var j: usize = 0;
                    while (j < count) : (j += 1) {
                        self.tasks[idx + j] = dest[j];
                    }
                }
                self.task_count -= 1;
                return true;
            }
        }
        return false;
    }

    /// Reset all stats (for testing / metrics reset).
    pub fn resetStats(self: *TaskScheduler) void {
        for (self.tasks[0..self.task_count]) |*task| {
            task.run_count = 0;
            task.items_processed = 0;
            task.elapsed_ns = 0;
        }
        self.total_ticks = 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const TestContext = struct {
    invocations: u32 = 0,
    /// How long each run takes.
    busy_ns: u64 = 0,
};

fn testTask(ctx_raw: *anyopaque) u64 {
    const ctx: *TestContext = @ptrCast(@alignCast(ctx_raw));
    ctx.invocations += 1;
    const time = @import("stdx").time;
    const until = time.monotonicNs() + ctx.busy_ns;
    while (time.monotonicNs() < until) {}
    return 5; // pretend we processed 5 items
}

fn testTask2(ctx_raw: *anyopaque) u64 {
    const ctx: *TestContext = @ptrCast(@alignCast(ctx_raw));
    ctx.invocations += 1;
    return 1;
}

test "TaskScheduler: init empty" {
    const sched = TaskScheduler.init();
    try std.testing.expectEqual(@as(u8, 0), sched.task_count);
    try std.testing.expectEqual(@as(u64, 0), sched.total_ticks);
}

test "TaskScheduler: register and tick" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    try sched.register("test_task", 0, testTask, @ptrCast(&ctx));
    try std.testing.expectEqual(@as(u8, 1), sched.task_count);

    // Tick with budget — task has interval 0, always due
    const executed = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 1), executed);
    try std.testing.expectEqual(@as(u32, 1), ctx.invocations);
    try std.testing.expectEqual(@as(u64, 1), sched.total_ticks);
}

test "TaskScheduler: task name stored correctly" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    try sched.register("ttl_sweep", 1000, testTask, @ptrCast(&ctx));

    const task = sched.getTask("ttl_sweep");
    try std.testing.expect(task != null);
    try std.testing.expectEqualStrings("ttl_sweep", task.?.nameSlice());
}

test "TaskScheduler: interval gating" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    // Register with a very large interval — should never fire on immediate re-tick
    try sched.register("slow_task", 999_999, testTask, @ptrCast(&ctx));

    // First tick fires (last_run_ms=0 → always overdue)
    _ = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 1), ctx.invocations);

    // Second tick immediately after — interval not elapsed
    _ = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 1), ctx.invocations);
}

test "TaskScheduler: multiple tasks" {
    var sched = TaskScheduler.init();
    var ctx1 = TestContext{};
    var ctx2 = TestContext{};

    try sched.register("task_a", 0, testTask, @ptrCast(&ctx1));
    try sched.register("task_b", 0, testTask2, @ptrCast(&ctx2));

    const executed = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 2), executed);
    try std.testing.expectEqual(@as(u32, 1), ctx1.invocations);
    try std.testing.expectEqual(@as(u32, 1), ctx2.invocations);
}

test "TaskScheduler: disable and enable" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    try sched.register("toggleable", 0, testTask, @ptrCast(&ctx));

    // Disable
    try std.testing.expect(sched.setEnabled("toggleable", false));

    _ = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 0), ctx.invocations);

    // Re-enable
    try std.testing.expect(sched.setEnabled("toggleable", true));

    _ = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 1), ctx.invocations);
}

test "TaskScheduler: unregister" {
    var sched = TaskScheduler.init();
    var ctx1 = TestContext{};
    var ctx2 = TestContext{};

    try sched.register("keep", 0, testTask, @ptrCast(&ctx1));
    try sched.register("remove", 0, testTask2, @ptrCast(&ctx2));

    try std.testing.expectEqual(@as(u8, 2), sched.task_count);

    // Remove the second
    try std.testing.expect(sched.unregister("remove"));
    try std.testing.expectEqual(@as(u8, 1), sched.task_count);

    // Tick — only first remains
    _ = sched.tick(10_000_000);
    try std.testing.expectEqual(@as(u32, 1), ctx1.invocations);
    try std.testing.expectEqual(@as(u32, 0), ctx2.invocations);
}

test "TaskScheduler: items_processed accumulates" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    try sched.register("counting", 0, testTask, @ptrCast(&ctx));

    _ = sched.tick(10_000_000);
    _ = sched.tick(10_000_000); // last_run_ms just set, but interval=0

    // First tick always runs. Second tick: interval=0 means it's always overdue in
    // the same ms, but elapsed might be 0 which is still >= 0, so runs again.
    const task = sched.getTask("counting");
    try std.testing.expect(task != null);
    try std.testing.expect(task.?.items_processed >= 5); // at least 1 run * 5 items
}

test "TaskScheduler: max tasks limit" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    var i: usize = 0;
    while (i < MAX_TASKS) : (i += 1) {
        try sched.register("t", 0, testTask, @ptrCast(&ctx));
    }

    // One more should fail
    try std.testing.expectError(
        error.TaskLimitReached,
        sched.register("overflow", 0, testTask, @ptrCast(&ctx)),
    );
}

test "TaskScheduler: resetStats" {
    var sched = TaskScheduler.init();
    var ctx = TestContext{};

    try sched.register("stat_task", 0, testTask, @ptrCast(&ctx));

    _ = sched.tick(10_000_000);
    try std.testing.expect(sched.total_ticks > 0);

    sched.resetStats();
    try std.testing.expectEqual(@as(u64, 0), sched.total_ticks);

    const task = sched.getTask("stat_task");
    try std.testing.expectEqual(@as(u64, 0), task.?.run_count);
    try std.testing.expectEqual(@as(u64, 0), task.?.items_processed);
}

test "TaskScheduler: a tick charges what tasks took, so a slow one holds the rest for the next tick" {
    var sched = TaskScheduler.init();
    var slow = TestContext{ .busy_ns = 3 * std.time.ns_per_ms };
    var quick = TestContext{};
    try sched.register("slow", 999_999, testTask, @ptrCast(&slow));
    try sched.register("quick", 999_999, testTask, @ptrCast(&quick));

    // The slow one takes more than the whole 2ms: the quick one waits.
    try std.testing.expectEqual(@as(u32, 1), sched.tick(2_000_000));
    try std.testing.expectEqual(@as(u32, 1), slow.invocations);
    try std.testing.expectEqual(@as(u32, 0), quick.invocations);
    try std.testing.expect(sched.getTask("slow").?.elapsed_ns >= 3 * std.time.ns_per_ms);

    // Next tick: the slow one is not due, the quick one runs.
    try std.testing.expectEqual(@as(u32, 1), sched.tick(2_000_000));
    try std.testing.expectEqual(@as(u32, 1), quick.invocations);
    try std.testing.expectEqual(@as(u32, 1), slow.invocations);
}

test "TaskScheduler: tasks that take little all run in one tick" {
    var sched = TaskScheduler.init();
    var ctxs: [4]TestContext = @splat(.{});
    for (&ctxs) |*c| try sched.register("t", 999_999, testTask, @ptrCast(c));
    try std.testing.expectEqual(@as(u32, 4), sched.tick(2_000_000));
}
