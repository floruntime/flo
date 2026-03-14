//! Actions WASM Runner
//!
//! Thin wrapper over the centralized WASM runner (`src/wasm/runner.zig`)
//! that adds per-shard concurrency control for action execution.
//!
//! The core WASM runtime (zware Store, host functions, module loading,
//! execution) lives in the shared runner. This module adds:
//! - ActionWasmConfig with concurrency limits
//! - tryAcquire()/release() semaphore
//!
//! ## Usage
//!
//! ```zig
//! var runner = try ActionWasmRunner.init(allocator);
//! defer runner.deinit();
//!
//! var module = try runner.loadModule(wasm_bytes, .{});
//! defer module.deinit();
//!
//! if (!runner.tryAcquire()) return error.ConcurrencyLimitReached;
//! defer runner.release();
//!
//! var result = try runner.execute(&module, raw_input_bytes);
//! defer result.deinit();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

// Re-export core types from centralized runner
const wasm = @import("../wasm/runner.zig");
pub const ExecutionResult = wasm.ExecutionResult;
pub const WasmKvContext = wasm.WasmKvContext;
pub const KvDispatchFn = wasm.KvDispatchFn;
pub const KvOp = wasm.KvOp;
pub const KvResult = wasm.KvResult;
pub const ActionWasmModule = wasm.WasmModule;

// Re-export ABI constants
pub const GUEST_ERROR_INVALID_INPUT = wasm.GUEST_ERROR_INVALID_INPUT;
pub const GUEST_ERROR_ALLOC_FAILED = wasm.GUEST_ERROR_ALLOC_FAILED;
pub const GUEST_ERROR_EXECUTION = wasm.GUEST_ERROR_EXECUTION;
pub const RC_SUCCESS = wasm.RC_SUCCESS;
pub const RC_NOT_FOUND = wasm.RC_NOT_FOUND;
pub const RC_BUFFER_TOO_SMALL = wasm.RC_BUFFER_TOO_SMALL;
pub const RC_STATE_ERROR = wasm.RC_STATE_ERROR;
pub const RC_INVALID_ARGS = wasm.RC_INVALID_ARGS;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for WASM action execution sandbox.
/// Extends the core WasmConfig with concurrency limits.
pub const ActionWasmConfig = struct {
    /// Maximum WASM linear memory in 64KB pages.
    max_memory_pages: u32 = 256,

    /// Maximum output size in bytes.
    max_output_size: u32 = 1024 * 1024,

    /// Action name (for logging)
    action_name: []const u8 = "wasm-action",

    /// Maximum concurrent WASM executions per shard.
    max_concurrent_executions: u32 = 4,

    /// Whether KV host functions are enabled for this action.
    enable_kv_access: bool = true,

    pub const default: ActionWasmConfig = .{};

    pub const testing_minimal: ActionWasmConfig = .{
        .max_memory_pages = 32,
        .max_output_size = 64 * 1024,
        .action_name = "wasm-test",
    };

    pub fn maxMemoryBytes(self: ActionWasmConfig) u64 {
        return @as(u64, self.max_memory_pages) * 65536;
    }

    /// Convert to core WasmConfig for the shared runner.
    pub fn toWasmConfig(self: ActionWasmConfig) wasm.WasmConfig {
        return .{
            .max_memory_pages = self.max_memory_pages,
            .max_output_size = self.max_output_size,
            .module_name = self.action_name,
            .enable_kv_access = self.enable_kv_access,
        };
    }
};

// =============================================================================
// ActionWasmRunner — Per-shard WASM execution engine for actions
// =============================================================================

/// Per-shard WASM runtime for action execution.
///
/// Wraps the centralized WasmRunner with a per-shard concurrency
/// semaphore. Each shard has one runner instance.
pub const ActionWasmRunner = struct {
    runner: wasm.WasmRunner,
    /// Number of currently executing WASM actions on this shard
    active_executions: u32 = 0,
    /// Maximum concurrent executions
    max_concurrent: u32 = 4,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        return initWithConfig(allocator, ActionWasmConfig.default);
    }

    pub fn initWithConfig(allocator: Allocator, config: ActionWasmConfig) !Self {
        return Self{
            .runner = try wasm.WasmRunner.init(allocator),
            .max_concurrent = config.max_concurrent_executions,
        };
    }

    pub fn deinit(self: *Self) void {
        self.runner.deinit();
    }

    /// Whether host functions are registered (delegates to core runner).
    pub fn hostFunctionsRegistered(self: *const Self) bool {
        return self.runner.host_functions_registered;
    }

    // =========================================================================
    // Concurrency Control
    // =========================================================================

    pub fn tryAcquire(self: *Self) bool {
        if (self.active_executions >= self.max_concurrent) return false;
        self.active_executions += 1;
        return true;
    }

    pub fn release(self: *Self) void {
        self.active_executions -= 1;
    }

    pub fn activeCount(self: *const Self) u32 {
        return self.active_executions;
    }

    pub fn availableSlots(self: *const Self) u32 {
        return self.max_concurrent - self.active_executions;
    }

    // =========================================================================
    // Module Loading & Execution (delegates to core runner)
    // =========================================================================

    pub fn loadModule(self: *Self, wasm_bytes: []const u8, config: ActionWasmConfig) !ActionWasmModule {
        return self.runner.loadModule(wasm_bytes, config.toWasmConfig());
    }

    pub fn execute(self: *Self, wasm_module: *ActionWasmModule, input: []const u8) !ExecutionResult {
        return self.runner.execute(wasm_module, input);
    }

    pub fn executeWithKv(self: *Self, wasm_module: *ActionWasmModule, input: []const u8, kv_ctx: ?*WasmKvContext) !ExecutionResult {
        return self.runner.executeWithKv(wasm_module, input, kv_ctx);
    }

    pub fn describe(self: *Self, wasm_module: *ActionWasmModule) !?[]u8 {
        return self.runner.describe(wasm_module);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ActionWasmConfig defaults" {
    const config = ActionWasmConfig.default;
    try std.testing.expectEqual(@as(u32, 256), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 256 * 65536), config.maxMemoryBytes());
}

test "ActionWasmConfig testing minimal" {
    const config = ActionWasmConfig.testing_minimal;
    try std.testing.expectEqual(@as(u32, 32), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 32 * 65536), config.maxMemoryBytes());
}

test "ActionWasmRunner init and deinit" {
    var runner = try ActionWasmRunner.init(std.testing.allocator);
    defer runner.deinit();
    try std.testing.expect(runner.hostFunctionsRegistered());
}

test "ActionWasmRunner load and execute rules engine" {
    const allocator = std.testing.allocator;

    // Read the pre-built rules engine WASM module
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Verify required exports
    try std.testing.expect(module.hasExport("handle"));
    try std.testing.expect(module.hasExport("alloc"));
    try std.testing.expect(module.hasExport("dealloc"));

    // Verify optional exports
    try std.testing.expect(module.has_init);
    try std.testing.expect(module.has_describe);
}

test "ActionWasmRunner execute eligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age > 18, country == "US" → eligible
    var result = try runner.execute(&module, "{\"age\": 25, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute ineligible - age" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age <= 18 → not eligible
    var result = try runner.execute(&module, "{\"age\": 15, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute ineligible - country" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: country != "US" → not eligible
    var result = try runner.execute(&module, "{\"age\": 25, \"country\": \"UK\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute fully ineligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age <= 18, country != "US"
    var result = try runner.execute(&module, "{\"age\": 10, \"country\": \"UK\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        result.outputStr(),
    );
}

test "ActionWasmRunner describe" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    const desc = try runner.describe(&module) orelse return error.NoDescription;
    defer allocator.free(desc);

    // Should contain the module name
    try std.testing.expect(std.mem.indexOf(u8, desc, "rules-engine") != null);
    try std.testing.expect(std.mem.indexOf(u8, desc, "1.0") != null);
}

test "ActionWasmRunner multiple executions on same module" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Execute multiple times (each gets fresh instance)
    const inputs = [_][]const u8{
        "{\"age\": 25, \"country\": \"US\"}",
        "{\"age\": 10, \"country\": \"UK\"}",
        "{\"age\": 30, \"country\": \"US\"}",
        "{\"age\": 18, \"country\": \"US\"}", // age == 18, NOT > 18
    };

    const expected = [_][]const u8{
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}", // 18 is not > 18
    };

    for (inputs, expected) |input, exp| {
        var result = try runner.execute(&module, input);
        defer result.deinit();
        try std.testing.expectEqualStrings(exp, result.outputStr());
    }
}

// =============================================================================
// Concurrency control tests
// =============================================================================

test "ActionWasmRunner concurrency semaphore" {
    var runner = try ActionWasmRunner.init(std.testing.allocator);
    defer runner.deinit();

    // Default max is 4
    try std.testing.expectEqual(@as(u32, 0), runner.activeCount());
    try std.testing.expectEqual(@as(u32, 4), runner.availableSlots());

    // Acquire 4 slots
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());

    // 5th should fail
    try std.testing.expect(!runner.tryAcquire());
    try std.testing.expectEqual(@as(u32, 4), runner.activeCount());
    try std.testing.expectEqual(@as(u32, 0), runner.availableSlots());

    // Release one and try again
    runner.release();
    try std.testing.expectEqual(@as(u32, 3), runner.activeCount());
    try std.testing.expect(runner.tryAcquire());

    // Release all
    runner.release();
    runner.release();
    runner.release();
    runner.release();
    try std.testing.expectEqual(@as(u32, 0), runner.activeCount());
}

test "ActionWasmRunner custom concurrency limit" {
    var runner = try ActionWasmRunner.initWithConfig(std.testing.allocator, .{
        .max_concurrent_executions = 2,
    });
    defer runner.deinit();

    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(!runner.tryAcquire()); // 3rd fails with limit=2

    runner.release();
    runner.release();
}

test "WasmKvContext round-trip" {
    var ctx = WasmKvContext{
        .namespace = "test-ns",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = std.testing.allocator,
        .kv_enabled = true,
    };
    const as_usize = ctx.toContext();
    const recovered = WasmKvContext.fromContext(as_usize);
    try std.testing.expectEqualStrings("test-ns", recovered.namespace);
    try std.testing.expect(recovered.kv_enabled);
    try std.testing.expect(recovered.kv_dispatch_fn == null);
}

test "ActionWasmRunner execute with binary input" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Binary input that happens to be valid JSON — runner doesn't care about format
    const binary_input = "{\"age\": 25, \"country\": \"US\"}";
    var result = try runner.execute(&module, binary_input);
    defer result.deinit();

    // Output is also just bytes — we interpret it as JSON here because the guest does
    try std.testing.expect(result.output.len > 0);
}
