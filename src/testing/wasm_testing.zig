//! WASM Action Testing Utilities
//!
//! Provides helpers for testing WASM actions in unit tests and e2e tests.
//! Includes pre-built demo WASM modules and assertion helpers.
//!
//! ## Usage
//!
//! ```zig
//! const wasm_testing = @import("wasm_testing.zig");
//! const WasmTestFixture = wasm_testing.WasmTestFixture;
//!
//! test "my wasm action" {
//!     var fixture = try WasmTestFixture.init(std.testing.allocator);
//!     defer fixture.deinit();
//!
//!     // Execute the built-in rules engine
//!     var result = try fixture.executeRulesEngine("{\"age\": 25, \"country\": \"US\"}");
//!     defer result.deinit();
//!
//!     try fixture.assertEligible(result, true);
//! }
//! ```

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

const wasm_runner = @import("../actions/wasm_runner.zig");
const ActionWasmRunner = wasm_runner.ActionWasmRunner;
const ActionWasmModule = wasm_runner.ActionWasmModule;
const ActionWasmConfig = wasm_runner.ActionWasmConfig;
const ExecutionResult = wasm_runner.ExecutionResult;

// =============================================================================
// Embedded WASM modules (pre-built for testing)
// =============================================================================

/// Pre-built rules engine WASM module bytes.
/// Built from wasm/actions-demo/rules_engine.zig
pub const rules_engine_wasm = @embedFile("../actions/testdata/rules_engine.wasm");

// =============================================================================
// WasmTestFixture — Reusable test fixture for WASM actions
// =============================================================================

/// A test fixture that provides a ready-to-use WASM runner with
/// pre-loaded demo modules. Handles lifecycle management automatically.
pub const WasmTestFixture = struct {
    allocator: Allocator,
    runner: ActionWasmRunner,
    rules_module: ?ActionWasmModule,

    const Self = @This();

    /// Initialize the fixture with a WASM runner.
    pub fn init(allocator: Allocator) !Self {
        return Self{
            .allocator = allocator,
            .runner = try ActionWasmRunner.init(allocator),
            .rules_module = null,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.rules_module != null) {
            self.rules_module.?.deinit();
        }
        self.runner.deinit();
    }

    /// Get or load the rules engine module (lazy loaded, cached).
    pub fn getRulesModule(self: *Self) !*ActionWasmModule {
        if (self.rules_module == null) {
            self.rules_module = try self.runner.loadModule(rules_engine_wasm, ActionWasmConfig.testing_minimal);
        }
        return &self.rules_module.?;
    }

    /// Execute the rules engine with the given JSON input.
    pub fn executeRulesEngine(self: *Self, input: []const u8) !ExecutionResult {
        const module = try self.getRulesModule();
        return self.runner.execute(module, input);
    }

    /// Execute any WASM bytes with the given input.
    pub fn executeWasm(self: *Self, wasm_bytes: []const u8, input: []const u8) !ExecutionResult {
        var module = try self.runner.loadModule(wasm_bytes, .{});
        defer module.deinit();
        return self.runner.execute(&module, input);
    }

    /// Load a custom WASM module with custom config.
    pub fn loadModule(self: *Self, wasm_bytes: []const u8, config: ActionWasmConfig) !ActionWasmModule {
        return self.runner.loadModule(wasm_bytes, config);
    }

    // =========================================================================
    // Assertion Helpers
    // =========================================================================

    /// Assert the output JSON contains "eligible": true or false
    pub fn assertEligible(self: *Self, result: ExecutionResult, expected: bool) !void {
        _ = self;
        const output = result.outputStr();
        const needle = if (expected) "\"eligible\":true" else "\"eligible\":false";
        if (std.mem.indexOf(u8, output, needle) == null) {
            std.debug.print("\n[WASM ASSERT] Expected eligible={}, got: {s}\n", .{ expected, output });
            return error.AssertionFailed;
        }
    }

    /// Assert the output JSON contains a specific key:value pair (string match).
    pub fn assertOutputContains(_: *Self, result: ExecutionResult, needle: []const u8) !void {
        const output = result.outputStr();
        if (std.mem.indexOf(u8, output, needle) == null) {
            std.debug.print("\n[WASM ASSERT] Expected output to contain '{s}', got: {s}\n", .{ needle, output });
            return error.AssertionFailed;
        }
    }

    /// Assert the output exactly matches the expected string.
    pub fn assertOutputEquals(_: *Self, result: ExecutionResult, expected: []const u8) !void {
        try testing.expectEqualStrings(expected, result.outputStr());
    }

    /// Assert execution failed with a specific error.
    pub fn assertExecutionFails(self: *Self, input: []const u8, expected_err: anyerror) !void {
        const module = try self.getRulesModule();
        const result = self.runner.execute(module, input);
        if (result) |_| {
            std.debug.print("\n[WASM ASSERT] Expected error {}, but execution succeeded\n", .{expected_err});
            return error.AssertionFailed;
        } else |err| {
            try testing.expectEqual(expected_err, err);
        }
    }
};

// =============================================================================
// Helper: Build test input JSON
// =============================================================================

/// Build a test input JSON string for the rules engine.
/// Caller owns the returned slice.
pub fn buildRulesInput(allocator: Allocator, age: i64, country: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{{\"age\": {d}, \"country\": \"{s}\"}}", .{ age, country });
}

// =============================================================================
// Tests
// =============================================================================

test "WasmTestFixture rules engine eligible" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    var result = try fixture.executeRulesEngine("{\"age\": 25, \"country\": \"US\"}");
    defer result.deinit();

    try fixture.assertEligible(result, true);
    try fixture.assertOutputContains(result, "\"rules_passed\":2");
}

test "WasmTestFixture rules engine ineligible" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    var result = try fixture.executeRulesEngine("{\"age\": 15, \"country\": \"US\"}");
    defer result.deinit();

    try fixture.assertEligible(result, false);
    try fixture.assertOutputContains(result, "\"rules_passed\":1");
}

test "WasmTestFixture multiple executions" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    // Execute multiple times
    {
        var r = try fixture.executeRulesEngine("{\"age\": 25, \"country\": \"US\"}");
        defer r.deinit();
        try fixture.assertEligible(r, true);
    }
    {
        var r = try fixture.executeRulesEngine("{\"age\": 10, \"country\": \"UK\"}");
        defer r.deinit();
        try fixture.assertEligible(r, false);
    }
    {
        var r = try fixture.executeRulesEngine("{\"age\": 99, \"country\": \"US\"}");
        defer r.deinit();
        try fixture.assertEligible(r, true);
    }
}

test "buildRulesInput helper" {
    const input = try buildRulesInput(testing.allocator, 30, "CA");
    defer testing.allocator.free(input);
    try testing.expectEqualStrings("{\"age\": 30, \"country\": \"CA\"}", input);
}

test "WasmTestFixture edge case: age exactly 18" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    // age == 18 is NOT > 18
    var result = try fixture.executeRulesEngine("{\"age\": 18, \"country\": \"US\"}");
    defer result.deinit();

    try fixture.assertEligible(result, false);
    try fixture.assertOutputContains(result, "\"rules_passed\":1");
}

test "WasmTestFixture edge case: age exactly 19" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    // age == 19 IS > 18
    var result = try fixture.executeRulesEngine("{\"age\": 19, \"country\": \"US\"}");
    defer result.deinit();

    try fixture.assertEligible(result, true);
}

test "WasmTestFixture with buildRulesInput" {
    var fixture = try WasmTestFixture.init(testing.allocator);
    defer fixture.deinit();

    const input = try buildRulesInput(testing.allocator, 25, "US");
    defer testing.allocator.free(input);

    var result = try fixture.executeRulesEngine(input);
    defer result.deinit();

    try fixture.assertEligible(result, true);
}
