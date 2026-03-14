//! WASM Processing Operator
//!
//! Executes a WASM module for each input record, passing the record's
//! value as input and emitting the WASM output as a new record.
//!
//! Uses the centralized WASM runner (`src/wasm/runner.zig`) shared
//! with the actions subsystem.
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: wasm
//!       name: transform
//!       module: ./transforms/enrich.wasm
//!   ```
//!
//! The `module` field specifies the path to the WASM binary. The handler
//! reads the bytes and calls `loadWasm()` after creation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const noOpSnapshot = @import("../operator.zig").noOpSnapshot;
const noOpRestore = @import("../operator.zig").noOpRestore;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;
const wasm_runner = @import("../../wasm/runner.zig");
const WasmRunner = wasm_runner.WasmRunner;
const WasmModule = wasm_runner.WasmModule;
const WasmConfig = wasm_runner.WasmConfig;
const log = @import("stdx").log;

pub const WasmOperator = struct {
    name: []const u8,
    module_path: []const u8,
    allocator: Allocator,
    runner: ?WasmRunner = null,
    module: ?WasmModule = null,

    const Self = @This();

    pub fn init(allocator: Allocator, name: []const u8, module_path: []const u8) Self {
        return .{
            .name = name,
            .module_path = module_path,
            .allocator = allocator,
        };
    }

    /// Load a WASM module from raw bytes. Called by the handler after creation.
    pub fn loadWasm(self: *Self, wasm_bytes: []const u8) !void {
        self.runner = try WasmRunner.init(self.allocator);
        errdefer {
            self.runner.?.deinit();
            self.runner = null;
        }
        self.module = try self.runner.?.loadModule(wasm_bytes, .{
            .module_name = self.name,
        });
    }

    pub fn deinit(self: *Self) void {
        if (self.module) |*m| m.deinit();
        if (self.runner) |*r| r.deinit();
    }

    /// Return an Operator interface backed by this WasmOperator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = close,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var runner = &(self.runner orelse return);
        const module = &(self.module orelse return);

        var result = runner.execute(module, rec.value) catch |err| {
            log.err("WASM operator '{s}' execution failed: {}", .{ self.name, err });
            return;
        };
        defer result.deinit();

        // Dupe output and key so the emitted record owns its memory
        const output = try self.allocator.dupe(u8, result.output);
        errdefer self.allocator.free(output);

        const key_dupe = if (rec.key.len > 0)
            try self.allocator.dupe(u8, rec.key)
        else
            @as([]const u8, &.{});

        try ctx.emit(.{
            .key = key_dupe,
            .value = output,
            .event_time_ms = rec.event_time_ms,
            .source = rec.source,
            .headers = &.{},
            .owns_memory = true,
            .tags = rec.tags,
        });
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {}

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;
const OutputCollector = @import("../collector.zig").OutputCollector;
const OperatorMetrics = @import("../context.zig").OperatorMetrics;

test "WasmOperator — getName" {
    var op = WasmOperator.init(testing.allocator, "my-transform", "./transform.wasm");
    const iface = op.operator();
    try testing.expectEqualStrings("my-transform", iface.getName());
}

test "WasmOperator — init without loadWasm skips records" {
    var op = WasmOperator.init(testing.allocator, "skip-op", "./missing.wasm");
    const iface = op.operator();

    var collector = OutputCollector.init(testing.allocator);
    defer collector.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = testing.allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 0,
        .operator_name = "skip-op",
    };

    // Should silently skip (no runner loaded)
    try iface.processElement(ProcessingRecord.init("k", "data", 100), &ctx);
    try testing.expectEqual(@as(usize, 0), collector.count());
}

test "WasmOperator — load and execute" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../../actions/testdata/rules_engine.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "rules-check", "./rules.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    try op.loadWasm(wasm_bytes);

    const iface = op.operator();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 0,
        .operator_name = "rules-check",
    };

    // Execute: eligible input
    try iface.processElement(
        ProcessingRecord.init("user-1", "{\"age\": 25, \"country\": \"US\"}", 100),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 1), collector.count());

    const output = collector.drain();
    try testing.expectEqualStrings("user-1", output[0].key);
    try testing.expectEqualStrings(
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        output[0].value,
    );
    try testing.expectEqual(@as(i64, 100), output[0].event_time_ms);
    try testing.expect(output[0].owns_memory);
}

test "WasmOperator — preserves tags" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../../actions/testdata/rules_engine.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "tagged-wasm", "./rules.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    try op.loadWasm(wasm_bytes);

    const iface = op.operator();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 2000,
        .current_watermark_ms = 0,
        .operator_name = "tagged-wasm",
    };

    // Create record with tags set
    var rec = ProcessingRecord.init("k1", "{\"age\": 30, \"country\": \"US\"}", 200);
    rec.tags = 0b101; // bits 0 and 2 set

    try iface.processElement(rec, &ctx);

    try testing.expectEqual(@as(usize, 1), collector.count());
    const output = collector.drain();
    try testing.expectEqual(@as(u32, 0b101), output[0].tags);
}

test "WasmOperator — multiple records" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../../actions/testdata/rules_engine.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "batch-wasm", "./rules.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    try op.loadWasm(wasm_bytes);

    const iface = op.operator();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 3000,
        .current_watermark_ms = 0,
        .operator_name = "batch-wasm",
    };

    // Process multiple records
    try iface.processElement(
        ProcessingRecord.init("u1", "{\"age\": 25, \"country\": \"US\"}", 100),
        &ctx,
    );
    try iface.processElement(
        ProcessingRecord.init("u2", "{\"age\": 15, \"country\": \"UK\"}", 200),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 2), collector.count());

    const output = collector.drain();
    try testing.expectEqualStrings(
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        output[0].value,
    );
    try testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        output[1].value,
    );
}
