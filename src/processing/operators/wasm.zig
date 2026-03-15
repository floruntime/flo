//! WASM Processing Operator
//!
//! Executes a WASM module for each input record. Supports three modes:
//!
//!   - **Map** (1→1): handle() returns packed ptr|len → single output record.
//!   - **Filter** (1→0): handle() returns 0 → record is dropped.
//!   - **FlatMap** (1→N): flo.emit() called 0..N times during handle() →
//!     each emitted record becomes an output. handle() return is ignored.
//!
//! ## Processing WASM Contract
//!
//! Guest exports (same as Actions ABI):
//!   - `handle(input_ptr: u32, input_len: u32) -> i64`
//!   - `alloc(size: u32) -> u32`
//!   - `dealloc(ptr: u32, size: u32) -> void`
//!
//! Host imports (provided by Flo runtime):
//!   - `flo.set_tag(name_ptr: u32, name_len: u32) -> i32`
//!   - `flo.emit(ptr: u32, len: u32) -> i32`         — FlatMap multi-emit
//!   - `flo.state_get(key_ptr, key_len, buf_ptr, buf_len) -> i32`
//!   - `flo.state_set(key_ptr, key_len, val_ptr, val_len) -> i32`
//!   - `flo.state_delete(key_ptr, key_len) -> i32`
//!   - `flo.kv_get(key_ptr, key_len, buf_ptr, buf_len) -> i32`
//!   - `flo.kv_set(key_ptr, key_len, val_ptr, val_len) -> i32`
//!   - `flo.kv_delete(key_ptr, key_len) -> i32`
//!   - `flo.log(level: u32, msg_ptr: u32, msg_len: u32) -> void`
//!
//! ## YAML example
//!
//!   ```yaml
//!   operators:
//!     - type: wasm
//!       name: enrich
//!       module: ./transforms/enrich.wasm
//!   sinks:
//!     - name: enriched-sink
//!       stream.name: enriched-output
//!       match: [enriched]          # receives records tagged by WASM
//!   ```
//!
//! The `module` field specifies the path to the WASM binary. The handler
//! reads the bytes and calls `loadWasm()` after creation.
//!
//! Tagging is dynamic: the WASM module itself calls `flo.set_tag("enriched")`
//! during `handle()`, and the bit is OR'd into the output record's tags.

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
const WasmKvContext = wasm_runner.WasmKvContext;
const EmittedRecord = wasm_runner.EmittedRecord;
const TagRegistry = @import("../definition.zig").TagRegistry;
const log = @import("stdx").log;

pub const WasmOperator = struct {
    name: []const u8,
    module_path: []const u8,
    allocator: Allocator,
    runner: ?WasmRunner = null,
    module: ?WasmModule = null,
    /// Pipeline's tag registry for resolving tag names in flo.set_tag().
    tag_registry: ?*const TagRegistry = null,

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

        // Build execution context with tag resolution for flo.set_tag()
        var wasm_ctx = WasmKvContext{
            .namespace = "",
            .kv_dispatch_fn = null,
            .kv_dispatch_ctx = null,
            .allocator = self.allocator,
            .kv_enabled = false,
            .tag_resolve_fn = if (self.tag_registry != null) tagResolveCallback else null,
            .tag_resolve_ctx = if (self.tag_registry) |reg| @ptrCast(@constCast(reg)) else null,
            .output_tags = 0,
            // State dispatch wired to operator's keyed state (if available)
            .state_dispatch_fn = if (ctx.keyed_state != null) stateDispatchCallback else null,
            .state_dispatch_ctx = if (ctx.keyed_state) |ks| @ptrCast(ks) else null,
            .state_enabled = ctx.keyed_state != null,
        };

        var result = runner.executeWithKv(module, rec.value, &wasm_ctx) catch |err| {
            log.err("WASM operator '{s}' execution failed: {}", .{ self.name, err });
            return;
        };
        defer result.deinit();

        // Merge: input tags + any tags set by the WASM module via flo.set_tag()
        const tags = rec.tags | wasm_ctx.output_tags;

        // Multi-emit mode: flo.emit() was called during handle()
        if (wasm_ctx.emit_called) {
            defer {
                for (wasm_ctx.emitted_records.items) |er| {
                    self.allocator.free(er.data);
                }
                wasm_ctx.emitted_records.deinit(self.allocator);
            }
            for (wasm_ctx.emitted_records.items) |er| {
                const output = try self.allocator.dupe(u8, er.data);
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
                    .tags = tags,
                });
            }
            return;
        }

        // Filter mode: handle() returned 0 → drop this record
        if (result.filtered) {
            return;
        }

        // Standard map mode: single output from handle() return value
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
            .tags = tags,
        });
    }

    /// Callback passed to the WASM host for flo.set_tag() resolution.
    fn tagResolveCallback(ctx_ptr: *anyopaque, tag_name: []const u8) ?u5 {
        const reg: *const TagRegistry = @ptrCast(@alignCast(ctx_ptr));
        return reg.resolve(tag_name);
    }

    /// Callback for flo.state_get/set/delete — bridges to KeyedStateAccess.
    fn stateDispatchCallback(ctx_ptr: *anyopaque, op: wasm_runner.StateOp, _: Allocator) wasm_runner.StateResult {
        const keyed_state = @as(*@import("../state.zig").KeyedStateAccess, @ptrCast(@alignCast(ctx_ptr)));
        const vs = keyed_state.getValueState();

        switch (op) {
            .get => |g| {
                const val = vs.get(g.key) catch {
                    return .{ .status = .err };
                };
                if (val) |v| {
                    return .{ .status = .success, .value = v };
                }
                return .{ .status = .not_found };
            },
            .set => |s| {
                vs.put(s.key, s.value) catch {
                    return .{ .status = .err };
                };
                return .{ .status = .success };
            },
            .delete => |d| {
                const existed = vs.get(d.key) catch {
                    return .{ .status = .err };
                };
                vs.delete(d.key) catch {
                    return .{ .status = .err };
                };
                return if (existed != null)
                    .{ .status = .success }
                else
                    .{ .status = .not_found };
            },
        }
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

test "WasmOperator — dynamic tagging via flo.set_tag" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../testdata/txn_classifier.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "txn-classify", "./txn_classifier.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    // Set up tag registry (same names the WASM module calls set_tag with)
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("high-value"); // bit 0
    _ = reg.getOrCreate("refund"); // bit 1
    _ = reg.getOrCreate("standard"); // bit 2
    op.tag_registry = &reg;

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
        .operator_name = "txn-classify",
    };

    // High-value transaction (amount >= 10000) → should tag "high-value" (bit 0)
    try iface.processElement(
        ProcessingRecord.init("t1", "{\"txn_id\": \"T1\", \"amount\": 25000, \"merchant\": \"ACME\"}", 100),
        &ctx,
    );
    // Refund transaction (amount < 0) → should tag "refund" (bit 1)
    try iface.processElement(
        ProcessingRecord.init("t2", "{\"txn_id\": \"R1\", \"amount\": -200, \"merchant\": \"STORE\"}", 200),
        &ctx,
    );
    // Standard transaction → should tag "standard" (bit 2)
    try iface.processElement(
        ProcessingRecord.init("t3", "{\"txn_id\": \"S1\", \"amount\": 50, \"merchant\": \"CAFE\"}", 300),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 3), collector.count());

    const output = collector.drain();
    try testing.expectEqual(@as(u32, 0b001), output[0].tags); // high-value
    try testing.expectEqual(@as(u32, 0b010), output[1].tags); // refund
    try testing.expectEqual(@as(u32, 0b100), output[2].tags); // standard

    // Verify output contains classification
    try testing.expect(std.mem.indexOf(u8, output[0].value, "\"class\":\"high-value\"") != null);
    try testing.expect(std.mem.indexOf(u8, output[1].value, "\"class\":\"refund\"") != null);
    try testing.expect(std.mem.indexOf(u8, output[2].value, "\"class\":\"standard\"") != null);
}

test "WasmOperator — dynamic tags merge with existing tags" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../testdata/txn_classifier.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "merge-wasm", "./txn_classifier.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("high-value"); // bit 0
    _ = reg.getOrCreate("refund"); // bit 1
    _ = reg.getOrCreate("standard"); // bit 2
    op.tag_registry = &reg;

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
        .operator_name = "merge-wasm",
    };

    // Record already has bit 3 set from a prior operator
    var rec = ProcessingRecord.init("t1", "{\"txn_id\": \"M1\", \"amount\": 50000, \"merchant\": \"BIG\"}", 100);
    rec.tags = 0b1000; // bit 3 pre-set

    try iface.processElement(rec, &ctx);

    try testing.expectEqual(@as(usize, 1), collector.count());
    const output = collector.drain();
    // Should have both pre-existing bit 3 AND "high-value" bit 0
    try testing.expectEqual(@as(u32, 0b1001), output[0].tags);
}

test "WasmOperator — filter drops record (handle returns 0)" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../testdata/txn_enricher.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "enricher-filter", "./txn_enricher.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("high-value");
    _ = reg.getOrCreate("standard");
    op.tag_registry = &reg;

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
        .operator_name = "enricher-filter",
    };

    // Input missing "amount" field → should be filtered (dropped)
    try iface.processElement(
        ProcessingRecord.init("k1", "{\"txn_id\": \"X1\", \"merchant\": \"NONE\"}", 100),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 0), collector.count());
}

test "WasmOperator — flatmap emits multiple records" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../testdata/txn_enricher.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "enricher-flatmap", "./txn_enricher.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("high-value");
    _ = reg.getOrCreate("standard");
    op.tag_registry = &reg;

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
        .operator_name = "enricher-flatmap",
    };

    // High-value transaction (amount >= 10000) → WASM calls flo.emit() twice
    try iface.processElement(
        ProcessingRecord.init("t1", "{\"txn_id\": \"HV1\", \"amount\": 50000, \"merchant\": \"BIG\"}", 100),
        &ctx,
    );

    // Should get 2 records: enriched + alert
    try testing.expectEqual(@as(usize, 2), collector.count());

    const output = collector.drain();
    // Both should inherit the key
    try testing.expectEqualStrings("t1", output[0].key);
    try testing.expectEqualStrings("t1", output[1].key);

    // First record should be the enriched one with "class":"high-value"
    try testing.expect(std.mem.indexOf(u8, output[0].value, "\"class\":\"high-value\"") != null);
    // Second record should be the alert
    try testing.expect(std.mem.indexOf(u8, output[1].value, "\"alert\":\"high-value-txn\"") != null);

    // Both should have "high-value" tag (bit 0)
    try testing.expectEqual(@as(u32, 0b01), output[0].tags);
    try testing.expectEqual(@as(u32, 0b01), output[1].tags);
}

test "WasmOperator — standard map with state counter" {
    const allocator = testing.allocator;
    const wasm_bytes = @embedFile("../testdata/txn_enricher.wasm");

    var op = try allocator.create(WasmOperator);
    op.* = WasmOperator.init(allocator, "enricher-state", "./txn_enricher.wasm");
    defer {
        op.deinit();
        allocator.destroy(op);
    }

    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("high-value");
    _ = reg.getOrCreate("standard");
    op.tag_registry = &reg;

    try op.loadWasm(wasm_bytes);
    const iface = op.operator();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    // Set up state backend + keyed state
    const StateBackend = @import("../state.zig").StateBackend;
    const KeyedStateAccess = @import("../state.zig").KeyedStateAccess;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var keyed_state = try KeyedStateAccess.init(allocator, &backend, "test-job", "enricher-state");
    defer keyed_state.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 3000,
        .current_watermark_ms = 0,
        .operator_name = "enricher-state",
        .keyed_state = &keyed_state,
    };

    // Standard transaction → single output with merchant_txn_count
    try iface.processElement(
        ProcessingRecord.init("s1", "{\"txn_id\": \"S1\", \"amount\": 50, \"merchant\": \"CAFE\"}", 100),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 1), collector.count());
    const out1 = collector.drain();
    // Should have "standard" tag (bit 1)
    try testing.expectEqual(@as(u32, 0b10), out1[0].tags);
    // Should have merchant_txn_count:1 (first time seeing CAFE)
    try testing.expect(std.mem.indexOf(u8, out1[0].value, "\"merchant_txn_count\":1") != null);

    collector.clear();

    // Second call for same merchant → count should be 2
    try iface.processElement(
        ProcessingRecord.init("s2", "{\"txn_id\": \"S2\", \"amount\": 75, \"merchant\": \"CAFE\"}", 200),
        &ctx,
    );

    try testing.expectEqual(@as(usize, 1), collector.count());
    const out2 = collector.drain();
    try testing.expect(std.mem.indexOf(u8, out2[0].value, "\"merchant_txn_count\":2") != null);
}
