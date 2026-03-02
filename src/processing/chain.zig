//! Operator Chain - Fused execution of chained operators
//!
//! A Chain takes a validated Topology and runs it:
//! 1. Poll source for records
//! 2. Feed each record through the operator chain
//! 3. Write final output to sink
//!
//! Fused execution: all operators in the chain run on the same shard
//! with zero copying — records pass through as borrowed slices.
//! This mirrors Flink's operator chaining optimization.
//!
//! - Synchronous single-threaded execution.
//! - Integration with shard event loop and backpressure.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("operator.zig").Operator;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const StreamElement = record_mod.StreamElement;
const Watermark = record_mod.Watermark;
const CheckpointBarrier = record_mod.CheckpointBarrier;
const OperatorContext = @import("context.zig").OperatorContext;
const OperatorMetrics = @import("context.zig").OperatorMetrics;
const OutputCollector = @import("collector.zig").OutputCollector;
const Topology = @import("topology.zig").Topology;
const Source = @import("endpoints/source.zig").Source;
const Sink = @import("endpoints/sink.zig").Sink;
const CheckpointCoordinator = @import("checkpoint/coordinator.zig").CheckpointCoordinator;
const CheckpointStore = @import("checkpoint/storage.zig").CheckpointStore;

// =============================================================================
// Chain - Fused operator execution
// =============================================================================

/// Executes a processing topology as a fused operator chain.
///
/// The chain manages per-operator collectors and contexts, feeding
/// records through each operator in sequence. After the last operator,
/// output records are written to the sink.
pub const Chain = struct {
    /// Topology this chain executes
    topology: *const Topology,
    /// Per-operator collectors (output buffer per operator stage)
    collectors: []OutputCollector,
    /// Per-operator metrics
    metrics: []OperatorMetrics,
    /// Allocator
    allocator: Allocator,
    /// Records processed count
    total_records: u64,
    /// Current watermark
    current_watermark_ms: i64,
    /// Optional checkpoint coordinator for barrier handling
    checkpoint_coordinator: ?*CheckpointCoordinator,

    const Self = @This();

    pub fn init(allocator: Allocator, topology: *const Topology) !Self {
        // Validate topology before creating chain
        try topology.validate();

        const op_count = topology.operatorCount();

        // Allocate one collector per operator
        const collectors = try allocator.alloc(OutputCollector, op_count);
        for (collectors) |*c| {
            c.* = OutputCollector.init(allocator);
        }

        const metrics = try allocator.alloc(OperatorMetrics, op_count);
        for (metrics) |*m| {
            m.* = OperatorMetrics{};
        }

        return .{
            .topology = topology,
            .collectors = collectors,
            .metrics = metrics,
            .allocator = allocator,
            .total_records = 0,
            .current_watermark_ms = Watermark.NONE,
            .checkpoint_coordinator = null,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.collectors) |*c| {
            c.deinit();
        }
        self.allocator.free(self.collectors);
        self.allocator.free(self.metrics);
    }

    /// Run the topology to completion (drain the source).
    /// Returns the total number of records consumed from the source.
    pub fn run(self: *Self) !u64 {
        const src = try self.topology.getSource();
        const snk = try self.topology.getSink();

        while (true) {
            const element = try src.poll() orelse break;

            switch (element) {
                .record => |rec| {
                    try self.processRecord(rec, snk);
                    self.total_records += 1;
                },
                .watermark => |wm| {
                    self.current_watermark_ms = wm.timestamp_ms;
                    try self.processWatermark(wm);
                },
                .barrier => |barrier| {
                    try self.processBarrier(barrier, src, snk);
                },
                .end_of_stream => {
                    try snk.flush();
                    break;
                },
            }
        }

        return self.total_records;
    }

    /// Run the topology for up to `max_records` records, then flush and return.
    ///
    /// Unlike `run()`, this does NOT drain the source to completion. It processes
    /// at most `max_records` records and returns the count processed in this batch.
    /// This enables incremental execution driven by the event loop's tick().
    ///
    /// Returns 0 if the source had no records available (temporarily exhausted).
    pub fn runBatch(self: *Self, max_records: u64) !u64 {
        const src = try self.topology.getSource();
        const snk = try self.topology.getSink();

        var batch_count: u64 = 0;

        while (batch_count < max_records) {
            const element = try src.poll() orelse break;

            switch (element) {
                .record => |rec| {
                    try self.processRecord(rec, snk);
                    self.total_records += 1;
                    batch_count += 1;
                },
                .watermark => |wm| {
                    self.current_watermark_ms = wm.timestamp_ms;
                    try self.processWatermark(wm);
                },
                .barrier => |barrier| {
                    try self.processBarrier(barrier, src, snk);
                },
                .end_of_stream => {
                    try snk.flush();
                    return batch_count;
                },
            }
        }

        // Flush accumulated output from this batch
        if (batch_count > 0) try snk.flush();

        return batch_count;
    }

    /// Process a single record through the operator chain
    fn processRecord(self: *Self, input: ProcessingRecord, snk: Sink) !void {
        const operators = self.topology.getOperators();

        if (operators.len == 0) {
            // No operators — source directly to sink
            try snk.write(input);
            return;
        }

        // Feed input to first operator
        self.collectors[0].clear();
        var ctx = self.makeContext(0);
        try operators[0].processElement(input, &ctx);

        // Chain: output of operator[i] → input of operator[i+1]
        var i: usize = 1;
        while (i < operators.len) : (i += 1) {
            self.collectors[i].clear();
            var next_ctx = self.makeContext(i);

            const prev_output = self.collectors[i - 1].drain();
            for (prev_output) |rec| {
                try operators[i].processElement(rec, &next_ctx);
            }
        }

        // Write final operator output to sink
        const final_output = self.collectors[operators.len - 1].drain();
        for (final_output) |rec| {
            try snk.write(rec);
        }
    }

    /// Forward watermark through all operators
    fn processWatermark(self: *Self, wm: Watermark) !void {
        const operators = self.topology.getOperators();
        for (operators, 0..) |op, i| {
            var ctx = self.makeContext(i);
            try op.processWatermark(wm, &ctx);
        }
    }

    /// Process a checkpoint barrier through the operator chain.
    ///
    /// Chandy-Lamport protocol:
    /// 1. Source already saved offsets before forwarding barrier
    /// 2. Each operator: snapshotState → acknowledge coordinator → forward
    /// 3. Sink: acknowledge coordinator
    fn processBarrier(self: *Self, barrier: CheckpointBarrier, src: Source, snk: Sink) !void {
        const coord = self.checkpoint_coordinator orelse return;
        const operators = self.topology.getOperators();

        // Step 1: Save source offsets
        if (src.snapshotOffsets(self.allocator)) |maybe_offsets| {
            if (maybe_offsets) |offsets| {
                defer self.allocator.free(offsets);
                try coord.acknowledgeSource(barrier.checkpoint_id, offsets);
            }
        } else |_| {}

        // Step 2: Snapshot each operator and acknowledge
        for (operators) |op| {
            const state_data = try op.snapshotState(barrier.checkpoint_id, self.allocator);
            defer if (state_data) |d| self.allocator.free(d);
            try coord.acknowledgeOperator(barrier.checkpoint_id, op.getName(), state_data);
        }

        // Step 3: Notify sink and acknowledge
        snk.notifyCheckpointComplete(barrier.checkpoint_id) catch {};
        try coord.acknowledgeSink(barrier.checkpoint_id);
    }

    /// Attach a checkpoint coordinator to enable barrier processing.
    pub fn setCheckpointCoordinator(self: *Self, coord: *CheckpointCoordinator) void {
        self.checkpoint_coordinator = coord;
    }

    /// Create an OperatorContext for operator at index
    fn makeContext(self: *Self, op_index: usize) OperatorContext {
        const operators = self.topology.getOperators();
        return OperatorContext{
            .collector = &self.collectors[op_index],
            .metrics = &self.metrics[op_index],
            .allocator = self.allocator,
            .current_processing_time_ms = std.time.milliTimestamp(),
            .current_watermark_ms = self.current_watermark_ms,
            .operator_name = operators[op_index].getName(),
        };
    }

    /// Get total records processed
    pub fn totalRecords(self: *const Self) u64 {
        return self.total_records;
    }

    /// Get metrics for a specific operator by index
    pub fn operatorMetrics(self: *const Self, index: usize) ?OperatorMetrics {
        if (index >= self.metrics.len) return null;
        return self.metrics[index];
    }
};

// =============================================================================
// Tests
// =============================================================================

const SliceSource = @import("endpoints/source.zig").SliceSource;
const CollectingSink = @import("endpoints/sink.zig").CollectingSink;
const PassthroughOperator = @import("operators/passthrough.zig").PassthroughOperator;
const ExprFilterOperator = @import("operators/expr_filter.zig").ExprFilterOperator;

test "Chain runs source through operators to sink" {
    const allocator = std.testing.allocator;

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 10),
        ProcessingRecord.init("k2", "v2", 20),
        ProcessingRecord.init("k3", "v3", 30),
    };

    var src = SliceSource.init("src", &records);
    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var pass_op = PassthroughOperator.init("pass");

    var topo = Topology.init(allocator, "test-chain");
    defer topo.deinit();
    try topo.setSource(src.source());
    try topo.addOperator(pass_op.operator());
    try topo.setSink(snk.sink());

    var chain = try Chain.init(allocator, &topo);
    defer chain.deinit();

    const count = try chain.run();

    try std.testing.expectEqual(@as(u64, 3), count);
    try std.testing.expectEqual(@as(usize, 3), snk.count());

    const output = snk.collected();
    try std.testing.expectEqual(@as(i64, 10), output[0].event_time_ms);
    try std.testing.expectEqual(@as(i64, 20), output[1].event_time_ms);
    try std.testing.expectEqual(@as(i64, 30), output[2].event_time_ms);
}

test "Chain with filter + passthrough" {
    const allocator = std.testing.allocator;

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "ignore-this", 10),
        ProcessingRecord.init("k2", "important-data", 20),
        ProcessingRecord.init("k3", "important-stuff", 30),
    };

    var src = SliceSource.init("src", &records);
    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var filter_op = ExprFilterOperator.init("keep-important", "value_contains:important");
    var pass_op = PassthroughOperator.init("pass");

    var topo = Topology.init(allocator, "filter-pass");
    defer topo.deinit();
    try topo.setSource(src.source());
    try topo.addOperator(filter_op.operator());
    try topo.addOperator(pass_op.operator());
    try topo.setSink(snk.sink());

    var chain = try Chain.init(allocator, &topo);
    defer chain.deinit();

    const count = try chain.run();

    try std.testing.expectEqual(@as(u64, 3), count); // 3 consumed from source
    try std.testing.expectEqual(@as(usize, 2), snk.count()); // 2 passed filter

    const output = snk.collected();
    try std.testing.expectEqualStrings("k2", output[0].key);
    try std.testing.expectEqualStrings("k3", output[1].key);
}

test "Chain with no operators passes through" {
    const allocator = std.testing.allocator;

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 100),
    };

    var src = SliceSource.init("src", &records);
    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var topo = Topology.init(allocator, "passthrough");
    defer topo.deinit();
    try topo.setSource(src.source());
    try topo.setSink(snk.sink());

    var chain = try Chain.init(allocator, &topo);
    defer chain.deinit();

    _ = try chain.run();

    try std.testing.expectEqual(@as(usize, 1), snk.count());
    try std.testing.expectEqualStrings("k1", snk.collected()[0].key);
}

test "Chain processes checkpoint barrier end-to-end" {
    const allocator = std.testing.allocator;

    // Custom source that emits: [record, barrier, record, end_of_stream]
    const BarrierSource = struct {
        step: u32 = 0,
        const BarrierSelf = @This();

        fn poll(ptr: *anyopaque) anyerror!?StreamElement {
            const self: *BarrierSelf = @ptrCast(@alignCast(ptr));
            self.step += 1;
            return switch (self.step) {
                1 => StreamElement{ .record = ProcessingRecord.init("k1", "v1", 10) },
                2 => StreamElement{ .barrier = .{ .checkpoint_id = 1, .timestamp_ms = 1000, .checkpoint_type = .full } },
                3 => StreamElement{ .record = ProcessingRecord.init("k2", "v2", 20) },
                4 => StreamElement{ .end_of_stream = {} },
                else => null,
            };
        }
        fn getName(_: *anyopaque) []const u8 {
            return "barrier-test-src";
        }
        fn close(_: *anyopaque) void {}
        fn snapshotOffsets(_: *anyopaque, off_alloc: Allocator) anyerror!?[]u8 {
            const buf = try off_alloc.alloc(u8, @sizeOf(u64));
            const pos: u64 = 1;
            @memcpy(buf[0..@sizeOf(u64)], std.mem.asBytes(&pos));
            return buf;
        }
        fn restoreOffsets(_: *anyopaque, _: []const u8) anyerror!void {}
        fn notifyCheckpointComplete(_: *anyopaque, _: u64) anyerror!void {}
    };

    var barrier_src = BarrierSource{};
    const src = Source{
        .ptr = @ptrCast(&barrier_src),
        .vtable = &.{
            .poll = &BarrierSource.poll,
            .getName = &BarrierSource.getName,
            .close = &BarrierSource.close,
            .snapshotOffsets = &BarrierSource.snapshotOffsets,
            .restoreOffsets = &BarrierSource.restoreOffsets,
            .notifyCheckpointComplete = &BarrierSource.notifyCheckpointComplete,
        },
    };

    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var pass_op = PassthroughOperator.init("pass");

    var topo = Topology.init(allocator, "checkpoint-e2e");
    defer topo.deinit();
    try topo.setSource(src);
    try topo.addOperator(pass_op.operator());
    try topo.setSink(snk.sink());

    // Set up checkpoint coordinator
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();
    var coord = CheckpointCoordinator.init(allocator, &store, 1); // 1 operator

    var chain_inst = try Chain.init(allocator, &topo);
    defer chain_inst.deinit();
    chain_inst.setCheckpointCoordinator(&coord);

    // Pre-trigger checkpoint so coordinator is expecting barrier ID 1
    const barrier = try coord.triggerCheckpoint();
    try std.testing.expectEqual(@as(u64, 1), barrier.checkpoint_id);

    const count = try chain_inst.run();

    // 2 records processed (barriers are not counted)
    try std.testing.expectEqual(@as(u64, 2), count);
    try std.testing.expectEqual(@as(usize, 2), snk.count());

    // Checkpoint should be complete
    try std.testing.expect(!coord.isPending());
    try std.testing.expectEqual(@as(?u64, 1), coord.lastCompletedId());
    try std.testing.expectEqual(@as(u64, 1), coord.completedCount());

    // Verify source offsets were stored
    const stored_offsets = try store.getSourceOffsets(1);
    try std.testing.expect(stored_offsets != null);
}

test "Chain runBatch processes limited records" {
    const allocator = std.testing.allocator;

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 10),
        ProcessingRecord.init("k2", "v2", 20),
        ProcessingRecord.init("k3", "v3", 30),
        ProcessingRecord.init("k4", "v4", 40),
        ProcessingRecord.init("k5", "v5", 50),
    };

    var src = SliceSource.init("src", &records);
    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var topo = Topology.init(allocator, "batch-test");
    defer topo.deinit();
    try topo.setSource(src.source());
    try topo.setSink(snk.sink());

    var chain_inst = try Chain.init(allocator, &topo);
    defer chain_inst.deinit();

    // Process only 2 records
    const batch1 = try chain_inst.runBatch(2);
    try std.testing.expectEqual(@as(u64, 2), batch1);
    try std.testing.expectEqual(@as(usize, 2), snk.count());

    // Process remaining 3 records
    const batch2 = try chain_inst.runBatch(10);
    try std.testing.expectEqual(@as(u64, 3), batch2);
    try std.testing.expectEqual(@as(usize, 5), snk.count());

    // Source exhausted — returns 0
    const batch3 = try chain_inst.runBatch(10);
    try std.testing.expectEqual(@as(u64, 0), batch3);
}
