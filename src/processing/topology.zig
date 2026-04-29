//! Processing Topology
//!
//! A Topology defines a directed acyclic graph (DAG) of operators
//! that form a stream processing pipeline. The builder API lets
//! users construct pipelines declaratively.
//!
//! - Linear chain (source → op1 → op2 → ... → sink).
//! - DAG with splits, joins, and keyBy shuffle boundaries.
//!
//! Example:
//!   var topo = Topology.init(allocator, "word-count");
//!   try topo.setSource(&my_source);
//!   try topo.addOperator(filter_op.operator());
//!   try topo.addOperator(map_op.operator());
//!   try topo.setSink(&my_sink);

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("operator.zig").Operator;
const Source = @import("endpoints/source.zig").Source;
const Sink = @import("endpoints/sink.zig").Sink;

// =============================================================================
// TopologyNode - A node in the processing DAG
// =============================================================================

/// Types of nodes in the topology
pub const NodeKind = enum {
    source,
    operator,
    sink,
};

/// A node in the processing topology
pub const TopologyNode = struct {
    kind: NodeKind,
    name: []const u8,
    /// Index into the operators array (for operator nodes)
    operator_index: ?usize,
};

// =============================================================================
// Topology - Builder for processing pipelines
// =============================================================================

/// Represents a stream processing topology.
///
/// Supports linear chains: source → operator* → sink.
/// The topology validates that exactly one source and one sink exist,
/// and operators are chained in insertion order.
pub const Topology = struct {
    /// Job name for this topology
    name: []const u8,
    /// Ordered list of operators in the chain
    operators: std.ArrayList(Operator),
    /// Source (set once)
    source_ref: ?Source,
    /// Sink (set once)
    sink_ref: ?Sink,
    /// Node metadata for introspection
    nodes: std.ArrayList(TopologyNode),
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, name: []const u8) Self {
        return .{
            .name = name,
            .operators = .empty,
            .source_ref = null,
            .sink_ref = null,
            .nodes = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.operators.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
    }

    /// Set the source for this topology
    pub fn setSource(self: *Self, src: Source) !void {
        if (self.source_ref != null) return error.SourceAlreadySet;
        self.source_ref = src;
        try self.nodes.append(self.allocator, .{
            .kind = .source,
            .name = src.getName(),
            .operator_index = null,
        });
    }

    /// Add an operator to the chain (appended in order)
    pub fn addOperator(self: *Self, op: Operator) !void {
        const idx = self.operators.items.len;
        try self.operators.append(self.allocator, op);
        try self.nodes.append(self.allocator, .{
            .kind = .operator,
            .name = op.getName(),
            .operator_index = idx,
        });
    }

    /// Set the sink for this topology
    pub fn setSink(self: *Self, snk: Sink) !void {
        if (self.sink_ref != null) return error.SinkAlreadySet;
        self.sink_ref = snk;
        try self.nodes.append(self.allocator, .{
            .kind = .sink,
            .name = snk.getName(),
            .operator_index = null,
        });
    }

    /// Validate the topology is complete (has source, sink)
    pub fn validate(self: *const Self) !void {
        if (self.source_ref == null) return error.NoSource;
        if (self.sink_ref == null) return error.NoSink;
    }

    /// Number of operators in the chain
    pub fn operatorCount(self: *const Self) usize {
        return self.operators.items.len;
    }

    /// Number of nodes (source + operators + sink)
    pub fn nodeCount(self: *const Self) usize {
        return self.nodes.items.len;
    }

    /// Get the ordered list of operators
    pub fn getOperators(self: *const Self) []const Operator {
        return self.operators.items;
    }

    /// Get the source
    pub fn getSource(self: *const Self) !Source {
        return self.source_ref orelse error.NoSource;
    }

    /// Get the sink
    pub fn getSink(self: *const Self) !Sink {
        return self.sink_ref orelse error.NoSink;
    }
};

// =============================================================================
// Tests
// =============================================================================

const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const SliceSource = @import("endpoints/source.zig").SliceSource;
const CollectingSink = @import("endpoints/sink.zig").CollectingSink;
const PassthroughOperator = @import("operators/passthrough.zig").PassthroughOperator;

test "Topology linear chain build" {
    const allocator = std.testing.allocator;
    var topo = Topology.init(allocator, "test-job");
    defer topo.deinit();

    var records = [_]ProcessingRecord{};
    var src = SliceSource.init("src", &records);
    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();

    var pass_op = PassthroughOperator.init("m1");

    try topo.setSource(src.source());
    try topo.addOperator(pass_op.operator());
    try topo.setSink(snk.sink());

    try topo.validate();
    try std.testing.expectEqual(@as(usize, 1), topo.operatorCount());
    try std.testing.expectEqual(@as(usize, 3), topo.nodeCount());
}

test "Topology rejects duplicate source" {
    const allocator = std.testing.allocator;
    var topo = Topology.init(allocator, "test");
    defer topo.deinit();

    var records = [_]ProcessingRecord{};
    var src1 = SliceSource.init("s1", &records);
    var src2 = SliceSource.init("s2", &records);

    try topo.setSource(src1.source());
    const result = topo.setSource(src2.source());
    try std.testing.expectError(error.SourceAlreadySet, result);
}

test "Topology validate fails without source" {
    const allocator = std.testing.allocator;
    var topo = Topology.init(allocator, "test");
    defer topo.deinit();

    var snk = CollectingSink.init(allocator, "snk");
    defer snk.deinit();
    try topo.setSink(snk.sink());

    try std.testing.expectError(error.NoSource, topo.validate());
}

test "Topology validate fails without sink" {
    const allocator = std.testing.allocator;
    var topo = Topology.init(allocator, "test");
    defer topo.deinit();

    var records = [_]ProcessingRecord{};
    var src = SliceSource.init("src", &records);
    try topo.setSource(src.source());

    try std.testing.expectError(error.NoSink, topo.validate());
}
