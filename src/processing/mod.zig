//! Flo-Processing Module Root
//!
//! Real-time stateful stream processing (like Apache Flink).
//! Consume Flo-Streams, maintain state in local Flo-KV, and emit
//! results — removing the need for a separate processing cluster.
//!
//! Foundation: Core data model, operator interface,
//! declarative operators (map, filter, flatmap, keyby, aggregate),
//! topology builder, and fused operator chain execution.
//!
//! Keyed State & Windowing: Keyed state access (ValueState,
//! ListState, MapState), window operators (tumbling, sliding, count),
//! triggers, watermark generation, multi-input watermark tracking,
//! timer service.
//!
//! Checkpointing & Fault Tolerance: Chandy-Lamport protocol,
//! checkpoint coordinator, barrier alignment, snapshot/restore, offsets.
//!
//! Advanced Features: Side outputs, processing metrics,
//! session windows, late data handling.

// Core data model
pub const record = @import("record.zig");
pub const ProcessingRecord = record.ProcessingRecord;
pub const Watermark = record.Watermark;
pub const CheckpointBarrier = record.CheckpointBarrier;
pub const StreamElement = record.StreamElement;
pub const Header = record.Header;
pub const SourceRef = record.SourceRef;

// Key schema for KV persistence
pub const keys = @import("keys.zig");

// Operator interface
pub const operator = @import("operator.zig");
pub const Operator = operator.Operator;

// Operator context and metrics
pub const context = @import("context.zig");
pub const OperatorContext = context.OperatorContext;
pub const OperatorMetrics = context.OperatorMetrics;

// Output collector
pub const collector = @import("collector.zig");
pub const OutputCollector = collector.OutputCollector;

// Source and sink interfaces
pub const endpoints = @import("endpoints/mod.zig");
pub const source = endpoints.source;
pub const Source = endpoints.Source;
pub const SliceSource = endpoints.SliceSource;

pub const sink = endpoints.sink;
pub const Sink = endpoints.Sink;
pub const CollectingSink = endpoints.CollectingSink;

// Job definition types
pub const job_definition = @import("definition.zig");
pub const JobDefinition = job_definition.JobDefinition;

// Job definition parser
pub const job_parser = @import("parser.zig");

// Topology builder
pub const topology = @import("topology.zig");
pub const Topology = topology.Topology;
pub const TopologyNode = topology.TopologyNode;
pub const NodeKind = topology.NodeKind;

// Fused operator chain execution
pub const chain = @import("chain.zig");
pub const Chain = chain.Chain;

// Keyed state
pub const state = @import("state.zig");
pub const KeyedStateAccess = state.KeyedStateAccess;
pub const ValueState = state.ValueState;
pub const ListState = state.ListState;
pub const MapState = state.MapState;

// Window subsystem
pub const window = struct {
    pub const assigner = @import("window/assigner.zig");
    pub const TimeWindow = assigner.TimeWindow;
    pub const WindowAssigner = assigner.WindowAssigner;

    pub const trigger = @import("window/trigger.zig");
    pub const TriggerType = trigger.TriggerType;
    pub const TriggerResult = trigger.TriggerResult;

    pub const function = @import("window/function.zig");
    pub const WindowFunction = function.WindowFunction;
    pub const ReduceWindowFn = function.ReduceWindowFn;
    pub const AggregateWindowFns = function.AggregateWindowFns;

    pub const window_operator = @import("window/operator.zig");
    pub const WindowOperator = window_operator.WindowOperator;

    // Session windows
    pub const session = @import("window/session.zig");
    pub const SessionWindowManager = session.SessionWindowManager;
    pub const SessionConfig = session.SessionConfig;

    // Late data handling
    pub const lateness = @import("window/lateness.zig");
    pub const LatenessTracker = lateness.LatenessTracker;
    pub const RecordTimeliness = lateness.RecordTimeliness;
};

// Time subsystem
pub const time = struct {
    pub const watermark = @import("time/watermark.zig");
    pub const WatermarkStrategy = watermark.WatermarkStrategy;
    pub const WatermarkGenerator = watermark.WatermarkGenerator;

    pub const tracker = @import("time/tracker.zig");
    pub const WatermarkTracker = tracker.WatermarkTracker;

    pub const timer = @import("time/timer.zig");
    pub const TimerService = timer.TimerService;
    pub const TimerEntry = timer.TimerEntry;
};

// Built-in declarative operators
pub const operators = struct {
    pub const expr_filter = @import("operators/expr_filter.zig");
    pub const ExprFilterOperator = expr_filter.ExprFilterOperator;

    pub const passthrough = @import("operators/passthrough.zig");
    pub const PassthroughOperator = passthrough.PassthroughOperator;

    pub const json_keyby = @import("operators/json_keyby.zig");
    pub const JsonKeyByOperator = json_keyby.JsonKeyByOperator;

    pub const json_aggregate = @import("operators/json_aggregate.zig");
    pub const JsonAggregateOperator = json_aggregate.JsonAggregateOperator;

    pub const json_map = @import("operators/json_map.zig");
    pub const JsonMapOperator = json_map.JsonMapOperator;

    pub const json_flatmap = @import("operators/json_flatmap.zig");
    pub const JsonFlatMapOperator = json_flatmap.JsonFlatMapOperator;

    pub const native_registry = @import("operators/native_registry.zig");
};

// Checkpoint subsystem
pub const checkpoint = struct {
    pub const storage = @import("checkpoint/storage.zig");
    pub const CheckpointStore = storage.CheckpointStore;
    pub const CheckpointMeta = storage.CheckpointMeta;
    pub const CheckpointStatus = storage.CheckpointStatus;

    pub const offsets = @import("checkpoint/offsets.zig");
    pub const SourceOffsetTracker = offsets.SourceOffsetTracker;

    pub const snapshot_mod = @import("checkpoint/snapshot.zig");
    pub const CheckpointSnapshot = snapshot_mod.CheckpointSnapshot;

    pub const coordinator = @import("checkpoint/coordinator.zig");
    pub const CheckpointCoordinator = coordinator.CheckpointCoordinator;

    pub const alignment = @import("checkpoint/alignment.zig");
    pub const BarrierAligner = alignment.BarrierAligner;
    pub const AlignmentResult = alignment.AlignmentResult;

    pub const recovery = @import("checkpoint/recovery.zig");
    pub const RecoveryManager = recovery.RecoveryManager;
    pub const RecoveryResult = recovery.RecoveryResult;
};

// Side outputs
pub const side_output = @import("side_output.zig");
pub const SideOutputManager = side_output.SideOutputManager;
pub const OutputTag = side_output.OutputTag;

// Processing metrics
pub const metrics_mod = @import("metrics.zig");
pub const LatencyHistogram = metrics_mod.LatencyHistogram;
pub const ThroughputCounter = metrics_mod.ThroughputCounter;
pub const BackpressureGauge = metrics_mod.BackpressureGauge;
pub const PipelineMetrics = metrics_mod.PipelineMetrics;

// Handler (Job Manager)
pub const handler = @import("handler.zig");

// =============================================================================
// Test imports — ensures all processing tests are discovered
// =============================================================================
test {
    // Foundation
    _ = @import("record.zig");
    _ = @import("collector.zig");
    _ = @import("context.zig");
    _ = @import("operator.zig");
    _ = @import("endpoints/source.zig");
    _ = @import("endpoints/sink.zig");
    _ = @import("topology.zig");
    _ = @import("chain.zig");
    // Declarative operators
    _ = @import("operators/expr_filter.zig");
    _ = @import("operators/passthrough.zig");
    _ = @import("operators/json_keyby.zig");
    _ = @import("operators/json_aggregate.zig");
    _ = @import("operators/json_map.zig");
    _ = @import("operators/json_flatmap.zig");
    _ = @import("operators/native_registry.zig");
    // Keyed State
    _ = @import("state.zig");
    // Windowing
    _ = @import("window/assigner.zig");
    _ = @import("window/trigger.zig");
    _ = @import("window/function.zig");
    _ = @import("window/operator.zig");
    // Time
    _ = @import("time/watermark.zig");
    _ = @import("time/tracker.zig");
    _ = @import("time/timer.zig");
    // Checkpointing
    _ = @import("checkpoint/storage.zig");
    _ = @import("checkpoint/offsets.zig");
    _ = @import("checkpoint/snapshot.zig");
    _ = @import("checkpoint/coordinator.zig");
    _ = @import("checkpoint/alignment.zig");
    _ = @import("checkpoint/recovery.zig");
    // Side outputs & Metrics
    _ = @import("side_output.zig");
    _ = @import("metrics.zig");
    _ = @import("window/session.zig");
    _ = @import("window/lateness.zig");
    // Keys
    _ = @import("keys.zig");
    // Handler/Job Manager
    _ = @import("handler.zig");
    // Definition & Parser
    _ = @import("definition.zig");
    _ = @import("parser.zig");
}
