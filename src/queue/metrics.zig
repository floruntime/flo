//! Per-queue counters, exported by the metrics registry. Only what the
//! enqueue apply and the dequeue handler record is kept: a series nothing
//! writes reads as a healthy zero.
const std = @import("std");
const Atomic = std.atomic.Value;

pub const Metrics = struct {
    enqueue_ops_total: Atomic(u64) = Atomic(u64).init(0),
    enqueue_messages_total: Atomic(u64) = Atomic(u64).init(0),
    enqueue_bytes_total: Atomic(u64) = Atomic(u64).init(0),

    dequeue_ops_total: Atomic(u64) = Atomic(u64).init(0),
    dequeue_messages_total: Atomic(u64) = Atomic(u64).init(0),
    dequeue_bytes_total: Atomic(u64) = Atomic(u64).init(0),
    dequeue_empty_total: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() Metrics {
        return .{};
    }

    pub fn recordEnqueue(self: *Metrics, message_count: usize, byte_count: usize) void {
        _ = self.enqueue_ops_total.fetchAdd(1, .monotonic);
        _ = self.enqueue_messages_total.fetchAdd(message_count, .monotonic);
        _ = self.enqueue_bytes_total.fetchAdd(byte_count, .monotonic);
    }

    pub fn recordDequeue(self: *Metrics, message_count: usize, byte_count: usize) void {
        _ = self.dequeue_ops_total.fetchAdd(1, .monotonic);
        _ = self.dequeue_messages_total.fetchAdd(message_count, .monotonic);
        _ = self.dequeue_bytes_total.fetchAdd(byte_count, .monotonic);
    }

    pub fn recordEmptyDequeue(self: *Metrics) void {
        _ = self.dequeue_ops_total.fetchAdd(1, .monotonic);
        _ = self.dequeue_empty_total.fetchAdd(1, .monotonic);
    }

    pub const Snapshot = struct {
        enqueue_ops_total: u64,
        enqueue_messages_total: u64,
        enqueue_bytes_total: u64,
        dequeue_ops_total: u64,
        dequeue_messages_total: u64,
        dequeue_bytes_total: u64,
        dequeue_empty_total: u64,
    };

    pub fn snapshot(self: *const Metrics) Snapshot {
        return .{
            .enqueue_ops_total = self.enqueue_ops_total.load(.monotonic),
            .enqueue_messages_total = self.enqueue_messages_total.load(.monotonic),
            .enqueue_bytes_total = self.enqueue_bytes_total.load(.monotonic),
            .dequeue_ops_total = self.dequeue_ops_total.load(.monotonic),
            .dequeue_messages_total = self.dequeue_messages_total.load(.monotonic),
            .dequeue_bytes_total = self.dequeue_bytes_total.load(.monotonic),
            .dequeue_empty_total = self.dequeue_empty_total.load(.monotonic),
        };
    }
};
