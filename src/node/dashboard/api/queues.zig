//! Dashboard API — Queue Endpoints
//!
//! - GET /queues       — All queues
//! - GET /queues/:name — Queue detail

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const Dispatcher = h.Dispatcher;
const MetricsRegistry = h.MetricsRegistry;
const queue_handler = @import("../../../queue/handler.zig");

/// GET /queues - List all queues
pub fn getQueues(allocator: Allocator, dispatchers: []*Dispatcher, metrics: *MetricsRegistry) ![]const u8 {
    _ = dispatchers;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    metrics.mutex.lock();
    defer metrics.mutex.unlock();

    var it = metrics.queues.iterator();
    while (it.next()) |entry| {
        const queue_entry = entry.value_ptr.*;

        // Skip internal/system queues (prefixed with '_')
        if (queue_handler.isInternalQueue(queue_entry.queue)) continue;

        try arr.next();
        const snap = queue_entry.metrics.snapshot();

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", queue_entry.queue);
        try obj.stringField("namespace", queue_entry.namespace);
        try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
        try obj.intField("available", snap.queue_available_current);
        try obj.intField("enqueued", snap.enqueue_ops_total);
        try obj.intField("dequeued", snap.dequeue_ops_total);
        try obj.intField("acked", snap.leases_completed_total);
        try obj.intField("nacked", snap.leases_failed_total);
        try obj.intField("dlq_count", snap.dlq_messages_total);
        try obj.intField("bytes_total", snap.enqueue_bytes_total);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /queues/:name - Queue detail
pub fn getQueueDetail(allocator: Allocator, queue_name: []const u8, dispatchers: []*Dispatcher, metrics: *MetricsRegistry) ![]const u8 {
    _ = dispatchers;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", queue_name);

    metrics.mutex.lock();
    defer metrics.mutex.unlock();

    var found = false;
    var it = metrics.queues.iterator();
    while (it.next()) |entry| {
        const queue_entry = entry.value_ptr.*;
        if (std.mem.eql(u8, queue_entry.queue, queue_name)) {
            const snap = queue_entry.metrics.snapshot();

            try obj.stringField("namespace", queue_entry.namespace);
            try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
            try obj.intField("available", snap.queue_available_current);
            try obj.intField("enqueued", snap.enqueue_ops_total);
            try obj.intField("dequeued", snap.dequeue_ops_total);
            try obj.intField("acked", snap.leases_completed_total);
            try obj.intField("nacked", snap.leases_failed_total);
            try obj.intField("dlq_count", snap.dlq_messages_total);
            try obj.intField("bytes_total", snap.enqueue_bytes_total);
            found = true;
            break;
        }
    }

    if (!found) {
        try obj.stringField("namespace", "default");
        try obj.intField("pending", 0);
        try obj.intField("available", 0);
        try obj.intField("enqueued", 0);
        try obj.intField("dequeued", 0);
        try obj.intField("acked", 0);
        try obj.intField("nacked", 0);
        try obj.intField("dlq_count", 0);
        try obj.intField("bytes_total", 0);
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}
