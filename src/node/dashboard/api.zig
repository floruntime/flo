//! Dashboard REST API Router
//!
//! Thin router that delegates to domain-specific modules in api/.
//! This is the only public interface — `handleRequest()` is called
//! by http_server.zig.
//!
//! ## Module Structure
//!
//! api/helpers.zig    — Shared types, BinaryReader, query parsing, routing
//! api/namespaces.zig — Namespace endpoints (cross-cutting)
//! api/kv.zig         — Key-value endpoints
//! api/streams.zig    — Stream & consumer group endpoints
//! api/queues.zig     — Queue endpoints
//! api/actions.zig    — Actions & workers endpoints
//! api/system.zig     — Cluster stats & metrics
//! api/workflows.zig  — Workflow & processing endpoints
//! api/timeseries.zig — Time-series measurement endpoints

const std = @import("std");
const Allocator = std.mem.Allocator;

const h = @import("api/helpers.zig");
const namespaces = @import("api/namespaces.zig");
const kv = @import("api/kv.zig");
const streams = @import("api/streams.zig");
const queues = @import("api/queues.zig");
const actions = @import("api/actions.zig");
const system = @import("api/system.zig");
const workflows = @import("api/workflows.zig");
const processing = @import("api/processing.zig");
const timeseries = @import("api/timeseries.zig");

const Dispatcher = h.Dispatcher;
const Core = h.Core;
const MetricsRegistry = h.MetricsRegistry;
const Method = @import("../../util/http/mod.zig").Method;

/// Handle an API request and return JSON response
/// Caller owns returned memory
pub fn handleRequest(
    allocator: Allocator,
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    body: []const u8,
    dispatchers: []*Dispatcher,
    cores: ?[]*Core,
    metrics: *MetricsRegistry,
) ![]const u8 {
    // === Namespaces (first-class resource) ===
    if (std.mem.eql(u8, path, "namespaces")) {
        return try namespaces.getNamespaces(allocator, dispatchers, cores, metrics);
    } else if (std.mem.startsWith(u8, path, "namespaces/")) {
        const rest = path[11..];
        if (std.mem.indexOf(u8, rest, "/")) |slash_idx| {
            const namespace = rest[0..slash_idx];
            const sub_resource = rest[slash_idx + 1 ..];
            if (std.mem.eql(u8, sub_resource, "streams")) {
                return try namespaces.getNamespaceStreams(allocator, namespace, dispatchers, cores, metrics);
            } else if (std.mem.eql(u8, sub_resource, "queues")) {
                return try namespaces.getNamespaceQueues(allocator, namespace, dispatchers, metrics);
            } else if (std.mem.eql(u8, sub_resource, "kv")) {
                return try namespaces.getNamespaceKV(allocator, namespace, dispatchers, metrics);
            } else {
                return try h.jsonError(allocator, "Unknown namespace sub-resource");
            }
        } else {
            return try namespaces.getNamespaceDetail(allocator, rest, dispatchers, cores, metrics);
        }
    }

    // === Streams & Consumer Groups ===
    else if (std.mem.eql(u8, path, "streams")) {
        return try streams.getStreams(allocator, dispatchers, cores, metrics);
    } else if (std.mem.startsWith(u8, path, "streams/")) {
        const rest = path[8..];
        if (std.mem.indexOf(u8, rest, "/")) |slash_idx| {
            const stream_name = rest[0..slash_idx];
            const sub = rest[slash_idx + 1 ..];
            if (std.mem.eql(u8, sub, "messages")) {
                return try streams.getStreamMessages(allocator, stream_name, query_string, dispatchers, cores);
            } else if (std.mem.startsWith(u8, sub, "groups/")) {
                const groups_rest = sub[7..];
                if (std.mem.indexOf(u8, groups_rest, "/")) |group_slash| {
                    const group_name = groups_rest[0..group_slash];
                    const group_sub = groups_rest[group_slash + 1 ..];
                    if (std.mem.eql(u8, group_sub, "pending")) {
                        return try streams.getGroupPending(allocator, stream_name, group_name, dispatchers, cores);
                    } else if (std.mem.eql(u8, group_sub, "members")) {
                        return try streams.getGroupMembers(allocator, stream_name, group_name, cores);
                    } else {
                        return try h.jsonError(allocator, "Unknown group sub-resource");
                    }
                } else {
                    return try streams.getGroupDetail(allocator, stream_name, groups_rest, dispatchers, cores);
                }
            } else {
                return try h.jsonError(allocator, "Unknown stream sub-resource");
            }
        } else {
            return try streams.getStreamDetail(allocator, rest, dispatchers, cores, metrics);
        }
    }

    // === Queues ===
    else if (std.mem.eql(u8, path, "queues")) {
        return try queues.getQueues(allocator, dispatchers, metrics);
    } else if (std.mem.startsWith(u8, path, "queues/")) {
        return try queues.getQueueDetail(allocator, path[7..], dispatchers, metrics);
    }

    // === KV ===
    else if (std.mem.eql(u8, path, "kv/namespaces")) {
        return try kv.getKVNamespaces(allocator, dispatchers, metrics);
    } else if (std.mem.startsWith(u8, path, "kv/namespaces/")) {
        const rest = path["kv/namespaces/".len..];
        if (std.mem.indexOf(u8, rest, "/")) |slash_idx| {
            const ns_name = rest[0..slash_idx];
            const sub = rest[slash_idx + 1 ..];
            if (std.mem.eql(u8, sub, "keys")) {
                return try kv.getKVKeys(allocator, ns_name, query_string, dispatchers);
            } else if (std.mem.startsWith(u8, sub, "keys/")) {
                const key_rest = sub["keys/".len..];
                if (std.mem.indexOf(u8, key_rest, "/")) |key_slash| {
                    const key_name = key_rest[0..key_slash];
                    const key_sub = key_rest[key_slash + 1 ..];
                    if (std.mem.eql(u8, key_sub, "history")) {
                        return try kv.getKVKeyHistory(allocator, ns_name, key_name, query_string, dispatchers);
                    } else {
                        return try h.jsonError(allocator, "Unknown KV key sub-resource");
                    }
                } else {
                    // /kv/namespaces/:ns/keys/:key — method-aware
                    if (method == .PUT) {
                        return try kv.putKVKey(allocator, ns_name, key_rest, body, dispatchers);
                    } else if (method == .DELETE) {
                        return try kv.deleteKVKey(allocator, ns_name, key_rest, dispatchers);
                    } else {
                        return try kv.getKVKeyValue(allocator, ns_name, key_rest, query_string, dispatchers);
                    }
                }
            } else {
                return try h.jsonError(allocator, "Unknown KV namespace sub-resource");
            }
        } else {
            return try namespaces.getNamespaceKV(allocator, rest, dispatchers, metrics);
        }
    }

    // === Time Series ===
    else if (std.mem.eql(u8, path, "timeseries")) {
        return try timeseries.getMeasurements(allocator, query_string, dispatchers, cores);
    } else if (std.mem.startsWith(u8, path, "timeseries/")) {
        const rest = path[11..]; // after "timeseries/"
        // FloQL query execution — must match before generic :measurement routes
        if (std.mem.eql(u8, rest, "floql")) {
            return try timeseries.executeFloql(allocator, method, query_string, body, dispatchers);
        }
        // Check for sub-resource: timeseries/:measurement/data
        if (std.mem.indexOf(u8, rest, "/data")) |slash_pos| {
            const mname = rest[0..slash_pos];
            return try timeseries.getSeriesData(allocator, mname, query_string, dispatchers, cores);
        }
        return try timeseries.getMeasurementDetail(allocator, rest, query_string, dispatchers, cores);
    }

    // === Workflows ===
    else if (std.mem.startsWith(u8, path, "workflow/")) {
        return try workflows.handleWorkflowRequest(allocator, method, path["workflow/".len..], query_string, body, dispatchers);
    }

    // === Processing ===
    else if (std.mem.eql(u8, path, "processing/jobs") or std.mem.startsWith(u8, path, "processing/")) {
        return try processing.handleProcessingRequest(allocator, method, path["processing/".len..], query_string, body, dispatchers);
    }

    // === Actions & Workers ===
    else if (std.mem.eql(u8, path, "actions")) {
        return try actions.getActions(allocator, dispatchers, cores);
    } else if (std.mem.startsWith(u8, path, "actions/")) {
        const rest = path[8..];
        if (std.mem.indexOf(u8, rest, "/")) |slash_idx| {
            const action_name = rest[0..slash_idx];
            const sub = rest[slash_idx + 1 ..];
            if (std.mem.eql(u8, sub, "runs")) {
                return try actions.getActionRuns(allocator, action_name, query_string, dispatchers, cores);
            } else if (std.mem.eql(u8, sub, "invoke")) {
                return try actions.invokeAction(allocator, action_name, dispatchers);
            } else {
                return try h.jsonError(allocator, "Unknown action sub-resource");
            }
        } else {
            return try actions.getActionDetail(allocator, rest, dispatchers, cores);
        }
    } else if (std.mem.eql(u8, path, "workers")) {
        return try actions.getWorkers(allocator, dispatchers, cores);
    }

    // === System ===
    else if (std.mem.eql(u8, path, "cluster/stats")) {
        return try system.getClusterStats(allocator, dispatchers, metrics);

    } else if (std.mem.eql(u8, path, "metrics")) {
        return try system.getMetricsJson(allocator, metrics);
    } else {
        return try h.jsonError(allocator, "Unknown endpoint");
    }
}
