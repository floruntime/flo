//! Processing Key Schema
//!
//! Key layout for Flo Processing jobs.
//!
//! 1. **Internal Prefix**: All keys start with `_` to distinguish from user KV data
//! 2. **Namespace via KV Layer**: Namespace is passed separately in KV commands;
//!    the KV layer adds `ns:{namespace}:kv:{key}` prefix internally
//! 3. **Prefix Scans**: Keys structured for efficient prefix-based iteration
//!
//! # Key Patterns
//!
//! ## Job Metadata Keys (primary state — persisted via Raft)
//! ```
//! _proc:{job_id}
//! ```
//! Contains encoded job state: name, source/sink streams, operators,
//! parallelism, records_processed, timestamps, definition_yaml,
//! error_message, savepoint_id.
//!
//! ## Job Namespace Index Keys (namespace-scoped listing)
//! ```
//! _proc_idx:{namespace}:{job_id}
//! ```
//! Marker value — used by `processing_list` to enumerate jobs in a namespace.
//!
//! ## Status Index Keys (ns=_sys — query by state)
//! ```
//! _proc:idx:status:{status}:{job_id}
//! ```
//! Enables "list all failed jobs" / "list all running jobs" queries.
//!
//! ## Time Index Keys (ns=_sys — query by creation time)
//! ```
//! _proc:idx:time:{padded_timestamp}:{job_id}
//! ```
//! Enables "jobs created in the last hour" range queries.
//!
//! ## History Stream (ns=_sys — append-only event log per job)
//! ```
//! _proc:hist:{job_id}
//! ```
//! Stream of lifecycle events: submitted, running, stopped, failed, etc.
//! Each record is a JSON event with timestamp, event type, and details.

const std = @import("std");
const mem = std.mem;

/// System namespace for global processing data (indexes, history streams)
pub const SYS_NAMESPACE = "_sys";

/// Key segment prefixes — all start with `_` to mark as internal
const proc_seg = "_proc:";
const proc_idx_seg = "_proc_idx:";
const proc_status_seg = "_proc:idx:status:";
const proc_time_seg = "_proc:idx:time:";
const proc_hist_seg = "_proc:hist:";
const sep: u8 = ':';

/// Maximum key size
pub const max_key_size = 512;

/// Marker value for index keys where only existence matters
pub const marker_value: []const u8 = &[_]u8{1};

// =============================================================================
// Job Metadata Keys
// =============================================================================

/// Build job metadata key: _proc:{job_id}
pub fn keyJob(allocator: mem.Allocator, job_id: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, proc_seg);
    try buf.appendSlice(allocator, job_id);

    return buf.toOwnedSlice(allocator);
}

/// Build job namespace index key: _proc_idx:{namespace}:{job_id}
pub fn keyJobIndex(allocator: mem.Allocator, namespace: []const u8, job_id: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, proc_idx_seg);
    try buf.appendSlice(allocator, namespace);
    try buf.append(allocator, sep);
    try buf.appendSlice(allocator, job_id);

    return buf.toOwnedSlice(allocator);
}

/// Build index prefix for scanning: _proc_idx:{namespace}:
pub fn keyJobIndexPrefix(allocator: mem.Allocator, namespace: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, proc_idx_seg);
    try buf.appendSlice(allocator, namespace);
    try buf.append(allocator, sep);

    return buf.toOwnedSlice(allocator);
}

/// Extract job_id from a job metadata key: _proc:{job_id} → job_id
pub fn extractJobIdFromKey(key: []const u8) ?[]const u8 {
    if (key.len <= proc_seg.len) return null;
    if (!mem.startsWith(u8, key, proc_seg)) return null;
    return key[proc_seg.len..];
}

/// Extract job_id from an index key: _proc_idx:{namespace}:{job_id} → job_id
pub fn extractJobIdFromIndex(key: []const u8) ?[]const u8 {
    if (!mem.startsWith(u8, key, proc_idx_seg)) return null;
    const after_prefix = key[proc_idx_seg.len..];
    // Find the separator after namespace
    const sep_pos = mem.indexOf(u8, after_prefix, &[_]u8{sep}) orelse return null;
    if (sep_pos + 1 >= after_prefix.len) return null;
    return after_prefix[sep_pos + 1 ..];
}

// =============================================================================
// Status Index Keys (ns=_sys)
// =============================================================================

/// Build status index key: _proc:idx:status:{status}:{job_id}
pub fn keyStatusIndex(allocator: mem.Allocator, status: []const u8, job_id: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}{s}:{s}", .{ proc_status_seg, status, job_id });
}

/// Build status index prefix for scanning: _proc:idx:status:{status}:
pub fn keyStatusPrefix(allocator: mem.Allocator, status: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}{s}:", .{ proc_status_seg, status });
}

/// Extract status and job_id from a status index key
pub fn parseStatusIndex(key: []const u8) ?struct { status: []const u8, job_id: []const u8 } {
    if (!mem.startsWith(u8, key, proc_status_seg)) return null;
    const rest = key[proc_status_seg.len..];
    const colon = mem.indexOfScalar(u8, rest, sep) orelse return null;
    if (colon + 1 >= rest.len) return null;
    return .{ .status = rest[0..colon], .job_id = rest[colon + 1 ..] };
}

// =============================================================================
// Time Index Keys (ns=_sys)
// =============================================================================

/// Build time index key: _proc:idx:time:{padded_timestamp}:{job_id}
pub fn keyTimeIndex(allocator: mem.Allocator, timestamp_ms: i64, job_id: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}{d:0>20}:{s}", .{
        proc_time_seg,
        @as(u64, @bitCast(timestamp_ms)),
        job_id,
    });
}

/// Build time index prefix for scanning: _proc:idx:time:
pub fn keyTimePrefix(allocator: mem.Allocator) ![]u8 {
    return allocator.dupe(u8, proc_time_seg);
}

// =============================================================================
// History Stream Names (ns=_sys)
// =============================================================================

/// Get stream name for job history: _proc:hist:{job_id}
pub fn historyStreamName(allocator: mem.Allocator, job_id: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}{s}", .{ proc_hist_seg, job_id });
}

/// Parse history stream name to extract job_id
pub fn parseHistoryStreamName(name: []const u8) ?[]const u8 {
    if (!mem.startsWith(u8, name, proc_hist_seg)) return null;
    const job_id = name[proc_hist_seg.len..];
    if (job_id.len == 0) return null;
    return job_id;
}

// =============================================================================
// Tests
// =============================================================================

test "keyJob" {
    const allocator = std.testing.allocator;
    const key = try keyJob(allocator, "job-abc123-42");
    defer allocator.free(key);
    try std.testing.expectEqualStrings("_proc:job-abc123-42", key);
}

test "keyJobIndex" {
    const allocator = std.testing.allocator;
    const key = try keyJobIndex(allocator, "default", "job-abc123-42");
    defer allocator.free(key);
    try std.testing.expectEqualStrings("_proc_idx:default:job-abc123-42", key);
}

test "keyJobIndexPrefix" {
    const allocator = std.testing.allocator;
    const prefix = try keyJobIndexPrefix(allocator, "prod");
    defer allocator.free(prefix);
    try std.testing.expectEqualStrings("_proc_idx:prod:", prefix);
}

test "extractJobIdFromKey" {
    try std.testing.expectEqualStrings("job-123", extractJobIdFromKey("_proc:job-123").?);
    try std.testing.expect(extractJobIdFromKey("_proc:") == null);
    try std.testing.expect(extractJobIdFromKey("_other:job-123") == null);
}

test "extractJobIdFromIndex" {
    try std.testing.expectEqualStrings("job-123", extractJobIdFromIndex("_proc_idx:default:job-123").?);
    try std.testing.expectEqualStrings("job-456", extractJobIdFromIndex("_proc_idx:my-ns:job-456").?);
    try std.testing.expect(extractJobIdFromIndex("_proc_idx:default") == null);
    try std.testing.expect(extractJobIdFromIndex("_proc_idx:") == null);
}

test "keyStatusIndex" {
    const allocator = std.testing.allocator;
    const key = try keyStatusIndex(allocator, "RUNNING", "job-abc123");
    defer allocator.free(key);
    try std.testing.expectEqualStrings("_proc:idx:status:RUNNING:job-abc123", key);
}

test "parseStatusIndex" {
    const result = parseStatusIndex("_proc:idx:status:FAILED:job-xyz").?;
    try std.testing.expectEqualStrings("FAILED", result.status);
    try std.testing.expectEqualStrings("job-xyz", result.job_id);
    try std.testing.expect(parseStatusIndex("_proc:idx:status:RUNNING") == null);
    try std.testing.expect(parseStatusIndex("_other:idx:status:RUNNING:j") == null);
}

test "keyTimeIndex" {
    const allocator = std.testing.allocator;
    const key = try keyTimeIndex(allocator, 1708000000000, "job-abc");
    defer allocator.free(key);
    try std.testing.expect(std.mem.startsWith(u8, key, "_proc:idx:time:"));
    try std.testing.expect(std.mem.endsWith(u8, key, ":job-abc"));
}

test "historyStreamName" {
    const allocator = std.testing.allocator;
    const name = try historyStreamName(allocator, "job-abc123");
    defer allocator.free(name);
    try std.testing.expectEqualStrings("_proc:hist:job-abc123", name);

    const parsed = parseHistoryStreamName(name).?;
    try std.testing.expectEqualStrings("job-abc123", parsed);
    try std.testing.expect(parseHistoryStreamName("_proc:hist:") == null);
    try std.testing.expect(parseHistoryStreamName("_other:hist:x") == null);
}
