//! Raft Snapshot Transfer — InstallSnapshot RPC for slow followers.
//!
//! When a follower is too far behind for log replication (the needed entries
//! have been compacted), the leader sends its latest snapshot using the
//! InstallSnapshot RPC, streamed in chunks.
//!
//! Protocol:
//!   1. Leader detects follower.next_index <= snapshot_index (entries compacted)
//!   2. Leader reads .fsnap file, creates a SnapshotSender
//!   3. Leader sends InstallSnapshotRequest chunks via transport (msg_type = 5)
//!   4. Follower SnapshotReceiver reassembles chunks into .fsnap bytes
//!   5. On final chunk: follower validates assembled snapshot, installs it
//!   6. Follower resets log to snapshot_index + 1, catches up via normal replication
//!
//! Wire format for InstallSnapshotRequest (45-byte prefix + chunk data):
//!   term:u64 | leader_id:u32 | snapshot_index:u64 | snapshot_term:u64 |
//!   chunk_offset:u64 | total_size:u64 | done:u8 | chunk_data:varies
//!
//! Wire format for InstallSnapshotResponse (21 bytes):
//!   term:u64 | success:u8 | from:u32 | bytes_received:u64

const std = @import("std");
const Allocator = std.mem.Allocator;
const node_mod = @import("node.zig");
const snapshot_mod = @import("../storage/snapshot.zig");
const checksum_mod = @import("../util/checksum.zig");

const NodeId = node_mod.NodeId;

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

/// Default chunk size: 256 KB. Fits well within transport MAX_PAYLOAD_SIZE (4MB).
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Maximum total snapshot size we'll accept: 1 GB.
pub const MAX_SNAPSHOT_SIZE: u64 = 1024 * 1024 * 1024;

// ═══════════════════════════════════════════════════════════════════════════════
// InstallSnapshotRequest — sent by leader, one per chunk
// ═══════════════════════════════════════════════════════════════════════════════

pub const InstallSnapshotRequest = struct {
    term: u64,
    leader_id: NodeId,
    snapshot_index: u64,
    snapshot_term: u64,
    chunk_offset: u64,
    total_size: u64,
    done: bool,
    chunk_data: []const u8,
};

pub const INSTALL_REQ_PREFIX: usize = 45;

pub fn serializeInstallRequest(req: InstallSnapshotRequest, buf: []u8) ?usize {
    const needed = INSTALL_REQ_PREFIX + req.chunk_data.len;
    if (buf.len < needed) return null;

    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], req.term, .little);
    off += 8;
    std.mem.writeInt(u32, buf[off..][0..4], req.leader_id, .little);
    off += 4;
    std.mem.writeInt(u64, buf[off..][0..8], req.snapshot_index, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.snapshot_term, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.chunk_offset, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.total_size, .little);
    off += 8;
    buf[off] = if (req.done) 1 else 0;
    off += 1;

    if (req.chunk_data.len > 0) {
        @memcpy(buf[off .. off + req.chunk_data.len], req.chunk_data);
        off += req.chunk_data.len;
    }

    return off;
}

pub fn deserializeInstallRequest(data: []const u8) ?InstallSnapshotRequest {
    if (data.len < INSTALL_REQ_PREFIX) return null;

    var off: usize = 0;
    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const leader_id = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    const snap_index = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const snap_term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const chunk_offset = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const total_size = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const done = data[off] != 0;
    off += 1;

    return .{
        .term = term,
        .leader_id = leader_id,
        .snapshot_index = snap_index,
        .snapshot_term = snap_term,
        .chunk_offset = chunk_offset,
        .total_size = total_size,
        .done = done,
        .chunk_data = data[off..],
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// InstallSnapshotResponse — returned by follower
// ═══════════════════════════════════════════════════════════════════════════════

pub const InstallSnapshotResponse = struct {
    term: u64,
    success: bool,
    from: NodeId,
    bytes_received: u64,
};

pub const INSTALL_RESP_SIZE: usize = 21;

pub fn serializeInstallResponse(resp: InstallSnapshotResponse, buf: []u8) ?usize {
    if (buf.len < INSTALL_RESP_SIZE) return null;

    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], resp.term, .little);
    off += 8;
    buf[off] = if (resp.success) 1 else 0;
    off += 1;
    std.mem.writeInt(u32, buf[off..][0..4], resp.from, .little);
    off += 4;
    std.mem.writeInt(u64, buf[off..][0..8], resp.bytes_received, .little);
    off += 8;

    return off;
}

pub fn deserializeInstallResponse(data: []const u8) ?InstallSnapshotResponse {
    if (data.len < INSTALL_RESP_SIZE) return null;

    var off: usize = 0;
    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const success = data[off] != 0;
    off += 1;
    const from = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    const bytes_received = std.mem.readInt(u64, data[off..][0..8], .little);

    return .{
        .term = term,
        .success = success,
        .from = from,
        .bytes_received = bytes_received,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SnapshotSender — leader side. Manages chunked transfer of a .fsnap.
// ═══════════════════════════════════════════════════════════════════════════════

pub const SenderState = enum(u8) {
    sending,
    awaiting_ack,
    complete,
    failed,
};

pub const SnapshotSender = struct {
    snapshot_data: []const u8,
    snapshot_index: u64,
    snapshot_term: u64,
    chunk_size: usize,
    bytes_sent: u64,
    state: SenderState,

    /// Stats for observability.
    chunks_sent: u32,
    chunks_acked: u32,
    retries: u32,

    pub fn init(
        snapshot_data: []const u8,
        snapshot_index: u64,
        snapshot_term: u64,
        chunk_size: usize,
    ) SnapshotSender {
        return .{
            .snapshot_data = snapshot_data,
            .snapshot_index = snapshot_index,
            .snapshot_term = snapshot_term,
            .chunk_size = if (chunk_size == 0) DEFAULT_CHUNK_SIZE else chunk_size,
            .bytes_sent = 0,
            .state = .sending,
            .chunks_sent = 0,
            .chunks_acked = 0,
            .retries = 0,
        };
    }

    /// Build the next InstallSnapshotRequest to send.
    /// Returns null if transfer is complete or failed.
    pub fn nextChunk(self: *SnapshotSender, term: u64, leader_id: NodeId) ?InstallSnapshotRequest {
        if (self.state == .complete or self.state == .failed) return null;
        if (self.state == .awaiting_ack) return null;

        const offset: usize = @intCast(self.bytes_sent);
        const remaining = self.snapshot_data.len - offset;
        if (remaining == 0) {
            self.state = .complete;
            return null;
        }

        const this_chunk = @min(remaining, self.chunk_size);
        const is_done = (offset + this_chunk) >= self.snapshot_data.len;

        self.state = .awaiting_ack;
        self.chunks_sent += 1;

        return .{
            .term = term,
            .leader_id = leader_id,
            .snapshot_index = self.snapshot_index,
            .snapshot_term = self.snapshot_term,
            .chunk_offset = self.bytes_sent,
            .total_size = @intCast(self.snapshot_data.len),
            .done = is_done,
            .chunk_data = self.snapshot_data[offset .. offset + this_chunk],
        };
    }

    /// Handle a response from the follower.
    pub const AckResult = enum { send_next, complete, retry, failed };

    pub fn handleResponse(self: *SnapshotSender, resp: InstallSnapshotResponse) AckResult {
        if (!resp.success) {
            self.retries += 1;
            if (self.retries >= 10) {
                self.state = .failed;
                return .failed;
            }
            // Retry: stay at same offset, allow nextChunk again
            self.state = .sending;
            return .retry;
        }

        self.chunks_acked += 1;

        // Advance past acknowledged bytes
        const offset: usize = @intCast(self.bytes_sent);
        const remaining = self.snapshot_data.len - offset;
        const this_chunk = @min(remaining, self.chunk_size);
        self.bytes_sent += @intCast(this_chunk);

        if (self.bytes_sent >= self.snapshot_data.len) {
            self.state = .complete;
            return .complete;
        }

        self.state = .sending;
        return .send_next;
    }

    pub fn isComplete(self: *const SnapshotSender) bool {
        return self.state == .complete;
    }

    pub fn progress(self: *const SnapshotSender) f32 {
        if (self.snapshot_data.len == 0) return 1.0;
        return @as(f32, @floatFromInt(self.bytes_sent)) /
            @as(f32, @floatFromInt(self.snapshot_data.len));
    }

    pub fn totalChunksExpected(self: *const SnapshotSender) u32 {
        if (self.snapshot_data.len == 0) return 0;
        return @intCast((self.snapshot_data.len + self.chunk_size - 1) / self.chunk_size);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// SnapshotReceiver — follower side. Reassembles chunks into .fsnap bytes.
// ═══════════════════════════════════════════════════════════════════════════════

pub const ReceiverState = enum(u8) {
    idle,
    receiving,
    complete,
    failed,
};

pub const SnapshotReceiver = struct {
    allocator: Allocator,
    buf: ?[]u8,
    total_size: u64,
    bytes_received: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    leader_id: NodeId,
    state: ReceiverState,

    /// Stats.
    chunks_received: u32,

    pub fn init(allocator: Allocator) SnapshotReceiver {
        return .{
            .allocator = allocator,
            .buf = null,
            .total_size = 0,
            .bytes_received = 0,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .leader_id = 0,
            .state = .idle,
            .chunks_received = 0,
        };
    }

    pub fn deinit(self: *SnapshotReceiver) void {
        if (self.buf) |b| {
            self.allocator.free(b);
            self.buf = null;
        }
    }

    /// Receive a chunk. Returns true when the snapshot is fully assembled.
    pub fn receiveChunk(self: *SnapshotReceiver, req: InstallSnapshotRequest) !bool {
        // Reject oversized snapshots
        if (req.total_size > MAX_SNAPSHOT_SIZE) return error.SnapshotTooLarge;

        switch (self.state) {
            .idle => {
                // First chunk: allocate buffer
                if (req.chunk_offset != 0) return error.InvalidChunkOffset;
                const size: usize = @intCast(req.total_size);
                self.buf = try self.allocator.alloc(u8, size);
                self.total_size = req.total_size;
                self.snapshot_index = req.snapshot_index;
                self.snapshot_term = req.snapshot_term;
                self.leader_id = req.leader_id;
                self.state = .receiving;
            },
            .receiving => {
                // Subsequent chunk: validate consistency
                if (req.snapshot_index != self.snapshot_index or
                    req.snapshot_term != self.snapshot_term)
                {
                    // New snapshot supersedes — reset
                    self.reset();
                    return self.receiveChunk(req);
                }
                if (req.chunk_offset != self.bytes_received) {
                    return error.InvalidChunkOffset;
                }
            },
            .complete, .failed => {
                return error.InvalidState;
            },
        }

        // Copy chunk data into buffer
        const buf = self.buf orelse return error.InvalidState;
        const offset: usize = @intCast(req.chunk_offset);
        const end = offset + req.chunk_data.len;

        if (end > buf.len) return error.ChunkOverflow;
        @memcpy(buf[offset..end], req.chunk_data);
        self.bytes_received += @intCast(req.chunk_data.len);
        self.chunks_received += 1;

        if (req.done) {
            if (self.bytes_received != self.total_size) {
                self.state = .failed;
                return error.IncompleteFinalChunk;
            }

            // Validate the assembled snapshot using SnapshotReader
            _ = snapshot_mod.SnapshotReader.init(buf) catch {
                self.state = .failed;
                return error.InvalidSnapshot;
            };

            self.state = .complete;
            return true;
        }

        return false;
    }

    /// Reset the receiver for a new transfer.
    pub fn reset(self: *SnapshotReceiver) void {
        if (self.buf) |b| {
            self.allocator.free(b);
            self.buf = null;
        }
        self.total_size = 0;
        self.bytes_received = 0;
        self.snapshot_index = 0;
        self.snapshot_term = 0;
        self.leader_id = 0;
        self.state = .idle;
        self.chunks_received = 0;
    }

    /// Get the assembled snapshot data. Only valid when state == .complete.
    pub fn snapshotData(self: *const SnapshotReceiver) ?[]const u8 {
        if (self.state != .complete) return null;
        return self.buf;
    }

    pub fn progressFloat(self: *const SnapshotReceiver) f32 {
        if (self.total_size == 0) return 0.0;
        return @as(f32, @floatFromInt(self.bytes_received)) /
            @as(f32, @floatFromInt(self.total_size));
    }

    pub fn isComplete(self: *const SnapshotReceiver) bool {
        return self.state == .complete;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers — determine if a follower needs snapshot
// ═══════════════════════════════════════════════════════════════════════════════

/// Returns true if the follower's next_index is behind the leader's earliest
/// available log entry (i.e., the log has been compacted past that point).
pub fn followerNeedsSnapshot(
    follower_next_index: u64,
    earliest_log_index: u64,
) bool {
    return follower_next_index < earliest_log_index;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Helper: build a minimal valid .fsnap snapshot using SnapshotBuilder.
fn buildTestSnapshot(allocator: Allocator, snap_index: u64, snap_term: u64) ![]u8 {
    var builder = snapshot_mod.SnapshotBuilder.init(allocator, 0, snap_index, snap_term, 1000);
    defer builder.deinit();

    // Add a small KV section
    try builder.addSection(.kv, "test-kv-data");

    return try builder.seal();
}

test "snapshot: InstallSnapshotRequest roundtrip" {
    const chunk = "Hello, snapshot chunk data!";
    const req = InstallSnapshotRequest{
        .term = 10,
        .leader_id = 1,
        .snapshot_index = 500,
        .snapshot_term = 8,
        .chunk_offset = 1024,
        .total_size = 65536,
        .done = false,
        .chunk_data = chunk,
    };

    var buf: [256]u8 = undefined;
    const written = serializeInstallRequest(req, &buf);
    try testing.expect(written != null);
    try testing.expectEqual(@as(usize, INSTALL_REQ_PREFIX + chunk.len), written.?);

    const recovered = deserializeInstallRequest(buf[0..written.?]).?;
    try testing.expectEqual(req.term, recovered.term);
    try testing.expectEqual(req.leader_id, recovered.leader_id);
    try testing.expectEqual(req.snapshot_index, recovered.snapshot_index);
    try testing.expectEqual(req.snapshot_term, recovered.snapshot_term);
    try testing.expectEqual(req.chunk_offset, recovered.chunk_offset);
    try testing.expectEqual(req.total_size, recovered.total_size);
    try testing.expectEqual(req.done, recovered.done);
    try testing.expectEqualSlices(u8, chunk, recovered.chunk_data);
}

test "snapshot: InstallSnapshotResponse roundtrip" {
    const resp = InstallSnapshotResponse{
        .term = 10,
        .success = true,
        .from = 2,
        .bytes_received = 4096,
    };

    var buf: [32]u8 = undefined;
    const written = serializeInstallResponse(resp, &buf);
    try testing.expect(written != null);

    const recovered = deserializeInstallResponse(buf[0..written.?]).?;
    try testing.expectEqual(resp.term, recovered.term);
    try testing.expectEqual(resp.success, recovered.success);
    try testing.expectEqual(resp.from, recovered.from);
    try testing.expectEqual(resp.bytes_received, recovered.bytes_received);
}

test "snapshot: sender single-chunk transfer" {
    // Small snapshot that fits in one chunk
    const data = "small-snapshot-data";
    var sender = SnapshotSender.init(data, 100, 5, 1024);

    try testing.expectEqual(@as(u32, 1), sender.totalChunksExpected());

    // Get the one chunk
    const chunk = sender.nextChunk(5, 1).?;
    try testing.expectEqual(@as(u64, 100), chunk.snapshot_index);
    try testing.expectEqual(@as(u64, 5), chunk.snapshot_term);
    try testing.expectEqual(@as(u64, 0), chunk.chunk_offset);
    try testing.expect(chunk.done);
    try testing.expectEqualSlices(u8, data, chunk.chunk_data);

    // Awaiting ack — nextChunk returns null
    try testing.expect(sender.nextChunk(5, 1) == null);

    // Ack it
    const result = sender.handleResponse(.{
        .term = 5,
        .success = true,
        .from = 2,
        .bytes_received = data.len,
    });
    try testing.expectEqual(SnapshotSender.AckResult.complete, result);
    try testing.expect(sender.isComplete());
    try testing.expectEqual(@as(f32, 1.0), sender.progress());
}

test "snapshot: sender multi-chunk transfer" {
    // 100 bytes with 30-byte chunks → 4 chunks (30+30+30+10)
    const data = "A" ** 100;
    var sender = SnapshotSender.init(data, 200, 3, 30);

    try testing.expectEqual(@as(u32, 4), sender.totalChunksExpected());

    var chunks_received: usize = 0;
    while (!sender.isComplete()) {
        const chunk = sender.nextChunk(3, 1) orelse break;
        chunks_received += 1;

        if (chunks_received < 4) {
            try testing.expect(!chunk.done);
        } else {
            try testing.expect(chunk.done);
        }

        const ack = sender.handleResponse(.{
            .term = 3,
            .success = true,
            .from = 2,
            .bytes_received = chunk.chunk_data.len,
        });

        if (chunks_received == 4) {
            try testing.expectEqual(SnapshotSender.AckResult.complete, ack);
        } else {
            try testing.expectEqual(SnapshotSender.AckResult.send_next, ack);
        }
    }

    try testing.expectEqual(@as(usize, 4), chunks_received);
    try testing.expect(sender.isComplete());
}

test "snapshot: sender retry on failure" {
    const data = "retry-test-data";
    var sender = SnapshotSender.init(data, 100, 5, 1024);

    _ = sender.nextChunk(5, 1).?;

    // Fail the ack
    const result = sender.handleResponse(.{
        .term = 5,
        .success = false,
        .from = 2,
        .bytes_received = 0,
    });
    try testing.expectEqual(SnapshotSender.AckResult.retry, result);
    try testing.expectEqual(@as(u32, 1), sender.retries);
    try testing.expect(!sender.isComplete());

    // Can get next chunk again (retransmit)
    const retry_chunk = sender.nextChunk(5, 1).?;
    try testing.expectEqual(@as(u64, 0), retry_chunk.chunk_offset);
}

test "snapshot: receiver single chunk with valid snapshot" {
    const allocator = testing.allocator;
    const snap_data = try buildTestSnapshot(allocator, 100, 5);
    defer allocator.free(snap_data);

    var receiver = SnapshotReceiver.init(allocator);
    defer receiver.deinit();

    const done = try receiver.receiveChunk(.{
        .term = 5,
        .leader_id = 1,
        .snapshot_index = 100,
        .snapshot_term = 5,
        .chunk_offset = 0,
        .total_size = @intCast(snap_data.len),
        .done = true,
        .chunk_data = snap_data,
    });

    try testing.expect(done);
    try testing.expect(receiver.isComplete());
    try testing.expectEqual(@as(u64, 100), receiver.snapshot_index);

    const assembled = receiver.snapshotData().?;
    try testing.expectEqualSlices(u8, snap_data, assembled);
}

test "snapshot: receiver multi-chunk reassembly" {
    const allocator = testing.allocator;
    const snap_data = try buildTestSnapshot(allocator, 200, 7);
    defer allocator.free(snap_data);

    var receiver = SnapshotReceiver.init(allocator);
    defer receiver.deinit();

    // Send in 32-byte chunks
    const chunk_size: usize = 32;
    var offset: usize = 0;
    var chunk_count: usize = 0;

    while (offset < snap_data.len) {
        const end = @min(offset + chunk_size, snap_data.len);
        const is_last = (end == snap_data.len);

        const done = try receiver.receiveChunk(.{
            .term = 7,
            .leader_id = 1,
            .snapshot_index = 200,
            .snapshot_term = 7,
            .chunk_offset = @intCast(offset),
            .total_size = @intCast(snap_data.len),
            .done = is_last,
            .chunk_data = snap_data[offset..end],
        });

        chunk_count += 1;

        if (is_last) {
            try testing.expect(done);
        } else {
            try testing.expect(!done);
        }

        offset = end;
    }

    try testing.expect(chunk_count > 1); // Actually split into multiple chunks
    try testing.expect(receiver.isComplete());

    const assembled = receiver.snapshotData().?;
    try testing.expectEqualSlices(u8, snap_data, assembled);

    // Validate via SnapshotReader
    const reader = try snapshot_mod.SnapshotReader.init(assembled);
    try testing.expectEqual(@as(u64, 200), reader.snapshotIndex());
    try testing.expectEqual(@as(u64, 7), reader.snapshotTerm());
}

test "snapshot: receiver rejects invalid chunk offset" {
    const allocator = testing.allocator;
    var receiver = SnapshotReceiver.init(allocator);
    defer receiver.deinit();

    // First chunk must have offset 0
    const result = receiver.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .snapshot_index = 50,
        .snapshot_term = 1,
        .chunk_offset = 100, // wrong — should be 0 for first chunk
        .total_size = 1000,
        .done = false,
        .chunk_data = "data",
    });

    try testing.expectError(error.InvalidChunkOffset, result);
}

test "snapshot: receiver reset on new snapshot" {
    const allocator = testing.allocator;
    const snap_data = try buildTestSnapshot(allocator, 300, 10);
    defer allocator.free(snap_data);

    var receiver = SnapshotReceiver.init(allocator);
    defer receiver.deinit();

    // Start receiving snapshot at index 100
    _ = try receiver.receiveChunk(.{
        .term = 5,
        .leader_id = 1,
        .snapshot_index = 100,
        .snapshot_term = 5,
        .chunk_offset = 0,
        .total_size = 1000,
        .done = false,
        .chunk_data = "partial-data-12345",
    });

    try testing.expectEqual(ReceiverState.receiving, receiver.state);

    // New snapshot arrives — should reset and start over
    receiver.reset();
    try testing.expectEqual(ReceiverState.idle, receiver.state);

    // Now receive the valid snapshot
    const done = try receiver.receiveChunk(.{
        .term = 10,
        .leader_id = 1,
        .snapshot_index = 300,
        .snapshot_term = 10,
        .chunk_offset = 0,
        .total_size = @intCast(snap_data.len),
        .done = true,
        .chunk_data = snap_data,
    });

    try testing.expect(done);
    try testing.expectEqual(@as(u64, 300), receiver.snapshot_index);
}

test "snapshot: followerNeedsSnapshot" {
    // Follower is behind compaction point
    try testing.expect(followerNeedsSnapshot(5, 100));

    // Follower is at the compaction point
    try testing.expect(!followerNeedsSnapshot(100, 100));

    // Follower is ahead
    try testing.expect(!followerNeedsSnapshot(200, 100));
}

test "snapshot: full sender-receiver transfer simulation" {
    const allocator = testing.allocator;

    // Leader builds a snapshot
    const snap_data = try buildTestSnapshot(allocator, 500, 12);
    defer allocator.free(snap_data);

    // Leader creates sender with small chunks for testing
    var sender = SnapshotSender.init(snap_data, 500, 12, 32);

    // Follower creates receiver
    var receiver = SnapshotReceiver.init(allocator);
    defer receiver.deinit();

    const term: u64 = 12;
    const leader_id: NodeId = 1;
    const follower_id: NodeId = 2;

    // Transfer loop
    var iterations: usize = 0;
    while (!sender.isComplete()) : (iterations += 1) {
        if (iterations > 1000) {
            try testing.expect(false); // infinite loop guard
            break;
        }

        // Leader generates next chunk
        const chunk = sender.nextChunk(term, leader_id) orelse break;

        // "Wire": serialize request
        var wire_buf: [4096]u8 = undefined;
        const wire_len = serializeInstallRequest(chunk, &wire_buf).?;

        // "Wire": deserialize on follower side
        const received = deserializeInstallRequest(wire_buf[0..wire_len]).?;

        // Follower processes chunk
        const done = try receiver.receiveChunk(received);

        // Follower sends response
        const resp = InstallSnapshotResponse{
            .term = term,
            .success = true,
            .from = follower_id,
            .bytes_received = receiver.bytes_received,
        };

        // "Wire": serialize response
        var resp_wire: [64]u8 = undefined;
        const resp_len = serializeInstallResponse(resp, &resp_wire).?;

        // "Wire": deserialize on leader side
        const leader_resp = deserializeInstallResponse(resp_wire[0..resp_len]).?;

        // Leader processes response
        const ack = sender.handleResponse(leader_resp);

        if (done) {
            try testing.expectEqual(SnapshotSender.AckResult.complete, ack);
        } else {
            try testing.expectEqual(SnapshotSender.AckResult.send_next, ack);
        }
    }

    try testing.expect(sender.isComplete());
    try testing.expect(receiver.isComplete());

    // Verify assembled snapshot matches original
    const assembled = receiver.snapshotData().?;
    try testing.expectEqualSlices(u8, snap_data, assembled);

    // Verify it's a structurally valid .fsnap
    const reader = try snapshot_mod.SnapshotReader.init(assembled);
    try testing.expectEqual(@as(u64, 500), reader.snapshotIndex());
    try testing.expectEqual(@as(u64, 12), reader.snapshotTerm());
    try testing.expect(reader.findSection(.kv) != null);
}
