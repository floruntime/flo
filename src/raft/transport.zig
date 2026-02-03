//! Raft Per-Shard TCP Transport — Wire format, serialization, framing.
//!
//! Each shard owns TCP connections to the same shard index on remote nodes.
//! Multiple Raft groups share one connection, demuxed by `group_id`.
//!
//! Wire format (per §12.5 NODE_NETWORK_DESIGN.md):
//!
//!   ┌─────────┬──────┬──────────┬─────────────┬─────────────┬─────────┐
//!   │msg_type │ _pad │ group_id │ source_node  │ payload_len │  crc32  │
//!   │  u8     │ [3]  │  u32     │   u32        │   u32       │  u32    │
//!   │ 1 byte  │  3   │ 4 bytes  │  4 bytes     │  4 bytes    │ 4 bytes │
//!   └─────────┴──────┴──────────┴─────────────┴─────────────┴─────────┘
//!   │                              payload (payload_len bytes)           │
//!   └─────────────────────────────────────────────────────────────────────┘
//!
//! CRC32C covers header[0..16] (everything except crc32 field) + payload.
//! Port scheme: raft_port(shard) = base_port + 500 + shard_id

const std = @import("std");
const Allocator = std.mem.Allocator;
const node_mod = @import("node.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const checksum_mod = @import("../util/checksum.zig");

const NodeId = node_mod.NodeId;
const VoteRequest = node_mod.VoteRequest;
const VoteResponse = node_mod.VoteResponse;
const AppendRequest = node_mod.AppendRequest;
const AppendResponse = node_mod.AppendResponse;
const Entry = entry_mod.Entry;
const ENTRY_HEADER_SIZE = entry_mod.HEADER_SIZE;

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub const HEADER_SIZE: usize = 20;
pub const RAFT_PORT_OFFSET: u16 = 500;
pub const MAX_PAYLOAD_SIZE: usize = 4 * 1024 * 1024; // 4 MB

// ═══════════════════════════════════════════════════════════════════════════════
// Message Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const MsgType = enum(u8) {
    append_entries = 1,
    append_entries_response = 2,
    request_vote = 3,
    request_vote_response = 4,
    install_snapshot = 5,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Raft Header — 20 bytes, extern for exact memory layout
// ═══════════════════════════════════════════════════════════════════════════════

pub const RaftHeader = extern struct {
    msg_type: u8 align(1),
    _pad: [3]u8 align(1),
    group_id: u32 align(1),
    source_node: u32 align(1),
    payload_len: u32 align(1),
    crc32: u32 align(1),

    comptime {
        if (@sizeOf(RaftHeader) != HEADER_SIZE) @compileError("RaftHeader must be 20 bytes");
    }

    pub fn asBytes(self: *const RaftHeader) *const [HEADER_SIZE]u8 {
        return @ptrCast(self);
    }

    pub fn fromBytes(data: *const [HEADER_SIZE]u8) *const RaftHeader {
        return @ptrCast(@alignCast(data));
    }

    pub fn msgType(self: *const RaftHeader) ?MsgType {
        return std.meta.intToEnum(MsgType, self.msg_type) catch null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// CRC
// ═══════════════════════════════════════════════════════════════════════════════

/// CRC covers the first 16 bytes of header + the full payload.
const CRC_HEADER_BYTES: usize = HEADER_SIZE - @sizeOf(u32); // 16

pub fn computeCrc(header_bytes: *const [HEADER_SIZE]u8, payload: []const u8) u32 {
    var stream = checksum_mod.ChecksumStream.init();
    stream.add(header_bytes[0..CRC_HEADER_BYTES]);
    if (payload.len > 0) {
        stream.add(payload);
    }
    return stream.checksum();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Port helpers
// ═══════════════════════════════════════════════════════════════════════════════

pub fn raftPort(base_port: u16, shard_id: u16) u16 {
    return base_port + RAFT_PORT_OFFSET + shard_id;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization — VoteRequest (29 bytes)
// ═══════════════════════════════════════════════════════════════════════════════

pub const VOTE_REQ_SIZE: usize = 29;

pub fn serializeVoteRequest(req: VoteRequest, buf: []u8) ?usize {
    if (buf.len < VOTE_REQ_SIZE) return null;
    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], req.term, .little);
    off += 8;
    std.mem.writeInt(u32, buf[off..][0..4], req.candidate_id, .little);
    off += 4;
    std.mem.writeInt(u64, buf[off..][0..8], req.last_log_index, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.last_log_term, .little);
    off += 8;
    buf[off] = if (req.is_pre_vote) 1 else 0;
    off += 1;
    return off;
}

pub fn deserializeVoteRequest(data: []const u8) ?VoteRequest {
    if (data.len < VOTE_REQ_SIZE) return null;
    var off: usize = 0;
    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const cand = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    const last_idx = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const last_term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const pre_vote = data[off] != 0;

    return .{
        .term = term,
        .candidate_id = cand,
        .last_log_index = last_idx,
        .last_log_term = last_term,
        .is_pre_vote = pre_vote,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization — VoteResponse (13 bytes)
// ═══════════════════════════════════════════════════════════════════════════════

pub const VOTE_RESP_SIZE: usize = 13;

pub fn serializeVoteResponse(resp: VoteResponse, buf: []u8) ?usize {
    if (buf.len < VOTE_RESP_SIZE) return null;
    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], resp.term, .little);
    off += 8;
    buf[off] = if (resp.vote_granted) 1 else 0;
    off += 1;
    std.mem.writeInt(u32, buf[off..][0..4], resp.from, .little);
    off += 4;
    return off;
}

pub fn deserializeVoteResponse(data: []const u8) ?VoteResponse {
    if (data.len < VOTE_RESP_SIZE) return null;
    var off: usize = 0;
    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const granted = data[off] != 0;
    off += 1;
    const from = std.mem.readInt(u32, data[off..][0..4], .little);

    return .{
        .term = term,
        .vote_granted = granted,
        .from = from,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization — AppendResponse (21 bytes)
// ═══════════════════════════════════════════════════════════════════════════════

pub const APPEND_RESP_SIZE: usize = 21;

pub fn serializeAppendResponse(resp: AppendResponse, buf: []u8) ?usize {
    if (buf.len < APPEND_RESP_SIZE) return null;
    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], resp.term, .little);
    off += 8;
    buf[off] = if (resp.success) 1 else 0;
    off += 1;
    std.mem.writeInt(u64, buf[off..][0..8], resp.match_index, .little);
    off += 8;
    std.mem.writeInt(u32, buf[off..][0..4], resp.from, .little);
    off += 4;
    return off;
}

pub fn deserializeAppendResponse(data: []const u8) ?AppendResponse {
    if (data.len < APPEND_RESP_SIZE) return null;
    var off: usize = 0;
    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const success = data[off] != 0;
    off += 1;
    const match_idx = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const from = std.mem.readInt(u32, data[off..][0..4], .little);

    return .{
        .term = term,
        .success = success,
        .match_index = match_idx,
        .from = from,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization — AppendRequest (variable size)
//   Fixed prefix: 40 bytes (term, leader_id, prev_log_index, prev_log_term,
//                           leader_commit, entry_count)
//   Then N entries, each: 40-byte header + payload_len payload bytes.
// ═══════════════════════════════════════════════════════════════════════════════

pub const APPEND_REQ_PREFIX: usize = 40;

/// Compute the serialized size for an AppendRequest.
pub fn appendRequestSize(req: AppendRequest) usize {
    var size: usize = APPEND_REQ_PREFIX;
    for (req.entries) |*e| {
        size += e.totalSize();
    }
    return size;
}

pub fn serializeAppendRequest(req: AppendRequest, buf: []u8) ?usize {
    const needed = appendRequestSize(req);
    if (buf.len < needed) return null;

    var off: usize = 0;
    std.mem.writeInt(u64, buf[off..][0..8], req.term, .little);
    off += 8;
    std.mem.writeInt(u32, buf[off..][0..4], req.leader_id, .little);
    off += 4;
    std.mem.writeInt(u64, buf[off..][0..8], req.prev_log_index, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.prev_log_term, .little);
    off += 8;
    std.mem.writeInt(u64, buf[off..][0..8], req.leader_commit, .little);
    off += 8;
    std.mem.writeInt(u32, buf[off..][0..4], @intCast(req.entries.len), .little);
    off += 4;

    // Serialize each entry
    for (req.entries) |*e| {
        const written = e.serialize(buf[off..]) orelse return null;
        off += written;
    }

    return off;
}

/// Deserialize AppendRequest header (entries returned as raw byte range).
/// Caller must parse individual entries from the returned payload slice.
pub const AppendRequestHeader = struct {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entry_count: u32,
    entries_data: []const u8,
};

pub fn deserializeAppendRequestHeader(data: []const u8) ?AppendRequestHeader {
    if (data.len < APPEND_REQ_PREFIX) return null;
    var off: usize = 0;

    const term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const leader_id = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    const prev_idx = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const prev_term = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const leader_commit = std.mem.readInt(u64, data[off..][0..8], .little);
    off += 8;
    const entry_count = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    return .{
        .term = term,
        .leader_id = leader_id,
        .prev_log_index = prev_idx,
        .prev_log_term = prev_term,
        .leader_commit = leader_commit,
        .entry_count = entry_count,
        .entries_data = data[off..],
    };
}

/// Parse individual entries from the entries_data portion.
/// Returns entries and count. Entries reference the input data (zero-copy payload).
pub fn parseEntries(entries_data: []const u8, entry_count: u32, out: []Entry) usize {
    var off: usize = 0;
    var count: usize = 0;
    const limit: usize = @intCast(entry_count);

    while (count < limit and count < out.len and off < entries_data.len) {
        if (Entry.deserialize(entries_data[off..])) |e| {
            out[count] = e;
            off += e.totalSize();
            count += 1;
        } else {
            break;
        }
    }

    return count;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Full Message Framing — Header + Payload
// ═══════════════════════════════════════════════════════════════════════════════

/// Encode a complete framed message (header + payload) into buf.
/// Returns the total bytes written (HEADER_SIZE + payload_len).
pub fn encodeMessage(
    msg_type: MsgType,
    group_id: u32,
    source_node: NodeId,
    payload: []const u8,
    buf: []u8,
) ?usize {
    const total = HEADER_SIZE + payload.len;
    if (buf.len < total) return null;

    var header = RaftHeader{
        .msg_type = @intFromEnum(msg_type),
        ._pad = .{ 0, 0, 0 },
        .group_id = group_id,
        .source_node = source_node,
        .payload_len = @intCast(payload.len),
        .crc32 = 0,
    };

    // Write header to buf first (for CRC computation)
    const hdr_bytes = header.asBytes();
    @memcpy(buf[0..HEADER_SIZE], hdr_bytes);

    // Write payload
    if (payload.len > 0) {
        @memcpy(buf[HEADER_SIZE..total], payload);
    }

    // Compute CRC over header[0..16] + payload
    const crc = computeCrc(buf[0..HEADER_SIZE], payload);
    std.mem.writeInt(u32, buf[16..20], crc, .little);

    return total;
}

/// Decode a framed message from a buffer. Validates CRC.
/// Returns the header and payload slice (referencing the input buffer).
pub const DecodedMessage = struct {
    header: RaftHeader,
    payload: []const u8,
};

pub fn decodeMessage(buf: []const u8) ?DecodedMessage {
    if (buf.len < HEADER_SIZE) return null;

    const header = RaftHeader.fromBytes(buf[0..HEADER_SIZE]);
    const payload_len: usize = @intCast(header.payload_len);

    if (payload_len > MAX_PAYLOAD_SIZE) return null;
    if (buf.len < HEADER_SIZE + payload_len) return null;

    const payload = buf[HEADER_SIZE .. HEADER_SIZE + payload_len];

    // Validate CRC
    const expected_crc = computeCrc(buf[0..HEADER_SIZE], payload);
    if (header.crc32 != expected_crc) return null;

    return .{
        .header = header.*,
        .payload = payload,
    };
}

/// Total message size from a header (for reading from a stream).
pub fn messageSize(header: *const RaftHeader) usize {
    return HEADER_SIZE + @as(usize, @intCast(header.payload_len));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "transport: RaftHeader is exactly 20 bytes" {
    try testing.expectEqual(@as(usize, 20), @sizeOf(RaftHeader));
    try testing.expectEqual(@as(usize, 20), HEADER_SIZE);
}

test "transport: header roundtrip" {
    var header = RaftHeader{
        .msg_type = @intFromEnum(MsgType.request_vote),
        ._pad = .{ 0, 0, 0 },
        .group_id = 1000,
        .source_node = 42,
        .payload_len = 256,
        .crc32 = 0xDEADBEEF,
    };

    const bytes = header.asBytes();
    const recovered = RaftHeader.fromBytes(bytes);

    try testing.expectEqual(header.msg_type, recovered.msg_type);
    try testing.expectEqual(header.group_id, recovered.group_id);
    try testing.expectEqual(header.source_node, recovered.source_node);
    try testing.expectEqual(header.payload_len, recovered.payload_len);
    try testing.expectEqual(header.crc32, recovered.crc32);
}

test "transport: VoteRequest roundtrip" {
    const req = VoteRequest{
        .term = 42,
        .candidate_id = 7,
        .last_log_index = 100,
        .last_log_term = 5,
        .is_pre_vote = true,
    };

    var buf: [64]u8 = undefined;
    const written = serializeVoteRequest(req, &buf);
    try testing.expect(written != null);
    try testing.expectEqual(@as(usize, VOTE_REQ_SIZE), written.?);

    const recovered = deserializeVoteRequest(&buf);
    try testing.expect(recovered != null);

    const r = recovered.?;
    try testing.expectEqual(req.term, r.term);
    try testing.expectEqual(req.candidate_id, r.candidate_id);
    try testing.expectEqual(req.last_log_index, r.last_log_index);
    try testing.expectEqual(req.last_log_term, r.last_log_term);
    try testing.expectEqual(req.is_pre_vote, r.is_pre_vote);
}

test "transport: VoteResponse roundtrip" {
    const resp = VoteResponse{
        .term = 10,
        .vote_granted = true,
        .from = 3,
    };

    var buf: [32]u8 = undefined;
    const written = serializeVoteResponse(resp, &buf);
    try testing.expect(written != null);

    const recovered = deserializeVoteResponse(&buf).?;
    try testing.expectEqual(resp.term, recovered.term);
    try testing.expectEqual(resp.vote_granted, recovered.vote_granted);
    try testing.expectEqual(resp.from, recovered.from);
}

test "transport: AppendResponse roundtrip" {
    const resp = AppendResponse{
        .term = 55,
        .success = false,
        .match_index = 999,
        .from = 2,
    };

    var buf: [32]u8 = undefined;
    const written = serializeAppendResponse(resp, &buf);
    try testing.expect(written != null);

    const recovered = deserializeAppendResponse(&buf).?;
    try testing.expectEqual(resp.term, recovered.term);
    try testing.expectEqual(resp.success, recovered.success);
    try testing.expectEqual(resp.match_index, recovered.match_index);
    try testing.expectEqual(resp.from, recovered.from);
}

test "transport: AppendRequest roundtrip with entries" {
    // Build some entries (buildEntry already computes CRC)
    const e1 = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 1, 0, "hello");
    const e2 = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 2, 0, "world");

    const entries = [_]Entry{ e1, e2 };
    const req = AppendRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    var buf: [4096]u8 = undefined;
    const written = serializeAppendRequest(req, &buf);
    try testing.expect(written != null);

    // Deserialize header
    const header = deserializeAppendRequestHeader(buf[0..written.?]).?;
    try testing.expectEqual(@as(u64, 1), header.term);
    try testing.expectEqual(@as(u32, 1), header.leader_id);
    try testing.expectEqual(@as(u32, 2), header.entry_count);

    // Parse entries
    var out: [8]Entry = undefined;
    const count = parseEntries(header.entries_data, header.entry_count, &out);
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expectEqual(@as(u64, 1), out[0].header.index);
    try testing.expectEqual(@as(u64, 2), out[1].header.index);
}

test "transport: AppendRequest roundtrip with zero entries (heartbeat)" {
    const req = AppendRequest{
        .term = 5,
        .leader_id = 1,
        .prev_log_index = 10,
        .prev_log_term = 4,
        .entries = &[_]Entry{},
        .leader_commit = 8,
    };

    var buf: [256]u8 = undefined;
    const written = serializeAppendRequest(req, &buf);
    try testing.expect(written != null);
    try testing.expectEqual(@as(usize, APPEND_REQ_PREFIX), written.?);

    const header = deserializeAppendRequestHeader(buf[0..written.?]).?;
    try testing.expectEqual(@as(u64, 5), header.term);
    try testing.expectEqual(@as(u32, 0), header.entry_count);
    try testing.expectEqual(@as(u64, 8), header.leader_commit);
}

test "transport: full message framing roundtrip" {
    const vote = VoteRequest{
        .term = 3,
        .candidate_id = 2,
        .last_log_index = 5,
        .last_log_term = 2,
    };

    var payload_buf: [64]u8 = undefined;
    const plen = serializeVoteRequest(vote, &payload_buf).?;

    var msg_buf: [256]u8 = undefined;
    const total = encodeMessage(.request_vote, 1000, 2, payload_buf[0..plen], &msg_buf).?;

    try testing.expectEqual(HEADER_SIZE + plen, total);

    // Decode
    const decoded = decodeMessage(msg_buf[0..total]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.request_vote)), decoded.header.msg_type);
    try testing.expectEqual(@as(u32, 1000), decoded.header.group_id);
    try testing.expectEqual(@as(u32, 2), decoded.header.source_node);
    try testing.expectEqual(@as(usize, plen), decoded.payload.len);

    // Deserialize payload
    const recovered = deserializeVoteRequest(decoded.payload).?;
    try testing.expectEqual(vote.term, recovered.term);
    try testing.expectEqual(vote.candidate_id, recovered.candidate_id);
}

test "transport: CRC corruption detected" {
    var payload_buf: [16]u8 = undefined;
    const plen = serializeVoteResponse(.{
        .term = 1,
        .vote_granted = true,
        .from = 1,
    }, &payload_buf).?;

    var msg_buf: [256]u8 = undefined;
    const total = encodeMessage(.request_vote_response, 1000, 1, payload_buf[0..plen], &msg_buf).?;

    // Verify valid decode works
    try testing.expect(decodeMessage(msg_buf[0..total]) != null);

    // Corrupt one payload byte
    msg_buf[HEADER_SIZE + 2] ^= 0xFF;

    // CRC check should fail
    try testing.expect(decodeMessage(msg_buf[0..total]) == null);
}

test "transport: multiple messages in a stream" {
    var stream_buf: [1024]u8 = undefined;
    var offset: usize = 0;

    // Write 3 messages
    var pb1: [64]u8 = undefined;
    const p1 = serializeVoteRequest(.{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    }, &pb1).?;
    const m1 = encodeMessage(.request_vote, 1000, 1, pb1[0..p1], stream_buf[offset..]).?;
    offset += m1;

    var pb2: [32]u8 = undefined;
    const p2 = serializeVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 }, &pb2).?;
    const m2 = encodeMessage(.request_vote_response, 1000, 2, pb2[0..p2], stream_buf[offset..]).?;
    offset += m2;

    var pb3: [32]u8 = undefined;
    const p3 = serializeAppendResponse(.{ .term = 1, .success = true, .match_index = 5, .from = 2 }, &pb3).?;
    const m3 = encodeMessage(.append_entries_response, 1000, 2, pb3[0..p3], stream_buf[offset..]).?;
    offset += m3;

    // Read all 3 back
    var read_off: usize = 0;

    const d1 = decodeMessage(stream_buf[read_off..offset]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.request_vote)), d1.header.msg_type);
    read_off += HEADER_SIZE + d1.payload.len;

    const d2 = decodeMessage(stream_buf[read_off..offset]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.request_vote_response)), d2.header.msg_type);
    read_off += HEADER_SIZE + d2.payload.len;

    const d3 = decodeMessage(stream_buf[read_off..offset]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.append_entries_response)), d3.header.msg_type);
}

test "transport: raft port calculation" {
    try testing.expectEqual(@as(u16, 9500), raftPort(9000, 0));
    try testing.expectEqual(@as(u16, 9501), raftPort(9000, 1));
    try testing.expectEqual(@as(u16, 9507), raftPort(9000, 7));
}

test "transport: 2-shard message exchange simulation" {
    // Simulate shard 0 on node 1 sending to shard 0 on node 2

    // Shard 0 / Node 1: leader sends AppendEntries heartbeat
    const ae_payload = [_]u8{};
    var ae_buf: [256]u8 = undefined;
    const ae_plen = serializeAppendRequest(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 10,
        .prev_log_term = 2,
        .entries = &[_]Entry{},
        .leader_commit = 9,
    }, &ae_buf).?;
    _ = ae_payload;

    // "Wire" buffer simulating TCP
    var wire: [512]u8 = undefined;
    const msg_len = encodeMessage(.append_entries, 1000, 1, ae_buf[0..ae_plen], &wire).?;

    // Shard 0 / Node 2: receives and decodes
    const decoded = decodeMessage(wire[0..msg_len]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.append_entries)), decoded.header.msg_type);
    try testing.expectEqual(@as(u32, 1000), decoded.header.group_id);
    try testing.expectEqual(@as(u32, 1), decoded.header.source_node);

    const ae_header = deserializeAppendRequestHeader(decoded.payload).?;
    try testing.expectEqual(@as(u64, 3), ae_header.term);
    try testing.expectEqual(@as(u32, 1), ae_header.leader_id);
    try testing.expectEqual(@as(u64, 9), ae_header.leader_commit);

    // Node 2 sends response back
    var resp_buf: [64]u8 = undefined;
    const resp_len = serializeAppendResponse(.{
        .term = 3,
        .success = true,
        .match_index = 10,
        .from = 2,
    }, &resp_buf).?;

    var wire2: [256]u8 = undefined;
    const resp_msg_len = encodeMessage(.append_entries_response, 1000, 2, resp_buf[0..resp_len], &wire2).?;

    // Node 1 receives response
    const resp_decoded = decodeMessage(wire2[0..resp_msg_len]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(MsgType.append_entries_response)), resp_decoded.header.msg_type);

    const ar = deserializeAppendResponse(resp_decoded.payload).?;
    try testing.expectEqual(@as(u64, 3), ar.term);
    try testing.expect(ar.success);
    try testing.expectEqual(@as(u64, 10), ar.match_index);
    try testing.expectEqual(@as(u32, 2), ar.from);
}
