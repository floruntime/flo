//! ReplyTo — where an answer goes.
//!
//! Every place that holds a request until it can be answered (a blocking
//! read's waiter, a parked write, a write forwarded to the leader, the
//! connection a forwarded request runs on) keeps one of these;
//! `Shard.deliverDeferred` sends the answer there.

const std = @import("std");

pub const ReplyTo = union(enum) {
    /// A client connection, owned by `shard`.
    socket: Socket,
    /// A node that forwarded the request to this one over the peer link;
    /// the answer goes back on that link under `forward_id`.
    remote: Remote,

    pub const Socket = struct {
        /// The shard that owns the connection's fd.
        shard: u16,
        fd: i32,
        /// The connection's generation on that shard: an fd closed and
        /// reused by a new client must not receive the old client's answer.
        conn_id: u32,
        /// For a request another shard sent here: the reply slot it holds
        /// there (`ReplyPool`), which the answer names. `NO_SLOT` when the
        /// client is this shard's own.
        slot: u16 = NO_SLOT,
        gen: u32 = 0,
    };

    pub const NO_SLOT: u16 = 0xFFFF;

    pub const Remote = struct {
        node: u32,
        forward_id: u32,
    };

    pub fn socketOf(shard: u16, fd: i32, conn_id: u32) ReplyTo {
        return .{ .socket = .{ .shard = shard, .fd = fd, .conn_id = conn_id } };
    }

    /// Whether this is the given client connection.
    pub fn isSocket(self: ReplyTo, shard: u16, fd: i32, conn_id: u32) bool {
        return switch (self) {
            .socket => |s| s.shard == shard and s.fd == fd and s.conn_id == conn_id,
            .remote => false,
        };
    }
};

test "ReplyTo: a connection is matched by shard, fd and generation together" {
    const a = ReplyTo.socketOf(2, 10, 7);
    try std.testing.expect(a.isSocket(2, 10, 7));
    try std.testing.expect(!a.isSocket(3, 10, 7));
    try std.testing.expect(!a.isSocket(2, 11, 7));
    try std.testing.expect(!a.isSocket(2, 10, 8));
    const r: ReplyTo = .{ .remote = .{ .node = 2, .forward_id = 10 } };
    try std.testing.expect(!r.isSocket(2, 10, 7));
}
