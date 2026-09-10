//! Peer handshake: each side proves it holds the cluster secret before the
//! link carries anything else. Three frames — a hello from the dialer with
//! its nonce, a hello back with the acceptor's nonce and its proof over
//! both, a proof back from the dialer — then the acceptor's verdict. The
//! proof is an HMAC over both nonces and both ids, prover's and verifier's,
//! so a transcript replays to nothing, a proof made for one peer does not
//! verify at another, and the id a frame carries is the id that was
//! proved. The secret itself never crosses the wire.
//!
//! This file is the codec and the arithmetic; the per-socket state machine
//! that drives it lives with the sockets, in the network.

const std = @import("std");
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const VERSION: u16 = 1;
pub const NONCE_LEN: usize = 32;
pub const MAC_LEN: usize = HmacSha256.mac_length;

/// What a peer says about itself: identity and where it can be dialled.
pub const Hello = struct {
    version: u16,
    node_id: u32,
    raft_port: u16,
    main_port: u16,
    ip4: [4]u8,
    nonce: [NONCE_LEN]u8,

    pub const SIZE: usize = 2 + 4 + 2 + 2 + 4 + NONCE_LEN;

    pub fn encode(self: Hello, buf: *[SIZE]u8) void {
        std.mem.writeInt(u16, buf[0..2], self.version, .little);
        std.mem.writeInt(u32, buf[2..6], self.node_id, .little);
        std.mem.writeInt(u16, buf[6..8], self.raft_port, .little);
        std.mem.writeInt(u16, buf[8..10], self.main_port, .little);
        buf[10..14].* = self.ip4;
        buf[14..46].* = self.nonce;
    }

    pub fn decode(buf: *const [SIZE]u8) Hello {
        return .{
            .version = std.mem.readInt(u16, buf[0..2], .little),
            .node_id = std.mem.readInt(u32, buf[2..6], .little),
            .raft_port = std.mem.readInt(u16, buf[6..8], .little),
            .main_port = std.mem.readInt(u16, buf[8..10], .little),
            .ip4 = buf[10..14].*,
            .nonce = buf[14..46].*,
        };
    }
};

/// The acceptor's hello carries its proof; the dialer's proof travels alone.
pub const HELLO_BACK_SIZE: usize = Hello.SIZE + MAC_LEN;
pub const VERIFY_SIZE: usize = MAC_LEN;

pub const Verdict = enum(u8) {
    accepted = 1,
    /// A live link already carries the dialer's id.
    rejected_live_link = 2,
    /// The dialer reached a node that carries its own id: itself, or a
    /// misconfigured twin.
    rejected_same_id = 3,
    /// No slot left for another peer.
    rejected_full = 4,
};

pub const Welcome = struct {
    verdict: Verdict,
    node_id: u32,

    pub const SIZE: usize = 1 + 4;

    pub fn encode(self: Welcome, buf: *[SIZE]u8) void {
        buf[0] = @intFromEnum(self.verdict);
        std.mem.writeInt(u32, buf[1..5], self.node_id, .little);
    }

    pub fn decode(buf: *const [SIZE]u8) ?Welcome {
        const verdict = std.enums.fromInt(Verdict, buf[0]) orelse return null;
        return .{ .verdict = verdict, .node_id = std.mem.readInt(u32, buf[1..5], .little) };
    }
};

/// The proof one side gives: bound to the other side's nonce first, then
/// its own, then the id it claims, then the id it is proving itself to.
/// Binding the verifier's id is what stops a node's own proof, relayed
/// back to it by a stranger, from verifying. The domain string keeps this
/// MAC from ever matching one computed for another purpose with the same
/// secret.
pub fn proof(secret: []const u8, their_nonce: *const [NONCE_LEN]u8, my_nonce: *const [NONCE_LEN]u8, my_node_id: u32, their_node_id: u32) [MAC_LEN]u8 {
    var mac = HmacSha256.init(secret);
    mac.update("flo-raft-hs-1");
    mac.update(their_nonce);
    mac.update(my_nonce);
    var ids: [8]u8 = undefined;
    std.mem.writeInt(u32, ids[0..4], my_node_id, .little);
    std.mem.writeInt(u32, ids[4..8], their_node_id, .little);
    mac.update(&ids);
    var out: [MAC_LEN]u8 = undefined;
    mac.final(&out);
    return out;
}

/// Constant time: a comparison that leaks where it stopped leaks the MAC.
pub fn proofMatches(expected: *const [MAC_LEN]u8, got: *const [MAC_LEN]u8) bool {
    return std.crypto.timing_safe.eql([MAC_LEN]u8, expected.*, got.*);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "handshake: hello and welcome round-trip" {
    var buf: [Hello.SIZE]u8 = undefined;
    const h = Hello{ .version = VERSION, .node_id = 7, .raft_port = 9500, .main_port = 9000, .ip4 = .{ 10, 0, 1, 12 }, .nonce = [_]u8{0x11} ** NONCE_LEN };
    h.encode(&buf);
    const d = Hello.decode(&buf);
    try testing.expectEqual(h.node_id, d.node_id);
    try testing.expectEqual(h.raft_port, d.raft_port);
    try testing.expectEqual(h.main_port, d.main_port);
    try testing.expectEqual(h.ip4, d.ip4);
    try testing.expectEqual(h.nonce, d.nonce);

    var wbuf: [Welcome.SIZE]u8 = undefined;
    (Welcome{ .verdict = .rejected_live_link, .node_id = 3 }).encode(&wbuf);
    const w = Welcome.decode(&wbuf).?;
    try testing.expectEqual(Verdict.rejected_live_link, w.verdict);
    try testing.expectEqual(@as(u32, 3), w.node_id);
    wbuf[0] = 99;
    try testing.expect(Welcome.decode(&wbuf) == null);
}

test "handshake: a proof verifies only with the same secret, nonces and ids, and each side's differs" {
    const nd = [_]u8{1} ** NONCE_LEN;
    const na = [_]u8{2} ** NONCE_LEN;
    // The acceptor (id 2) proves to the dialer (id 1) over (dialer nonce,
    // acceptor nonce, 2, 1); the dialer checks it with the same inputs.
    const from_acceptor = proof("s3cret", &nd, &na, 2, 1);
    try testing.expect(proofMatches(&proof("s3cret", &nd, &na, 2, 1), &from_acceptor));
    try testing.expect(!proofMatches(&proof("other", &nd, &na, 2, 1), &from_acceptor));
    try testing.expect(!proofMatches(&proof("s3cret", &nd, &na, 3, 1), &from_acceptor));
    try testing.expect(!proofMatches(&proof("s3cret", &na, &nd, 2, 1), &from_acceptor));
    // Made for a different verifier, it does not pass at this one: a
    // stranger relaying node 2's proof for node 3 to node 1 fails.
    try testing.expect(!proofMatches(&proof("s3cret", &nd, &na, 2, 3), &from_acceptor));
    // The dialer's proof over the mirrored inputs is a different value, so
    // an acceptor's proof echoed back does not pass as the dialer's.
    const from_dialer = proof("s3cret", &na, &nd, 1, 2);
    try testing.expect(!proofMatches(&from_dialer, &from_acceptor));
}
