//! Sealing for an established peer link. Once the handshake has ended,
//! every frame either way is AEAD-sealed: the header travels in the clear
//! (the receiver needs its length) but is authenticated, the payload is
//! encrypted, and a 16-byte tag follows it.
//!
//! Each direction has its own key, derived from the cluster secret and a
//! hash of every handshake frame both sides exchanged — both hellos with
//! the addresses they advertised, both nonces and ids, both proofs and the
//! verdict — so a link's keys are its own: nothing recorded from another
//! link, or altered in this one's handshake, opens here. The nonce is a
//! counter both ends keep in step, never sent; a frame replayed, reordered,
//! dropped or altered fails to open, and the link closes.

const std = @import("std");
const Aead = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
const Hkdf = std.crypto.kdf.hkdf.HkdfSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const TAG_LEN: usize = Aead.tag_length;
pub const KEY_LEN: usize = Aead.key_length;

/// The running hash of a link's handshake, frame bytes in the order they
/// crossed the wire.
pub const Transcript = struct {
    hash: Sha256 = Sha256.init(.{}),

    pub fn add(self: *Transcript, frame: []const u8) void {
        self.hash.update(frame);
    }

    /// The digest so far, leaving the transcript open for more.
    pub fn peek(self: *const Transcript) [Sha256.digest_length]u8 {
        var copy = self.hash;
        var out: [Sha256.digest_length]u8 = undefined;
        copy.final(&out);
        return out;
    }
};

pub const Direction = enum {
    dialer_to_acceptor,
    acceptor_to_dialer,

    fn label(self: Direction) []const u8 {
        return switch (self) {
            .dialer_to_acceptor => "flo peer link: dialer to acceptor",
            .acceptor_to_dialer => "flo peer link: acceptor to dialer",
        };
    }
};

/// One direction's key and the count of frames sealed (or opened) under it.
pub const Key = struct {
    key: [KEY_LEN]u8,
    counter: u64 = 0,

    pub fn derive(secret: []const u8, transcript: *const [Sha256.digest_length]u8, direction: Direction) Key {
        var prk = Hkdf.extract("flo peer link v2", secret);
        defer std.crypto.secureZero(u8, &prk);
        var info: [Sha256.digest_length + 64]u8 = undefined;
        const label = direction.label();
        @memcpy(info[0..Sha256.digest_length], transcript);
        @memcpy(info[Sha256.digest_length..][0..label.len], label);
        var key: [KEY_LEN]u8 = undefined;
        Hkdf.expand(&key, info[0 .. Sha256.digest_length + label.len], prk);
        return .{ .key = key };
    }

    fn nonce(self: *const Key) [Aead.nonce_length]u8 {
        var n = [_]u8{0} ** Aead.nonce_length;
        std.mem.writeInt(u64, n[4..12], self.counter, .little);
        return n;
    }

    /// Seal `payload` under `header` into `out` (payload length plus
    /// `TAG_LEN`), and move to the next counter.
    pub fn seal(self: *Key, header: []const u8, payload: []const u8, out: []u8) void {
        std.debug.assert(out.len == payload.len + TAG_LEN);
        Aead.encrypt(out[0..payload.len], out[payload.len..][0..TAG_LEN], payload, header, self.nonce(), self.key);
        self.counter += 1;
    }

    /// Open a sealed body (ciphertext then tag) in place; the plaintext is
    /// its first `body.len - TAG_LEN` bytes.
    pub fn open(self: *Key, header: []const u8, body: []u8) error{AuthenticationFailed}!void {
        std.debug.assert(body.len >= TAG_LEN);
        const len = body.len - TAG_LEN;
        try Aead.decrypt(body[0..len], body[0..len], body[len..][0..TAG_LEN].*, header, self.nonce(), self.key);
        self.counter += 1;
    }

    pub fn wipe(self: *Key) void {
        std.crypto.secureZero(u8, &self.key);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn pair(secret: []const u8, frames: []const []const u8) struct { send: Key, recv: Key } {
    var t: Transcript = .{};
    for (frames) |f| t.add(f);
    const digest = t.peek();
    return .{ .send = Key.derive(secret, &digest, .dialer_to_acceptor), .recv = Key.derive(secret, &digest, .dialer_to_acceptor) };
}

test "seal: a frame sealed one end opens at the other, in order, and the counters move together" {
    var k = pair("s", &.{ "hello", "hello back", "verify", "welcome" });
    const header = "HEADER-20-BYTES-----";
    var bodies: [3][5 + TAG_LEN]u8 = undefined;
    for (&bodies, [_][]const u8{ "first", "secnd", "third" }) |*b, msg| k.send.seal(header, msg, b);
    for (&bodies, [_][]const u8{ "first", "secnd", "third" }) |*b, msg| {
        try k.recv.open(header, b);
        try testing.expectEqualStrings(msg, b[0..5]);
    }
    try testing.expectEqual(@as(u64, 3), k.send.counter);
    try testing.expectEqual(@as(u64, 3), k.recv.counter);
}

test "seal: an altered header or body, a replay, a skipped frame, or another transcript does not open" {
    const header = "HEADER-20-BYTES-----";
    var k = pair("s", &.{ "a", "b" });
    var body: [4 + TAG_LEN]u8 = undefined;

    // An altered header.
    k.send.seal(header, "data", &body);
    try testing.expectError(error.AuthenticationFailed, k.recv.open("HEADER-20-BYTES----X", &body));

    // An altered body.
    k = pair("s", &.{ "a", "b" });
    k.send.seal(header, "data", &body);
    body[0] ^= 1;
    try testing.expectError(error.AuthenticationFailed, k.recv.open(header, &body));

    // A replay: the counter has moved past it.
    k = pair("s", &.{ "a", "b" });
    k.send.seal(header, "data", &body);
    const copy = body;
    try k.recv.open(header, &body);
    body = copy;
    try testing.expectError(error.AuthenticationFailed, k.recv.open(header, &body));

    // A dropped frame: the next one is sealed under a counter not reached.
    k = pair("s", &.{ "a", "b" });
    k.send.seal(header, "lost", &body);
    k.send.seal(header, "next", &body);
    try testing.expectError(error.AuthenticationFailed, k.recv.open(header, &body));

    // A different handshake, or a different secret, gives different keys.
    k = pair("s", &.{ "a", "b" });
    var other = pair("s", &.{ "a", "c" });
    k.send.seal(header, "data", &body);
    try testing.expectError(error.AuthenticationFailed, other.recv.open(header, &body));
    other = pair("t", &.{ "a", "b" });
    k = pair("s", &.{ "a", "b" });
    k.send.seal(header, "data", &body);
    try testing.expectError(error.AuthenticationFailed, other.recv.open(header, &body));
}

test "seal: the two directions of a link have different keys" {
    var t: Transcript = .{};
    t.add("handshake");
    const d = t.peek();
    const a = Key.derive("s", &d, .dialer_to_acceptor);
    const b = Key.derive("s", &d, .acceptor_to_dialer);
    try testing.expect(!std.mem.eql(u8, &a.key, &b.key));
}
