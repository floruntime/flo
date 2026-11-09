//! Two valid namespace names whose 32-bit hashes, the old way of telling
//! namespaces apart below the edge (`@truncate(Wyhash.hash(0, name))`), are
//! equal. Under that hash their streams, queues, series and keys shared one
//! map slot; with dense namespace ids they must not.
//!
//! Found by a birthday search over "ns0", "ns1", ...: the first repeat of a
//! truncated hash came at "ns77364", equal to "ns46332"'s (0x30cb4f98).

const std = @import("std");

pub const a = "ns46332";
pub const b = "ns77364";

fn oldHash(name: []const u8) u32 {
    return @truncate(std.hash.Wyhash.hash(0, name));
}

comptime {
    @setEvalBranchQuota(10_000);
    if (oldHash(a) != oldHash(b)) @compileError("the pair no longer collides on the old hash");
}
