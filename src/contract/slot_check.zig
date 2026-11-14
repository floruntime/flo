//! For tests: the slot a stored record belongs to, derived from what the
//! store holds. Code that moves a slot's records has to find them this way,
//! so each machine's test checks every family it stores can be placed, and
//! is placed where its entries were routed.

const std = @import("std");
const router = @import("../node/router.zig");
const store_mod = @import("store.zig");

pub const NamespaceId = router.NamespaceId;

/// The namespace an id-keyed record (`[ns:u32 BE][...]`) is in.
pub fn nsOf(key: []const u8) NamespaceId {
    return .of(std.mem.readInt(u32, key[0..4], .big));
}

/// What follows the namespace in an id-keyed record, up to the first NUL:
/// a ready key's scope, a request key's holder.
pub fn nameAfterNs(key: []const u8) []const u8 {
    const rest = key[4..];
    return rest[0 .. std.mem.indexOfScalar(u8, rest, 0) orelse rest.len];
}

/// Checks every record of family `fam` in `store` against `want`, the slot
/// its entries were routed by: `derive` gives a record's slot from its key
/// and value (null when it can't), and a record it can't place fails as one
/// placed wrong. Returns how many it checked.
pub fn expectFamily(
    store: *store_mod.Store,
    fam: u16,
    want: u32,
    ctx: anytype,
    comptime derive: fn (@TypeOf(ctx), *store_mod.Store, []const u8, []const u8) ?u32,
) !usize {
    var n: usize = 0;
    var it = store.range(fam, "");
    while (it.next()) |rec| {
        const got = derive(ctx, store, rec.key(), rec.value);
        if (got != want) {
            std.debug.print("family {d}: record {x} derives slot {?d}, routed by slot {d}\n", .{ fam, rec.key(), got, want });
            return error.TestUnexpectedResult;
        }
        n += 1;
    }
    return n;
}
