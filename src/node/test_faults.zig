//! Faults only a build with `-Dtest-faults` carries. The e2e tests use them
//! to reach states no valid request can, such as a halted shard. A build
//! without the option compiles none of this in.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");

/// Whether this build carries them.
pub const enabled = @import("build_options").test_faults;

/// A kv set of this key, in any namespace, halts its shard where it
/// applies, on every replica, as an applier that refused it would.
pub const HALT_KEY = "flo-test-fault-halt";

pub fn halts(entry: *const entry_mod.Entry) bool {
    if (entry.header.entry_type != @backingInt(entry_mod.EntryType.kv_put)) return false;
    var diag: schema.Diagnostic = .{};
    const put = kv_entries.Put.decodeAll(entry.payload, &diag) catch return false;
    return std.mem.endsWith(u8, put.key, HALT_KEY);
}
