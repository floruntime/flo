//! Faults only a build with `-Dtest-faults` carries. The e2e tests use them
//! to reach states no valid request can, such as a halted shard. A build
//! without the option compiles none of this in.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");

/// Whether this build carries them.
pub const enabled = @import("build_options").test_faults;

/// A kv set of a key ending in one of these, in any namespace, does it on
/// every replica where the set applies.
pub const Fault = enum {
    /// Halts its shard, as an applier that refused it would.
    halt,
    /// Panics in apply.
    panic,
    /// Writes through a null pointer in apply.
    segv,
    /// Panics in the step after apply every replica takes (triggers,
    /// metrics).
    panic_after_apply,
    /// Panics in the leader's own step after apply, for its term (a run's
    /// first step): nothing records it.
    panic_leader_step,
    /// Panics answering a read that waited for the key: only a node with
    /// such a reader reaches it, so nothing records it.
    panic_answering_waiter,

    pub fn key(f: Fault) []const u8 {
        return switch (f) {
            .halt => HALT_KEY,
            .panic => "flo-test-fault-panic",
            .segv => "flo-test-fault-segv",
            .panic_after_apply => "flo-test-fault-panic-after-apply",
            .panic_leader_step => "flo-test-fault-panic-leader-step",
            .panic_answering_waiter => "flo-test-fault-panic-answering-waiter",
        };
    }
};

pub const HALT_KEY = "flo-test-fault-halt";

/// Set in a server's environment, the faults are off: the build applies
/// every entry, as a fixed build would.
pub const OFF_ENV = "FLO_TEST_FAULTS_OFF";

/// The fault a kv set of `entry` asks for, if any.
pub fn of(entry: *const entry_mod.Entry) ?Fault {
    if (entry.header.entry_type != @backingInt(entry_mod.EntryType.kv_put)) return null;
    if (std.c.getenv(OFF_ENV) != null) return null;
    var diag: schema.Diagnostic = .{};
    const put = kv_entries.Put.decodeAll(entry.payload, &diag) catch return null;
    return ofKey(put.key);
}

/// The fault a key asks for, if any.
pub fn ofKey(key: []const u8) ?Fault {
    inline for (comptime std.enums.values(Fault)) |f| {
        if (std.mem.endsWith(u8, key, f.key())) return f;
    }
    return null;
}

/// Crash as `f` does in apply; a halt is the caller's to answer.
pub fn crash(f: Fault) void {
    switch (f) {
        .panic => @panic("test fault: panic in apply"),
        .segv => {
            const p: *allowzero volatile u8 = @ptrFromInt(0);
            p.* = 1;
        },
        .halt, .panic_after_apply, .panic_leader_step, .panic_answering_waiter => {},
    }
}
