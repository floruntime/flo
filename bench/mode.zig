//! The first line a bench prints: the modes its code was compiled in, read
//! from the modules themselves, so a figure can't carry the wrong label.

const std = @import("std");
const builtin = @import("builtin");
const src = @import("src");
const stdx = @import("stdx");

/// For a bench whose measured code is linked into it.
pub fn print() void {
    std.debug.print("modes: bench {t}, server code {t}, stdx {t}\n", .{ builtin.mode, src.optimize_mode, stdx.optimize_mode });
}

/// For a bench that times a running server: that server's mode is its own,
/// and it prints it when it starts.
pub fn printForClient() void {
    std.debug.print("modes: bench {t}, client code {t}, stdx {t}; the server's is on its \"Build:\" start line\n", .{ builtin.mode, src.optimize_mode, stdx.optimize_mode });
}
