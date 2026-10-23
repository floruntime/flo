//! Kills a process group when the test process dies.
//!
//! A test the runner kills at its time limit (or one that crashes) runs no
//! cleanup, so the servers and commands it started would keep running. Each
//! gets a watchdog: a shell whose stdin is a pipe only the test process holds
//! the write end of. When the test process dies, by any means, the kernel
//! closes that end; `read` returns at end of file and the watchdog kills the
//! group. When the harness collects the process itself, it tells the watchdog
//! `done` first, while the process is still unreaped, so the watchdog never
//! signals a group id that may already belong to someone else.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Child = @import("../../process.zig").Child;

pub const Watchdog = struct {
    child: Child,
    argv: [5][]const u8,
    pgid_text: []const u8,
    allocator: Allocator,

    /// Watch `pgid`, which must be a group the caller leads (spawned with
    /// pgid 0). The watchdog has its own group, so killing `pgid` doesn't
    /// take it too.
    pub fn start(allocator: Allocator, pgid: std.c.pid_t) !*Watchdog {
        const self = try allocator.create(Watchdog);
        errdefer allocator.destroy(self);
        const pgid_text = try std.fmt.allocPrint(allocator, "{d}", .{pgid});
        errdefer allocator.free(pgid_text);
        self.* = .{
            .allocator = allocator,
            .pgid_text = pgid_text,
            .argv = .{ "/bin/sh", "-c", "read -r line; [ \"$line\" = done ] || kill -s KILL -- -\"$1\"", "flo-e2e-watchdog", pgid_text },
            .child = undefined,
        };
        self.child = Child.init(&self.argv, allocator);
        self.child.stdin_behavior = .Pipe;
        self.child.stdout_behavior = .Ignore;
        self.child.stderr_behavior = .Ignore;
        self.child.pgid = 0;
        try self.child.spawn();
        return self;
    }

    /// Stand the watchdog down and collect it. Call it before collecting the
    /// watched process (see `awaitExit`).
    pub fn release(self: *Watchdog) void {
        if (self.child.stdin) |f| _ = std.c.write(f.handle, "done\n", 5);
        _ = self.child.wait() catch {};
        self.allocator.free(self.pgid_text);
        self.allocator.destroy(self);
    }
};

const P_PID: c_int = 1;
const WNOHANG: c_int = 1;
const WEXITED: c_int = 4;
const WNOWAIT: c_int = switch (builtin.os.tag) {
    .linux => 0x1000000,
    else => 0x20, // macOS and the BSDs it shares <sys/wait.h> with
};
extern "c" fn waitid(idtype: c_int, id: std.c.pid_t, infop: *[128]u8, options: c_int) c_int;

/// Whether `pid` has exited, leaving it unreaped so its id can't be reused
/// yet. `block` waits for the exit. Any error but ECHILD (nothing left to
/// wait for) is returned, after a log line: the caller can't tell, so it
/// falls back to stopping the watchdog and reaping as before.
pub fn awaitExit(pid: std.c.pid_t, block: bool) error{WaitidFailed}!bool {
    // WNOHANG with nothing to report leaves the zeroed info as it was.
    var info = std.mem.zeroes([128]u8);
    const options = WEXITED | WNOWAIT | (if (block) 0 else WNOHANG);
    while (waitid(P_PID, pid, &info, options) != 0) {
        const errno = std.c._errno().*;
        if (errno == @backingInt(std.c.E.INTR)) continue;
        if (errno == @backingInt(std.c.E.CHILD)) return true;
        std.debug.print("[e2e] waitid on pid {d} failed (errno {d}); reaping without the exit check\n", .{ pid, errno });
        return error.WaitidFailed;
    }
    return block or !std.mem.allEqual(u8, &info, 0);
}
