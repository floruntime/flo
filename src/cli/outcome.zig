//! How a command ends. Every failure is one of five outcomes, each with its
//! own exit code, so a script can tell "absent" from "fix your command" from
//! "refused" from "retry" from "can't reach":
//!
//!   0 ok · 1 not found · 2 usage · 3 refused · 4 retryable · 5 transport
//!   (or a cut-off answer, or the CLI out of memory)
//!
//! Commands report through these helpers rather than printing and choosing
//! an error themselves.

const std = @import("std");
const commander = @import("commander/mod.zig");
const base = @import("client/base.zig");
const proto = @import("../protocol/proto.zig");

const Context = commander.Context;
const Error = commander.Error;

/// The process exit code for a command's result.
pub fn exitCode(result: Error!void) u8 {
    result catch |err| return switch (err) {
        error.HelpRequested, error.VersionRequested => 0,
        error.NotFound => 1,
        error.Usage,
        error.UnknownCommand,
        error.UnknownFlag,
        error.MissingFlagValue,
        error.InvalidFlagValue,
        error.MissingRequiredFlag,
        error.MissingRequiredArg,
        error.TooManyArgs,
        error.InvalidArgs,
        error.InvalidUtf8,
        error.Overflow,
        => 2,
        error.Refused => 3,
        error.Retryable => 4,
        // The CLI couldn't finish for a reason outside the command and the
        // server's answer, like a lost connection.
        error.Transport, error.OutOfMemory => 5,
    };
    return 0;
}

/// The outcome a response status stands for.
pub fn ofStatus(status: proto.StatusCode) Error {
    return switch (status) {
        .not_found => error.NotFound,
        .overloaded, .rate_limited, .unavailable => error.Retryable,
        .ok,
        .error_generic,
        .bad_request,
        .cross_core_transaction,
        .no_active_transaction,
        .group_locked,
        .unauthorized,
        .conflict,
        .internal_error,
        _,
        => error.Refused,
    };
}

/// Returns if the server answered ok; otherwise says why and fails with the
/// status's outcome.
pub fn check(ctx: *Context, resp: base.Response) Error!void {
    if (resp.status == .ok) return;
    return refused(ctx, resp);
}

/// Says why the server refused `resp` and returns its outcome.
pub fn refused(ctx: *Context, resp: base.Response) Error {
    return refusal(ctx, resp.status, resp.errorMessage(), "", .{});
}

/// A refusal with `status` and the server's `message`, after a prefix saying
/// what was refused (e.g. "line 4: "), and its outcome.
pub fn refusal(ctx: *Context, status: proto.StatusCode, message: []const u8, comptime prefix: []const u8, args: anytype) Error {
    const bug = if (status == .internal_error) "server error (a bug): " else "";
    ctx.printErr("Error: " ++ prefix ++ "{s}{s} [{s}]\n", args ++ .{ bug, message, statusName(status) });
    return ofStatus(status);
}

/// Connecting to `endpoint` failed.
pub fn connectFailed(ctx: *Context, err: anyerror, endpoint: []const u8) Error {
    if (isLocal(err)) {
        ctx.printErr("Error: {s} is not an endpoint (host:port): {s}\n", .{ endpoint, @errorName(err) });
        return error.Usage;
    }
    ctx.printErr("Error: can't reach flo at {s}: {s}\n", .{ endpoint, @errorName(err) });
    return error.Transport;
}

/// A request that failed before an answer came back. An encoding failure is
/// the command's input; anything else is the connection.
pub fn requestFailed(ctx: *Context, err: anyerror) Error {
    if (isLocal(err)) {
        ctx.printErr("Error: the request can't be built: {s}\n", .{@errorName(err)});
        return error.Usage;
    }
    ctx.printErr("Error: the request failed: {s}\n", .{@errorName(err)});
    return error.Transport;
}

/// An answer the CLI can't read: the server speaks another version of the
/// protocol, or the connection garbled it.
pub fn malformed(ctx: *Context, comptime what: []const u8) Error {
    ctx.printErr("Error: malformed " ++ what ++ " from the server\n", .{});
    return error.Transport;
}

/// The command's input is wrong: a flag, an argument, or a file or stdin it
/// was told to read.
pub fn usage(ctx: *Context, comptime fmt: []const u8, args: anytype) Error {
    ctx.printErr("Error: " ++ fmt ++ "\n", args);
    return error.Usage;
}

/// Something asked for is absent.
pub fn notFound(ctx: *Context, comptime fmt: []const u8, args: anytype) Error {
    ctx.printErr("Error: " ++ fmt ++ "\n", args);
    return error.NotFound;
}

/// Refused here rather than by a server: a local command (server start,
/// init) that can't do what it was asked.
pub fn localRefusal(ctx: *Context, comptime fmt: []const u8, args: anytype) Error {
    ctx.printErr("Error: " ++ fmt ++ "\n", args);
    return error.Refused;
}

/// A Context whose output goes nowhere, for testing printers.
pub const QuietContext = struct {
    command: *commander.Command,
    ctx: Context,
    null_fd: std.posix.fd_t,

    pub fn init(allocator: std.mem.Allocator) !QuietContext {
        const command = try commander.newBuilder(allocator).name("test").build();
        const fd = std.c.open("/dev/null", .{ .ACCMODE = .WRONLY });
        if (fd < 0) return error.NoNullDevice;
        return .{
            .command = command,
            .null_fd = fd,
            .ctx = .{ .command = command, .args = &.{}, .allocator = allocator, .stdout_file = fd, .stderr_file = fd },
        };
    }

    pub fn deinit(self: *QuietContext) void {
        _ = std.c.close(self.null_fd);
        self.command.deinit();
    }
};

/// A server for tests that answers the first request on one connection with
/// `status` and `body` (a refusal's message, or an ok answer's data), then
/// closes it, so a command's handling of an answer can be driven without a
/// real server. It stops listening once a command connects, so a command
/// that reconnects to retry fails at once instead of waiting.
pub const FakeServer = struct {
    listen_fd: std.posix.socket_t,
    port: u16,
    status: proto.StatusCode,
    body: []const u8,
    thread: std.Thread = undefined,

    pub fn start(status: proto.StatusCode, body: []const u8) !*FakeServer {
        const net = @import("stdx").net;
        const fd = try net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer net.sysClose(fd);
        const addr = net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
        try net.sysBind(fd, addr.anyPtr(), addr.anyLen());
        try net.sysListen(fd, 1);
        const local = try net.sysLocalIp4(fd);
        const self = try std.heap.page_allocator.create(FakeServer);
        self.* = .{ .listen_fd = fd, .port = local.port, .status = status, .body = body };
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
        return self;
    }

    fn serve(self: *FakeServer) void {
        const net = @import("stdx").net;
        const conn = net.sysAccept(self.listen_fd, null, null, 0);
        net.sysClose(self.listen_fd);
        const c = conn catch return;
        defer net.sysClose(c);
        var req: [64 * 1024]u8 = undefined;
        var out: [1024]u8 = undefined;
        while (true) {
            var got: usize = 0;
            while (got < @sizeOf(proto.RequestHeader)) {
                const n = net.sysRead(c, req[got..@sizeOf(proto.RequestHeader)]) catch return;
                if (n == 0) return;
                got += n;
            }
            const header = std.mem.bytesToValue(proto.RequestHeader, req[0..@sizeOf(proto.RequestHeader)]);
            var left: usize = header.payload_length;
            while (left > 0) {
                const n = net.sysRead(c, req[0..@min(left, req.len)]) catch return;
                if (n == 0) return;
                left -= n;
            }
            const frame = if (self.status == .ok)
                proto.Response.serializeNew(.ok, header.request_id, self.body, &out) catch return
            else
                proto.Response.serializeError(self.status, header.request_id, self.body, &out);
            _ = net.sysWrite(c, frame) catch return;
            // One answer, then the connection closes: a command that retried
            // would see a lost connection (exit 5), not hang the test.
            return;
        }
    }

    pub fn stop(self: *FakeServer) void {
        const net = @import("stdx").net;
        // Wake an accept that no command reached, so the join returns.
        if (net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, self.port, 1000)) |fd| net.sysClose(fd) else |_| {}
        self.thread.join();
        std.heap.page_allocator.destroy(self);
    }

    /// The `--endpoint` value that reaches it.
    pub fn endpoint(self: *const FakeServer, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "127.0.0.1:{d}", .{self.port}) catch unreachable;
    }
};

/// A root command as the CLI builds it (with `--endpoint`), writing nowhere,
/// for running one command family in a test.
pub fn testRoot(allocator: std.mem.Allocator, family: *commander.Command) !*commander.Command {
    const root = commander.Command.init(allocator, .{ .name = "flo" });
    try root.addPersistentFlag(.{ .long = "endpoint", .short = 'e', .description = "", .value_type = .string, .default = .{ .string = "" } });
    try root.addPersistentFlag(.{ .long = "namespace", .short = 'n', .description = "", .value_type = .string, .default = .{ .string = "default" } });
    try root.addPersistentFlag(.{ .long = "output", .short = 'o', .description = "", .value_type = .string, .default = .{ .string = "table" } });
    try root.addCommand(family);
    // Every command writes nowhere: the test runner owns stdout.
    quiet(root, std.c.open("/dev/null", .{ .ACCMODE = .WRONLY }));
    return root;
}

fn quiet(cmd: *commander.Command, fd: std.posix.fd_t) void {
    cmd.setOut(fd);
    cmd.setErr(fd);
    for (cmd.commands.items) |child| quiet(child, fd);
}

var interrupt_seen = std.atomic.Value(bool).init(false);

/// From now on Ctrl-C ends a long-running command (a follow, a watch)
/// normally, with exit 0, instead of killing it. No SA_RESTART: a blocking
/// read returns Interrupted, so the loop sees it at once.
pub fn endOnInterrupt() void {
    const handler = struct {
        fn handle(_: std.c.SIG) callconv(.c) void {
            interrupt_seen.store(true, .release);
        }
    }.handle;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
}

/// Ctrl-C was pressed since `endOnInterrupt`.
pub fn interrupted() bool {
    return interrupt_seen.load(.acquire);
}

fn isLocal(err: anyerror) bool {
    const local = [_]anyerror{
        error.InvalidEndpoint,
        error.InvalidCharacter,
        error.Overflow,
        error.OptionsBufferTooSmall,
        error.BufferOverflow,
        error.WriteFailed,
        error.NoSpaceLeft,
        error.CursorTooLong,
        error.LabelsTooLong,
        error.EmptyBatch,
        error.BatchTooLarge,
        error.PathTooLong,
    };
    for (local) |l| if (err == l) return true;
    return false;
}

fn statusName(status: proto.StatusCode) []const u8 {
    return std.enums.tagName(proto.StatusCode, status) orelse "unknown";
}

test "outcome: each status has its exit code" {
    try std.testing.expectEqual(@as(u8, 1), exitCode(ofStatus(.not_found)));
    inline for (.{ .bad_request, .conflict, .unauthorized, .group_locked, .internal_error, .error_generic }) |s|
        try std.testing.expectEqual(@as(u8, 3), exitCode(ofStatus(s)));
    inline for (.{ .overloaded, .rate_limited, .unavailable }) |s|
        try std.testing.expectEqual(@as(u8, 4), exitCode(ofStatus(s)));
    try std.testing.expectEqual(@as(u8, 3), exitCode(ofStatus(@enumFromInt(200))));
    try std.testing.expectEqual(@as(u8, 0), exitCode({}));
    try std.testing.expectEqual(@as(u8, 2), exitCode(error.UnknownFlag));
    try std.testing.expectEqual(@as(u8, 5), exitCode(error.Transport));
    try std.testing.expectEqual(@as(u8, 5), exitCode(error.OutOfMemory));
}

/// The first error print in `source` after which the command can still
/// succeed: from the print, control reaches a bare `return;` or
/// `return {};`, or falls off the end of its function, before it returns or
/// breaks with an outcome. Returns its line number. Prints that only inform
/// (verbose, warning, note, hint, indented follow-on lines, report lines),
/// and prints in
/// functions that return no error (their caller decides), don't count.
fn printThenSucceed(source: []const u8) ?usize {
    var pos: usize = 0;
    return nextPrintThenSucceed(source, &pos);
}

fn nextPrintThenSucceed(source: []const u8, pos: *usize) ?usize {
    while (std.mem.indexOfPos(u8, source, pos.*, "printErr")) |at| {
        pos.* = at + "printErr".len;
        // printErr( or printErrf(
        var open = pos.*;
        if (open < source.len and source[open] == 'f') open += 1;
        if (open >= source.len or source[open] != '(') continue;
        if (isCommentedOut(source, at) or isDefinition(source, at)) continue;
        if (!returnsError(source, at)) continue;
        if (!isFailurePrint(source[open + 1 ..])) continue;
        const end = statementEnd(source, open) orelse continue;
        if (!reachesOutcome(source[end..])) return lineOf(source, at);
    }
    return null;
}

fn lineStart(source: []const u8, at: usize) usize {
    return if (std.mem.lastIndexOfScalar(u8, source[0..at], '\n')) |nl| nl + 1 else 0;
}

fn isCommentedOut(source: []const u8, at: usize) bool {
    return std.mem.indexOf(u8, source[lineStart(source, at)..at], "//") != null;
}

fn isDefinition(source: []const u8, at: usize) bool {
    return std.mem.endsWith(u8, std.mem.trimEnd(u8, source[lineStart(source, at)..at], " "), "fn");
}

/// Whether the function around `at` returns an error union: the nearest
/// line above it that declares a function, so "fn " in a comment or in an
/// identifier such as `run_fn` doesn't count.
fn returnsError(source: []const u8, at: usize) bool {
    var end = lineStart(source, at);
    while (end > 0) {
        const start = lineStart(source, end - 1);
        const line = std.mem.trimStart(u8, source[start .. end - 1], " \t");
        if (declaresFn(line)) {
            const body = std.mem.indexOfScalarPos(u8, source, start, '{') orelse return false;
            return std.mem.indexOfScalar(u8, source[start..body], '!') != null;
        }
        end = start;
    }
    return false;
}

fn declaresFn(line: []const u8) bool {
    var rest = line;
    for ([_][]const u8{ "pub ", "export ", "inline ", "noinline " }) |kw| {
        if (std.mem.startsWith(u8, rest, kw)) rest = rest[kw.len..];
    }
    return std.mem.startsWith(u8, rest, "fn ");
}

fn isFailurePrint(args: []const u8) bool {
    const t = std.mem.trimStart(u8, args, " \t\r\n");
    if (t.len == 0 or t[0] != '"') return true;
    // Report lines (validate's ERR/WARN) are decided by the summary after them.
    for ([_][]const u8{ "[verbose]", "Warning", "Note", "Hint", "Tip", "  ", "ERR", "WARN" }) |p| {
        if (std.mem.startsWith(u8, t[1..], p)) return false;
    }
    return true;
}

/// Where the call opened at `open` ends: just past its `;` or `,`, or at the
/// `}` that closes its block.
fn statementEnd(source: []const u8, open: usize) ?usize {
    var i = open;
    var depth: usize = 0;
    while (i < source.len) : (i += 1) {
        switch (source[i]) {
            '"' => i = skipString(source, i),
            '(', '{' => depth += 1,
            ')' => depth -|= 1,
            '}' => if (depth == 0) return i else {
                depth -= 1;
            },
            ';', ',' => if (depth == 0) return i + 1,
            else => {},
        }
    }
    return null;
}

fn skipString(source: []const u8, quote: usize) usize {
    var i = quote + 1;
    while (i < source.len and source[i] != '"') : (i += 1) {
        if (source[i] == '\\') i += 1;
    }
    return i;
}

/// Follows control after an error print: through the end of its block into
/// the enclosing ones, until a statement in that path returns or breaks
/// with an outcome. A bare `return;` or `return {};` anywhere on the way,
/// however nested or conditional, is a path to success; so is falling off
/// the end of the function. A return of anything but an error or an
/// outcome helper's value counts as success too, since it may be one.
fn reachesOutcome(rest: []const u8) bool {
    var i: usize = 0;
    var depth: usize = 0; // braces the scan has entered
    var parens: usize = 0;
    var stmt_start: usize = 0;
    while (i < rest.len) : (i += 1) {
        if (depth == 0 and parens == 0 and i == stmt_start) {
            const t = std.mem.trimStart(u8, rest[i..], " \t\r\n");
            // The next declaration: control fell off the function's end.
            for ([_][]const u8{ "fn ", "pub fn ", "test \"", "pub const ", "const " }) |decl| {
                if (std.mem.startsWith(u8, t, decl) and isTopLevel(rest, i)) return false;
            }
        }
        switch (rest[i]) {
            '"' => i = skipString(rest, i),
            '/' => if (i + 1 < rest.len and rest[i + 1] == '/') {
                i = std.mem.indexOfScalarPos(u8, rest, i, '\n') orelse return false;
                if (parens == 0) stmt_start = i + 1;
            },
            '(' => parens += 1,
            ')' => parens -|= 1,
            '{' => {
                depth += 1;
                if (parens == 0) stmt_start = i + 1;
            },
            '}' => {
                if (depth > 0) depth -= 1;
                if (parens == 0) stmt_start = i + 1;
            },
            ';', ',' => if (parens == 0) {
                const stmt = std.mem.trim(u8, rest[stmt_start..i], " \t\r\n");
                stmt_start = i + 1;
                if (bareReturn(stmt)) return false;
                if (depth != 0) continue;
                if (returnedValue(stmt)) |value| return isOutcome(value);
                if (std.mem.startsWith(u8, stmt, "break :")) {
                    // `break :label value` hands the value on like a return.
                    const after = stmt["break :".len..];
                    const space = std.mem.indexOfScalar(u8, after, ' ') orelse return false;
                    return isOutcome(std.mem.trim(u8, after[space..], " \t\r\n"));
                }
            },
            else => {},
        }
    }
    return false;
}

/// A statement that returns success: `return;`, `return {};`, or either
/// at the end of an unbraced `if`/`else` (`if (x) return;`).
fn bareReturn(stmt: []const u8) bool {
    for ([_][]const u8{ "return", "return {}" }) |ret| {
        if (std.mem.eql(u8, stmt, ret)) return true;
        if (std.mem.endsWith(u8, stmt, ret)) {
            const before = stmt[0 .. stmt.len - ret.len];
            const trimmed = std.mem.trimEnd(u8, before, " \t\r\n");
            if (trimmed.len < before.len and (std.mem.endsWith(u8, trimmed, ")") or std.mem.endsWith(u8, trimmed, "else"))) return true;
        }
    }
    return false;
}

/// The value a top-level `return <value>` statement returns.
fn returnedValue(stmt: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, stmt, "return ")) return null;
    return std.mem.trim(u8, stmt["return ".len..], " \t\r\n");
}

/// Whether a returned value is a failure: an error value, an outcome
/// helper's result, or a caught error passed on.
fn isOutcome(value: []const u8) bool {
    for ([_][]const u8{ "error.", "outcome." }) |p| if (std.mem.startsWith(u8, value, p)) return true;
    for ([_][]const u8{ "err", "e", "failure" }) |name| if (std.mem.eql(u8, value, name)) return true;
    return false;
}

/// Whether the line holding `i` starts at column 0: a file-level declaration.
fn isTopLevel(rest: []const u8, i: usize) bool {
    var j = i;
    while (j < rest.len and (rest[j] == '\n' or rest[j] == '\r')) j += 1;
    return j < rest.len and rest[j] != ' ' and rest[j] != '\t';
}

fn lineOf(source: []const u8, at: usize) usize {
    return std.mem.count(u8, source[0..at], "\n") + 1;
}

test "outcome: the lint finds an error print the command can succeed after" {
    const cases = [_]struct { src: []const u8, line: ?usize }{
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\",\n        .{});\n\n    return;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    // say no more\n    return;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{}); return;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    return {};\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    cmd.printErrf(\"Error: x\\n\", .{});\n    return;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    if (bad) {\n        ctx.printErr(\"Error: x; y\\n\", .{});\n    }\n    ok();\n}", .line = 3 },
        // A conditional bare return is a path to success, braced or not.
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    if (v) { return; }\n    return error.Usage;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    if (limit > 0) return;\n    return error.NotFound;\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    if (a) f() else return;\n    return error.NotFound;\n}", .line = 2 },
        // "fn " in a comment or an identifier above the print isn't the function's signature.
        .{ .src = "fn f() E!void {\n    // the fn above: not a signature\n    if (run_fn == null) {}\n    ctx.printErr(\"Error: x\\n\", .{});\n    return;\n}", .line = 4 },
        // A return of something that may succeed isn't an outcome.
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    return printTable(ctx);\n}", .line = 2 },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    return outcome.usage(ctx, \"y\", .{});\n}", .line = null },
        // A labelled break hands on a value; only an outcome counts.
        .{ .src = "fn f() E!void {\n    const r = blk: {\n        ctx.printErr(\"Error: x\\n\", .{});\n        break :blk limit;\n    };\n    _ = r;\n}", .line = 3 },
        .{ .src = "fn f() E!void {\n    const r = blk: {\n        ctx.printErr(\"Error: x\\n\", .{});\n        break :blk error.Usage;\n    };\n    _ = r;\n}", .line = null },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"Error: x\\n\", .{});\n    ctx.printErr(\"  hint\\n\", .{});\n    return error.Usage;\n}", .line = null },
        .{ .src = "fn f() E!void {\n    ctx.printErr(\"[verbose] x\\n\", .{});\n    ok();\n}", .line = null },
        .{ .src = "fn f() E!void {\n    x() catch |e| {\n        ctx.printErr(\"Error: {}\\n\", .{e});\n        return outcome.usage(ctx, \"y\", .{});\n    };\n}", .line = null },
        .{ .src = "fn f() E!void {\n    if (a) {\n        ctx.printErr(\"Error: a\\n\", .{});\n    } else {\n        ctx.printErr(\"Error: b\\n\", .{});\n    }\n    return error.Refused;\n}", .line = null },
        .{ .src = "fn f() E!void {\n    switch (e) {\n        error.A => {},\n        else => ctx.printErr(\"Error: {}\\n\", .{e}),\n    }\n    return error.Usage;\n}", .line = null },
        .{ .src = "fn f() E!void {\n    const v = x() catch |e| {\n        ctx.printErr(\"line: {}\\n\", .{e});\n        break :blk error.Usage;\n    };\n}", .line = null },
        .{ .src = "fn count() usize {\n    ctx.printErr(\"ERR x\\n\", .{});\n    return 1;\n}", .line = null },
        .{ .src = "pub fn printErr(self: *C, comptime fmt: []const u8) void {\n    w(fmt);\n}", .line = null },
        .{ .src = "fn f() E!void {\n    if (a) {\n        ctx.printErr(\"Error: a\\n\", .{});\n    }\n}\n\nfn g() void {}", .line = 3 },
    };
    for (cases) |c| try std.testing.expectEqual(c.line, printThenSucceed(c.src));
}

test "outcome: no CLI command prints an error and then exits 0" {
    const stdx = @import("stdx");
    const a = std.testing.allocator;
    var stack: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (stack.items) |p| a.free(p);
        stack.deinit(a);
    }
    try stack.append(a, try a.dupe(u8, "src/cli"));
    var i: usize = 0;
    var files: usize = 0;
    var found: usize = 0;
    while (i < stack.items.len) : (i += 1) {
        const dir_path = stack.items[i];
        var dir = try stdx.fs.openDir(dir_path, .{ .iterate = true });
        defer stdx.fs.closeDir(dir);
        var it = dir.iterate();
        while (try it.next(stdx.io.instance())) |entry| {
            const path = try std.fs.path.join(a, &.{ dir_path, entry.name });
            if (entry.kind == .directory) {
                try stack.append(a, path);
                continue;
            }
            defer a.free(path);
            // The helpers themselves print and then return an outcome.
            if (!std.mem.endsWith(u8, entry.name, ".zig") or std.mem.eql(u8, entry.name, "outcome.zig")) continue;
            const source = try stdx.fs.readFileAlloc(a, path, 1 << 20);
            defer a.free(source);
            files += 1;
            var pos: usize = 0;
            while (nextPrintThenSucceed(source, &pos)) |line| {
                std.debug.print("{s}:{d}: prints an error, then can return success; return the outcome instead\n", .{ path, line });
                found += 1;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), found);
    try std.testing.expect(files > 10);
}
