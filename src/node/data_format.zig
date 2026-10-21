//! The data directory's format: the file `FORMAT`, one decimal number and a
//! newline. A binary reads every format up to its own and refuses a newer
//! one at boot, by name, rather than misread it. A directory without the
//! file is format 1, and gets one.

const std = @import("std");
const stdx = @import("stdx");
const log = stdx.log;
const Allocator = std.mem.Allocator;

/// The newest format this binary reads, and the one it writes.
pub const CURRENT: u32 = 1;
pub const FILENAME = "FORMAT";

pub const Error = error{ DataFormatTooNew, DataFormatUnreadable };

/// Why a directory may not be read, for the caller to show.
pub const Refusal = struct { err: Error, message: []const u8 };

/// Room for a refusal: a path and some words.
pub const MESSAGE_MAX = std.fs.max_path_bytes + 256;

/// The server's boot: refuse a format this binary doesn't read, and write
/// `CURRENT` into a directory that has none.
pub fn check(allocator: Allocator, data_path: []const u8) !void {
    var buf: [MESSAGE_MAX]u8 = undefined;
    const found = read(allocator, data_path, &buf) catch |err| {
        log.err("cannot read {s}/{s}: {s}", .{ data_path, FILENAME, @errorName(err) });
        return err;
    };
    if (found.refusal) |r| {
        log.err("{s}", .{r.message});
        return r.err;
    }
    if (found.missing) {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);
        write(allocator, data_path, path) catch |err| {
            log.err("cannot write {s}: {s}", .{ path, @errorName(err) });
            return err;
        };
    }
}

/// For the offline tools: why `data_path` may not be read, or null. A
/// directory without FORMAT is format 1. Writes nothing.
pub fn checkReadOnly(allocator: Allocator, data_path: []const u8, buf: *[MESSAGE_MAX]u8) !?Refusal {
    return (try read(allocator, data_path, buf)).refusal;
}

const Found = struct { missing: bool = false, refusal: ?Refusal = null };

fn read(allocator: Allocator, data_path: []const u8, buf: *[MESSAGE_MAX]u8) !Found {
    const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
    defer allocator.free(path);
    const text = stdx.fs.readFileAlloc(allocator, path, 64) catch |err| switch (err) {
        error.FileNotFound => return .{ .missing = true },
        error.FileTooBig => return .{ .refusal = unreadable(buf, path, null) },
        else => return err,
    };
    defer allocator.free(text);
    const format = parse(text) orelse return .{ .refusal = unreadable(buf, path, text) };
    if (format > CURRENT) return .{ .refusal = .{
        .err = error.DataFormatTooNew,
        .message = std.fmt.bufPrint(buf, "data directory format {d}, this binary reads up to {d} ({s}); start a flo that reads format {d}", .{ format, CURRENT, path, format }) catch "data directory format too new",
    } };
    return .{};
}

/// A format number, with no leading zero, and at most one newline; null
/// for anything else.
fn parse(text: []const u8) ?u32 {
    const digits = if (std.mem.endsWith(u8, text, "\n")) text[0 .. text.len - 1] else text;
    if (digits.len == 0 or digits.len > 9 or digits[0] == '0') return null;
    for (digits) |c| if (!std.ascii.isDigit(c)) return null;
    return std.fmt.parseInt(u32, digits, 10) catch null;
}

fn unreadable(buf: *[MESSAGE_MAX]u8, path: []const u8, text: ?[]const u8) Refusal {
    // What the file holds is shown only while it is short and printable.
    const raw = text orelse "";
    const shown = if (std.mem.endsWith(u8, raw, "\n")) raw[0 .. raw.len - 1] else raw;
    const printable = text != null and shown.len <= 32 and for (shown) |c| {
        if (!std.ascii.isPrint(c)) break false;
    } else true;
    return .{
        .err = error.DataFormatUnreadable,
        .message = (if (printable)
            std.fmt.bufPrint(buf, "{s} holds \"{s}\", not a data directory format; it should hold a number such as {d}", .{ path, shown, CURRENT })
        else
            std.fmt.bufPrint(buf, "{s} doesn't hold a data directory format; it should hold a number such as {d}", .{ path, CURRENT })) catch "data directory format unreadable",
    };
}

/// Written whole or not at all: a torn FORMAT would refuse every later boot.
fn write(allocator: Allocator, data_path: []const u8, path: []const u8) !void {
    const tmp = try std.fs.path.join(allocator, &.{ data_path, FILENAME ++ ".tmp" });
    defer allocator.free(tmp);
    {
        const file = try stdx.fs.createFile(tmp, .{});
        defer stdx.fs.closeFile(file);
        var buf: [16]u8 = undefined;
        try stdx.fs.writeAll(file, std.fmt.bufPrint(&buf, "{d}\n", .{CURRENT}) catch unreachable);
        try stdx.fs.sync(file);
    }
    try stdx.fs.renameDurable(tmp, path);
}

const testing = std.testing;

fn tmpDataDir(tmp: *std.testing.TmpDir) ![]u8 {
    return stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
}

fn writeFormat(dir: []const u8, text: []const u8) !void {
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    const f = try stdx.fs.createFile(path, .{ .truncate = true });
    defer stdx.fs.closeFile(f);
    try stdx.fs.writeAll(f, text);
}

test "data format: a directory without FORMAT is format 1 and gets the file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    try check(testing.allocator, dir);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    const text = try stdx.fs.readFileAlloc(testing.allocator, path, 64);
    defer testing.allocator.free(text);
    try testing.expectEqualStrings("1\n", text);
    // And a second boot reads it.
    try check(testing.allocator, dir);
}

test "data format: a newer format is refused, and so is one that isn't a number" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    try writeFormat(dir, "2\n");
    try testing.expectError(error.DataFormatTooNew, check(testing.allocator, dir));
    for ([_][]const u8{ "", "\n", "0\n", "one\n", "1 \n", "-1\n", "1\n1\n", "99999999999\n", "1\n\n\n", "01", "002\n" }) |text| {
        try writeFormat(dir, text);
        try testing.expectError(error.DataFormatUnreadable, check(testing.allocator, dir));
    }
    for ([_][]const u8{ "1", "1\n" }) |text| {
        try writeFormat(dir, text);
        try check(testing.allocator, dir);
    }
}

test "data format: the offline check refuses what boot refuses, and writes nothing" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    var buf: [MESSAGE_MAX]u8 = undefined;
    // No FORMAT: format 1, and none is written.
    try testing.expect(try checkReadOnly(testing.allocator, dir, &buf) == null);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    try testing.expectError(error.FileNotFound, stdx.fs.openFile(path, .{}));

    try writeFormat(dir, "2\n");
    const newer = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatTooNew, newer.err);
    var want: [MESSAGE_MAX]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory format 2, this binary reads up to 1 ({s}); start a flo that reads format 2", .{path}), newer.message);

    try writeFormat(dir, "two\n");
    const garbage = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatUnreadable, garbage.err);
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s} holds \"two\", not a data directory format; it should hold a number such as 1", .{path}), garbage.message);
}

test "data format: a FORMAT that can't be read fails both checks, rather than reading as missing" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    try stdx.fs.makePath(path);
    var buf: [MESSAGE_MAX]u8 = undefined;
    try testing.expectError(error.IsDir, check(testing.allocator, dir));
    try testing.expectError(error.IsDir, checkReadOnly(testing.allocator, dir, &buf));
}
