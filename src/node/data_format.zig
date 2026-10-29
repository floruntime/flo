//! The data directory's format: the file `FORMAT`, one decimal number and a
//! newline, checked first at boot. A binary reads only its own format: there
//! is no migration, so an older directory is wiped (`flo server wipe`) and a
//! newer one needs a newer binary. An empty directory gets `FORMAT` before
//! anything else is written; a non-empty one without it is either an older
//! Flo's, from before the gate, or not Flo's at all, and both are refused.

const std = @import("std");
const stdx = @import("stdx");
const log = stdx.log;
const Allocator = std.mem.Allocator;

/// The format this binary reads and writes.
pub const CURRENT: u32 = 2;
pub const FILENAME = "FORMAT";
pub const SYSTEM_FILENAME = "SYSTEM";
pub const LOCK_FILENAME = "flo.lock";
pub const PID_FILENAME = "flo.pid";
pub const ADMIN_SOCKET_FILENAME = "admin.sock";
/// Where `flo server wipe --keep-as NAME` moves what it keeps: a dot
/// directory, which a fresh directory may hold.
pub const KEPT_PREFIX = ".flo-kept-";

pub const Error = error{ DataFormatTooNew, DataFormatTooOld, DataFormatUnreadable, DataFormatMissing, NotADataDir };

/// Why a directory may not be read, for the caller to show.
pub const Refusal = struct { err: Error, message: []const u8 };

/// Room for a refusal: two paths and some words.
pub const MESSAGE_MAX = 2 * std.fs.max_path_bytes + 256;

/// The server's boot: refuse a directory this binary doesn't read, and write
/// `CURRENT` into an empty one before anything else is.
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
    if (found.fresh) {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);
        write(allocator, data_path, path) catch |err| {
            log.err("cannot write {s}: {s}", .{ path, @errorName(err) });
            return err;
        };
    }
}

/// For the offline tools: why `data_path` may not be read, or null. An
/// empty directory isn't refused here, and nothing is written.
pub fn checkReadOnly(allocator: Allocator, data_path: []const u8, buf: *[MESSAGE_MAX]u8) !?Refusal {
    return (try read(allocator, data_path, buf)).refusal;
}

/// Whether the error is a refusal of the data directory itself: one an
/// operator fixes by wiping it or pointing at another, never by retrying.
pub fn isRefusal(err: anyerror) bool {
    return switch (err) {
        error.DataFormatTooNew, error.DataFormatTooOld, error.DataFormatUnreadable, error.DataFormatMissing, error.NotADataDir => true,
        else => false,
    };
}

/// What one entry of a data directory is.
pub const Kind = enum {
    /// Only Flo writes it, and it holds what Flo writes: a `FORMAT` that
    /// parses, a `SYSTEM` that loads, a shard directory holding `segs/`.
    marker,
    /// What a directory may hold and still be empty (`emptyAllows`).
    allowed,
    /// Anything else, a name Flo uses holding something else among them.
    other,
};

/// What the entry `name` of `dir` is, read through `dir`, never following
/// a link: a link named like a marker is `other`.
pub fn classify(allocator: Allocator, dir: stdx.fs.Dir, name: []const u8, kind: stdx.fs.File.Kind) Kind {
    if (emptyAllows(name)) return .allowed;
    return if (isMarker(allocator, dir, name, kind)) .marker else .other;
}

fn isMarker(allocator: Allocator, dir: stdx.fs.Dir, name: []const u8, kind: stdx.fs.File.Kind) bool {
    const io = stdx.io.instance();
    if (std.mem.eql(u8, name, FILENAME)) {
        if (kind != .file) return false;
        var buf: [64]u8 = undefined;
        const text = dir.readFile(io, name, &buf) catch return false;
        return parse(text) != null;
    }
    if (std.mem.eql(u8, name, SYSTEM_FILENAME)) {
        if (kind != .file) return false;
        var buf: [4096]u8 = undefined;
        const text = dir.readFile(io, name, &buf) catch return false;
        _ = @import("manifest.zig").SystemManifest.parse(allocator, text) catch return false;
        return true;
    }
    if (kind != .directory or !isShardDir(name)) return false;
    var path_buf: [16]u8 = undefined;
    const segs = std.fmt.bufPrint(&path_buf, "{s}/segs", .{name}) catch return false;
    const st = dir.statFile(io, segs, .{ .follow_symlinks = false }) catch return false;
    return st.kind == .directory;
}

/// What a directory's entries say it is.
pub const Contents = struct {
    /// It holds one of Flo's markers: Flo wrote here.
    flo: bool = false,
    /// The first entry that is neither a marker nor allowed in an empty one.
    other: ?[]const u8 = null,
    other_buf: [std.fs.max_name_bytes]u8 = undefined,
};

/// Reads the entries of `dir`, opened by the caller, not following any.
pub fn scan(allocator: Allocator, dir: stdx.fs.Dir) !Contents {
    var c: Contents = .{};
    var it = dir.iterate();
    while (try it.next(stdx.io.instance())) |entry| {
        switch (classify(allocator, dir, entry.name, entry.kind)) {
            .marker => c.flo = true,
            .allowed => {},
            .other => if (c.other == null) {
                const n = @min(entry.name.len, c.other_buf.len);
                @memcpy(c.other_buf[0..n], entry.name[0..n]);
                c.other = c.other_buf[0..n];
            },
        }
    }
    return c;
}

/// A shard's directory name: its number in five digits.
pub fn isShardDir(name: []const u8) bool {
    if (name.len != 5) return false;
    for (name) |ch| if (!std.ascii.isDigit(ch)) return false;
    return true;
}

/// What a directory may hold and still be empty: what a mount point brings
/// (`lost+found`), dotfiles (a wipe's kept copy among them), and what a
/// server leaves before it writes `FORMAT`: its lock and PID file, its admin
/// socket, and a `FORMAT` or `SYSTEM` write a crash cut short.
pub fn emptyAllows(name: []const u8) bool {
    if (name.len > 0 and name[0] == '.') return true;
    for ([_][]const u8{ "lost+found", LOCK_FILENAME, PID_FILENAME, ADMIN_SOCKET_FILENAME, FILENAME ++ ".tmp", SYSTEM_FILENAME ++ ".tmp" }) |n| {
        if (std.mem.eql(u8, name, n)) return true;
    }
    return false;
}

const Found = struct { fresh: bool = false, refusal: ?Refusal = null };

fn read(allocator: Allocator, data_path: []const u8, buf: *[MESSAGE_MAX]u8) !Found {
    const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
    defer allocator.free(path);
    const text = stdx.fs.readFileAlloc(allocator, path, 64) catch |err| switch (err) {
        error.FileNotFound => return unformatted(allocator, data_path, buf),
        // Not a file of a number: refused as not holding a format, the same
        // as one holding something else.
        error.FileTooBig, error.IsDir => return .{ .refusal = unreadable(buf, path, null) },
        else => return err,
    };
    defer allocator.free(text);
    const format = parse(text) orelse return .{ .refusal = unreadable(buf, path, text) };
    if (format > CURRENT) return .{ .refusal = .{
        .err = error.DataFormatTooNew,
        .message = std.fmt.bufPrint(buf, "data directory format {d}, this binary reads format {d} ({s}); start a flo that reads format {d}", .{ format, CURRENT, path, format }) catch "data directory format too new",
    } };
    if (format < CURRENT) return .{ .refusal = .{
        .err = error.DataFormatTooOld,
        .message = std.fmt.bufPrint(buf, "data directory {s} was written by an older Flo (format {d}); this server needs format {d}. There is no migration: run flo server wipe --data-dir {s}", .{ data_path, format, CURRENT, data_path }) catch "data directory format too old",
    } };
    return .{};
}

/// A directory without `FORMAT`: empty, an older Flo's, or not Flo's.
fn unformatted(allocator: Allocator, data_path: []const u8, buf: *[MESSAGE_MAX]u8) !Found {
    const dir = try stdx.fs.openDir(data_path, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    const c = try scan(allocator, dir);
    if (c.flo) return .{ .refusal = .{
        .err = error.DataFormatMissing,
        .message = std.fmt.bufPrint(buf, "data directory {s} was written by an older Flo (before format gating); this server needs format {d}. There is no migration: run flo server wipe --data-dir {s}", .{ data_path, CURRENT, data_path }) catch "data directory written before format gating",
    } };
    // Not Flo's: wiping is never suggested, since the directory may be
    // something else's (a data_dir set to /var/lib, say).
    if (c.other) |name| return .{ .refusal = .{
        .err = error.NotADataDir,
        .message = std.fmt.bufPrint(buf, "{s} is not a Flo data directory and isn't empty (it holds {s}); check data_dir", .{ data_path, name }) catch "not a Flo data directory",
    } };
    return .{ .fresh = true };
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
    // Exclusive, so the temporary is this write's own file, never one a
    // link named: a crash's leftover goes first.
    stdx.fs.deleteFile(tmp) catch |err| if (err != error.FileNotFound) return err;
    {
        const file = try stdx.fs.createFile(tmp, .{ .exclusive = true });
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

test "data format: an empty directory gets FORMAT, and so does one holding only what an empty one may" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    const io = stdx.io.instance();
    try tmp.dir.createDir(io, "lost+found", .default_dir);
    try tmp.dir.createDir(io, ".flo-kept-before", .default_dir);
    for ([_][]const u8{ ".hidden", LOCK_FILENAME, PID_FILENAME, ADMIN_SOCKET_FILENAME, FILENAME ++ ".tmp" }) |name| {
        stdx.fs.closeFile(try tmp.dir.createFile(io, name, .{}));
    }
    try check(testing.allocator, dir);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    const text = try stdx.fs.readFileAlloc(testing.allocator, path, 64);
    defer testing.allocator.free(text);
    try testing.expectEqualStrings("2\n", text);
    // And a second boot reads it.
    try check(testing.allocator, dir);
}

test "data format: a Flo directory without FORMAT is an older Flo's, refused with wipe, and nothing is written" {
    const io = stdx.io.instance();
    for ([_]enum { system, shard }{ .system, .shard }) |marker| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir = try tmpDataDir(&tmp);
        defer testing.allocator.free(dir);
        switch (marker) {
            .system => try tmp.dir.writeFile(io, .{ .sub_path = SYSTEM_FILENAME, .data = "{\"shards\": 2}" }),
            .shard => try tmp.dir.createDirPath(io, "00003/segs"),
        }
        try testing.expectError(error.DataFormatMissing, check(testing.allocator, dir));
        var buf: [MESSAGE_MAX]u8 = undefined;
        const r = (try checkReadOnly(testing.allocator, dir, &buf)).?;
        try testing.expectEqual(error.DataFormatMissing, r.err);
        var want: [MESSAGE_MAX]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory {s} was written by an older Flo (before format gating); this server needs format 2. There is no migration: run flo server wipe --data-dir {s}", .{ dir, dir }), r.message);
        try testing.expectError(error.FileNotFound, tmp.dir.statFile(io, FILENAME, .{}));
    }
}

test "data format: a non-empty directory that isn't Flo's is refused, without suggesting wipe" {
    const io = stdx.io.instance();
    // A file named like a shard's directory, or a directory one digit
    // short, is no shard's: neither makes the directory Flo's.
    for ([_]enum { file, short_dir, shard_named_file }{ .file, .short_dir, .shard_named_file }) |what| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir = try tmpDataDir(&tmp);
        defer testing.allocator.free(dir);
        const name = switch (what) {
            .file => "notes.txt",
            .short_dir => "0000",
            .shard_named_file => "00000",
        };
        if (what == .short_dir) try tmp.dir.createDir(io, name, .default_dir) else stdx.fs.closeFile(try tmp.dir.createFile(io, name, .{}));
        try testing.expectError(error.NotADataDir, check(testing.allocator, dir));
        var buf: [MESSAGE_MAX]u8 = undefined;
        const r = (try checkReadOnly(testing.allocator, dir, &buf)).?;
        try testing.expectEqual(error.NotADataDir, r.err);
        var want: [MESSAGE_MAX]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s} is not a Flo data directory and isn't empty (it holds {s}); check data_dir", .{ dir, name }), r.message);
        try testing.expect(std.mem.indexOf(u8, r.message, "wipe") == null);
        try testing.expectError(error.FileNotFound, tmp.dir.statFile(io, FILENAME, .{}));
    }
}

test "data format: a marker is what Flo writes, not only its name" {
    const io = stdx.io.instance();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = testing.allocator;
    // Each of these names a marker and holds something else.
    try tmp.dir.createDirPath(io, "fake/FORMAT"); // a directory named FORMAT
    try tmp.dir.createDirPath(io, "junk");
    try tmp.dir.writeFile(io, .{ .sub_path = "junk/SYSTEM", .data = "hello" });
    try tmp.dir.writeFile(io, .{ .sub_path = "junk/FORMAT", .data = "two\n" });
    try tmp.dir.createDirPath(io, "junk/94110"); // a shard's name, no segs/
    try tmp.dir.createDirPath(io, "real/00000/segs");
    try tmp.dir.writeFile(io, .{ .sub_path = "real/SYSTEM", .data = "{\"shards\": 1}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "real/FORMAT", .data = "2\n" });
    try tmp.dir.createDirPath(io, "links");
    try tmp.dir.symLink(io, "../real/SYSTEM", "links/SYSTEM", .{});
    try tmp.dir.symLink(io, "../real/00000", "links/00001", .{ .is_directory = true });

    for ([_][]const u8{ "fake", "junk", "links" }) |sub| {
        const d = try tmp.dir.openDir(io, sub, .{ .iterate = true });
        defer stdx.fs.closeDir(d);
        const c = try scan(a, d);
        if (c.flo) std.debug.print("{s} read as Flo's\n", .{sub});
        try testing.expect(!c.flo);
        try testing.expect(c.other != null);
    }
    const real = try tmp.dir.openDir(io, "real", .{ .iterate = true });
    defer stdx.fs.closeDir(real);
    for ([_]struct { name: []const u8, kind: stdx.fs.File.Kind }{ .{ .name = "00000", .kind = .directory }, .{ .name = "SYSTEM", .kind = .file }, .{ .name = "FORMAT", .kind = .file } }) |e| {
        try testing.expectEqual(Kind.marker, classify(a, real, e.name, e.kind));
    }
}

test "data format: FORMAT is written through no link a crash or anyone left in its place" {
    const io = stdx.io.instance();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    try tmp.dir.createDirPath(io, "elsewhere");
    try tmp.dir.writeFile(io, .{ .sub_path = "elsewhere/target", .data = "keep" });
    try tmp.dir.createDirPath(io, "data");
    try tmp.dir.symLink(io, "../elsewhere/target", "data/" ++ FILENAME ++ ".tmp", .{});
    const data = try std.fs.path.join(testing.allocator, &.{ dir, "data" });
    defer testing.allocator.free(data);
    try check(testing.allocator, data);
    var buf: [8]u8 = undefined;
    try testing.expectEqualStrings("keep", try tmp.dir.readFile(io, "elsewhere/target", &buf));
    try testing.expectEqualStrings("2\n", try tmp.dir.readFile(io, "data/" ++ FILENAME, &buf));
}

test "data format: every refusal is one, and nothing else is" {
    inline for (.{ Error.DataFormatTooNew, Error.DataFormatTooOld, Error.DataFormatUnreadable, Error.DataFormatMissing, Error.NotADataDir }) |e| try testing.expect(isRefusal(e));
    try testing.expect(!isRefusal(error.FileNotFound));
    try testing.expect(!isRefusal(error.DataDirInUse));
}

test "data format: a newer format is refused, and so is one that isn't a number" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    try writeFormat(dir, "3\n");
    try testing.expectError(error.DataFormatTooNew, check(testing.allocator, dir));
    // An older format: no migration, so refused, naming wipe.
    try writeFormat(dir, "1\n");
    try testing.expectError(error.DataFormatTooOld, check(testing.allocator, dir));
    var buf: [MESSAGE_MAX]u8 = undefined;
    var want: [MESSAGE_MAX]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory {s} was written by an older Flo (format 1); this server needs format 2. There is no migration: run flo server wipe --data-dir {s}", .{ dir, dir }), (try checkReadOnly(testing.allocator, dir, &buf)).?.message);
    for ([_][]const u8{ "", "\n", "0\n", "one\n", "1 \n", "-1\n", "1\n1\n", "99999999999\n", "1\n\n\n", "01", "002\n" }) |text| {
        try writeFormat(dir, text);
        try testing.expectError(error.DataFormatUnreadable, check(testing.allocator, dir));
    }
    for ([_][]const u8{ "2", "2\n" }) |text| {
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
    // Empty: nothing to refuse, and no FORMAT is written.
    try testing.expect(try checkReadOnly(testing.allocator, dir, &buf) == null);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    try testing.expectError(error.FileNotFound, stdx.fs.openFile(path, .{}));

    try writeFormat(dir, "3\n");
    const newer = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatTooNew, newer.err);
    var want: [MESSAGE_MAX]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory format 3, this binary reads format 2 ({s}); start a flo that reads format 3", .{path}), newer.message);

    try writeFormat(dir, "two\n");
    const garbage = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatUnreadable, garbage.err);
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s} holds \"two\", not a data directory format; it should hold a number such as 2", .{path}), garbage.message);
}

test "data format: a FORMAT that is a directory is refused by both checks, rather than read as missing" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmpDataDir(&tmp);
    defer testing.allocator.free(dir);
    const path = try std.fs.path.join(testing.allocator, &.{ dir, FILENAME });
    defer testing.allocator.free(path);
    try stdx.fs.makePath(path);
    var buf: [MESSAGE_MAX]u8 = undefined;
    try testing.expectError(error.DataFormatUnreadable, check(testing.allocator, dir));
    try testing.expectEqual(error.DataFormatUnreadable, (try checkReadOnly(testing.allocator, dir, &buf)).?.err);
}
