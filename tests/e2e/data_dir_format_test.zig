//! A server starts only on an empty data directory or one in its own format,
//! and `flo server wipe` empties a stopped node's: only a Flo directory's,
//! only with --yes, never a running server's, and never through a symlink.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

fn writeFile(path: []const u8, text: []const u8) !void {
    const f = try stdx.fs.createFile(path, .{ .truncate = true });
    defer stdx.fs.closeFile(f);
    try stdx.fs.writeAll(f, text);
}

fn exists(path: []const u8) bool {
    stdx.fs.access(path, .{}) catch return false;
    return true;
}

/// The names in `dir`, sorted and joined with spaces.
fn listing(allocator: std.mem.Allocator, dir_path: []const u8) ![]u8 {
    const dir = try stdx.fs.openDir(dir_path, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    var it = dir.iterate();
    while (try it.next(stdx.io.instance())) |e| try names.append(allocator, try allocator.dupe(u8, e.name));
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return std.mem.join(allocator, " ", names.items);
}

/// Starts `server` on what its data directory holds, which it must refuse
/// with exit 78 and `says` in its log.
fn expectRefused(server: *ServerProcess, says: []const u8) !void {
    server.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, server.startWithTimeout(3000));
    var why: [64]u8 = undefined;
    try testing.expectEqualStrings("exited with code 78", server.abnormalExit(&why).?);
    try testing.expect(try server.logsContain(says));
}

/// A stopped node's data directory, written by a server that ran on it.
fn ranOnce(allocator: std.mem.Allocator) !*ServerProcess {
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 2 });
    errdefer server.deinit();
    try server.start();
    server.stop();
    return server;
}

test "e2e/data-dir: a non-empty directory that isn't Flo's refuses to start, exit 78, without suggesting wipe, and is left as it was" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer server.deinit();
    const notes = try std.fmt.allocPrint(allocator, "{s}/notes.txt", .{server.data_dir});
    defer allocator.free(notes);
    try writeFile(notes, "not flo's\n");

    const says = try std.fmt.allocPrint(allocator, "{s} is not a Flo data directory and isn't empty (it holds notes.txt); check data_dir", .{server.data_dir});
    defer allocator.free(says);
    try expectRefused(server, says);
    try testing.expect(!try server.logsContain("wipe"));
    const left = try listing(allocator, server.data_dir);
    defer allocator.free(left);
    // Not even its lock file is left behind.
    try testing.expectEqualStrings("notes.txt", left);
}

test "e2e/data-dir: a Flo directory without FORMAT refuses to start, exit 78, naming wipe; after wipe the server starts on it afresh" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const format = try std.fmt.allocPrint(allocator, "{s}/FORMAT", .{server.data_dir});
    defer allocator.free(format);
    try stdx.fs.deleteFile(format);

    const says = try std.fmt.allocPrint(allocator, "data directory {s} was written by an older Flo (before format gating); this server needs format 2. There is no migration: run flo server wipe --data-dir {s}", .{ server.data_dir, server.data_dir });
    defer allocator.free(says);
    try expectRefused(server, says);
    try testing.expect(!exists(format));

    // `server status` says the same, and what SYSTEM records.
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var status = try cli.runRaw(&.{ "server", "status", "--data-dir", server.data_dir });
    defer status.deinit();
    try testing.expectEqual(@as(u8, 0), status.exit_code);
    const refused_line = try std.fmt.allocPrint(allocator, "  Format:     refused: {s}\n", .{says});
    defer allocator.free(refused_line);
    try testing.expect(status.stdoutContains(refused_line));
    try testing.expect(status.stdoutContains("  Shards:     2\n"));

    var wiped = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer wiped.deinit();
    try testing.expectEqual(@as(u8, 0), wiped.exit_code);
    const left = try listing(allocator, server.data_dir);
    defer allocator.free(left);
    try testing.expectEqualStrings("flo.lock", left);

    const again = try ServerProcess.initWithConfig(allocator, .{ .shards = 2, .data_dir_of = server.data_dir });
    defer again.deinit();
    try again.start();
    var buf: [8]u8 = undefined;
    try testing.expectEqualStrings("2\n", try stdx.fs.readFile(format, &buf));
    again.stop();
    var fresh = try cli.runRaw(&.{ "server", "status", "--data-dir", server.data_dir });
    defer fresh.deinit();
    try testing.expect(fresh.stdoutContains("  Format:     2\n"));
}

test "e2e/data-dir: wipe refuses a running server's directory, and deletes nothing" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer server.deinit();
    try server.start();
    defer server.stop();
    const before = try listing(allocator, server.data_dir);
    defer allocator.free(before);

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var wiped = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer wiped.deinit();
    try testing.expectEqual(@as(u8, 3), wiped.exit_code);
    // The lock's own log line comes first.
    const want = try std.fmt.allocPrint(allocator, "\nError: data directory {s} is in use by a running flo server; stop it first\n", .{server.data_dir});
    defer allocator.free(want);
    try testing.expect(std.mem.endsWith(u8, wiped.stderr, want));
    const after = try listing(allocator, server.data_dir);
    defer allocator.free(after);
    try testing.expectEqualStrings(before, after);
}

test "e2e/data-dir: wipe without --yes lists what it would delete and deletes nothing" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const before = try listing(allocator, server.data_dir);
    defer allocator.free(before);

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var dry = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir });
    defer dry.deinit();
    try testing.expectEqual(@as(u8, 0), dry.exit_code);
    // FORMAT and SYSTEM first, so a wipe cut short can't leave a directory a
    // server starts on.
    const want = try std.fmt.allocPrint(allocator, "Would delete 4 entries from {s}:\n  FORMAT\n  SYSTEM\n  00000\n  00001\nEverything in them is gone for good. Run again with --yes to do it.\n", .{server.data_dir});
    defer allocator.free(want);
    try testing.expectEqualStrings(want, dry.stdout);
    const after = try listing(allocator, server.data_dir);
    defer allocator.free(after);
    try testing.expectEqualStrings(before, after);
}

test "e2e/data-dir: wipe deletes the directory's contents, never the directory or what a symlink in it names" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const root = std.fs.path.dirname(server.data_dir).?;

    // Outside the data directory: a directory and a file, each named by a
    // link inside it, one at the top and one in a shard's directory.
    const outside = try std.fmt.allocPrint(allocator, "{s}/outside", .{root});
    defer allocator.free(outside);
    try stdx.fs.makePath(outside);
    const kept_file = try std.fmt.allocPrint(allocator, "{s}/precious", .{outside});
    defer allocator.free(kept_file);
    try writeFile(kept_file, "keep me\n");
    const link_dir = try std.fmt.allocPrintSentinel(allocator, "{s}/link-to-dir", .{server.data_dir}, 0);
    defer allocator.free(link_dir);
    const link_file = try std.fmt.allocPrintSentinel(allocator, "{s}/00000/link-to-file", .{server.data_dir}, 0);
    defer allocator.free(link_file);
    const outside_z = try allocator.dupeSentinel(u8, outside, 0);
    defer allocator.free(outside_z);
    const kept_z = try allocator.dupeSentinel(u8, kept_file, 0);
    defer allocator.free(kept_z);
    try testing.expectEqual(@as(c_int, 0), std.c.symlink(outside_z, link_dir));
    try testing.expectEqual(@as(c_int, 0), std.c.symlink(kept_z, link_file));
    // And what an empty directory may keep: a mount point's lost+found and
    // a dotfile.
    const lost = try std.fmt.allocPrint(allocator, "{s}/lost+found", .{server.data_dir});
    defer allocator.free(lost);
    try stdx.fs.makePath(lost);
    const dot = try std.fmt.allocPrint(allocator, "{s}/.keep", .{server.data_dir});
    defer allocator.free(dot);
    try writeFile(dot, "");

    const dir_z = try allocator.dupeSentinel(u8, server.data_dir, 0);
    defer allocator.free(dir_z);
    const before = (try stdx.fs.lstatMeta(dir_z)).?;

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var wiped = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer wiped.deinit();
    try testing.expectEqual(@as(u8, 0), wiped.exit_code);
    const want = try std.fmt.allocPrint(allocator, "Deleted 5 entries from {s}. The next start begins with an empty data directory.\n", .{server.data_dir});
    defer allocator.free(want);
    try testing.expectEqualStrings(want, wiped.stdout);

    // The same directory, as a mount point would need: emptied in place,
    // not removed and made again.
    const after = (try stdx.fs.lstatMeta(dir_z)).?;
    try testing.expectEqual(before.dev, after.dev);
    try testing.expectEqual(before.ino, after.ino);
    const left = try listing(allocator, server.data_dir);
    defer allocator.free(left);
    try testing.expectEqualStrings(".keep flo.lock lost+found", left);
    // What the links named is untouched.
    var buf: [16]u8 = undefined;
    try testing.expectEqualStrings("keep me\n", try stdx.fs.readFile(kept_file, &buf));
}

test "e2e/data-dir: wipe --keep-as moves the contents aside, where the next start leaves them" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    for ([_][]const u8{ "", "a/b", "..", "." }) |bad| {
        var refused = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--keep-as", bad, "--yes" });
        defer refused.deinit();
        try testing.expectEqual(@as(u8, 2), refused.exit_code);
        try testing.expect(refused.stderrContains(if (bad.len == 0) "Error: --keep-as needs a name" else "Error: --keep-as must be"));
    }
    const untouched = try listing(allocator, server.data_dir);
    defer allocator.free(untouched);
    try testing.expectEqualStrings("00000 00001 FORMAT SYSTEM flo.lock", untouched);

    var kept = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--keep-as", "before", "--yes" });
    defer kept.deinit();
    try testing.expectEqual(@as(u8, 0), kept.exit_code);
    const left = try listing(allocator, server.data_dir);
    defer allocator.free(left);
    try testing.expectEqualStrings(".flo-kept-before flo.lock", left);
    const aside_path = try std.fmt.allocPrint(allocator, "{s}/.flo-kept-before", .{server.data_dir});
    defer allocator.free(aside_path);
    const aside = try listing(allocator, aside_path);
    defer allocator.free(aside);
    try testing.expectEqualStrings("00000 00001 FORMAT SYSTEM", aside);

    // The next start begins afresh beside the kept copy; a second wipe
    // won't reuse the name.
    try server.start();
    server.stop();
    var again = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--keep-as", "before", "--yes" });
    defer again.deinit();
    try testing.expectEqual(@as(u8, 3), again.exit_code);
    const want = try std.fmt.allocPrint(allocator, "Error: {s}/.flo-kept-before already exists; give --keep-as another name", .{server.data_dir});
    defer allocator.free(want);
    try testing.expectEqualStrings(want, std.mem.trim(u8, again.stderr, &std.ascii.whitespace));
}

test "e2e/data-dir: wipe refuses a directory that holds no Flo data, and deletes nothing" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer server.deinit();
    const notes = try std.fmt.allocPrint(allocator, "{s}/notes.txt", .{server.data_dir});
    defer allocator.free(notes);
    try writeFile(notes, "not flo's\n");

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var refused = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer refused.deinit();
    try testing.expectEqual(@as(u8, 3), refused.exit_code);
    const want = try std.fmt.allocPrint(allocator, "Error: {s} holds no Flo data (no FORMAT or SYSTEM Flo wrote, no shard directory); wipe deletes only a Flo data directory's contents", .{server.data_dir});
    defer allocator.free(want);
    try testing.expectEqualStrings(want, std.mem.trim(u8, refused.stderr, &std.ascii.whitespace));
    try testing.expect(exists(notes));
}

test "e2e/data-dir: wipe needs the directory named, and never deletes a default one" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const before = try listing(allocator, server.data_dir);
    defer allocator.free(before);
    const root = std.fs.path.dirname(server.data_dir).?;
    // A config that names no data_dir: the default would be ~/.flo/data.
    const bare = try std.fmt.allocPrint(allocator, "{s}/bare.toml", .{root});
    defer allocator.free(bare);
    try writeFile(bare, "[server]\nport = 9000\n");

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    for ([_]struct { args: []const []const u8, says: []const u8 }{
        .{ .args = &.{ "server", "wipe", "--yes" }, .says = "Error: wipe needs --data-dir, or -c with a config that sets data_dir: it never deletes a default directory\n" },
        .{ .args = &.{ "server", "wipe", "--data-dir", "", "--yes" }, .says = "Error: --data-dir is empty; wipe needs the directory it deletes named\n" },
        .{ .args = &.{ "server", "wipe", "-c", "", "--yes" }, .says = "Error: wipe needs --data-dir, or -c with a config that sets data_dir: it never deletes a default directory\n" },
        .{ .args = &.{ "server", "wipe", "-c", bare, "--yes" }, .says = "sets no data_dir, and wipe never deletes a default directory; give --data-dir\n" },
    }) |c| {
        var r = try cli.runRaw(c.args);
        defer r.deinit();
        try testing.expectEqual(@as(u8, 2), r.exit_code);
        try testing.expect(std.mem.endsWith(u8, r.stderr, c.says));
    }
    // A config that names it is enough.
    const named = try std.fmt.allocPrint(allocator, "{s}/named.toml", .{root});
    defer allocator.free(named);
    const named_text = try std.fmt.allocPrint(allocator, "[server]\ndata_dir = \"{s}\"\n", .{server.data_dir});
    defer allocator.free(named_text);
    try writeFile(named, named_text);
    var by_config = try cli.runRaw(&.{ "server", "wipe", "-c", named });
    defer by_config.deinit();
    try testing.expectEqual(@as(u8, 0), by_config.exit_code);
    try testing.expect(by_config.stdoutContains(server.data_dir));
    const after = try listing(allocator, server.data_dir);
    defer allocator.free(after);
    try testing.expectEqualStrings(before, after);
}

test "e2e/data-dir: names that only look like Flo's markers are no Flo data: boot refuses, wipe refuses, nothing goes" {
    const allocator = testing.allocator;
    const server = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer server.deinit();
    const d = server.data_dir;
    const root = std.fs.path.dirname(d).?;
    // A SYSTEM that isn't one, a directory named FORMAT, a shard-named
    // directory with no log, and a SYSTEM link to a real one elsewhere.
    const real = try std.fmt.allocPrint(allocator, "{s}/real-SYSTEM", .{root});
    defer allocator.free(real);
    try writeFile(real, "{\"shards\": 1}");
    for ([_][]const u8{ "FORMAT", "94110" }) |n| {
        const p = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ d, n });
        defer allocator.free(p);
        try stdx.fs.makePath(p);
    }
    const resume_path = try std.fmt.allocPrint(allocator, "{s}/resume.txt", .{d});
    defer allocator.free(resume_path);
    try writeFile(resume_path, "mine\n");
    const link = try std.fmt.allocPrintSentinel(allocator, "{s}/SYSTEM", .{d}, 0);
    defer allocator.free(link);
    const real_z = try allocator.dupeSentinel(u8, real, 0);
    defer allocator.free(real_z);
    try testing.expectEqual(@as(c_int, 0), std.c.symlink(real_z, link));
    const before = try listing(allocator, d);
    defer allocator.free(before);

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var wiped = try cli.runRaw(&.{ "server", "wipe", "--data-dir", d, "--yes" });
    defer wiped.deinit();
    try testing.expectEqual(@as(u8, 3), wiped.exit_code);
    try testing.expect(wiped.stderrContains("holds no Flo data"));
    server.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, server.startWithTimeout(3000));
    var why: [64]u8 = undefined;
    try testing.expectEqualStrings("exited with code 78", server.abnormalExit(&why).?);
    try testing.expect(try server.logsContain("FORMAT doesn't hold a data directory format"));
    const after = try listing(allocator, d);
    defer allocator.free(after);
    try testing.expectEqualStrings(before, after);
}

test "e2e/data-dir: wipe names what Flo never wrote before deleting it, in the dry run and with --yes" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    const stray = try std.fmt.allocPrint(allocator, "{s}/resume.txt", .{server.data_dir});
    defer allocator.free(stray);
    try writeFile(stray, "mine\n");

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var dry = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir });
    defer dry.deinit();
    try testing.expectEqual(@as(u8, 0), dry.exit_code);
    const want = try std.fmt.allocPrint(allocator, "Would delete 5 entries from {s}:\n  FORMAT\n  SYSTEM\n  resume.txt\n  00000\n  00001\n1 of them Flo never writes, and would go too:\n  resume.txt\nEverything in them is gone for good. Run again with --yes to do it.\n", .{server.data_dir});
    defer allocator.free(want);
    try testing.expectEqualStrings(want, dry.stdout);
    try testing.expect(exists(stray));

    var yes = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer yes.deinit();
    try testing.expectEqual(@as(u8, 0), yes.exit_code);
    const warned = try std.fmt.allocPrint(allocator, "Warning: 1 entries in {s} are not Flo's, and go with the rest:\n  resume.txt\n", .{server.data_dir});
    defer allocator.free(warned);
    try testing.expectEqualStrings(warned, yes.stderr);
    try testing.expect(!exists(stray));
}

test "e2e/data-dir: a wipe cut short leaves a directory the server refuses, naming wipe, and a second wipe finishes" {
    const allocator = testing.allocator;
    const server = try ranOnce(allocator);
    defer server.deinit();
    // A stray directory whose contents can't be deleted: the wipe stops
    // there, after FORMAT and SYSTEM and before the shards.
    const stuck = try std.fmt.allocPrint(allocator, "{s}/stray/inner", .{server.data_dir});
    defer allocator.free(stuck);
    try stdx.fs.makePath(stuck);
    const stray_z = try std.fmt.allocPrintSentinel(allocator, "{s}/stray", .{server.data_dir}, 0);
    defer allocator.free(stray_z);
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(stray_z, 0o500));
    defer _ = std.c.chmod(stray_z, 0o700);

    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();
    var cut = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer cut.deinit();
    try testing.expectEqual(@as(u8, 3), cut.exit_code);
    try testing.expect(cut.stderrContains("/stray: "));
    try testing.expect(cut.stderrContains("run wipe again to finish"));
    const left = try listing(allocator, server.data_dir);
    defer allocator.free(left);
    try testing.expectEqualStrings("00000 00001 flo.lock stray", left);

    const says = try std.fmt.allocPrint(allocator, "data directory {s} was written by an older Flo (before format gating); this server needs format 2. There is no migration: run flo server wipe --data-dir {s}", .{ server.data_dir, server.data_dir });
    defer allocator.free(says);
    const again = try ServerProcess.initWithConfig(allocator, .{ .shards = 2, .data_dir_of = server.data_dir });
    defer again.deinit();
    try expectRefused(again, says);

    try testing.expectEqual(@as(c_int, 0), std.c.chmod(stray_z, 0o700));
    var done = try cli.runRaw(&.{ "server", "wipe", "--data-dir", server.data_dir, "--yes" });
    defer done.deinit();
    try testing.expectEqual(@as(u8, 0), done.exit_code);
    const empty = try listing(allocator, server.data_dir);
    defer allocator.free(empty);
    try testing.expectEqualStrings("flo.lock", empty);
}
