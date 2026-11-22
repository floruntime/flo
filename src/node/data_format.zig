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
pub const CURRENT: u32 = 55;

/// What each format stores: the log's segment version and a hash of the
/// entry layouts in it. A new segment version or layout is a new format, so
/// an older directory is refused here at boot, naming wipe, rather than at a
/// segment open. The check below fails the build until they move together.
/// Refusal reasons and ops are never stored, so they don't change the hash.
const formats = [_]struct { format: u32, segment_version: u16, entries: u64 }{
    .{ .format = 1, .segment_version = 2, .entries = 0x69be2cbe871ae8d3 },
    .{ .format = 2, .segment_version = 3, .entries = 0x2bef332533330bdf },
    .{ .format = 3, .segment_version = 4, .entries = 0x3ff1b4bff4565f2 },
    // Namespace ids are dense and allocated by shard 0, not name hashes.
    .{ .format = 4, .segment_version = 4, .entries = 0x3ff1b4bff4565f2 },
    // Action entries are schemas, and deliveries are logged.
    .{ .format = 5, .segment_version = 5, .entries = 0x8a485f4c721d6c8d },
    // KV entries carry their conditions, decided when they apply.
    .{ .format = 6, .segment_version = 6, .entries = 0xaf37ccaa7271c1e5 },
    // A stream delete without --force is decided when it applies, so one
    // logged before would now be refused if its stream held anything.
    .{ .format = 7, .segment_version = 6, .entries = 0xaf37ccaa7271c1e5 },
    // A config entry names its cluster, generation and next id.
    .{ .format = 8, .segment_version = 6, .entries = 0xaf37ccaa7271c1e5 },
    // Workflow enablement and stream settings are logged; the layouts the
    // hash covers are unchanged.
    .{ .format = 9, .segment_version = 6, .entries = 0xaf37ccaa7271c1e5 },
    // KV keys lead with their namespace's id, and a batch names it once;
    // log payloads name the namespace by id.
    .{ .format = 10, .segment_version = 7, .entries = 0xa27e039b5f22bd5e },
    // Queue entries are schemas, and deliveries and leases are logged.
    .{ .format = 11, .segment_version = 8, .entries = 0xd744c48806095be5 },
    // A run's first step is logged: a log from before holds runs that took
    // theirs unlogged, which replay here would take again.
    .{ .format = 12, .segment_version = 8, .entries = 0xd744c48806095be5 },
    // An enqueue carries its leader's queue caps; dead letters are no
    // longer evicted.
    .{ .format = 13, .segment_version = 9, .entries = 0x360066e8135ecf4c },
    // Stream trims, deletes and group commits are read under their entry's
    // namespace id, not a name-qualified key; the layouts are unchanged.
    .{ .format = 14, .segment_version = 9, .entries = 0x360066e8135ecf4c },
    // Conditions are checked at apply: a completion of a finished run, a
    // start after its workflow's disable and a stop of a job that isn't
    // running now change nothing; the layouts are unchanged.
    .{ .format = 15, .segment_version = 9, .entries = 0x360066e8135ecf4c },
    // Consumer groups are on the delivery model: their deliveries, leases
    // and dead letters are logged per record.
    .{ .format = 16, .segment_version = 10, .entries = 0xcf412e86efc40d19 },
    // Workflow keys lead with their namespace's id, not its name; the
    // layouts are unchanged.
    .{ .format = 17, .segment_version = 10, .entries = 0xcf412e86efc40d19 },
    // A KV request no longer carries a routing key, so a value takes the
    // room it held: a stored value may be longer.
    .{ .format = 18, .segment_version = 11, .entries = 0xd84de9970c60f47e },
    // Action entries name their namespace by id.
    .{ .format = 19, .segment_version = 12, .entries = 0x6a7954352202e5d7 },
    // The newest segment is appended to until it is sealed at 64 MiB, so a
    // segment's header holds marks a flush updates; the entry layouts are
    // unchanged.
    .{ .format = 20, .segment_version = 13, .entries = 0x6a7954352202e5d7 },
    // Keys route over a fixed 4096 slots, not a partition count chosen at
    // creation, and a run id carries its definition's slot: above 128
    // shards a key's owner differs. SYSTEM no longer records partitions.
    // The layouts are unchanged.
    .{ .format = 21, .segment_version = 13, .entries = 0x6a7954352202e5d7 },
    // Namespace binds are gone: shards read names from the namespace table.
    .{ .format = 22, .segment_version = 14, .entries = 0x7356ae3192283144 },
    // A group release says how its records were handed back (failed,
    // abandoned or unread), not whether the leader freed them.
    .{ .format = 23, .segment_version = 15, .entries = 0xc9eb5825e509ecdd },
    // The cluster-settings entries (the memory-budget pool share and per-node
    // RAM) are logged, so their layouts join the pinned hash.
    .{ .format = 24, .segment_version = 16, .entries = 0x55eb3403850faf9 },
    // No entry names a shard: an action invoke and run drop the caller's
    // shard (the calling workflow's name says it), and an action extend
    // names its delivery's count, checked at apply.
    .{ .format = 25, .segment_version = 17, .entries = 0xf5b0a7ef40e425d2 },
    // Time-series points are contract family state (`ts.series`,
    // `ts.point`), so a snapshot holds them; the layouts are unchanged.
    .{ .format = 26, .segment_version = 17, .entries = 0xf5b0a7ef40e425d2 },
    // Channels: messages a group takes in from another, outbox trims, and
    // the test machine's probe.
    .{ .format = 27, .segment_version = 18, .entries = 0xf9606d0319816e24 },
    // A stream append can arrive inside a channel inbound, its record found
    // at its place there; an older build reads such a record as missing.
    // A channel message carries the entry flags it applies with.
    .{ .format = 28, .segment_version = 19, .entries = 0x336fb98011d1835f },
    // Time series: each measurement field's newest time and late-point
    // counts (`ts.field`), so a processing source reads behind a window.
    .{ .format = 29, .segment_version = 19, .entries = 0x336fb98011d1835f },
    // A channel message whose target is full is parked under it, and a
    // `channel_unpark` applies a target's parked messages once it has room.
    .{ .format = 30, .segment_version = 20, .entries = 0xc4e3a23975b2cf62 },
    // A pipeline's sink writes and its source cursor are one entry
    // (`processing_sink_batch`), sent as channel messages; the cursor is
    // the sinks machine's (`processing.cursor`). `processing_checkpoint`
    // is gone.
    .{ .format = 31, .segment_version = 21, .entries = 0xf3bbabb586de9d4c },
    // A channel's outbox record counts the payload bytes it holds
    // (`channel.out_meta`), so a snapshot holds them; the log is unchanged.
    .{ .format = 32, .segment_version = 21, .entries = 0xf3bbabb586de9d4c },
    // A message carries who it was sent for (`origin`): its kind and its
    // namespace, in the inbound, a sink batch and the sender's outbox
    // record; and an outbox keeps each namespace's share (`channel.out_ns`).
    .{ .format = 33, .segment_version = 22, .entries = 0xa7b17987ff316599 },
    // A consumer group's operator entries: a steal, a requeue and a drop;
    // and a group's record keeps its holder's last touch (`touched_ns`).
    .{ .format = 34, .segment_version = 23, .entries = 0x95cded206a3cc3cc },
    // A queue's operator entries: a requeue and a drop of dead messages.
    .{ .format = 35, .segment_version = 24, .entries = 0x56b2734987bd4319 },
    // A workflow step's action run is a call (`step_call`, its retry and
    // clear), recorded on the workflow's shard (`action.calls`); its start
    // goes to the action's slot as a message, answered by `call_reply`
    // and, when the run ends, `call_done`. A task its await couldn't give
    // goes back to its run's shard the same way (`action_abandon_send`,
    // `action_abandon`).
    .{ .format = 36, .segment_version = 25, .entries = 0x6f01d8b343532827 },
    // A stream's existence, name and settings are the streams machine's
    // (`stream.meta`, `stream.retention`), kept in snapshots; the log is
    // unchanged.
    .{ .format = 37, .segment_version = 25, .entries = 0x6f01d8b343532827 },
    // The last non-provisional memory budget is logged (`settings_budget_clean`),
    // so a replica restarted while the budget is frozen recovers the held value.
    .{ .format = 38, .segment_version = 26, .entries = 0x2e7fd9793d6057d1 },
    // A step's call (`step_call`, its retry and clear, and the `call_reply`
    // and `call_done` that answer it) is one record for an action or a child
    // workflow (`step.calls`). A child's start goes to its workflow's shard
    // as `child_start`, kept there as the child's parent (`workflow.parents`)
    // until the child ends; a start that can't be made is `child_refuse`.
    .{ .format = 39, .segment_version = 27, .entries = 0x63d104871f8a2730 },
    // A stream's appends are records the snapshot keeps (`stream.record`,
    // and `stream.reclaim` for appends a trim left to remove), and its last
    // id, floor and counts are in `stream.meta`; the log is unchanged.
    .{ .format = 40, .segment_version = 27, .entries = 0x63d104871f8a2730 },
    // A minted run id carries its leader's term (`{shard}-{term}`), so a
    // shard's next leader never mints its last leader's ids; a run id field
    // holds 42 bytes.
    .{ .format = 41, .segment_version = 28, .entries = 0x6d5c0936fb61791f },
    // A processing job lives in `processing.job` on its first source's
    // shard, applied by the processing machine with its pipelines' cursors;
    // a sink batch carries the source records it covers, kept with its
    // cursor (`processing.cursor.records`).
    .{ .format = 42, .segment_version = 29, .entries = 0xecc2f68c0588f846 },
    // HARDSTATE carries the vouch a guarded node took, with the guard, so a
    // restart part way through catching up keeps it; the log is unchanged.
    .{ .format = 43, .segment_version = 29, .entries = 0xecc2f68c0588f846 },
    // A stream's delete leaves a tombstone holding its last id and floor,
    // its appends removed later by `stream_clean` entries and the tombstone
    // dropped by `stream_forget`, both new to the log.
    .{ .format = 44, .segment_version = 29, .entries = 0xecc2f68c0588f846 },
    // A processing job's name is at most 4096 bytes and a workflow's version
    // at most 256, so a logged definition past either no longer parses: a
    // replayed workflow create would halt. The log is unchanged.
    .{ .format = 45, .segment_version = 29, .entries = 0xecc2f68c0588f846 },
    // A repair skip may name one message of an inbound (`seq`); a call's
    // reply may say it was skipped (`Answer.skipped`, with `why`).
    .{ .format = 46, .segment_version = 30, .entries = 0xf25fb51a91e5d97f },
    // Workflow runs are applied state (`workflow.run`, `.hist`, `.out`,
    // `.idem`, `.per_def`); a completion names its run's workflow; a child
    // whose workflow is disabled is refused to its parent (`Answer.refused`).
    .{ .format = 47, .segment_version = 31, .entries = 0x96cb0c6a272e3e58 },
    // A run's steps are logged (`workflow_step`, which replaces
    // `workflow_step_started`), with its wait and its call (`step_call`
    // goes); the calls family moves to the runs machine; a completion is a
    // cancel's alone.
    .{ .format = 48, .segment_version = 32, .entries = 0x3c67c9e3c68aaa5f },
    // A client's signal is logged (`workflow_signal`), kept in
    // `workflow.sig`, and wakes a run waiting on its type where it applies.
    .{ .format = 49, .segment_version = 33, .entries = 0x3c67c9e3c68aaa5f },
    // A KV delete or a smaller overwrite drops the key's history; the
    // leader's reap (`kv_reap`) drops expired keys and old tombstones; a
    // namespace's counts carry its version floor.
    .{ .format = 50, .segment_version = 34, .entries = 0xcfbf4b10fbb9d7aa },
    // A trigger's start says the stream it read and its event, and moves
    // its workflow's place in `workflow.per_def`; idempotency keys with a
    // leading '_' are the server's.
    .{ .format = 51, .segment_version = 35, .entries = 0xcfbf4b10fbb9d7aa },
    // Each shard's leader logs the memory share it is held to
    // (`settings_budget_bind`, kept in `settings.bind`); a record's resident
    // figure is 160 bytes, and the channels' outboxes and parked messages
    // count toward the pool.
    .{ .format = 52, .segment_version = 36, .entries = 0x80669ad44c490e20 },
    // Channels have two lanes, each its own seqs: an inbound and a trim name
    // theirs, and channel state is keyed by peer and lane.
    .{ .format = 53, .segment_version = 37, .entries = 0x1b85e5e8bc66f111 },
    // A sink's messages ride a channel lane of their own: channel state
    // keyed by lane 2 holds them, and its lanes 0 and 1 none.
    .{ .format = 54, .segment_version = 38, .entries = 0x1b85e5e8bc66f111 },
    // A run's history entries (`history_chunk`, `_index`, `_drop`), kept by
    // the stream machine in `history.wf` and `history.act`.
    .{ .format = 55, .segment_version = 39, .entries = 0xdf4678ed30444e0c },
};

comptime {
    for (formats[1..], formats[0 .. formats.len - 1]) |row, prev| {
        // A format may change what stored bytes mean without changing a
        // layout, so two formats can share a segment version.
        if (row.format != prev.format + 1 or row.segment_version < prev.segment_version)
            @compileError("formats: each row is the next format, with the same segment version or a newer one");
    }
    const row = formats[formats.len - 1];
    if (row.format != CURRENT)
        @compileError(std.fmt.comptimePrint("formats: the last row is format {d}, but CURRENT is {d}", .{ row.format, CURRENT }));
    const segment_version = @import("../storage/ual/segment.zig").SEGMENT_VERSION;
    if (segment_version != row.segment_version)
        @compileError(std.fmt.comptimePrint("SEGMENT_VERSION is {d}, but format {d} stores segment version {d}: a new segment version is a new format; bump CURRENT and add its row", .{ segment_version, CURRENT, row.segment_version }));
    const entries = @import("../protocol/ops.zig").entryTableHash(&@import("../protocol/table.zig").entry_table);
    if (entries != row.entries)
        @compileError(std.fmt.comptimePrint("the stored entry layouts hash to 0x{x}, but format {d} pins 0x{x}: a layout change needs a new SEGMENT_VERSION and format; bump both and add the row", .{ entries, CURRENT, row.entries }));
}
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
    try testing.expectEqualStrings(std.fmt.comptimePrint("{d}\n", .{CURRENT}), text);
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
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory {s} was written by an older Flo (before format gating); this server needs format {d}. There is no migration: run flo server wipe --data-dir {s}", .{ dir, CURRENT, dir }), r.message);
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
    try tmp.dir.writeFile(io, .{ .sub_path = "real/FORMAT", .data = std.fmt.comptimePrint("{d}\n", .{CURRENT}) });
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
    try testing.expectEqualStrings(std.fmt.comptimePrint("{d}\n", .{CURRENT}), try tmp.dir.readFile(io, "data/" ++ FILENAME, &buf));
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
    try writeFormat(dir, std.fmt.comptimePrint("{d}\n", .{CURRENT + 1}));
    try testing.expectError(error.DataFormatTooNew, check(testing.allocator, dir));
    // An older format: no migration, so refused, naming wipe.
    try writeFormat(dir, "1\n");
    try testing.expectError(error.DataFormatTooOld, check(testing.allocator, dir));
    var buf: [MESSAGE_MAX]u8 = undefined;
    var want: [MESSAGE_MAX]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory {s} was written by an older Flo (format 1); this server needs format {d}. There is no migration: run flo server wipe --data-dir {s}", .{ dir, CURRENT, dir }), (try checkReadOnly(testing.allocator, dir, &buf)).?.message);
    for ([_][]const u8{ "", "\n", "0\n", "one\n", "1 \n", "-1\n", "1\n1\n", "99999999999\n", "1\n\n\n", "01", "002\n" }) |text| {
        try writeFormat(dir, text);
        try testing.expectError(error.DataFormatUnreadable, check(testing.allocator, dir));
    }
    for ([_][]const u8{ std.fmt.comptimePrint("{d}", .{CURRENT}), std.fmt.comptimePrint("{d}\n", .{CURRENT}) }) |text| {
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

    try writeFormat(dir, std.fmt.comptimePrint("{d}\n", .{CURRENT + 1}));
    const newer = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatTooNew, newer.err);
    var want: [MESSAGE_MAX]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "data directory format {d}, this binary reads format {d} ({s}); start a flo that reads format {d}", .{ CURRENT + 1, CURRENT, path, CURRENT + 1 }), newer.message);

    try writeFormat(dir, "two\n");
    const garbage = (try checkReadOnly(testing.allocator, dir, &buf)).?;
    try testing.expectEqual(error.DataFormatUnreadable, garbage.err);
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s} holds \"two\", not a data directory format; it should hold a number such as {d}", .{ path, CURRENT }), garbage.message);
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
