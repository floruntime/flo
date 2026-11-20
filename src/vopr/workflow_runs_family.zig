//! Workflow runs as the server keeps them: the runs machine
//! (`workflow/runs_machine.zig`) on the definitions' store (a start reads
//! whether its workflow is enabled), the checker comparing its families'
//! checksums, and each entry checked against a ledger of the runs the
//! entries made, kept apart from the machine:
//!
//! - a start creates its run, its first history event, its idempotency key
//!   and its workflow's count; one whose key started a run already creates
//!   nothing, one whose run exists is refused, a client or schedule start
//!   of a disabled workflow is refused, and a disabled child creates no run
//!   and is refused to its parent without the entry being refused;
//! - a schedule start fires its slot, which never moves back; no other
//!   start moves it, whatever its key;
//! - a run's first step is taken once, and never after it ends;
//! - a completion ends its run once, with its history (the last 256) and
//!   step outputs; another with the same status changes nothing, one with
//!   another status is refused;
//! - a child's parent hears its start (only a child start's), its refusal,
//!   a disabled workflow, and its end, each once;
//! - the store holds what the ledger holds, and on its own: each workflow's
//!   count is its runs not ended, each run's history is its last seqs, and
//!   each idempotency key names a run of that workflow with that key.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const channels = @import("../contract/channels.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const rm = @import("../workflow/runs_machine.zig");
const defs_family = @import("workflow_defs_family.zig");

const Allocator = std.mem.Allocator;
const EntryType = entry_mod.EntryType;
const RunStatus = rm.RunStatus;
const NS: @import("../namespace/table.zig").Id = .default;
const NAMES = defs_family.NAMES;

/// Runs are never removed, so the workload has room for each to start.
const RUNS = blk: {
    @setEvalBranchQuota(100_000);
    var out: [128][]const u8 = undefined;
    for (&out, 0..) |*x, i| x.* = std.fmt.comptimePrint("r{d}", .{i});
    break :blk out;
};
/// Client keys, and a schedule's (its slot last): slots out of order, so a
/// later start can carry an earlier slot.
const IDEMS = [_][]const u8{ "k0", "k1", "schedule:16:w:3000", "schedule:16:w:1000", "schedule:16:w:2000" };
const SLOT_OF = [_]?i64{ null, null, 3000, 1000, 2000 };
const EVENTS = [_][]const u8{ "workflow_started", "schedule_started", "trigger_started", "child_started" };
const SCHEDULE = 1;
const TRIGGER = 2;
const CHILD = 3;
const STEPS = [_][]const u8{ "a", "b", "c" };
const OUTCOMES = [_][]const u8{ "done", "failed" };
const OUTPUTS = [_][]const u8{ "o0", "o1" };
const PARENT_RUNS = [_][]const u8{ "p0", "p1" };
const PARENT_SLOTS = [_]u16{ 3, 700 };
const ENDS = [_]RunStatus{ .completed, .failed, .cancelled, .timed_out };
const INPUT = "{}";
/// The whole store is checked at every index this divides, as the
/// checksums are sampled; the run an entry names, at every index.
const SWEEP_EVERY = 16;
const DISABLED_WHY = "workflow is disabled";

/// A run's history past its start's event: a step's events are "e" at 0
/// ms, a cancel's is "c" at its entry's stamp. Not named by seq: a step
/// made at a rev ahead of the leader's may apply after another step.
const STEP_EVENT = "e";
const CANCEL_EVENT = "c";

pub const WorkflowRuns = struct {
    scratch: std.heap.ArenaAllocator,
    ledger: Ledger = .{},

    const RunL = struct {
        wf: usize,
        status: RunStatus,
        stepped: bool,
        idem: ?usize,
        created_ms: i64,
        completed_ms: i64 = 0,
        output: ?usize = null,
        hist_next: u64,
        /// The start's event, at seq 0.
        started: usize,
        /// Its steps taken: a step applies only at this.
        rev: u64 = 0,
        /// The seq of a cancel's event, at `completed_ms`.
        ended_at: ?u64 = null,
        outs: [STEPS.len]?usize = @splat(null),
    };
    const PerDefL = struct { active: u32, fired_ms: i64 };
    const ParentL = struct { slot: usize, parent_run: usize, started: bool };

    const Ledger = struct {
        runs: [RUNS.len]?RunL = @splat(null),
        per_def: [NAMES.len]?PerDefL = @splat(null),
        idem: [NAMES.len][IDEMS.len]?usize = @splat(@splat(null)),
        parents: [RUNS.len]?ParentL = @splat(null),
    };

    pub const FAMILIES = [_]u16{ rm.RunFam.ID, rm.HistFam.ID, rm.OutFam.ID, rm.IdemFam.ID, rm.PerDefFam.ID, rm.Parents.ID };
    pub const FAMILY_NAMES = [_][]const u8{ "workflow.run", "workflow.hist", "workflow.out", "workflow.idem", "workflow.per_def", "workflow.parents" };

    pub const Kind = enum { start, step, complete, child_start, child_refuse };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        broke: ?[]const u8 = null,
    };

    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
        }
    };

    pub fn init(allocator: Allocator) WorkflowRuns {
        return .{ .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *WorkflowRuns) void {
        self.scratch.deinit();
    }

    /// Replay re-applies from the start: the ledger goes with the store.
    pub fn reset(self: *WorkflowRuns) void {
        self.ledger = .{};
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(EntryType, @fromBackingInt(entry_type))) {
            .workflow_start, .workflow_step, .workflow_complete, .child_start, .child_refuse => true,
            else => false,
        };
    }

    fn indexOf(comptime list: []const []const u8, s: []const u8) ?usize {
        for (list, 0..) |x, i| if (std.mem.eql(u8, x, s)) return i;
        return null;
    }

    fn slotIndex(slot: u16) ?usize {
        for (PARENT_SLOTS, 0..) |s, i| if (s == slot) return i;
        return null;
    }

    /// A message the ledger says the entry sends a parent.
    const Send = struct { entry_type: EntryType, slot: usize, run: usize, answer: ?ae.Answer = null, why: ?[]const u8 = null, done: ?ae.DoneStatus = null, result: []const u8 = "" };
    const Want = struct { refused: bool = false, sends: [2]?Send = @splat(null) };

    fn doneOf(s: RunStatus) ae.DoneStatus {
        return switch (s) {
            .completed => .completed,
            .cancelled => .cancelled,
            .timed_out => .timed_out,
            else => .failed,
        };
    }

    /// What the ledger says `e` does, made so in the ledger: a refusal
    /// returns before it writes. Null for an entry the workload never makes.
    fn predict(self: *WorkflowRuns, defs: *const defs_family.WorkflowDefs, e: *const entry_mod.Entry, kind: Kind) ?Want {
        var want: Want = .{};
        const l = &self.ledger;
        const stamp_ms: i64 = @intCast(e.header.timestamp_ns / std.time.ns_per_ms);
        var diag: schema.Diagnostic = .{};
        switch (kind) {
            .child_start => {
                const p = ae.ChildStart.decodeAll(e.payload, &diag) catch return null;
                const r = indexOf(&RUNS, p.run) orelse return null;
                const slot = slotIndex(p.parent_slot) orelse return null;
                const pr = indexOf(&PARENT_RUNS, p.parent_run) orelse return null;
                if (l.parents[r]) |f| {
                    // The same call sent again: the first stands.
                    if (f.slot == slot and f.parent_run == pr) return want;
                    want.sends[0] = .{ .entry_type = .call_reply, .slot = slot, .run = r, .answer = .conflict };
                    return want;
                }
                l.parents[r] = .{ .slot = slot, .parent_run = pr, .started = false };
            },
            .child_refuse => {
                const p = ae.ChildRefuse.decodeAll(e.payload, &diag) catch return null;
                const r = indexOf(&RUNS, p.run) orelse return null;
                const f = l.parents[r] orelse return want;
                if (f.started) return want;
                want.sends[0] = .{ .entry_type = .call_reply, .slot = f.slot, .run = r, .answer = p.answer };
                l.parents[r] = null;
            },
            .start, .step, .complete => {
                const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
                const r = indexOf(&RUNS, cmd.key[4..]) orelse return null;
                switch (kind) {
                    .step => {
                        const st = rm.Step.decodeAll(cmd.value, &diag) catch return null;
                        var run = l.runs[r] orelse return .{ .refused = true };
                        if (!std.mem.eql(u8, st.workflow, NAMES[run.wf])) return null;
                        if (run.status.isTerminal() or st.rev != run.rev) return .{ .refused = true };
                        run.hist_next += st.events.count;
                        var outs = st.outputs.iterator();
                        while (outs.next() catch return null) |o| {
                            run.outs[indexOf(&STEPS, o.step) orelse return null] = indexOf(&OUTCOMES, o.outcome) orelse return null;
                        }
                        run.rev += 1;
                        run.stepped = true;
                        l.runs[r] = run;
                    },
                    .start => {
                        const s = rm.Start.parse(cmd.value) orelse return null;
                        const w = indexOf(&NAMES, s.workflow) orelse return null;
                        const ev = indexOf(&EVENTS, s.event) orelse return null;
                        const k: ?usize = if (s.idem) |key| indexOf(&IDEMS, key) orelse return null else null;
                        // A key that started a run answers with it.
                        if (k) |ki| if (l.idem[w][ki] != null) return want;
                        if (l.runs[r] != null) return .{ .refused = true };
                        const disabled = if (defs.ledger[w]) |d| !d.enabled else false;
                        if (disabled) {
                            if (ev == CHILD) {
                                const f = l.parents[r] orelse return want;
                                if (f.started) return want;
                                want.sends[0] = .{ .entry_type = .call_reply, .slot = f.slot, .run = r, .answer = .refused, .why = DISABLED_WHY };
                                l.parents[r] = null;
                                return want;
                            }
                            if (ev != TRIGGER) return .{ .refused = true };
                        }
                        l.runs[r] = .{ .wf = w, .status = s.status, .stepped = false, .idem = k, .created_ms = stamp_ms, .hist_next = 1, .started = ev };
                        if (k) |ki| l.idem[w][ki] = r;
                        var per = l.per_def[w] orelse PerDefL{ .active = 0, .fired_ms = 0 };
                        if (!s.status.isTerminal()) per.active += 1;
                        if (ev == SCHEDULE) if (k) |ki| if (SLOT_OF[ki]) |slot| {
                            per.fired_ms = @max(per.fired_ms, slot);
                        };
                        l.per_def[w] = per;
                        if (ev == CHILD) if (l.parents[r]) |f| if (!f.started) {
                            l.parents[r].?.started = true;
                            want.sends[0] = .{ .entry_type = .call_reply, .slot = f.slot, .run = r, .answer = .started };
                        };
                    },
                    .complete => {
                        const c = rm.Complete.parse(cmd.value) orelse return null;
                        var run = l.runs[r] orelse return null;
                        if (run.status.isTerminal()) {
                            if (run.status == c.status) return want;
                            return .{ .refused = true };
                        }
                        run.status = c.status;
                        run.completed_ms = stamp_ms;
                        run.output = if (c.output) |o| indexOf(&OUTPUTS, o) orelse return null else null;
                        run.ended_at = run.hist_next;
                        run.hist_next += 1;
                        l.runs[r] = run;
                        if (l.per_def[run.wf]) |*per| per.active -|= 1;
                        if (l.parents[r]) |f| {
                            want.sends[0] = .{ .entry_type = .call_done, .slot = f.slot, .run = r, .done = doneOf(c.status), .result = if (run.output) |o| OUTPUTS[o] else "" };
                            l.parents[r] = null;
                        }
                    },
                    else => unreachable,
                }
            },
        }
        return want;
    }

    /// The run `e` names.
    fn runOf(e: *const entry_mod.Entry, kind: Kind) ?usize {
        var diag: schema.Diagnostic = .{};
        return switch (kind) {
            .child_start => indexOf(&RUNS, (ae.ChildStart.decodeAll(e.payload, &diag) catch return null).run),
            .child_refuse => indexOf(&RUNS, (ae.ChildRefuse.decodeAll(e.payload, &diag) catch return null).run),
            else => indexOf(&RUNS, (entry_mod.CommandPayload.deserialize(e.payload) orelse return null).key[4..]),
        };
    }

    fn kindOf(t: EntryType) Kind {
        return switch (t) {
            .workflow_start => .start,
            .workflow_step => .step,
            .workflow_complete => .complete,
            .child_start => .child_start,
            else => .child_refuse,
        };
    }

    /// Apply `e` as the shard does, on the definitions' `store`, then check
    /// it against the ledger. A refusal must leave every family as it was.
    pub fn apply(self: *WorkflowRuns, store: *store_mod.Store, defs: *const defs_family.WorkflowDefs, e: *const entry_mod.Entry) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        const kind = kindOf(@fromBackingInt(e.header.entry_type));
        const want = self.predict(defs, e, kind) orelse return error.Halt;
        var before: [FAMILIES.len + 1]u64 = undefined;
        for (FAMILIES, before[0..FAMILIES.len]) |fam, *b| b.* = store.families[fam].commits;
        before[FAMILIES.len] = store.families[channels.OutFam.ID].commits;
        const seqs = nextSeqs(store);
        var out: persistence.Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&rm.machine, store, &self.scratch, e, &out);
        var effect: Effect = .{ .kind = kind, .refused = out.refused() != null };
        if (effect.refused) {
            for (FAMILIES, before[0..FAMILIES.len]) |fam, b| if (b != store.families[fam].commits) return error.RefusalChangedState;
            if (before[FAMILIES.len] != store.families[channels.OutFam.ID].commits) return error.RefusalChangedState;
            if (!want.refused) effect.broke = "the runs machine refused an entry the ledger applies";
        } else if (want.refused) effect.broke = "the runs machine applied an entry the ledger refuses";
        if (effect.broke == null) effect.broke = sentRule(store, seqs, want.sends);
        // The whole store at the indices the checksums are sampled at; the
        // run the entry names at every one.
        if (effect.broke == null) effect.broke = self.rule(store, runOf(e, kind).?, e.header.index % SWEEP_EVERY == 0);
        return effect;
    }

    /// The groups a parent's slot can be in, under the store's place.
    const GROUPS = 2;

    /// Each group's next outbox seq: the outbox is never drained here, so
    /// an entry's messages are found by these, not by a walk.
    fn nextSeqs(store: *store_mod.Store) [GROUPS]u64 {
        var out: [GROUPS]u64 = undefined;
        for (&out, 0..) |*seq, g| {
            var kb: [channels.GROUP_KEY]u8 = undefined;
            const got = (view_mod.ReadView{ .store = store }).get(channels.MetaFam, channels.groupKey(&kb, @intCast(g))) catch null;
            seq.* = if (got) |m| m.v.next_seq else 1;
        }
        return out;
    }

    /// The messages the entry left in the outbox, against the ledger's.
    fn sentRule(store: *store_mod.Store, before: [GROUPS]u64, wanted: [2]?Send) ?[]const u8 {
        var n: usize = 0;
        const after = nextSeqs(store);
        for (before, after, 0..) |from, to, g| for (from..to) |seq| {
            var kb: [channels.OUT_KEY]u8 = undefined;
            const item = ((view_mod.ReadView{ .store = store }).get(channels.OutFam, channels.outKey(&kb, @intCast(g), seq)) catch null) orelse
                return "an entry's message is missing from the outbox";
            const m = item.v;
            if (n >= wanted.len) return "an entry sent a parent more than the ledger's messages";
            const w = wanted[n] orelse return "an entry sent a parent a message the ledger doesn't";
            n += 1;
            if (m.entry_type != @backingInt(w.entry_type) or m.slot != PARENT_SLOTS[w.slot]) return "a parent's message is not the ledger's";
            var diag: schema.Diagnostic = .{};
            switch (w.entry_type) {
                .call_reply => {
                    const rp = ae.Reply.decodeAll(m.payload, &diag) catch return "a parent's reply doesn't decode";
                    if (!std.mem.eql(u8, rp.run, RUNS[w.run]) or rp.answer != w.answer.?) return "a parent's reply is not the ledger's";
                    if (w.why) |why| if (rp.why == null or !std.mem.eql(u8, rp.why.?, why)) return "a parent's refusal doesn't say why";
                },
                .call_done => {
                    const d = ae.Done.decodeAll(m.payload, &diag) catch return "a parent's call_done doesn't decode";
                    if (!std.mem.eql(u8, d.run, RUNS[w.run]) or d.status != w.done.? or !std.mem.eql(u8, d.result, w.result)) return "a parent's call_done is not the ledger's";
                },
                else => unreachable,
            }
        };
        if (n < wanted.len and wanted[n] != null) return "a parent the ledger tells heard nothing";
        return null;
    }

    /// The families against the ledger, and the invariants that hold of the
    /// store alone; a run's history and step outputs only for `touched`, the
    /// one run the entry names.
    fn rule(self: *WorkflowRuns, store: *store_mod.Store, touched: usize, sweep: bool) ?[]const u8 {
        return self.ruleOrOom(store, touched, sweep) catch "out of memory checking the runs";
    }

    fn ruleOrOom(self: *WorkflowRuns, store: *store_mod.Store, touched: usize, sweep: bool) error{OutOfMemory}!?[]const u8 {
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        const rv: view_mod.ReadView = .{ .store = store };
        const l = &self.ledger;

        if (!sweep) return self.runRule(store, a, touched, true);
        // On the store alone: counts, and idempotency keys naming their run.
        var active: [NAMES.len]u32 = @splat(0);
        var runs = rv.range(rm.RunFam, "");
        var stored: usize = 0;
        while (runs.next() catch return "a run could not be read") |item| : (stored += 1) {
            const w = indexOf(&NAMES, item.value.v.workflow) orelse return "a run of a workflow the workload never names";
            const s = std.enums.fromInt(RunStatus, item.value.v.status) orelse return "a run's status is not a status";
            if (!s.isTerminal()) active[w] += 1;
        }
        for (NAMES, active, l.per_def) |name, n, want| {
            const got = rm.perDefOf(rv, view_mod.idKey(try a.alloc(u8, 4 + name.len), NS, name) catch unreachable);
            if ((if (got) |g| g.active else 0) != n) return "a workflow's count is not its runs not ended";
            if ((got == null) != (want == null)) return "a workflow's record is not the ledger's";
            if (got) |g| if (g.active != want.?.active or g.fired_ms != want.?.fired_ms) return "a workflow's count or fired slot is not the ledger's";
        }
        var idems = rv.range(rm.IdemFam, "");
        var held_idems: usize = 0;
        while (idems.next() catch return "an idempotency key could not be read") |item| : (held_idems += 1) {
            const run = rm.runOf(rv, view_mod.idKey(try a.alloc(u8, 4 + item.value.v.run.len), NS, item.value.v.run) catch unreachable) orelse
                return "an idempotency key names no run";
            const want = try rm.idemKey(a, NS, run.workflow, run.idempotency_key);
            if (!std.mem.eql(u8, want, item.key)) return "an idempotency key names a run of another workflow or key";
        }

        // Against the ledger, run by run.
        var want_runs: usize = 0;
        var want_idems: usize = 0;
        for (l.idem) |row| for (row) |x| {
            if (x != null) want_idems += 1;
        };
        if (held_idems != want_idems) return "the idempotency keys are not the ledger's";
        for (0..RUNS.len) |r| {
            if (try self.runRule(store, a, r, r == touched)) |why| return why;
            if (l.runs[r] != null) want_runs += 1;
        }
        if (stored != want_runs) return "the runs are not the ledger's";
        return null;
    }

    /// Run `r` against the ledger: its record, its parent's, its workflow's
    /// count, its idempotency key, and if `walk`, its history and step
    /// outputs.
    fn runRule(self: *WorkflowRuns, store: *store_mod.Store, a: Allocator, r: usize, walk: bool) error{OutOfMemory}!?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = store };
        const l = &self.ledger;
        const name = RUNS[r];
        const want = l.runs[r];
        const parent = l.parents[r];
        const key = view_mod.idKey(try a.alloc(u8, 4 + name.len), NS, name) catch unreachable;
        const got = rm.runOf(rv, key);
        const pgot = rm.parentOf(rv, NS, name);
        if ((pgot == null) != (parent == null)) return "a child's parent record is not the ledger's";
        if (parent) |p| {
            const g = pgot.?;
            if (g.parent_slot != PARENT_SLOTS[p.slot] or !std.mem.eql(u8, g.parent_run, PARENT_RUNS[p.parent_run])) return "a child's parent is not the ledger's";
            if ((g.state == .started) != p.started) return "whether a child started is not the ledger's";
            if (g.state == .started and got == null) return "a started child's run is gone";
        }
        const run = want orelse {
            if (got != null) return "a run the ledger doesn't hold is stored";
            return null;
        };
        const g = got orelse return "a run the ledger holds is gone";
        if (!std.mem.eql(u8, g.workflow, NAMES[run.wf])) return "a run's workflow is not its start's";
        if (g.status != @backingInt(run.status)) return "a run's status is not the ledger's";
        if ((g.rev > 0) != run.stepped or g.rev != run.rev) return "whether a run's first step was taken is not the ledger's";
        if (g.created_at_ms != run.created_ms or g.completed_at_ms != run.completed_ms) return "a run's stamps are not the ledger's";
        if (!std.mem.eql(u8, g.idempotency_key, if (run.idem) |k| IDEMS[k] else "")) return "a run's idempotency key is not its start's";
        if ((g.has_output != 0) != (run.output != null) or !std.mem.eql(u8, g.output, if (run.output) |o| OUTPUTS[o] else "")) return "a run's output is not its completion's";
        const kept = @min(run.hist_next, rm.HIST_KEEP);
        if (g.hist_next != run.hist_next or g.hist_dropped != run.hist_next - kept) return "a run's history counts are not the ledger's";
        // Its workflow's record, whatever the sweep finds of the rest.
        const wname = NAMES[run.wf];
        const per = rm.perDefOf(rv, view_mod.idKey(try a.alloc(u8, 4 + wname.len), NS, wname) catch unreachable);
        const per_want = l.per_def[run.wf].?;
        if (per == null or per.?.active != per_want.active or per.?.fired_ms != per_want.fired_ms) return "a workflow's count or fired slot is not the ledger's";
        if (run.idem) |k| {
            const got_idem = (rv.get(rm.IdemFam, try rm.idemKey(a, NS, wname, IDEMS[k])) catch return "an idempotency key could not be read") orelse return "the idempotency keys are not the ledger's";
            if (!std.mem.eql(u8, got_idem.v.run, RUNS[l.idem[run.wf][k].?])) return "the idempotency keys are not the ledger's";
        }
        if (!walk) return null;
        const prefix = try rm.runRange(a, NS, name);
        var hist = rv.range(rm.HistFam, prefix);
        var seq = run.hist_next - kept;
        while (hist.next() catch return "a history event could not be read") |item| : (seq += 1) {
            if (seq >= run.hist_next) return "a run holds more history than its last 256 events";
            if (std.mem.readInt(u64, item.key[item.key.len - 8 ..][0..8], .big) != seq) return "a run's history is not its last seqs";
            const h = item.value.v;
            if (seq == 0) {
                if (!std.mem.eql(u8, h.event, EVENTS[run.started]) or !std.mem.eql(u8, h.detail, INPUT) or h.at_ms != run.created_ms) return "a run's first event is not its start's";
            } else if (run.ended_at == seq) {
                if (!std.mem.eql(u8, h.event, CANCEL_EVENT) or h.at_ms != run.completed_ms) return "a run's last event is not its cancel's";
            } else {
                if (!std.mem.eql(u8, h.event, STEP_EVENT) or h.at_ms != 0 or h.detail.len != 0) return "a run's history is not its steps'";
            }
        }
        if (seq != run.hist_next) return "a run's history is missing events";
        var outs = rv.range(rm.OutFam, prefix);
        var got_outs: [STEPS.len]?usize = @splat(null);
        while (outs.next() catch return "a step output could not be read") |item| {
            const step = indexOf(&STEPS, item.key[prefix.len..]) orelse return "a step output under a step no step names";
            const o = indexOf(&OUTCOMES, item.value.v.outcome) orelse return "a step's outcome is not its step's";
            if (!std.mem.eql(u8, item.value.v.output, OUTPUTS[o])) return "a step's output is not its step's";
            got_outs[step] = o;
        }
        if (!std.mem.eql(?usize, &got_outs, &run.outs)) return "a run's step outputs are not its steps'";
        return null;
    }

    pub fn checksums(store: *store_mod.Store) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(store, fam);
        return sums;
    }
};

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

/// A runs entry, as the workflows handler and a parent's call make them,
/// from the leader's own applied ledger: starts of every kind (some of a
/// disabled workflow, some repeating a key or a run), steps at a run's
/// rev (some at one another leader moved past, some carrying enough
/// events that a run's history passes what it keeps), child calls and
/// refusals, and cancels of runs it holds.
pub fn make(r: std.Random, buf: []u8, leader: *const WorkflowRuns) ?Made {
    const l = &leader.ledger;
    // The runs as the leader holds them, so most entries find what they
    // name: a free id for a start, a pending parent for its child's start,
    // a run not yet stepped or ended for a step or a completion.
    var free: Pick = .{};
    var live: Pick = .{};
    var unstepped: Pick = .{};
    var held: Pick = .{};
    var pending: Pick = .{};
    for (l.runs, l.parents, 0..) |x, p, i| {
        if (x) |run| {
            held.add(i);
            if (!run.status.isTerminal()) live.add(i);
            if (!run.status.isTerminal() and !run.stepped) unstepped.add(i);
        } else {
            if (p == null) free.add(i);
            if (p != null and !p.?.started) pending.add(i);
        }
    }
    const any = r.uintLessThan(usize, RUNS.len);
    var kb: [16]u8 = undefined;
    const roll = r.uintLessThan(u8, 100);
    if (roll < 15) {
        const run = free.or_(r, 50, any);
        const p = ae.ChildStart.encodeInto(buf, .{
            .ns = NS.int(),
            .parent_slot = PARENT_SLOTS[r.uintLessThan(usize, PARENT_SLOTS.len)],
            .parent_run = PARENT_RUNS[r.uintLessThan(usize, PARENT_RUNS.len)],
            .parent_workflow = "pwf",
            .workflow = NAMES[r.uintLessThan(usize, NAMES.len)],
            .version = "",
            .run = RUNS[run],
            .input = INPUT,
        }) catch return null;
        return .{ .entry_type = .child_start, .payload = p };
    }
    if (roll < 22) {
        const answers = [_]ae.Answer{ .no_action, .conflict };
        const p = ae.ChildRefuse.encodeInto(buf, .{ .ns = NS.int(), .run = RUNS[pending.or_(r, 70, any)], .answer = answers[r.uintLessThan(usize, answers.len)] }) catch return null;
        return .{ .entry_type = .child_refuse, .payload = p };
    }
    // A step of a run the leader holds, at its rev, or now and then a rev
    // another leader moved past.
    if (roll < 34 and held.n > 0) {
        // Now and then the run with the longest history, so some pass
        // what a run keeps.
        var longest: ?usize = null;
        for (live.ids[0..live.n]) |i| {
            if (longest == null or l.runs[i].?.hist_next > l.runs[longest.?].?.hist_next) longest = i;
        }
        const c = if (longest != null and r.uintLessThan(u8, 100) < 25) longest.? else live.or_(r, 80, unstepped.or_(r, 30, held.at(r)));
        const run = l.runs[c].?;
        const rev = if (r.uintLessThan(u8, 100) < 10) run.rev +% 1 else run.rev;
        const vb = buf[buf.len / 2 ..];
        const value = stepValue(r, vb, rev, NAMES[run.wf], run.hist_next) orelse return null;
        return command(buf[0 .. buf.len / 2], .workflow_step, view_mod.idKey(&kb, NS, RUNS[c]) catch unreachable, value);
    }
    // A completion names a run the leader holds, and its workflow.
    if (roll < 60 and held.n > 0) {
        const c = live.or_(r, 75, held.at(r));
        const vb = buf[buf.len / 2 ..];
        const value = completeValue(r, vb, ENDS[r.uintLessThan(usize, ENDS.len)], NAMES[l.runs[c].?.wf]) orelse return null;
        return command(buf[0 .. buf.len / 2], .workflow_complete, view_mod.idKey(&kb, NS, RUNS[c]) catch unreachable, value);
    }
    const ev = r.uintLessThan(usize, EVENTS.len);
    const run = if (ev == CHILD) pending.or_(r, 70, any) else free.or_(r, 75, any);
    const idem: []const u8 = if (ev == CHILD or r.boolean()) "" else IDEMS[r.uintLessThan(usize, IDEMS.len)];
    var vb: [128]u8 = undefined;
    return command(buf, .workflow_start, view_mod.idKey(&kb, NS, RUNS[run]) catch unreachable, startValue(&vb, NAMES[r.uintLessThan(usize, NAMES.len)], EVENTS[ev], idem));
}

/// Run ids to pick from.
const Pick = struct {
    ids: [RUNS.len]usize = undefined,
    n: usize = 0,

    fn add(self: *Pick, i: usize) void {
        self.ids[self.n] = i;
        self.n += 1;
    }

    fn at(self: *const Pick, r: std.Random) usize {
        return self.ids[r.uintLessThan(usize, self.n)];
    }

    /// One of these, `percent` of the time when there are any; else `other`.
    fn or_(self: *const Pick, r: std.Random, percent: u8, other: usize) usize {
        if (self.n == 0 or r.uintLessThan(u8, 100) >= percent) return other;
        return self.at(r);
    }
};

fn command(buf: []u8, t: EntryType, key: []const u8, value: []const u8) ?Made {
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = NS, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
    const n = cmd.serialize(buf) orelse return null;
    return .{ .entry_type = t, .payload = buf[0..n] };
}

fn startValue(buf: []u8, wf: []const u8, event: []const u8, idem: []const u8) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeInt(u16, @intCast(wf.len), .little) catch unreachable;
    w.writeAll(wf) catch unreachable;
    w.writeInt(u16, 1, .little) catch unreachable;
    w.writeAll("1") catch unreachable;
    w.writeByte(@backingInt(RunStatus.running)) catch unreachable;
    w.writeInt(u16, @intCast(event.len), .little) catch unreachable;
    w.writeAll(event) catch unreachable;
    w.writeInt(u16, @intCast(idem.len), .little) catch unreachable;
    w.writeAll(idem) catch unreachable;
    w.writeAll(INPUT) catch unreachable;
    return w.buffered();
}

/// A step at `rev`: some
/// events, now and then nearly a step's most, so a run's history passes
/// what it keeps; maybe some step outputs; no wait.
fn stepValue(r: std.Random, buf: []u8, rev: u64, wf: []const u8, hist_next: u64) ?[]const u8 {
    // Long steps are rare, but likelier for a run already long: every
    // replica's checksums walk them all.
    const long_percent: u8 = if (hist_next >= rm.MAX_STEP_EVENTS) 50 else 4;
    const events: u32 = if (r.uintLessThan(u8, 100) < long_percent) r.intRangeAtMost(u32, 56, rm.MAX_STEP_EVENTS) else r.uintLessThan(u32, 4);
    var eb: [rm.MAX_STEP_EVENTS * 24]u8 = undefined;
    var ew: schema.Writer = .{ .buf = &eb };
    for (0..events) |_| rm.Event.encode(&ew, .{ .event = STEP_EVENT, .detail = "", .at_ms = 0 }) catch return null;
    var ob: [STEPS.len * 32]u8 = undefined;
    var ow: schema.Writer = .{ .buf = &ob };
    var outs: u32 = 0;
    for (STEPS) |step| if (r.uintLessThan(u8, 100) < 30) {
        const o = r.uintLessThan(usize, OUTCOMES.len);
        rm.StepOutput.encode(&ow, .{ .step = step, .outcome = OUTCOMES[o], .output = OUTPUTS[o] }) catch return null;
        outs += 1;
    };
    const value: rm.Step.Value = .{
        .rev = rev,
        .step = "",
        .events = .{ .bytes = ew.written(), .count = events },
        .outputs = .{ .bytes = ow.written(), .count = outs },
        .wait = .none,
        .wait_kind = .action,
        .wait_slot = 0,
        .wait_on = "",
        .wait_due_ms = 0,
        .wait_timeout_to = "",
        .progress = "",
        .settles = "",
        .takes = .{ .bytes = "", .count = 0 },
        .caller_slot = 0,
        .workflow = wf,
        .ends = 0,
        .has_output = false,
        .output = "",
        .tags = "",
        .call = "",
    };
    return rm.Step.encodeInto(buf, value) catch null;
}

/// A cancel's completion, as `Complete.parse` reads it: maybe an output.
fn completeValue(r: std.Random, buf: []u8, status: RunStatus, wf: []const u8) ?[]const u8 {
    const c: rm.Complete = .{
        .status = status,
        .output = if (r.boolean()) OUTPUTS[r.uintLessThan(usize, OUTPUTS.len)] else null,
        .event = CANCEL_EVENT,
        .detail = "",
        .tags = "",
        .workflow = wf,
    };
    return c.encode(buf);
}
