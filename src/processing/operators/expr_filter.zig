//! Expression-Based Filter Operator
//!
//! A declarative filter operator that evaluates string-based condition
//! expressions against ProcessingRecord fields. ExprFilterOperator is
//! configured from YAML job definitions via the NativeOperatorRegistry.
//!
//! Supported condition expressions:
//!
//!   Record-level:
//!   - `value_contains:<substring>`  — value contains the substring
//!   - `key_contains:<substring>`    — key contains the substring
//!   - `key_equals:<exact>`          — key equals the exact string
//!   - `key_prefix:<prefix>`         — key starts with the prefix
//!   - `value_prefix:<prefix>`       — value starts with the prefix
//!   - `not_empty`                   — value is non-empty
//!   - `key_not_empty`               — key is non-empty
//!   - `min_length:<n>`              — value length >= n
//!
//!   JSON field — `json:<path><op><value>`:
//!   - `json:<path>=<value>`         — field equals value
//!   - `json:<path>!=<value>`        — field does not equal value
//!   - `json:<path>^=<value>`        — field starts with value (prefix)
//!   - `json:<path>*=<value>`        — field contains value (substring)
//!   - `json:<path>!^=<value>`       — field does not start with value
//!   - `json:<path>!*=<value>`       — field does not contain value
//!   - `json:<path>><value>`         — field > value (numeric)
//!   - `json:<path>>=<value>`        — field >= value (numeric)
//!   - `json:<path><<value>`         — field < value (numeric)
//!   - `json:<path><=<value>`        — field <= value (numeric)
//!
//!   Compound (up to 8 sub-conditions):
//!   - `<cond> OR <cond> [OR ...]`   — any sub-condition matches
//!   - `<cond> AND <cond> [AND ...]` — all sub-conditions match
//!
//!   OR and AND cannot be mixed in one expression. Use classify rules for
//!   complex routing instead of deeply nested boolean logic.
//!
//!   A condition this list doesn't describe is refused, never read as
//!   "match everything". A JSON condition matches only a record whose value
//!   is JSON with the field present and comparable: a missing field, a
//!   non-JSON value, or a field of another type matches no operator, `!=`
//!   and `!*=` included.
//!
//! YAML examples:
//!   ```yaml
//!   operators:
//!     - type: filter
//!       name: keep-important
//!       condition: "value_contains:important"
//!     - type: filter
//!       name: keep-payments-or-kyc
//!       condition: "value_contains:payment OR value_contains:kyc"
//!     - type: filter
//!       name: high-value-approved
//!       condition: "json:amount>10000 AND json:status=approved"
//!     - type: classify
//!       name: route-payments
//!       rules:
//!         - condition: "json:type^=payment"
//!           tag: payments
//!         - condition: "json:type*=transfer"
//!           tag: transfers
//!         - condition: "json:amount>10000"
//!           tag: high-value
//!   ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const noOpSnapshot = @import("../operator.zig").noOpSnapshot;
const noOpRestore = @import("../operator.zig").noOpRestore;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;

pub const ExprFilterOperator = struct {
    name: []const u8,
    condition: []const u8,
    /// Parsed condition kind (computed once at init)
    parsed: ParsedCondition,

    const Self = @This();

    /// Maximum number of sub-conditions in an OR/AND compound expression.
    const MAX_COMPOUND: usize = 8;

    /// A single (non-compound) parsed condition — used inside Compound to
    /// avoid self-referencing the tagged union.
    const SingleCondition = union(enum) {
        value_contains: []const u8,
        key_contains: []const u8,
        key_equals: []const u8,
        key_prefix: []const u8,
        value_prefix: []const u8,
        not_empty,
        key_not_empty,
        json_expr: JsonExpr,
        min_length: usize,
    };

    /// Fixed-size array of sub-conditions for compound expressions.
    const Compound = struct {
        items: [MAX_COMPOUND]SingleCondition = undefined,
        len: u8 = 0,
    };

    /// Parsed condition for efficient evaluation
    const ParsedCondition = union(enum) {
        /// A single atomic condition (no compound logic)
        single: SingleCondition,
        /// Any sub-condition matches (short-circuit)
        or_expr: Compound,
        /// All sub-conditions match (short-circuit)
        and_expr: Compound,
    };

    /// JSON comparison operator
    const JsonOp = enum {
        eq, // =
        neq, // !=
        prefix, // ^=
        contains, // *=
        not_prefix, // !^=
        not_contains, // !*=
        gt, // >
        gte, // >=
        lt, // <
        lte, // <=
    };

    /// A parsed JSON field expression: path + operator + value
    const JsonExpr = struct {
        path: []const u8,
        op: JsonOp,
        value: []const u8,
    };

    pub const ConditionError = error{InvalidCondition};

    /// Create an expression-based filter operator.
    /// `condition` is the raw condition string from YAML (e.g., "value_contains:hello").
    /// Both `name` and `condition` must outlive the operator (typically allocated by parser).
    pub fn init(name: []const u8, condition: []const u8) ConditionError!Self {
        var why: []const u8 = undefined;
        return .{
            .name = name,
            .condition = condition,
            .parsed = try parseCondition(condition, &why),
        };
    }

    /// Why `condition` is not one this filter understands, or null.
    pub fn check(condition: []const u8) ?[]const u8 {
        var why: []const u8 = undefined;
        _ = parseCondition(condition, &why) catch return why;
        return null;
    }

    /// Return an Operator interface backed by this ExprFilterOperator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = close,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.evaluate(rec)) {
            try ctx.emit(rec);
        }
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {
        // Stateless — watermarks pass through via chain
    }

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}

    // =========================================================================
    // Condition evaluation
    // =========================================================================

    pub fn evaluate(self: *const Self, rec: ProcessingRecord) bool {
        return evaluateCondition(&self.parsed, rec);
    }

    fn evaluateCondition(cond: *const ParsedCondition, rec: ProcessingRecord) bool {
        return switch (cond.*) {
            .single => |s| evaluateSingle(&s, rec),
            .or_expr => |compound| {
                for (compound.items[0..compound.len]) |*sub| {
                    if (evaluateSingle(sub, rec)) return true;
                }
                return false;
            },
            .and_expr => |compound| {
                for (compound.items[0..compound.len]) |*sub| {
                    if (!evaluateSingle(sub, rec)) return false;
                }
                return true;
            },
        };
    }

    fn evaluateSingle(cond: *const SingleCondition, rec: ProcessingRecord) bool {
        return switch (cond.*) {
            .value_contains => |substr| std.mem.indexOf(u8, rec.value, substr) != null,
            .key_contains => |substr| std.mem.indexOf(u8, rec.key, substr) != null,
            .key_equals => |exact| std.mem.eql(u8, rec.key, exact),
            .key_prefix => |prefix| std.mem.startsWith(u8, rec.key, prefix),
            .value_prefix => |prefix| std.mem.startsWith(u8, rec.value, prefix),
            .not_empty => rec.value.len > 0,
            .key_not_empty => rec.key.len > 0,
            .json_expr => |expr| evaluateJsonExpr(rec.value, expr),
            .min_length => |n| rec.value.len >= n,
        };
    }

    // =========================================================================
    // Condition parsing
    // =========================================================================

    fn parseCondition(cond: []const u8, why: *[]const u8) ConditionError!ParsedCondition {
        const has_or = std.mem.indexOf(u8, cond, " OR ") != null;
        const has_and = std.mem.indexOf(u8, cond, " AND ") != null;
        if (has_or and has_and) return fail(why, "AND and OR can't be mixed in one condition");
        if (has_or) return .{ .or_expr = try parseCompound(cond, " OR ", why) };
        if (has_and) return .{ .and_expr = try parseCompound(cond, " AND ", why) };
        return .{ .single = try parseSingleCondition(std.mem.trim(u8, cond, whitespace), why) };
    }

    const whitespace = " \t\r\n";

    fn fail(why: *[]const u8, reason: []const u8) ConditionError {
        why.* = reason;
        return error.InvalidCondition;
    }

    /// The sub-conditions of `cond` joined by `sep`, each one checked.
    fn parseCompound(cond: []const u8, sep: []const u8, why: *[]const u8) ConditionError!Compound {
        var compound = Compound{};
        var parts = std.mem.splitSequence(u8, cond, sep);
        while (parts.next()) |raw| {
            const part = std.mem.trim(u8, raw, whitespace);
            if (part.len == 0) return fail(why, "an AND or OR has nothing on one side");
            if (compound.len >= MAX_COMPOUND) return fail(why, "more than 8 conditions joined by AND or OR");
            compound.items[compound.len] = try parseSingleCondition(part, why);
            compound.len += 1;
        }
        return compound;
    }

    /// Parse a single (non-compound) condition expression.
    fn parseSingleCondition(cond: []const u8, why: *[]const u8) ConditionError!SingleCondition {
        if (cond.len == 0) return fail(why, "the condition is empty");
        if (std.mem.eql(u8, cond, "not_empty")) return .not_empty;
        if (std.mem.eql(u8, cond, "key_not_empty")) return .key_not_empty;

        if (std.mem.startsWith(u8, cond, "json:")) return .{ .json_expr = try parseJsonExpr(cond[5..], why) };

        const parts = splitOnce(cond, ':') orelse return fail(why, "unknown condition");
        const prefix = parts[0];
        const arg = parts[1];
        const known = for ([_][]const u8{ "value_contains", "key_contains", "key_equals", "key_prefix", "value_prefix", "min_length" }) |k| {
            if (std.mem.eql(u8, prefix, k)) break true;
        } else false;
        if (!known) return fail(why, "unknown condition");
        // An empty argument would match every record (every value contains "").
        if (arg.len == 0) return fail(why, "needs a value after the colon");
        if (std.mem.eql(u8, prefix, "value_contains")) return .{ .value_contains = arg };
        if (std.mem.eql(u8, prefix, "key_contains")) return .{ .key_contains = arg };
        if (std.mem.eql(u8, prefix, "key_equals")) return .{ .key_equals = arg };
        if (std.mem.eql(u8, prefix, "key_prefix")) return .{ .key_prefix = arg };
        if (std.mem.eql(u8, prefix, "value_prefix")) return .{ .value_prefix = arg };
        if (std.mem.eql(u8, prefix, "min_length")) {
            const n = std.fmt.parseInt(usize, arg, 10) catch return fail(why, "min_length needs a whole number");
            return .{ .min_length = n };
        }
        return fail(why, "unknown condition");
    }

    /// Parse a JSON expression after the `json:` prefix.
    /// Scans for the first operator character to split path from op+value.
    fn parseJsonExpr(expr: []const u8, why: *[]const u8) ConditionError!JsonExpr {
        // Find the start of the operator: first occurrence of = ! ^ * > <
        var i: usize = 0;
        while (i < expr.len) : (i += 1) {
            const c = expr[i];
            if (c == '=' or c == '!' or c == '^' or c == '*' or c == '>' or c == '<') break;
        }
        if (i == 0) return fail(why, "a json: condition needs a field before its operator");
        if (i >= expr.len) return fail(why, "a json: condition needs an operator (= != ^= *= !^= !*= > >= < <=)");

        const path = expr[0..i];
        const rest = expr[i..];
        if (std.mem.indexOfAny(u8, path, whitespace) != null) return fail(why, "a json: condition has no spaces around its operator");
        if (std.mem.indexOfScalar(u8, path, '[') != null) return fail(why, "a json: path can't index an array; use $.a.b");

        // Match operators longest-first to avoid ambiguity
        const ops = [_]struct { text: []const u8, op: JsonOp }{
            .{ .text = "!^=", .op = .not_prefix },
            .{ .text = "!*=", .op = .not_contains },
            .{ .text = "!=", .op = .neq },
            .{ .text = "^=", .op = .prefix },
            .{ .text = "*=", .op = .contains },
            .{ .text = ">=", .op = .gte },
            .{ .text = "<=", .op = .lte },
            .{ .text = "=", .op = .eq },
            .{ .text = ">", .op = .gt },
            .{ .text = "<", .op = .lt },
        };

        for (ops) |entry| {
            if (std.mem.startsWith(u8, rest, entry.text)) {
                const value = rest[entry.text.len..];
                if (value.len == 0) return fail(why, "a json: condition needs a value after its operator");
                if (std.mem.indexOfScalar(u8, whitespace, value[0]) != null) return fail(why, "a json: condition has no spaces around its operator");
                if (value[0] == '=') return fail(why, "a json: condition compares with =, not ==");
                if (value.len >= 2 and (value[0] == '"' or value[0] == '\'') and value[value.len - 1] == value[0])
                    return fail(why, "a json: value is written without quotes");
                switch (entry.op) {
                    .gt, .gte, .lt, .lte => if (!isPlainNumber(value)) return fail(why, "a json: >, >=, < or <= needs a finite decimal number"),
                    else => {},
                }
                return .{ .path = path, .op = entry.op, .value = value };
            }
        }

        return fail(why, "a json: condition needs an operator (= != ^= *= !^= !*= > >= < <=)");
    }

    /// A finite decimal number: digits, an optional sign, point and exponent;
    /// not hex, not `_`-separated, not nan or inf, and not too big for f64.
    fn isPlainNumber(value: []const u8) bool {
        for (value) |c| switch (c) {
            '0'...'9', '-', '+', '.', 'e', 'E' => {},
            else => return false,
        };
        const n = std.fmt.parseFloat(f64, value) catch return false;
        return std.math.isFinite(n);
    }

    /// Split a string on the first occurrence of `sep`. Returns [before, after] or null.
    fn splitOnce(s: []const u8, sep: u8) ?[2][]const u8 {
        const idx = std.mem.indexOfScalar(u8, s, sep) orelse return null;
        return .{ s[0..idx], s[idx + 1 ..] };
    }

    // =========================================================================
    // JSON expression evaluation
    // =========================================================================

    /// Evaluate a JSON field expression against a record value.
    fn evaluateJsonExpr(value: []const u8, expr: JsonExpr) bool {
        const parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, value, .{}) catch return false;
        defer parsed.deinit();

        const root = parsed.value;
        const field = resolveJsonField(root, expr.path) orelse return false;

        return switch (field) {
            .string => |s| evalStringOp(s, expr.op, expr.value),
            .integer => |n| evalNumericOp(@floatFromInt(n), expr.op, expr.value),
            .float => |f| evalNumericOp(f, expr.op, expr.value),
            .bool => |b| evalBoolOp(b, expr.op, expr.value),
            else => false,
        };
    }

    /// Resolve a (possibly dotted, possibly `$.`-prefixed) JSONPath against a parsed value.
    /// Accepts `$.a.b`, `$a`, and plain `a.b` — mirroring the keyby/map convention so the
    /// `json:$.field…` syntax used throughout the docs works in filter/classify conditions.
    fn resolveJsonField(root: std.json.Value, raw_path: []const u8) ?std.json.Value {
        var path = raw_path;
        if (std.mem.startsWith(u8, path, "$.")) {
            path = path[2..];
        } else if (std.mem.startsWith(u8, path, "$")) {
            path = path[1..];
        }

        var current = root;
        var it = std.mem.splitScalar(u8, path, '.');
        while (it.next()) |seg| {
            if (seg.len == 0) continue;
            if (current != .object) return null;
            current = current.object.get(seg) orelse return null;
        }
        return current;
    }

    fn evalStringOp(s: []const u8, op: JsonOp, value: []const u8) bool {
        return switch (op) {
            .eq => std.mem.eql(u8, s, value),
            .neq => !std.mem.eql(u8, s, value),
            .prefix => std.mem.startsWith(u8, s, value),
            .contains => std.mem.indexOf(u8, s, value) != null,
            .not_prefix => !std.mem.startsWith(u8, s, value),
            .not_contains => std.mem.indexOf(u8, s, value) == null,
            .gt, .gte, .lt, .lte => false, // numeric ops on strings → false
        };
    }

    fn evalNumericOp(n: f64, op: JsonOp, value: []const u8) bool {
        const expected = std.fmt.parseFloat(f64, value) catch return false;
        return switch (op) {
            .eq => n == expected,
            .neq => n != expected,
            .gt => n > expected,
            .gte => n >= expected,
            .lt => n < expected,
            .lte => n <= expected,
            .prefix, .contains, .not_prefix, .not_contains => false,
        };
    }

    fn evalBoolOp(b: bool, op: JsonOp, value: []const u8) bool {
        const expected = if (std.mem.eql(u8, value, "true"))
            true
        else if (std.mem.eql(u8, value, "false"))
            false
        else
            return false;
        return switch (op) {
            .eq => b == expected,
            .neq => b != expected,
            else => false,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ExprFilterOperator — value_contains" {
    var op = try ExprFilterOperator.init("test-filter", "value_contains:hello");
    const rec_match = ProcessingRecord.init("k", "say hello world", 1000);
    const rec_miss = ProcessingRecord.init("k", "goodbye world", 1000);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));

    // vtable works
    const iface = op.operator();
    try std.testing.expectEqualStrings("test-filter", iface.getName());
}

test "ExprFilterOperator — key_equals" {
    var op = try ExprFilterOperator.init("key-filter", "key_equals:user-42");
    const rec_match = ProcessingRecord.init("user-42", "data", 0);
    const rec_miss = ProcessingRecord.init("user-43", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — key_prefix" {
    var op = try ExprFilterOperator.init("prefix-filter", "key_prefix:order-");
    const rec_match = ProcessingRecord.init("order-123", "data", 0);
    const rec_miss = ProcessingRecord.init("user-123", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — not_empty" {
    var op = try ExprFilterOperator.init("nonempty-filter", "not_empty");
    const rec_match = ProcessingRecord.init("k", "some data", 0);
    const rec_empty = ProcessingRecord.init("k", "", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_empty));
}

test "ExprFilterOperator — min_length" {
    var op = try ExprFilterOperator.init("min-len", "min_length:5");
    const rec_ok = ProcessingRecord.init("k", "abcde", 0);
    const rec_short = ProcessingRecord.init("k", "abc", 0);

    try std.testing.expect(op.evaluate(rec_ok));
    try std.testing.expect(!op.evaluate(rec_short));
}

test "ExprFilterOperator — a condition it doesn't understand is refused, not read as match-all" {
    const cases = [_]struct { cond: []const u8, why: []const u8 }{
        .{ .cond = "something_weird", .why = "unknown condition" },
        .{ .cond = "valeu_contains:x", .why = "unknown condition" },
        .{ .cond = "", .why = "the condition is empty" },
        .{ .cond = "min_length:five", .why = "min_length needs a whole number" },
        .{ .cond = "json:amount", .why = "a json: condition needs an operator (= != ^= *= !^= !*= > >= < <=)" },
        .{ .cond = "json:=x", .why = "a json: condition needs a field before its operator" },
        .{ .cond = "json:amount>", .why = "a json: condition needs a value after its operator" },
        .{ .cond = "json:amount>lots", .why = "a json: >, >=, < or <= needs a finite decimal number" },
        .{ .cond = "not_empty AND valeu_contains:x", .why = "unknown condition" },
        .{ .cond = "not_empty OR key_not_empty AND not_empty", .why = "AND and OR can't be mixed in one condition" },
        .{ .cond = "not_empty AND  AND key_not_empty", .why = "an AND or OR has nothing on one side" },
        .{ .cond = "not_empty OR not_empty OR not_empty OR not_empty OR not_empty OR not_empty OR not_empty OR not_empty OR not_empty", .why = "more than 8 conditions joined by AND or OR" },
        .{ .cond = "value_contains:", .why = "needs a value after the colon" },
        .{ .cond = "value_prefix:", .why = "needs a value after the colon" },
        .{ .cond = "key_prefix:", .why = "needs a value after the colon" },
        .{ .cond = "key_contains:", .why = "needs a value after the colon" },
        .{ .cond = "key_equals:", .why = "needs a value after the colon" },
        .{ .cond = "min_length:", .why = "needs a value after the colon" },
        .{ .cond = "json:status = done", .why = "a json: condition has no spaces around its operator" },
        .{ .cond = "json:x >5", .why = "a json: condition has no spaces around its operator" },
        .{ .cond = "json:x> 5", .why = "a json: condition has no spaces around its operator" },
        .{ .cond = "json:status=\"done\"", .why = "a json: value is written without quotes" },
        .{ .cond = "json:status='done'", .why = "a json: value is written without quotes" },
        .{ .cond = "json:x==5", .why = "a json: condition compares with =, not ==" },
        .{ .cond = "json:items[0].id=5", .why = "a json: path can't index an array; use $.a.b" },
        .{ .cond = "json:x>nan", .why = "a json: >, >=, < or <= needs a finite decimal number" },
        .{ .cond = "json:x<inf", .why = "a json: >, >=, < or <= needs a finite decimal number" },
        .{ .cond = "json:x>=1e400", .why = "a json: >, >=, < or <= needs a finite decimal number" },
        .{ .cond = "json:x>0x10", .why = "a json: >, >=, < or <= needs a finite decimal number" },
        .{ .cond = "json:x>1_000", .why = "a json: >, >=, < or <= needs a finite decimal number" },
    };
    for (cases) |c| {
        try std.testing.expectError(error.InvalidCondition, ExprFilterOperator.init("bad", c.cond));
        try std.testing.expectEqualStrings(c.why, ExprFilterOperator.check(c.cond).?);
    }
    try std.testing.expectEqual(@as(?[]const u8, null), ExprFilterOperator.check("json:$.amount>=10.5 AND key_prefix:o-"));
    try std.testing.expectEqual(@as(?[]const u8, null), ExprFilterOperator.check("json:x>-1.5e3"));
    // A YAML block scalar leaves a newline; tabs and newlines trim like spaces.
    try std.testing.expectEqual(@as(?[]const u8, null), ExprFilterOperator.check("\tvalue_contains:a\n"));
    try std.testing.expectEqual(@as(?[]const u8, null), ExprFilterOperator.check("not_empty AND key_not_empty\n"));
}

test "ExprFilterOperator — exactly 8 joined conditions are allowed" {
    var op = try ExprFilterOperator.init("eight", "value_contains:a OR value_contains:b OR value_contains:c OR value_contains:d OR value_contains:e OR value_contains:f OR value_contains:g OR value_contains:h");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "h", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "z", 0)));
}

test "ExprFilterOperator — a JSON condition matches only a present, comparable field" {
    // The rule, decided rather than incidental: a missing field, a non-JSON
    // value or a field of another type matches nothing, negations included.
    const missing = ProcessingRecord.fromValue("{\"other\":1}", 0);
    const not_json = ProcessingRecord.fromValue("plain text", 0);
    const wrong_type = ProcessingRecord.fromValue("{\"type\":{\"nested\":true}}", 0);
    for ([_][]const u8{ "json:type=refund", "json:type!=refund", "json:type!*=refund", "json:type!^=re", "json:type>1" }) |cond| {
        var op = try ExprFilterOperator.init("rule", cond);
        try std.testing.expect(!op.evaluate(missing));
        try std.testing.expect(!op.evaluate(not_json));
        try std.testing.expect(!op.evaluate(wrong_type));
    }
}

test "ExprFilterOperator — value_prefix" {
    var op = try ExprFilterOperator.init("vp", "value_prefix:ERROR");
    const rec_match = ProcessingRecord.init("k", "ERROR: something broke", 0);
    const rec_miss = ProcessingRecord.init("k", "INFO: all good", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — json equals (new syntax)" {
    var op = try ExprFilterOperator.init("json-eq", "json:status=approved");
    const rec_match = ProcessingRecord.init("k", "{\"status\":\"approved\",\"amount\":100}", 0);
    const rec_miss = ProcessingRecord.init("k", "{\"status\":\"pending\",\"amount\":50}", 0);
    const rec_bad = ProcessingRecord.init("k", "not-json", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
    try std.testing.expect(!op.evaluate(rec_bad));
}

test "ExprFilterOperator — json not equals" {
    var op = try ExprFilterOperator.init("jneq", "json:type!=refund");
    const rec_pass = ProcessingRecord.init("k", "{\"type\":\"payment\"}", 0);
    const rec_fail = ProcessingRecord.init("k", "{\"type\":\"refund\"}", 0);

    try std.testing.expect(op.evaluate(rec_pass));
    try std.testing.expect(!op.evaluate(rec_fail));
}

test "ExprFilterOperator — json prefix (^=)" {
    var op = try ExprFilterOperator.init("jpfx", "json:type^=payment");
    const rec_match = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\",\"id\":\"x12345\"}", 0);
    const rec_exact = ProcessingRecord.init("k", "{\"type\":\"payment\",\"id\":\"x1\"}", 0);
    const rec_miss = ProcessingRecord.init("k", "{\"type\":\"refund.partial\",\"id\":\"x99\"}", 0);
    const rec_bad = ProcessingRecord.init("k", "not-json", 0);
    const rec_num = ProcessingRecord.init("k", "{\"type\":42}", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(op.evaluate(rec_exact));
    try std.testing.expect(!op.evaluate(rec_miss));
    try std.testing.expect(!op.evaluate(rec_bad));
    try std.testing.expect(!op.evaluate(rec_num));
}

test "ExprFilterOperator — json contains (*=)" {
    var op = try ExprFilterOperator.init("jcnt", "json:type*=transfer");
    const rec_match = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\",\"id\":\"x12345\"}", 0);
    const rec_mid = ProcessingRecord.init("k", "{\"type\":\"bank_transfer_ach\",\"id\":\"b1\"}", 0);
    const rec_miss = ProcessingRecord.init("k", "{\"type\":\"payment.refund\",\"id\":\"r1\"}", 0);
    const rec_nofield = ProcessingRecord.init("k", "{\"action\":\"transfer\"}", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(op.evaluate(rec_mid));
    try std.testing.expect(!op.evaluate(rec_miss));
    try std.testing.expect(!op.evaluate(rec_nofield));
}

test "ExprFilterOperator — json not prefix (!^=)" {
    var op = try ExprFilterOperator.init("jnpfx", "json:type!^=payment");
    const rec_no = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\"}", 0);
    const rec_yes = ProcessingRecord.init("k", "{\"type\":\"refund.partial\"}", 0);

    try std.testing.expect(!op.evaluate(rec_no)); // starts with payment → negated = false
    try std.testing.expect(op.evaluate(rec_yes)); // doesn't start with payment → negated = true
}

test "ExprFilterOperator — json not contains (!*=)" {
    var op = try ExprFilterOperator.init("jncnt", "json:type!*=transfer");
    const rec_has = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\"}", 0);
    const rec_not = ProcessingRecord.init("k", "{\"type\":\"payment.refund\"}", 0);

    try std.testing.expect(!op.evaluate(rec_has));
    try std.testing.expect(op.evaluate(rec_not));
}

test "ExprFilterOperator — json numeric comparisons" {
    // greater than
    var gt = try ExprFilterOperator.init("gt", "json:amount>100");
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":200}", 0)));
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":50}", 0)));

    // greater or equal
    var gte = try ExprFilterOperator.init("gte", "json:amount>=100");
    try std.testing.expect(gte.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!gte.evaluate(ProcessingRecord.init("k", "{\"amount\":99}", 0)));

    // less than
    var lt = try ExprFilterOperator.init("lt", "json:amount<100");
    try std.testing.expect(lt.evaluate(ProcessingRecord.init("k", "{\"amount\":50}", 0)));
    try std.testing.expect(!lt.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));

    // less or equal
    var lte = try ExprFilterOperator.init("lte", "json:amount<=100");
    try std.testing.expect(lte.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!lte.evaluate(ProcessingRecord.init("k", "{\"amount\":101}", 0)));

    // numeric ops on string fields → false
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":\"200\"}", 0)));
}

test "ExprFilterOperator — json integer equality" {
    var op = try ExprFilterOperator.init("jeqi", "json:code=200");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"code\":200}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"code\":404}", 0)));
}

test "ExprFilterOperator — json boolean equality" {
    var op_t = try ExprFilterOperator.init("jbt", "json:active=true");
    var op_f = try ExprFilterOperator.init("jbf", "json:active!=true");
    const rec_true = ProcessingRecord.init("k", "{\"active\":true}", 0);
    const rec_false = ProcessingRecord.init("k", "{\"active\":false}", 0);

    try std.testing.expect(op_t.evaluate(rec_true));
    try std.testing.expect(!op_t.evaluate(rec_false));
    try std.testing.expect(!op_f.evaluate(rec_true));
    try std.testing.expect(op_f.evaluate(rec_false));
}

test "ExprFilterOperator — json missing field" {
    var op = try ExprFilterOperator.init("miss", "json:nonexistent=x");
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"other\":\"y\"}", 0)));
}

// =========================================================================
// Compound (OR / AND) tests
// =========================================================================

test "ExprFilterOperator — OR matches either sub-condition" {
    var op = try ExprFilterOperator.init("or-filter", "value_contains:payment OR value_contains:kyc");

    // Matches first branch
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "payment.transfer", 0)));
    // Matches second branch
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "kyc.approved", 0)));
    // Matches neither
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "refund.issued", 0)));
    // Matches both (still true)
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "payment kyc combined", 0)));
}

test "ExprFilterOperator — AND requires all sub-conditions" {
    var op = try ExprFilterOperator.init("and-filter", "value_contains:payment AND value_contains:approved");

    // Both match
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "payment approved", 0)));
    // Only first matches
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "payment pending", 0)));
    // Only second matches
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "approved refund", 0)));
    // Neither matches
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "refund pending", 0)));
}

test "ExprFilterOperator — OR with three branches" {
    var op = try ExprFilterOperator.init("or3", "value_contains:error OR value_contains:warn OR value_contains:fatal");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "error occurred", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "warn: low disk", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "fatal crash", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "info: all good", 0)));
}

test "ExprFilterOperator — OR with json expressions" {
    var op = try ExprFilterOperator.init("or-json", "json:type^=payment OR json:type^=kyc");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"type\":\"payment.deposit\"}", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"type\":\"kyc.verified\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"type\":\"refund.partial\"}", 0)));
}

test "ExprFilterOperator — AND with json expressions" {
    var op = try ExprFilterOperator.init("and-json", "json:amount>100 AND json:status=approved");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"amount\":200,\"status\":\"approved\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"amount\":50,\"status\":\"approved\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"amount\":200,\"status\":\"pending\"}", 0)));
}

test "ExprFilterOperator — single condition with OR in value is not compound" {
    // "value_contains:OR" should NOT be treated as compound — "OR" is inside the arg
    var op = try ExprFilterOperator.init("not-compound", "value_contains:OR-gate");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "OR-gate open", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "AND-gate open", 0)));
}

test "ExprFilterOperator — json:$. JSONPath-prefixed paths resolve (doc syntax)" {
    // The docs use `$.`-prefixed paths in conditions; they must behave like plain paths.
    var eq = try ExprFilterOperator.init("dollar-eq", "json:$.level=error");
    try std.testing.expect(eq.evaluate(ProcessingRecord.init("k", "{\"level\":\"error\"}", 0)));
    try std.testing.expect(!eq.evaluate(ProcessingRecord.init("k", "{\"level\":\"info\"}", 0)));

    // Numeric comparison with `$.` prefix, including float values.
    var gt = try ExprFilterOperator.init("dollar-gt", "json:$.amount>100");
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":250}", 0)));
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":72.5e1}", 0))); // 725.0 float
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":5}", 0)));

    // Nested dotted path under `$.`.
    var nested = try ExprFilterOperator.init("dollar-nested", "json:$.meta.region=us-east");
    try std.testing.expect(nested.evaluate(ProcessingRecord.init("k", "{\"meta\":{\"region\":\"us-east\"}}", 0)));
    try std.testing.expect(!nested.evaluate(ProcessingRecord.init("k", "{\"meta\":{\"region\":\"eu-west\"}}", 0)));
}

test "ExprFilterOperator — plain float numeric comparison" {
    // Floats must compare numerically (previously `.float` fell through to false).
    var op = try ExprFilterOperator.init("float-gt", "json:latency_ms>5000");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"latency_ms\":6000.5}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"latency_ms\":10.0}", 0)));
}
