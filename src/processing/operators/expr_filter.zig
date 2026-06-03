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
//!   Note: OR and AND cannot be mixed in a single expression.
//!   OR is checked first (lower precedence). Use classify rules for
//!   complex routing instead of deeply nested boolean logic.
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
        always_true,
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

    /// Create an expression-based filter operator.
    /// `condition` is the raw condition string from YAML (e.g., "value_contains:hello").
    /// Both `name` and `condition` must outlive the operator (typically allocated by parser).
    pub fn init(name: []const u8, condition: []const u8) Self {
        return .{
            .name = name,
            .condition = condition,
            .parsed = parseCondition(condition),
        };
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
            .always_true => true,
        };
    }

    // =========================================================================
    // Condition parsing
    // =========================================================================

    fn parseCondition(cond: []const u8) ParsedCondition {
        // Compound: split on " OR " first (lower precedence), then " AND "
        if (splitCompound(cond, " OR ")) |compound| return .{ .or_expr = compound };
        if (splitCompound(cond, " AND ")) |compound| return .{ .and_expr = compound };

        return .{ .single = parseSingleCondition(cond) };
    }

    /// Try to split `cond` on `sep` (e.g. " OR "). Returns a Compound if
    /// two or more branches are found, null if `sep` does not appear.
    fn splitCompound(cond: []const u8, sep: []const u8) ?Compound {
        // Quick check — if sep is not present at all, skip iteration.
        if (std.mem.indexOf(u8, cond, sep) == null) return null;

        var compound = Compound{};
        var rest: []const u8 = cond;
        while (rest.len > 0) {
            if (compound.len >= MAX_COMPOUND) break;
            if (std.mem.indexOf(u8, rest, sep)) |pos| {
                const part = std.mem.trim(u8, rest[0..pos], " ");
                if (part.len > 0) {
                    compound.items[compound.len] = parseSingleCondition(part);
                    compound.len += 1;
                }
                rest = rest[pos + sep.len ..];
            } else {
                const part = std.mem.trim(u8, rest, " ");
                if (part.len > 0) {
                    compound.items[compound.len] = parseSingleCondition(part);
                    compound.len += 1;
                }
                break;
            }
        }
        if (compound.len < 2) return null; // not actually compound
        return compound;
    }

    /// Parse a single (non-compound) condition expression.
    fn parseSingleCondition(cond: []const u8) SingleCondition {
        // Simple keyword conditions
        if (std.mem.eql(u8, cond, "not_empty")) return .not_empty;
        if (std.mem.eql(u8, cond, "key_not_empty")) return .key_not_empty;

        // JSON expression: json:<path><op><value> or json_field:<path><op><value> (legacy)
        if (std.mem.startsWith(u8, cond, "json:")) {
            if (parseJsonExpr(cond[5..])) |expr| return .{ .json_expr = expr };
        }

        // Prefix:arg conditions
        if (splitOnce(cond, ':')) |parts| {
            const prefix = parts[0];
            const arg = parts[1];

            if (std.mem.eql(u8, prefix, "value_contains")) return .{ .value_contains = arg };
            if (std.mem.eql(u8, prefix, "key_contains")) return .{ .key_contains = arg };
            if (std.mem.eql(u8, prefix, "key_equals")) return .{ .key_equals = arg };
            if (std.mem.eql(u8, prefix, "key_prefix")) return .{ .key_prefix = arg };
            if (std.mem.eql(u8, prefix, "value_prefix")) return .{ .value_prefix = arg };
            if (std.mem.eql(u8, prefix, "min_length")) {
                const n = std.fmt.parseInt(usize, arg, 10) catch return .always_true;
                return .{ .min_length = n };
            }
        }

        return .always_true;
    }

    /// Parse a JSON expression after the `json:` prefix.
    /// Scans for the first operator character to split path from op+value.
    fn parseJsonExpr(expr: []const u8) ?JsonExpr {
        // Find the start of the operator: first occurrence of = ! ^ * > <
        var i: usize = 0;
        while (i < expr.len) : (i += 1) {
            const c = expr[i];
            if (c == '=' or c == '!' or c == '^' or c == '*' or c == '>' or c == '<') break;
        }
        if (i == 0 or i >= expr.len) return null;

        const path = expr[0..i];
        const rest = expr[i..];

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
                if (value.len == 0) return null; // operator with no value
                return .{ .path = path, .op = entry.op, .value = value };
            }
        }

        return null;
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
    var op = ExprFilterOperator.init("test-filter", "value_contains:hello");
    const rec_match = ProcessingRecord.init("k", "say hello world", 1000);
    const rec_miss = ProcessingRecord.init("k", "goodbye world", 1000);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));

    // vtable works
    const iface = op.operator();
    try std.testing.expectEqualStrings("test-filter", iface.getName());
}

test "ExprFilterOperator — key_equals" {
    var op = ExprFilterOperator.init("key-filter", "key_equals:user-42");
    const rec_match = ProcessingRecord.init("user-42", "data", 0);
    const rec_miss = ProcessingRecord.init("user-43", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — key_prefix" {
    var op = ExprFilterOperator.init("prefix-filter", "key_prefix:order-");
    const rec_match = ProcessingRecord.init("order-123", "data", 0);
    const rec_miss = ProcessingRecord.init("user-123", "data", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — not_empty" {
    var op = ExprFilterOperator.init("nonempty-filter", "not_empty");
    const rec_match = ProcessingRecord.init("k", "some data", 0);
    const rec_empty = ProcessingRecord.init("k", "", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_empty));
}

test "ExprFilterOperator — min_length" {
    var op = ExprFilterOperator.init("min-len", "min_length:5");
    const rec_ok = ProcessingRecord.init("k", "abcde", 0);
    const rec_short = ProcessingRecord.init("k", "abc", 0);

    try std.testing.expect(op.evaluate(rec_ok));
    try std.testing.expect(!op.evaluate(rec_short));
}

test "ExprFilterOperator — always_true for unknown condition" {
    var op = ExprFilterOperator.init("unknown", "something_weird");
    const rec = ProcessingRecord.init("k", "v", 0);
    try std.testing.expect(op.evaluate(rec));
}

test "ExprFilterOperator — value_prefix" {
    var op = ExprFilterOperator.init("vp", "value_prefix:ERROR");
    const rec_match = ProcessingRecord.init("k", "ERROR: something broke", 0);
    const rec_miss = ProcessingRecord.init("k", "INFO: all good", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
}

test "ExprFilterOperator — json equals (new syntax)" {
    var op = ExprFilterOperator.init("json-eq", "json:status=approved");
    const rec_match = ProcessingRecord.init("k", "{\"status\":\"approved\",\"amount\":100}", 0);
    const rec_miss = ProcessingRecord.init("k", "{\"status\":\"pending\",\"amount\":50}", 0);
    const rec_bad = ProcessingRecord.init("k", "not-json", 0);

    try std.testing.expect(op.evaluate(rec_match));
    try std.testing.expect(!op.evaluate(rec_miss));
    try std.testing.expect(!op.evaluate(rec_bad));
}

test "ExprFilterOperator — json not equals" {
    var op = ExprFilterOperator.init("jneq", "json:type!=refund");
    const rec_pass = ProcessingRecord.init("k", "{\"type\":\"payment\"}", 0);
    const rec_fail = ProcessingRecord.init("k", "{\"type\":\"refund\"}", 0);

    try std.testing.expect(op.evaluate(rec_pass));
    try std.testing.expect(!op.evaluate(rec_fail));
}

test "ExprFilterOperator — json prefix (^=)" {
    var op = ExprFilterOperator.init("jpfx", "json:type^=payment");
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
    var op = ExprFilterOperator.init("jcnt", "json:type*=transfer");
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
    var op = ExprFilterOperator.init("jnpfx", "json:type!^=payment");
    const rec_no = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\"}", 0);
    const rec_yes = ProcessingRecord.init("k", "{\"type\":\"refund.partial\"}", 0);

    try std.testing.expect(!op.evaluate(rec_no)); // starts with payment → negated = false
    try std.testing.expect(op.evaluate(rec_yes)); // doesn't start with payment → negated = true
}

test "ExprFilterOperator — json not contains (!*=)" {
    var op = ExprFilterOperator.init("jncnt", "json:type!*=transfer");
    const rec_has = ProcessingRecord.init("k", "{\"type\":\"payment.transfer\"}", 0);
    const rec_not = ProcessingRecord.init("k", "{\"type\":\"payment.refund\"}", 0);

    try std.testing.expect(!op.evaluate(rec_has));
    try std.testing.expect(op.evaluate(rec_not));
}

test "ExprFilterOperator — json numeric comparisons" {
    // greater than
    var gt = ExprFilterOperator.init("gt", "json:amount>100");
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":200}", 0)));
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":50}", 0)));

    // greater or equal
    var gte = ExprFilterOperator.init("gte", "json:amount>=100");
    try std.testing.expect(gte.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!gte.evaluate(ProcessingRecord.init("k", "{\"amount\":99}", 0)));

    // less than
    var lt = ExprFilterOperator.init("lt", "json:amount<100");
    try std.testing.expect(lt.evaluate(ProcessingRecord.init("k", "{\"amount\":50}", 0)));
    try std.testing.expect(!lt.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));

    // less or equal
    var lte = ExprFilterOperator.init("lte", "json:amount<=100");
    try std.testing.expect(lte.evaluate(ProcessingRecord.init("k", "{\"amount\":100}", 0)));
    try std.testing.expect(!lte.evaluate(ProcessingRecord.init("k", "{\"amount\":101}", 0)));

    // numeric ops on string fields → false
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":\"200\"}", 0)));
}

test "ExprFilterOperator — json integer equality" {
    var op = ExprFilterOperator.init("jeqi", "json:code=200");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"code\":200}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"code\":404}", 0)));
}

test "ExprFilterOperator — json boolean equality" {
    var op_t = ExprFilterOperator.init("jbt", "json:active=true");
    var op_f = ExprFilterOperator.init("jbf", "json:active!=true");
    const rec_true = ProcessingRecord.init("k", "{\"active\":true}", 0);
    const rec_false = ProcessingRecord.init("k", "{\"active\":false}", 0);

    try std.testing.expect(op_t.evaluate(rec_true));
    try std.testing.expect(!op_t.evaluate(rec_false));
    try std.testing.expect(!op_f.evaluate(rec_true));
    try std.testing.expect(op_f.evaluate(rec_false));
}

test "ExprFilterOperator — json missing field" {
    var op = ExprFilterOperator.init("miss", "json:nonexistent=x");
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"other\":\"y\"}", 0)));
}

// =========================================================================
// Compound (OR / AND) tests
// =========================================================================

test "ExprFilterOperator — OR matches either sub-condition" {
    var op = ExprFilterOperator.init("or-filter", "value_contains:payment OR value_contains:kyc");

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
    var op = ExprFilterOperator.init("and-filter", "value_contains:payment AND value_contains:approved");

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
    var op = ExprFilterOperator.init("or3", "value_contains:error OR value_contains:warn OR value_contains:fatal");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "error occurred", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "warn: low disk", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "fatal crash", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "info: all good", 0)));
}

test "ExprFilterOperator — OR with json expressions" {
    var op = ExprFilterOperator.init("or-json", "json:type^=payment OR json:type^=kyc");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"type\":\"payment.deposit\"}", 0)));
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"type\":\"kyc.verified\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"type\":\"refund.partial\"}", 0)));
}

test "ExprFilterOperator — AND with json expressions" {
    var op = ExprFilterOperator.init("and-json", "json:amount>100 AND json:status=approved");

    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"amount\":200,\"status\":\"approved\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"amount\":50,\"status\":\"approved\"}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"amount\":200,\"status\":\"pending\"}", 0)));
}

test "ExprFilterOperator — single condition with OR in value is not compound" {
    // "value_contains:OR" should NOT be treated as compound — "OR" is inside the arg
    var op = ExprFilterOperator.init("not-compound", "value_contains:OR-gate");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "OR-gate open", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "AND-gate open", 0)));
}

test "ExprFilterOperator — json:$. JSONPath-prefixed paths resolve (doc syntax)" {
    // The docs use `$.`-prefixed paths in conditions; they must behave like plain paths.
    var eq = ExprFilterOperator.init("dollar-eq", "json:$.level=error");
    try std.testing.expect(eq.evaluate(ProcessingRecord.init("k", "{\"level\":\"error\"}", 0)));
    try std.testing.expect(!eq.evaluate(ProcessingRecord.init("k", "{\"level\":\"info\"}", 0)));

    // Numeric comparison with `$.` prefix, including float values.
    var gt = ExprFilterOperator.init("dollar-gt", "json:$.amount>100");
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":250}", 0)));
    try std.testing.expect(gt.evaluate(ProcessingRecord.init("k", "{\"amount\":72.5e1}", 0))); // 725.0 float
    try std.testing.expect(!gt.evaluate(ProcessingRecord.init("k", "{\"amount\":5}", 0)));

    // Nested dotted path under `$.`.
    var nested = ExprFilterOperator.init("dollar-nested", "json:$.meta.region=us-east");
    try std.testing.expect(nested.evaluate(ProcessingRecord.init("k", "{\"meta\":{\"region\":\"us-east\"}}", 0)));
    try std.testing.expect(!nested.evaluate(ProcessingRecord.init("k", "{\"meta\":{\"region\":\"eu-west\"}}", 0)));
}

test "ExprFilterOperator — plain float numeric comparison" {
    // Floats must compare numerically (previously `.float` fell through to false).
    var op = ExprFilterOperator.init("float-gt", "json:latency_ms>5000");
    try std.testing.expect(op.evaluate(ProcessingRecord.init("k", "{\"latency_ms\":6000.5}", 0)));
    try std.testing.expect(!op.evaluate(ProcessingRecord.init("k", "{\"latency_ms\":10.0}", 0)));
}
