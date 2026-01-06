/// FloQL Parser — LL(1) recursive descent parser for the FloQL query language.
///
/// Grammar:
///   query     = source ("|" stage)*
///   source    = measurement ["{" filter "}"] "[" range "]"
///   stage     = function "(" args ")"
///   filter    = tag_match ("," tag_match)*
///   tag_match = key ("=" | "!=" | "=~") value
///   range     = duration | duration ".." duration
///   args      = (arg ("," arg)*)?
///   arg       = number | string | duration | identifier
///   function  = identifier
///
/// All parsed strings borrow from the original source. No allocations for
/// string content — only for collections (filters, stages).
const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");

const Query = ast.Query;
const Source = ast.Source;
const Stage = ast.Stage;
const TagFilter = ast.TagFilter;
const FilterOp = ast.FilterOp;
const TimeRange = ast.TimeRange;

pub const ParseError = error{
    UnexpectedChar,
    UnexpectedEnd,
    ExpectedMeasurement,
    ExpectedCloseBrace,
    ExpectedCloseBracket,
    ExpectedOpenParen,
    ExpectedCloseParen,
    InvalidDuration,
    InvalidNumber,
    UnknownFunction,
    InvalidPredicate,
    OutOfMemory,
};

pub const Parser = struct {
    source: []const u8,
    pos: usize,

    pub fn parse(source: []const u8, allocator: Allocator) ParseError!Query {
        var p = Parser{ .source = source, .pos = 0 };
        const src = try p.parseSource(allocator);

        var stages: std.ArrayList(Stage) = .empty;
        errdefer {
            for (stages.items) |*s| s.deinit(allocator);
            stages.deinit(allocator);
        }

        while (true) {
            p.skipWhitespace();
            if (!p.consumeIf('|')) break;
            const stage = try p.parseStage(allocator);
            stages.append(allocator, stage) catch return error.OutOfMemory;
        }

        const owned_stages = allocator.dupe(Stage, stages.items) catch return error.OutOfMemory;
        stages.deinit(allocator);

        return Query{
            .source = src,
            .stages = owned_stages,
        };
    }

    // ========================================================================
    // Source Parser: measurement{filters}[range]
    // ========================================================================

    fn parseSource(self: *Parser, allocator: Allocator) ParseError!Source {
        self.skipWhitespace();

        // Parse measurement name (alphanumeric + underscores + dots + hyphens)
        const measurement = self.parseIdentifier() orelse return error.ExpectedMeasurement;

        // Optional tag filter: {key=value, ...}
        self.skipWhitespace();
        var filters: []const TagFilter = &.{};
        if (self.consumeIf('{')) {
            filters = try self.parseFilters(allocator);
            if (!self.consumeIf('}')) return error.ExpectedCloseBrace;
        }

        // Time range: [duration] or [from..to]
        self.skipWhitespace();
        if (!self.consumeIf('[')) {
            // Default 1h if no range specified
            return Source{
                .measurement = measurement,
                .filters = filters,
                .range = .{ .duration_ms = 3600000 },
            };
        }

        const range = try self.parseRange();
        if (!self.consumeIf(']')) return error.ExpectedCloseBracket;

        return Source{
            .measurement = measurement,
            .filters = filters,
            .range = range,
        };
    }

    fn parseFilters(self: *Parser, allocator: Allocator) ParseError![]const TagFilter {
        var filters: std.ArrayList(TagFilter) = .empty;
        errdefer filters.deinit(allocator);

        // First filter
        const first = try self.parseTagFilter();
        filters.append(allocator, first) catch return error.OutOfMemory;

        // Additional filters separated by commas
        while (true) {
            self.skipWhitespace();
            if (!self.consumeIf(',')) break;
            const f = try self.parseTagFilter();
            filters.append(allocator, f) catch return error.OutOfMemory;
        }

        const result = allocator.dupe(TagFilter, filters.items) catch return error.OutOfMemory;
        filters.deinit(allocator);
        return result;
    }

    fn parseTagFilter(self: *Parser) ParseError!TagFilter {
        self.skipWhitespace();
        const key = self.parseIdentifier() orelse return error.UnexpectedEnd;
        self.skipWhitespace();

        // Parse operator: =, !=, =~, !~
        const op: FilterOp = blk: {
            if (self.consumeStr("!~")) break :blk .nregex;
            if (self.consumeStr("!=")) break :blk .neq;
            if (self.consumeStr("=~")) break :blk .regex;
            if (self.consumeIf('=')) break :blk .eq;
            return error.UnexpectedChar;
        };

        self.skipWhitespace();
        const value = try self.parseStringValue();

        return TagFilter{
            .key = key,
            .op = op,
            .value = value,
        };
    }

    fn parseRange(self: *Parser) ParseError!TimeRange {
        self.skipWhitespace();

        // Try to parse as a simple duration: [1h], [30m], [7d]
        const start_pos = self.pos;
        if (self.parseDurationLiteral()) |dur_ms| {
            self.skipWhitespace();
            // Check for ".." (explicit range)
            if (self.consumeStr("..")) {
                const to_dur = self.parseDurationLiteral() orelse return error.InvalidDuration;
                return TimeRange{
                    .duration_ms = 0,
                    .from_ms = dur_ms,
                    .to_ms = to_dur,
                };
            }
            // Simple duration
            return TimeRange{ .duration_ms = dur_ms };
        }

        // Try as absolute timestamps (integer ms)
        self.pos = start_pos;
        if (self.parseIntLiteral()) |from_ms| {
            self.skipWhitespace();
            if (self.consumeStr("..")) {
                const to_ms = self.parseIntLiteral() orelse return error.InvalidNumber;
                return TimeRange{
                    .from_ms = from_ms,
                    .to_ms = to_ms,
                };
            }
            // Single absolute timestamp as duration
            return TimeRange{ .from_ms = from_ms };
        }

        return error.InvalidDuration;
    }

    // ========================================================================
    // Stage Parser: function(args)
    // ========================================================================

    fn parseStage(self: *Parser, allocator: Allocator) ParseError!Stage {
        self.skipWhitespace();
        const func_name = self.parseIdentifier() orelse return error.UnknownFunction;
        self.skipWhitespace();

        if (!self.consumeIf('(')) return error.ExpectedOpenParen;
        self.skipWhitespace();

        const stage = try self.buildStage(func_name, allocator);
        self.skipWhitespace();
        if (!self.consumeIf(')')) return error.ExpectedCloseParen;

        return stage;
    }

    fn buildStage(self: *Parser, name: []const u8, allocator: Allocator) ParseError!Stage {
        _ = allocator;

        // Window: window(5m)
        if (std.mem.eql(u8, name, "window")) {
            self.skipWhitespace();
            const interval = self.parseDurationLiteral() orelse return error.InvalidDuration;
            return Stage{ .window = .{ .interval_ms = interval } };
        }

        // Aggregation functions: avg(), sum(), count(), min(), max()
        if (ast.AggFunction.fromString(name)) |agg_fn| {
            return Stage{ .aggregate = .{ .function = agg_fn } };
        }

        // Rate: rate(1m)
        if (std.mem.eql(u8, name, "rate")) {
            self.skipWhitespace();
            const interval = self.parseDurationLiteral() orelse return error.InvalidDuration;
            return Stage{ .rate = .{ .interval_ms = interval } };
        }

        // Delta: delta()
        if (std.mem.eql(u8, name, "delta")) {
            return Stage{ .delta = .{} };
        }

        // Where: where(value > 90)
        if (std.mem.eql(u8, name, "where")) {
            self.skipWhitespace();
            // Skip "value" keyword if present
            if (self.consumeStr("value")) {
                self.skipWhitespace();
            }
            const pred = try self.parsePredicate();
            return Stage{ .where_filter = .{ .predicate = pred } };
        }

        // Group by: group_by(host)
        if (std.mem.eql(u8, name, "group_by")) {
            self.skipWhitespace();
            const tag = self.parseIdentifier() orelse return error.UnexpectedEnd;
            return Stage{ .group_by = .{ .tag = tag } };
        }

        // Top-K: topk(5)
        if (std.mem.eql(u8, name, "topk")) {
            self.skipWhitespace();
            const k = self.parseUintLiteral() orelse return error.InvalidNumber;
            return Stage{ .topk = .{ .k = k } };
        }

        // Bottom-K: bottomk(5)
        if (std.mem.eql(u8, name, "bottomk")) {
            self.skipWhitespace();
            const k = self.parseUintLiteral() orelse return error.InvalidNumber;
            return Stage{ .bottomk = .{ .k = k } };
        }

        // Alias: alias("name") or alias(name)
        if (std.mem.eql(u8, name, "alias")) {
            self.skipWhitespace();
            const alias_name = try self.parseArgString();
            return Stage{ .alias_stage = .{ .name = alias_name } };
        }

        // Field: field(user)
        if (std.mem.eql(u8, name, "field")) {
            self.skipWhitespace();
            const field_name = self.parseIdentifier() orelse return error.UnexpectedEnd;
            return Stage{ .field = .{ .name = field_name } };
        }

        // Math ops
        if (std.mem.eql(u8, name, "abs")) return Stage{ .abs_stage = {} };
        if (std.mem.eql(u8, name, "ceil")) return Stage{ .ceil_stage = {} };
        if (std.mem.eql(u8, name, "floor")) return Stage{ .floor_stage = {} };

        // Math: math(value * 100), math(* 100), math(/ 1024)
        if (std.mem.eql(u8, name, "math")) {
            self.skipWhitespace();
            // Optional "value" keyword
            if (self.consumeStr("value")) {
                self.skipWhitespace();
            }
            // Parse operator
            const math_op: ast.MathOp = blk: {
                if (self.consumeIf('+')) break :blk .add;
                if (self.consumeIf('-')) break :blk .sub;
                if (self.consumeIf('*')) break :blk .mul;
                if (self.consumeIf('/')) break :blk .div;
                if (self.consumeIf('%')) break :blk .mod;
                return error.InvalidPredicate;
            };
            self.skipWhitespace();
            const operand = self.parseFloatLiteral() orelse return error.InvalidNumber;
            return Stage{ .math = .{ .op = math_op, .operand = operand } };
        }

        // Round: round(2)
        if (std.mem.eql(u8, name, "round")) {
            self.skipWhitespace();
            const decimals = self.parseUintLiteral() orelse 0;
            return Stage{ .round_stage = .{ .decimals = decimals } };
        }

        // Percentile: percentile(99)
        if (std.mem.eql(u8, name, "percentile")) {
            self.skipWhitespace();
            const p = self.parseFloatLiteral() orelse return error.InvalidNumber;
            return Stage{ .percentile = .{ .percentile = p } };
        }

        // first(), last()
        if (std.mem.eql(u8, name, "first")) return Stage{ .first = {} };
        if (std.mem.eql(u8, name, "last")) return Stage{ .last = {} };

        return error.UnknownFunction;
    }

    // ========================================================================
    // Predicate Parser: > N, >= N, < N, <= N, == N, != N
    // ========================================================================

    fn parsePredicate(self: *Parser) ParseError!ast.Predicate {
        self.skipWhitespace();

        const op: ast.CompareOp = blk: {
            if (self.consumeStr(">=")) break :blk .gte;
            if (self.consumeStr("<=")) break :blk .lte;
            if (self.consumeStr("!=")) break :blk .neq;
            if (self.consumeStr("==")) break :blk .eq;
            if (self.consumeIf('>')) break :blk .gt;
            if (self.consumeIf('<')) break :blk .lt;
            return error.InvalidPredicate;
        };

        self.skipWhitespace();
        const value = self.parseFloatLiteral() orelse return error.InvalidNumber;

        return ast.Predicate{
            .op = op,
            .value = value,
        };
    }

    // ========================================================================
    // Lexer Primitives
    // ========================================================================

    fn skipWhitespace(self: *Parser) void {
        while (self.pos < self.source.len and (self.source[self.pos] == ' ' or self.source[self.pos] == '\t' or self.source[self.pos] == '\n' or self.source[self.pos] == '\r')) {
            self.pos += 1;
        }
    }

    fn peek(self: *Parser) ?u8 {
        if (self.pos < self.source.len) return self.source[self.pos];
        return null;
    }

    fn consumeIf(self: *Parser, ch: u8) bool {
        if (self.pos < self.source.len and self.source[self.pos] == ch) {
            self.pos += 1;
            return true;
        }
        return false;
    }

    fn consumeStr(self: *Parser, s: []const u8) bool {
        if (self.pos + s.len <= self.source.len and std.mem.eql(u8, self.source[self.pos..][0..s.len], s)) {
            self.pos += s.len;
            return true;
        }
        return false;
    }

    /// Parse an identifier: [a-zA-Z_][a-zA-Z0-9_.-]*
    fn parseIdentifier(self: *Parser) ?[]const u8 {
        const start = self.pos;
        if (self.pos >= self.source.len) return null;

        const first = self.source[self.pos];
        if (!std.ascii.isAlphabetic(first) and first != '_') return null;
        self.pos += 1;

        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (std.ascii.isAlphanumeric(c) or c == '_' or c == '-' or c == '.') {
                self.pos += 1;
            } else {
                break;
            }
        }

        if (self.pos == start) return null;
        return self.source[start..self.pos];
    }

    /// Parse a quoted or unquoted string value: "hello" or 'hello' or hello
    fn parseStringValue(self: *Parser) ParseError![]const u8 {
        if (self.pos >= self.source.len) return error.UnexpectedEnd;

        const quote = self.source[self.pos];
        if (quote == '"' or quote == '\'') {
            self.pos += 1; // skip opening quote
            const start = self.pos;
            while (self.pos < self.source.len and self.source[self.pos] != quote) {
                self.pos += 1;
            }
            if (self.pos >= self.source.len) return error.UnexpectedEnd;
            const value = self.source[start..self.pos];
            self.pos += 1; // skip closing quote
            return value;
        }

        // Unquoted: read until special char
        const start = self.pos;
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == ',' or c == '}' or c == ']' or c == ')' or c == '|' or c == ' ') break;
            self.pos += 1;
        }
        if (self.pos == start) return error.UnexpectedEnd;
        return self.source[start..self.pos];
    }

    /// Parse a string argument (quoted or identifier)
    fn parseArgString(self: *Parser) ParseError![]const u8 {
        if (self.pos >= self.source.len) return error.UnexpectedEnd;
        const c = self.source[self.pos];
        if (c == '"' or c == '\'') {
            return self.parseStringValue();
        }
        return self.parseIdentifier() orelse error.UnexpectedEnd;
    }

    /// Parse a duration literal: 30s, 5m, 1h, 7d
    fn parseDurationLiteral(self: *Parser) ?i64 {
        const start = self.pos;
        // Read digits
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        if (self.pos == start) return null;
        // Read unit char
        if (self.pos >= self.source.len) {
            self.pos = start;
            return null;
        }
        const unit = self.source[self.pos];
        if (unit != 's' and unit != 'm' and unit != 'h' and unit != 'd') {
            self.pos = start;
            return null;
        }
        self.pos += 1;

        const dur_str = self.source[start..self.pos];
        return ast.parseDuration(dur_str);
    }

    /// Parse an integer literal
    fn parseIntLiteral(self: *Parser) ?i64 {
        const start = self.pos;
        if (self.pos < self.source.len and self.source[self.pos] == '-') self.pos += 1;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        if (self.pos == start or (self.pos == start + 1 and self.source[start] == '-')) {
            self.pos = start;
            return null;
        }
        return std.fmt.parseInt(i64, self.source[start..self.pos], 10) catch {
            self.pos = start;
            return null;
        };
    }

    /// Parse an unsigned integer literal
    fn parseUintLiteral(self: *Parser) ?u32 {
        const start = self.pos;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        if (self.pos == start) return null;
        return std.fmt.parseInt(u32, self.source[start..self.pos], 10) catch {
            self.pos = start;
            return null;
        };
    }

    /// Parse a float literal (e.g., 99, 99.9, 3.14)
    fn parseFloatLiteral(self: *Parser) ?f64 {
        const start = self.pos;
        if (self.pos < self.source.len and self.source[self.pos] == '-') self.pos += 1;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
            self.pos += 1;
        }
        // Optional decimal
        if (self.pos < self.source.len and self.source[self.pos] == '.') {
            self.pos += 1;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) {
                self.pos += 1;
            }
        }
        if (self.pos == start) return null;
        return std.fmt.parseFloat(f64, self.source[start..self.pos]) catch {
            self.pos = start;
            return null;
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "floql_parse_simple_query" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu_usage{host=\"web-01\"}[1h] | window(5m) | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("cpu_usage", q.source.measurement);
    try std.testing.expectEqual(@as(usize, 1), q.source.filters.len);
    try std.testing.expectEqualStrings("host", q.source.filters[0].key);
    try std.testing.expectEqualStrings("web-01", q.source.filters[0].value);
    try std.testing.expectEqual(FilterOp.eq, q.source.filters[0].op);
    try std.testing.expectEqual(@as(i64, 3600000), q.source.range.duration_ms);

    try std.testing.expectEqual(@as(usize, 2), q.stages.len);

    // window(5m)
    try std.testing.expectEqual(@as(i64, 300000), q.stages[0].window.interval_ms);

    // avg()
    try std.testing.expectEqual(ast.AggFunction.avg, q.stages[1].aggregate.function);
}

test "floql_parse_rate" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("http_requests_total{service='api'}[1h] | rate(1m)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("http_requests_total", q.source.measurement);
    try std.testing.expectEqual(@as(usize, 1), q.source.filters.len);
    try std.testing.expectEqualStrings("service", q.source.filters[0].key);
    try std.testing.expectEqualStrings("api", q.source.filters[0].value);

    try std.testing.expectEqual(@as(usize, 1), q.stages.len);
    try std.testing.expectEqual(@as(i64, 60000), q.stages[0].rate.interval_ms);
}

test "floql_parse_chained_pipeline" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("http_requests_total[6h] | rate(1m) | window(5m) | avg() | where(value > 100)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("http_requests_total", q.source.measurement);
    try std.testing.expectEqual(@as(usize, 0), q.source.filters.len);
    try std.testing.expectEqual(@as(i64, 21600000), q.source.range.duration_ms); // 6h

    try std.testing.expectEqual(@as(usize, 4), q.stages.len);
    // rate(1m)
    try std.testing.expectEqual(@as(i64, 60000), q.stages[0].rate.interval_ms);
    // window(5m)
    try std.testing.expectEqual(@as(i64, 300000), q.stages[1].window.interval_ms);
    // avg()
    try std.testing.expectEqual(ast.AggFunction.avg, q.stages[2].aggregate.function);
    // where(value > 100)
    try std.testing.expectEqual(ast.CompareOp.gt, q.stages[3].where_filter.predicate.op);
    try std.testing.expectEqual(@as(f64, 100.0), q.stages[3].where_filter.predicate.value);
}

test "floql_parse_field_selection" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{host='web-01'}[1h] | field(user) | window(5m) | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("cpu", q.source.measurement);
    try std.testing.expectEqual(@as(usize, 3), q.stages.len);
    try std.testing.expectEqualStrings("user", q.stages[0].field.name);
}

test "floql_parse_group_by" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("temperature{env='prod'}[24h] | window(1h) | avg() | group_by(sensor)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), q.stages.len);
    try std.testing.expectEqualStrings("sensor", q.stages[2].group_by.tag);
}

test "floql_parse_topk" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("http_requests{region='eu'}[1h] | rate(5m) | group_by(host) | topk(5)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), q.stages.len);
    try std.testing.expectEqual(@as(u32, 5), q.stages[2].topk.k);
}

test "floql_parse_percentile" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("request_latency{service='api'}[1h] | window(1m) | percentile(99)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), q.stages.len);
    try std.testing.expectEqual(@as(f64, 99.0), q.stages[1].percentile.percentile);
}

test "floql_parse_no_filter" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu_usage[30m] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("cpu_usage", q.source.measurement);
    try std.testing.expectEqual(@as(usize, 0), q.source.filters.len);
    try std.testing.expectEqual(@as(i64, 1800000), q.source.range.duration_ms);
}

test "floql_parse_multiple_filters" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{host='web-01', region='eu'}[1h] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), q.source.filters.len);
    try std.testing.expectEqualStrings("host", q.source.filters[0].key);
    try std.testing.expectEqualStrings("web-01", q.source.filters[0].value);
    try std.testing.expectEqualStrings("region", q.source.filters[1].key);
    try std.testing.expectEqualStrings("eu", q.source.filters[1].value);
}

test "floql_parse_neq_filter" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{env!='test'}[1h] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.source.filters.len);
    try std.testing.expectEqual(FilterOp.neq, q.source.filters[0].op);
    try std.testing.expectEqualStrings("test", q.source.filters[0].value);
}

test "floql_parse_alias" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu[1h] | avg() | alias(\"busy_cpu\")", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), q.stages.len);
    try std.testing.expectEqualStrings("busy_cpu", q.stages[1].alias_stage.name);
}

test "floql_parse_abs_ceil_floor" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("temp[1h] | abs() | ceil()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), q.stages.len);
    // abs and ceil are void tags
    _ = q.stages[0].abs_stage;
    _ = q.stages[1].ceil_stage;
}

test "floql_parse_where_gte" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu[1h] | where(value >= 50.5)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.stages.len);
    try std.testing.expectEqual(ast.CompareOp.gte, q.stages[0].where_filter.predicate.op);
    try std.testing.expectEqual(@as(f64, 50.5), q.stages[0].where_filter.predicate.value);
}

test "floql_parse_math_mul" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu[1h] | math(value * 100)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.stages.len);
    try std.testing.expectEqual(ast.MathOp.mul, q.stages[0].math.op);
    try std.testing.expectEqual(@as(f64, 100.0), q.stages[0].math.operand);
}

test "floql_parse_math_shorthand" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu[1h] | math(/ 1024)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.stages.len);
    try std.testing.expectEqual(ast.MathOp.div, q.stages[0].math.op);
    try std.testing.expectEqual(@as(f64, 1024.0), q.stages[0].math.operand);
}

test "floql_parse_math_add_float" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("temp[1h] | math(+ 273.15)", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.stages.len);
    try std.testing.expectEqual(ast.MathOp.add, q.stages[0].math.op);
    try std.testing.expectEqual(@as(f64, 273.15), q.stages[0].math.operand);
}

test "floql_parse_regex_filter" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{host=~\"web-*\"}[1h] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.source.filters.len);
    try std.testing.expectEqual(FilterOp.regex, q.source.filters[0].op);
    try std.testing.expectEqualStrings("host", q.source.filters[0].key);
    try std.testing.expectEqualStrings("web-*", q.source.filters[0].value);
}

test "floql_parse_nregex_filter" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{env!~\"test*\"}[1h] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), q.source.filters.len);
    try std.testing.expectEqual(FilterOp.nregex, q.source.filters[0].op);
    try std.testing.expectEqualStrings("env", q.source.filters[0].key);
    try std.testing.expectEqualStrings("test*", q.source.filters[0].value);
}

test "floql_parse_mixed_filters" {
    const allocator = std.testing.allocator;
    var q = try Parser.parse("cpu{region=\"eu\", host=~\"web-*\", env!=\"staging\"}[1h] | avg()", allocator);
    defer q.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), q.source.filters.len);
    try std.testing.expectEqual(FilterOp.eq, q.source.filters[0].op);
    try std.testing.expectEqual(FilterOp.regex, q.source.filters[1].op);
    try std.testing.expectEqual(FilterOp.neq, q.source.filters[2].op);
}
