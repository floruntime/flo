//! Simple TOML parser for Flo configuration files.
//! Supports a subset of TOML: tables, strings, integers, booleans.
//! Does not support arrays, inline tables, or multi-line strings.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    boolean: bool,
    table: Table,

    pub fn asString(self: Value) ?[]const u8 {
        return if (self == .string) self.string else null;
    }

    pub fn asInt(self: Value) ?i64 {
        return if (self == .integer) self.integer else null;
    }

    pub fn asBool(self: Value) ?bool {
        return if (self == .boolean) self.boolean else null;
    }

    pub fn asTable(self: *Value) ?*Table {
        return if (self.* == .table) &self.table else null;
    }
};

pub const Table = struct {
    allocator: Allocator,
    entries: std.StringHashMap(Value),

    pub fn init(allocator: Allocator) Table {
        return .{
            .allocator = allocator,
            .entries = std.StringHashMap(Value).init(allocator),
        };
    }

    pub fn deinit(self: *Table) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            // Free string keys (we duped them)
            self.allocator.free(entry.key_ptr.*);
            // Recursively free nested tables
            if (entry.value_ptr.* == .table) {
                entry.value_ptr.table.deinit();
            }
            // Free string values (we duped them)
            if (entry.value_ptr.* == .string) {
                self.allocator.free(entry.value_ptr.string);
            }
        }
        self.entries.deinit();
    }

    pub fn get(self: *const Table, key: []const u8) ?Value {
        return self.entries.get(key);
    }

    pub fn getTable(self: *Table, key: []const u8) ?*Table {
        if (self.entries.getPtr(key)) |val_ptr| {
            if (val_ptr.* == .table) {
                return &val_ptr.table;
            }
        }
        return null;
    }

    pub fn getString(self: *const Table, key: []const u8) ?[]const u8 {
        if (self.entries.get(key)) |val| {
            return val.asString();
        }
        return null;
    }

    pub fn getInt(self: *const Table, key: []const u8) ?i64 {
        if (self.entries.get(key)) |val| {
            return val.asInt();
        }
        return null;
    }

    pub fn getBool(self: *const Table, key: []const u8) ?bool {
        if (self.entries.get(key)) |val| {
            return val.asBool();
        }
        return null;
    }
};

pub const ParseError = error{
    UnexpectedCharacter,
    UnterminatedString,
    InvalidNumber,
    InvalidBoolean,
    DuplicateKey,
    InvalidTableHeader,
    OutOfMemory,
};

pub const Parser = struct {
    allocator: Allocator,
    input: []const u8,
    pos: usize,
    line: usize,

    pub fn init(allocator: Allocator, input: []const u8) Parser {
        return .{
            .allocator = allocator,
            .input = input,
            .pos = 0,
            .line = 1,
        };
    }

    pub fn parse(self: *Parser) ParseError!Table {
        var root = Table.init(self.allocator);
        errdefer root.deinit();

        var current_table: *Table = &root;

        while (self.pos < self.input.len) {
            self.skipWhitespaceAndComments();
            if (self.pos >= self.input.len) break;

            const c = self.input[self.pos];

            if (c == '[') {
                // Table header
                const table_name = try self.parseTableHeader();
                current_table = try self.getOrCreateTable(&root, table_name);
                self.allocator.free(table_name);
            } else if (isKeyChar(c)) {
                // Key-value pair
                const key = try self.parseKey();
                errdefer self.allocator.free(key);

                self.skipWhitespace();
                if (self.pos >= self.input.len or self.input[self.pos] != '=') {
                    self.allocator.free(key);
                    return error.UnexpectedCharacter;
                }
                self.pos += 1; // Skip '='
                self.skipWhitespace();

                const value = try self.parseValue();

                try current_table.entries.put(key, value);
            } else if (c == '\n') {
                self.pos += 1;
                self.line += 1;
            } else {
                return error.UnexpectedCharacter;
            }
        }

        return root;
    }

    fn parseTableHeader(self: *Parser) ParseError![]const u8 {
        std.debug.assert(self.input[self.pos] == '[');
        self.pos += 1;

        const start = self.pos;
        while (self.pos < self.input.len and self.input[self.pos] != ']' and self.input[self.pos] != '\n') {
            self.pos += 1;
        }

        if (self.pos >= self.input.len or self.input[self.pos] != ']') {
            return error.InvalidTableHeader;
        }

        const name = std.mem.trim(u8, self.input[start..self.pos], " \t");
        self.pos += 1; // Skip ']'

        return self.allocator.dupe(u8, name) catch return error.OutOfMemory;
    }

    fn getOrCreateTable(self: *Parser, root: *Table, name: []const u8) ParseError!*Table {
        // Handle dotted keys like "server.network"
        var it = std.mem.splitScalar(u8, name, '.');
        var current = root;

        while (it.next()) |part| {
            const trimmed = std.mem.trim(u8, part, " \t");
            if (current.entries.getPtr(trimmed)) |existing| {
                if (existing.* == .table) {
                    current = &existing.table;
                } else {
                    return error.DuplicateKey;
                }
            } else {
                const key = self.allocator.dupe(u8, trimmed) catch return error.OutOfMemory;
                try current.entries.put(key, .{ .table = Table.init(self.allocator) });
                current = &current.entries.getPtr(trimmed).?.table;
            }
        }

        return current;
    }

    fn parseKey(self: *Parser) ParseError![]const u8 {
        const start = self.pos;
        while (self.pos < self.input.len and (isKeyChar(self.input[self.pos]) or self.input[self.pos] == '_' or self.input[self.pos] == '-')) {
            self.pos += 1;
        }
        const key = self.input[start..self.pos];
        return self.allocator.dupe(u8, key) catch return error.OutOfMemory;
    }

    fn parseValue(self: *Parser) ParseError!Value {
        if (self.pos >= self.input.len) return error.UnexpectedCharacter;

        const c = self.input[self.pos];

        if (c == '"') {
            return .{ .string = try self.parseString() };
        } else if (c == 't' or c == 'f') {
            return .{ .boolean = try self.parseBoolean() };
        } else if (std.ascii.isDigit(c) or c == '-' or c == '+') {
            return .{ .integer = try self.parseInteger() };
        } else {
            return error.UnexpectedCharacter;
        }
    }

    fn parseString(self: *Parser) ParseError![]const u8 {
        std.debug.assert(self.input[self.pos] == '"');
        self.pos += 1;

        const start = self.pos;
        while (self.pos < self.input.len and self.input[self.pos] != '"' and self.input[self.pos] != '\n') {
            if (self.input[self.pos] == '\\' and self.pos + 1 < self.input.len) {
                self.pos += 2; // Skip escape sequence
            } else {
                self.pos += 1;
            }
        }

        if (self.pos >= self.input.len or self.input[self.pos] != '"') {
            return error.UnterminatedString;
        }

        const str = self.input[start..self.pos];
        self.pos += 1; // Skip closing '"'

        return self.allocator.dupe(u8, str) catch return error.OutOfMemory;
    }

    fn parseInteger(self: *Parser) ParseError!i64 {
        const start = self.pos;

        // Handle sign
        if (self.pos < self.input.len and (self.input[self.pos] == '-' or self.input[self.pos] == '+')) {
            self.pos += 1;
        }

        // Parse digits
        while (self.pos < self.input.len and (std.ascii.isDigit(self.input[self.pos]) or self.input[self.pos] == '_')) {
            self.pos += 1;
        }

        const num_str = self.input[start..self.pos];
        // Remove underscores for parsing
        var clean: std.ArrayListUnmanaged(u8) = .empty;
        defer clean.deinit(self.allocator);
        for (num_str) |ch| {
            if (ch != '_') {
                clean.append(self.allocator, ch) catch return error.OutOfMemory;
            }
        }

        return std.fmt.parseInt(i64, clean.items, 10) catch return error.InvalidNumber;
    }

    fn parseBoolean(self: *Parser) ParseError!bool {
        if (self.pos + 4 <= self.input.len and std.mem.eql(u8, self.input[self.pos .. self.pos + 4], "true")) {
            self.pos += 4;
            return true;
        } else if (self.pos + 5 <= self.input.len and std.mem.eql(u8, self.input[self.pos .. self.pos + 5], "false")) {
            self.pos += 5;
            return false;
        }
        return error.InvalidBoolean;
    }

    fn skipWhitespace(self: *Parser) void {
        while (self.pos < self.input.len) {
            const c = self.input[self.pos];
            if (c == ' ' or c == '\t') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn skipWhitespaceAndComments(self: *Parser) void {
        while (self.pos < self.input.len) {
            const c = self.input[self.pos];
            if (c == ' ' or c == '\t' or c == '\r') {
                self.pos += 1;
            } else if (c == '#') {
                // Skip to end of line
                while (self.pos < self.input.len and self.input[self.pos] != '\n') {
                    self.pos += 1;
                }
            } else if (c == '\n') {
                self.pos += 1;
                self.line += 1;
            } else {
                break;
            }
        }
    }

    fn isKeyChar(c: u8) bool {
        return std.ascii.isAlphanumeric(c) or c == '_' or c == '-';
    }
};

/// Parse a TOML string into a Table
pub fn parse(allocator: Allocator, input: []const u8) ParseError!Table {
    var parser = Parser.init(allocator, input);
    return parser.parse();
}

/// Parse a TOML file into a Table
pub fn parseFile(allocator: Allocator, path: []const u8) !Table {
    const file = try @import("stdx").fs.openFile(path, .{});
    defer @import("stdx").fs.closeFile(file);

    const content = try file.readToEndAlloc(allocator, 1024 * 1024); // 1MB max
    defer allocator.free(content);

    return parse(allocator, content);
}

// Tests
test "parse simple key-value" {
    const allocator = std.testing.allocator;
    const input = "port = 9000\n";

    var table = try parse(allocator, input);
    defer table.deinit();

    try std.testing.expectEqual(@as(i64, 9000), table.getInt("port").?);
}

test "parse string value" {
    const allocator = std.testing.allocator;
    const input =
        \\name = "flo"
    ;

    var table = try parse(allocator, input);
    defer table.deinit();

    try std.testing.expectEqualStrings("flo", table.getString("name").?);
}

test "parse boolean values" {
    const allocator = std.testing.allocator;
    const input =
        \\enabled = true
        \\disabled = false
    ;

    var table = try parse(allocator, input);
    defer table.deinit();

    try std.testing.expectEqual(true, table.getBool("enabled").?);
    try std.testing.expectEqual(false, table.getBool("disabled").?);
}

test "parse table section" {
    const allocator = std.testing.allocator;
    const input =
        \\[server]
        \\port = 9000
        \\bind = "0.0.0.0"
    ;

    var table = try parse(allocator, input);
    defer table.deinit();

    const server = table.getTable("server").?;
    try std.testing.expectEqual(@as(i64, 9000), server.getInt("port").?);
    try std.testing.expectEqualStrings("0.0.0.0", server.getString("bind").?);
}

test "parse with comments" {
    const allocator = std.testing.allocator;
    const input =
        \\# This is a comment
        \\port = 9000  # inline comment
        \\
        \\# Another comment
        \\name = "test"
    ;

    var table = try parse(allocator, input);
    defer table.deinit();

    try std.testing.expectEqual(@as(i64, 9000), table.getInt("port").?);
    try std.testing.expectEqualStrings("test", table.getString("name").?);
}

test "parse complete server config" {
    const allocator = std.testing.allocator;
    const input =
        \\[server]
        \\port = 9000
        \\bind = "0.0.0.0"
        \\data_dir = "./data"
        \\threads = 4
        \\
        \\[storage]
        \\hot_window_seconds = 90
        \\memtable_size_mb = 64
        \\
        \\[logging]
        \\level = "info"
    ;

    var table = try parse(allocator, input);
    defer table.deinit();

    const server = table.getTable("server").?;
    try std.testing.expectEqual(@as(i64, 9000), server.getInt("port").?);
    try std.testing.expectEqualStrings("0.0.0.0", server.getString("bind").?);
    try std.testing.expectEqualStrings("./data", server.getString("data_dir").?);
    try std.testing.expectEqual(@as(i64, 4), server.getInt("threads").?);

    const storage = table.getTable("storage").?;
    try std.testing.expectEqual(@as(i64, 90), storage.getInt("hot_window_seconds").?);
    try std.testing.expectEqual(@as(i64, 64), storage.getInt("memtable_size_mb").?);

    const logging = table.getTable("logging").?;
    try std.testing.expectEqualStrings("info", logging.getString("level").?);
}
