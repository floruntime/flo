//! Flo Processing WASM Demo: Transaction Classifier
//!
//! A streaming operator compiled to WebAssembly that classifies
//! financial transactions by amount and tags them for sink routing.
//!
//! Demonstrates the **Processing WASM Contract**:
//!
//! ## Guest Exports (required)
//!
//!   - `handle(input_ptr: u32, input_len: u32) -> i64`
//!     Process one record. Returns packed (output_ptr << 32) | output_len.
//!   - `alloc(size: u32) -> u32`
//!   - `dealloc(ptr: u32, size: u32) -> void`
//!
//! ## Host Imports (provided by Flo)
//!
//!   - `flo.set_tag(name_ptr: u32, name_len: u32) -> i32`
//!     Tag the output record for match-based sink routing.
//!     Returns 0 on success, -1 if the tag name is unknown.
//!
//!   - `flo.kv_get(key_ptr, key_len, buf_ptr, buf_len) -> i32`
//!     Read a value from the Flo KV store.
//!
//!   - `flo.kv_set(key_ptr, key_len, val_ptr, val_len) -> i32`
//!     Write a value to the Flo KV store.
//!
//!   - `flo.log(level: u32, msg_ptr: u32, msg_len: u32) -> void`
//!     Emit a log message to the Flo server log.
//!
//! ## Classification Rules
//!
//!   - amount >= 10000  → tag "high-value"
//!   - amount < 0       → tag "refund"
//!   - otherwise        → tag "standard"
//!
//! ## Example
//!
//! Input:  {"txn_id": "T100", "amount": 15000, "merchant": "ACME"}
//! Output: {"txn_id": "T100", "amount": 15000, "merchant": "ACME", "class": "high-value"}
//! Tags:   high-value (set via flo.set_tag)
//!
//! ## Pipeline YAML
//!
//! ```yaml
//! kind: Processing
//! name: txn-pipeline
//! sources:
//!   - stream:
//!       name: raw-transactions
//! operators:
//!   - type: wasm
//!     name: classify-txn
//!     module: ./txn_classifier.wasm
//! sinks:
//!   - name: all-txns
//!     stream:
//!       name: classified-txns
//!   - name: high-value-txns
//!     stream:
//!       name: high-value-out
//!     match: [high-value]
//!   - name: refund-txns
//!     stream:
//!       name: refunds-out
//!     match: [refund]
//! ```
//!
//! ## Build
//!
//!   zig build-lib -target wasm32-freestanding -O ReleaseSmall txn_classifier.zig
//!
//! Or use the build.zig:
//!   cd examples/processing/txn-classifier && zig build

// =============================================================================
// Host imports — provided by Flo runtime
// =============================================================================

/// Tag the current output record. The tag name is resolved by the pipeline's
/// TagRegistry. Returns 0 on success, or a negative error code.
extern "flo" fn set_tag(name_ptr: [*]const u8, name_len: u32) i32;

/// Log a message to the Flo server log.
/// Levels: 0=debug, 1=info, 2=warn, 3=error.
extern "flo" fn log(level: u32, msg_ptr: [*]const u8, msg_len: u32) void;

// =============================================================================
// Heap — Bump allocator for guest memory
// =============================================================================

var heap: [65536]u8 = undefined;
var heap_offset: usize = 0;

export fn alloc(size: u32) u32 {
    const s: usize = @intCast(size);
    const aligned = (heap_offset + 7) & ~@as(usize, 7);
    if (aligned + s > heap.len) return 0;
    const ptr: [*]u8 = @ptrCast(&heap[aligned]);
    heap_offset = aligned + s;
    return @intFromPtr(ptr);
}

export fn dealloc(_: u32, _: u32) void {}

fn resetHeap() void {
    heap_offset = 0;
}

// =============================================================================
// Output buffer
// =============================================================================

var output_buf: [4096]u8 = undefined;

// =============================================================================
// handle — Main entry point
// =============================================================================

/// Classify a transaction by amount and tag the output record.
///
/// Input JSON:  {"txn_id": "<id>", "amount": <number>, "merchant": "<name>"}
/// Output JSON: input fields + "class": "<classification>"
///
/// Side effect: calls flo.set_tag() with the classification name.
///
/// Returns: packed i64 = (output_ptr << 32) | output_len
///          -1 = invalid input
///          -2 = alloc failed
export fn handle(input_ptr: [*]const u8, input_len: u32) i64 {
    resetHeap();

    const input = input_ptr[0..input_len];

    // Parse amount field
    const amount = parseIntField(input, "amount") orelse return -1;

    // Classify
    const class: []const u8 = if (amount >= 10000)
        "high-value"
    else if (amount < 0)
        "refund"
    else
        "standard";

    // Tag the output record via host function
    _ = set_tag(class.ptr, @intCast(class.len));

    // Build output: copy input JSON but insert "class" field before closing }
    const output_len = buildOutput(&output_buf, input, class);
    if (output_len == 0) return -2;

    // Copy to guest-allocated memory
    const out_ptr = alloc(@intCast(output_len));
    if (out_ptr == 0) return -2;

    const dest: [*]u8 = @ptrFromInt(out_ptr);
    for (0..output_len) |i| {
        dest[i] = output_buf[i];
    }

    return (@as(i64, out_ptr) << 32) | @as(i64, @intCast(output_len));
}

// =============================================================================
// init — Optional initialization
// =============================================================================

export fn init() i32 {
    resetHeap();
    const msg = "txn-classifier initialized";
    log(1, msg.ptr, msg.len);
    return 0;
}

// =============================================================================
// Output formatting
// =============================================================================

/// Build output JSON: insert ,"class":"<class>" before the closing }.
/// Returns bytes written, or 0 on failure.
fn buildOutput(buf: []u8, input: []const u8, class: []const u8) usize {
    // Find the last '}' in the input
    var close_brace: usize = input.len;
    while (close_brace > 0) {
        close_brace -= 1;
        if (input[close_brace] == '}') break;
    }
    if (close_brace == 0 and input[0] != '}') return 0;

    // Copy everything before the closing brace
    const prefix_len = close_brace;
    const suffix = ",\"class\":\"";
    const total = prefix_len + suffix.len + class.len + 2; // +2 for "}
    if (total > buf.len) return 0;

    var pos: usize = 0;

    // Copy input prefix (everything before })
    for (0..prefix_len) |i| {
        buf[pos] = input[i];
        pos += 1;
    }

    // Append ,"class":"<class>"}
    copySlice(buf[pos..], suffix);
    pos += suffix.len;

    copySlice(buf[pos..], class);
    pos += class.len;

    buf[pos] = '"';
    pos += 1;
    buf[pos] = '}';
    pos += 1;

    return pos;
}

// =============================================================================
// Minimal JSON parsing (pure freestanding — no std)
// =============================================================================

fn parseIntField(json: []const u8, field_name: []const u8) ?i64 {
    const key_start = findKey(json, field_name) orelse return null;
    var i = key_start;
    while (i < json.len and json[i] != ':') : (i += 1) {}
    if (i >= json.len) return null;
    i += 1;
    while (i < json.len and isWhitespace(json[i])) : (i += 1) {}

    var negative = false;
    if (i < json.len and json[i] == '-') {
        negative = true;
        i += 1;
    }

    var value: i64 = 0;
    var found_digit = false;
    while (i < json.len and json[i] >= '0' and json[i] <= '9') {
        value = value * 10 + @as(i64, json[i] - '0');
        found_digit = true;
        i += 1;
    }

    if (!found_digit) return null;
    return if (negative) -value else value;
}

fn findKey(json: []const u8, key: []const u8) ?usize {
    var i: usize = 0;
    while (i + key.len + 2 <= json.len) {
        if (json[i] == '"') {
            const name_start = i + 1;
            const name_end = name_start + key.len;
            if (name_end < json.len and
                json[name_end] == '"' and
                sliceEql(json[name_start..name_end], key))
            {
                return name_end + 1;
            }
        }
        i += 1;
    }
    return null;
}

fn sliceEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (ca != cb) return false;
    }
    return true;
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

fn copySlice(dest: []u8, src: []const u8) void {
    for (0..src.len) |i| {
        dest[i] = src[i];
    }
}
