//! Flo Processing WASM Demo: Transaction Enricher
//!
//! Demonstrates three operator modes introduced with the extended
//! Processing WASM Contract:
//!
//!   - **Filter** — handle() returns 0 to drop records with missing fields.
//!   - **FlatMap** — flo.emit() called multiple times to split a record.
//!   - **State** — flo.state_get / flo.state_set to count per-merchant txns.
//!
//! ## Classification Rules
//!
//!   - Missing "amount" field → FILTER (drop record, return 0).
//!   - amount >= 10000 → FLATMAP: emit the enriched record AND an "alert"
//!     record, tag both "high-value".
//!   - otherwise → MAP: emit single enriched record, tag "standard".
//!
//! In all emitted cases the output includes a "merchant_txn_count" field
//! tracked via flo.state_get / flo.state_set.
//!
//! ## Build
//!
//!   zig build-lib -target wasm32-freestanding -O ReleaseSmall txn_enricher.zig

// =============================================================================
// Host imports — provided by Flo runtime
// =============================================================================

extern "flo" fn set_tag(name_ptr: [*]const u8, name_len: u32) i32;
extern "flo" fn emit(ptr: [*]const u8, len: u32) i32;
extern "flo" fn state_get(key_ptr: [*]const u8, key_len: u32, buf_ptr: [*]u8, buf_len: u32) i32;
extern "flo" fn state_set(key_ptr: [*]const u8, key_len: u32, val_ptr: [*]const u8, val_len: u32) i32;
extern "flo" fn log(level: u32, msg_ptr: [*]const u8, msg_len: u32) void;

// =============================================================================
// Heap — bump allocator
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
// Scratch buffers
// =============================================================================

var output_buf: [4096]u8 = undefined;
var state_buf: [64]u8 = undefined;

// =============================================================================
// handle — Main entry point
// =============================================================================

export fn handle(input_ptr: [*]const u8, input_len: u32) i64 {
    resetHeap();
    const input = input_ptr[0..input_len];

    // --- Filter: drop records with no "amount" field ---
    const amount = parseIntField(input, "amount") orelse return 0; // FILTER

    // --- State: read + increment merchant txn counter ---
    const merchant = parseStringField(input, "merchant") orelse "unknown";
    const count = readAndIncrementCounter(merchant);

    // --- Classify ---
    if (amount >= 10000) {
        // FLATMAP: emit enriched record + alert record
        _ = set_tag("high-value".ptr, "high-value".len);

        // Emit #1: enriched record
        const len1 = buildEnrichedOutput(&output_buf, input, "high-value", count);
        if (len1 > 0) _ = emit(output_buf[0..len1].ptr, @intCast(len1));

        // Emit #2: alert
        const len2 = buildAlert(&output_buf, input, amount, count);
        if (len2 > 0) _ = emit(output_buf[0..len2].ptr, @intCast(len2));

        // Return positive (non-zero, non-error) to indicate success.
        // Since flo.emit() was called, the return value is ignored for output.
        return 1;
    }

    // --- MAP: single enriched output ---
    _ = set_tag("standard".ptr, "standard".len);

    const out_len = buildEnrichedOutput(&output_buf, input, "standard", count);
    if (out_len == 0) return -2;

    const out_ptr = alloc(@intCast(out_len));
    if (out_ptr == 0) return -2;

    const dest: [*]u8 = @ptrFromInt(out_ptr);
    for (0..out_len) |i| {
        dest[i] = output_buf[i];
    }

    return (@as(i64, out_ptr) << 32) | @as(i64, @intCast(out_len));
}

export fn init() i32 {
    resetHeap();
    const msg = "txn-enricher initialized";
    log(1, msg.ptr, msg.len);
    return 0;
}

// =============================================================================
// State helpers
// =============================================================================

fn readAndIncrementCounter(merchant: []const u8) u32 {
    // Read current count from per-operator state
    var current: u32 = 0;
    const rc = state_get(merchant.ptr, @intCast(merchant.len), &state_buf, state_buf.len);
    if (rc > 0) {
        current = parseU32(state_buf[0..@intCast(rc)]);
    }

    current += 1;

    // Write updated count back to state
    const written = writeU32(&state_buf, current);
    _ = state_set(merchant.ptr, @intCast(merchant.len), state_buf[0..written].ptr, @intCast(written));

    return current;
}

fn parseU32(buf: []const u8) u32 {
    var val: u32 = 0;
    for (buf) |c| {
        if (c >= '0' and c <= '9') {
            val = val * 10 + @as(u32, c - '0');
        }
    }
    return val;
}

fn writeU32(buf: []u8, val: u32) usize {
    if (val == 0) {
        buf[0] = '0';
        return 1;
    }
    var v = val;
    var i: usize = 0;
    while (v > 0) : (i += 1) {
        buf[i] = @intCast((v % 10) + '0');
        v /= 10;
    }
    // Reverse
    var left: usize = 0;
    var right: usize = i - 1;
    while (left < right) {
        const tmp = buf[left];
        buf[left] = buf[right];
        buf[right] = tmp;
        left += 1;
        right -= 1;
    }
    return i;
}

// =============================================================================
// Output formatting
// =============================================================================

/// Build: {<input-fields>, "class": "<cls>", "merchant_txn_count": <cnt>}
fn buildEnrichedOutput(buf: []u8, input: []const u8, class: []const u8, count: u32) usize {
    // Find closing brace
    var close: usize = input.len;
    while (close > 0) {
        close -= 1;
        if (input[close] == '}') break;
    }
    if (close == 0 and input[0] != '}') return 0;

    var pos: usize = 0;
    // Copy everything before }
    if (pos + close > buf.len) return 0;
    copySlice(buf[pos..], input[0..close]);
    pos += close;

    // Append ,"class":"<cls>","merchant_txn_count":<count>}
    const f1 = ",\"class\":\"";
    const f2 = "\",\"merchant_txn_count\":";
    if (pos + f1.len + class.len + f2.len + 12 > buf.len) return 0;

    copySlice(buf[pos..], f1);
    pos += f1.len;
    copySlice(buf[pos..], class);
    pos += class.len;
    copySlice(buf[pos..], f2);
    pos += f2.len;

    var count_buf: [12]u8 = undefined;
    const count_len = writeU32(&count_buf, count);
    copySlice(buf[pos..], count_buf[0..count_len]);
    pos += count_len;

    buf[pos] = '}';
    pos += 1;

    return pos;
}

/// Build: {"alert":"high-value-txn","amount":<amt>,"merchant_txn_count":<cnt>}
fn buildAlert(buf: []u8, _: []const u8, amount: i64, count: u32) usize {
    const prefix = "{\"alert\":\"high-value-txn\",\"amount\":";
    var pos: usize = 0;

    if (prefix.len + 30 > buf.len) return 0;
    copySlice(buf[pos..], prefix);
    pos += prefix.len;

    // Write amount (may be negative)
    pos += writeI64(buf[pos..], amount);

    const f = ",\"merchant_txn_count\":";
    copySlice(buf[pos..], f);
    pos += f.len;

    var count_buf: [12]u8 = undefined;
    const count_len = writeU32(&count_buf, count);
    copySlice(buf[pos..], count_buf[0..count_len]);
    pos += count_len;

    buf[pos] = '}';
    pos += 1;

    return pos;
}

fn writeI64(buf: []u8, val: i64) usize {
    var pos: usize = 0;
    var v: u64 = undefined;
    if (val < 0) {
        buf[pos] = '-';
        pos += 1;
        v = @intCast(-val);
    } else {
        v = @intCast(val);
    }
    if (v == 0) {
        buf[pos] = '0';
        return pos + 1;
    }
    const start = pos;
    while (v > 0) : (pos += 1) {
        buf[pos] = @intCast((v % 10) + '0');
        v /= 10;
    }
    // Reverse digits
    var left = start;
    var right = pos - 1;
    while (left < right) {
        const tmp = buf[left];
        buf[left] = buf[right];
        buf[right] = tmp;
        left += 1;
        right -= 1;
    }
    return pos;
}

// =============================================================================
// Minimal JSON parsing (freestanding — no std)
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

fn parseStringField(json: []const u8, field_name: []const u8) ?[]const u8 {
    const key_start = findKey(json, field_name) orelse return null;
    var i = key_start;
    while (i < json.len and json[i] != ':') : (i += 1) {}
    if (i >= json.len) return null;
    i += 1;
    while (i < json.len and isWhitespace(json[i])) : (i += 1) {}
    if (i >= json.len or json[i] != '"') return null;
    i += 1; // skip opening quote
    const start = i;
    while (i < json.len and json[i] != '"') : (i += 1) {}
    if (i >= json.len) return null;
    return json[start..i];
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
