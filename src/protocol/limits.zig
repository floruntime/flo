//! Limits the wire, the log and the server share. They live here so the
//! protocol module, which imports nothing from the server, can check
//! schemas against them, and the server reads the same values.

/// The most a log entry's command payload holds: its header, key and value.
pub const MAX_PERSIST_PAYLOAD: usize = 65536;

/// A log entry's command header: namespace id u32, key length u16, value
/// length u32.
pub const COMMAND_PREFIX_SIZE: usize = 4 + 2 + 4;

/// The longest key, namespace-qualified, that a request or entry carries.
pub const MAX_QUALIFIED_KEY: usize = 4096;

/// A namespace's id as it leads a stored KV key: fixed width, so one
/// namespace's keys are one prefix.
pub const NAMESPACE_ID_BYTES: usize = 4;

/// The longest KV key as state and the log keep it: its namespace's id, then
/// the longest key a request carries, in any namespace.
pub const MAX_KV_KEY: usize = NAMESPACE_ID_BYTES + MAX_QUALIFIED_KEY;

/// The longest namespace name.
pub const MAX_NAMESPACE_NAME: usize = 63;

/// The most ops a KV transaction can hold, whatever the operator sets: what
/// the entry's format and the buffers are sized for.
pub const MAX_OPS_PER_TXN: u16 = 256;

/// The most ops one KV transaction buffers unless the operator says otherwise
/// (`[kv] max_txn_ops`). A full transaction is one log entry, and the shard
/// does nothing else while it applies one: 85-88 µs at this many small ops,
/// 324-336 µs at the built-in most (`zig build bench`: ReleaseSafe,
/// on a shared Apple-silicon Mac under load).
pub const DEFAULT_MAX_OPS_PER_TXN: u16 = 64;

/// The most key and value bytes a KV transaction can hold, whatever the
/// operator sets, summed over its ops: what the entry's format and the buffers
/// are sized for. A transaction gathers them over many requests, so its commit
/// entry is several times the request frame.
pub const MAX_PAYLOAD_PER_TXN: usize = 1024 * 1024;

/// The most key and value bytes one KV transaction buffers unless the operator
/// says otherwise (`[kv] max_txn_bytes`). Large enough for the largest value a
/// plain set takes, with the longest key (`kv/txn.zig` checks it). A full
/// transaction applies in 126-139 µs at this and the default ops, 270-330 µs
/// at the default ops and 1 MiB, and 529-577 µs at both built-in mosts
/// (`zig build bench`: ReleaseSafe, on a shared Apple-silicon Mac
/// under load).
pub const DEFAULT_MAX_BYTES_PER_TXN: usize = 256 * 1024;

/// The largest log entry payload any family proposes: a full KV
/// transaction (`kv_entries.MAX_PAYLOAD`, which checks it equals this). The
/// Raft ring, the apply buffer and a replication frame must each hold one,
/// or an entry could be accepted and then never applied or sent to a
/// follower. It is apart from the request frame, which is a fraction of it.
pub const MAX_LOG_ENTRY_PAYLOAD: usize = 1_057_798;
