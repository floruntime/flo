//! Limits the wire, the log and the server share. They live here so the
//! protocol module, which imports nothing from the server, can check
//! schemas against them, and the server reads the same values.

/// The most a log entry's command payload holds: its header, key and value.
pub const MAX_PERSIST_PAYLOAD: usize = 65536;

/// A log entry's command header: namespace hash u32, key length u16, value
/// length u32.
pub const COMMAND_PREFIX_SIZE: usize = 4 + 2 + 4;

/// The longest key, namespace-qualified, that a request or entry carries.
pub const MAX_QUALIFIED_KEY: usize = 4096;

/// The longest namespace name.
pub const MAX_NAMESPACE_NAME: usize = 63;

/// The most ops one KV transaction buffers.
pub const MAX_OPS_PER_TXN: u16 = 256;

/// The most key and value bytes one KV transaction buffers, summed over its
/// ops. A transaction gathers them over many requests, so its commit entry
/// is several times the request frame.
pub const MAX_PAYLOAD_PER_TXN: usize = 1024 * 1024;

/// The largest log entry payload any family proposes: a full KV
/// transaction (`kv_entries.MAX_PAYLOAD`, which checks it equals this). The
/// Raft ring, the apply buffer and a replication frame must each hold one,
/// or an entry could be accepted and then never applied or sent to a
/// follower. It is apart from the request frame, which is a fraction of it.
pub const MAX_LOG_ENTRY_PAYLOAD: usize = 1_055_234;
