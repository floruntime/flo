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
