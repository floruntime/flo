//! Types shared with the layers below Raft (persistence, protocol results)
//! without importing the node.

/// What `RaftNode.propose` handed back: where the entry sits in the log,
/// under which term, and the header timestamp the caller chose.
pub const ProposeResult = struct {
    index: u64,
    term: u64,
    /// The entry's header timestamp, for a responder that answers from it.
    timestamp_ns: u64,
};
