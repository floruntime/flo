# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not.

| Pin | Expected today | Why it exists |
|---|---|---|
| `eviction-wedge.json` | FAILS (convergence, ~109k ticks) | A core follower's gap falls below the leader's 8 KiB ring, `getRange` returns 0, and the pump heartbeats forever — there is no snapshot/catch-up path. Passes once one exists. |

A pin is re-recorded from a fresh swarm when an unrelated fix shifts the interleaving
enough that its seed no longer reaches the bug (`eviction-wedge.json` moved from seed
10 to small-ring seed 1 when the node started drawing its own timer jitter); the row
above describes the bug, not the seed.

Retired pins (fix landed, pin went green):

- `stale-suffix-commit.json` — a follower capped `commit_index` at its own
  `lastIndex()` instead of the last entry the RPC delivered, committing a deposed
  leader's stale suffix on a heartbeat. Fixed by making the follower commit only the
  verified prefix; the case lives on as a unit test in `src/raft/node.zig`.
- `truncate-evict-hang.json` — after `truncateAfter` a conflict, the ring's counters
  and cursors disagreed; `evictOldest` unmapped live entries via stale headers and
  zeroed `entry_count` with bytes still resident, so `append`'s evict loop spun
  forever. Fixed by rewinding the ring's write cursor on truncation; the interleaving
  lives on as unit tests in `src/storage/ual/ual.zig` and `src/raft/log.zig`.
