# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not.

| Pin | Expected today | Why it exists |
|---|---|---|
| `eviction-wedge.json` | FAILS (convergence, ~165k ticks) | A core follower's gap falls below the leader's 8 KiB ring, `getRange` returns 0, and the pump heartbeats forever — there is no snapshot/catch-up path. Passes once one exists. |
| `truncate-evict-hang.json` | HANGS (watchdog kill; never reaches tick 10k) | After `truncateAfter` a conflict, the ring's counters and cursors disagree; `evictOldest` unmaps live entries via stale headers and zeroes `entry_count` with bytes still resident, so `append`'s evict loop spins forever. Passes (converges or fails honestly) once truncation reclaims ring space correctly. |

A pin is re-recorded from a fresh swarm when an unrelated fix shifts the interleaving
enough that its seed no longer reaches the bug (`truncate-evict-hang.json` moved from seed
2672 to 7090 this way); the row above describes the bug, not the seed.

Retired pins (fix landed, pin went green): `stale-suffix-commit.json` — a follower
capped `commit_index` at its own `lastIndex()` instead of the last entry the RPC
delivered, committing a deposed leader's stale suffix on a heartbeat. Fixed by making
the follower commit only the verified prefix; the case lives on as a unit test in
`src/raft/node.zig`.
