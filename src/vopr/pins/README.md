# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not.

| Pin | Expected today | Why it exists |
|---|---|---|
| `eviction-wedge.json` | FAILS (convergence, ~165k ticks) | A core follower's gap falls below the leader's 8 KiB ring, `getRange` returns 0, and the pump heartbeats forever — there is no snapshot/catch-up path. Passes once one exists. |
| `stale-suffix-commit.json` | FAILS (state-machine safety, 798 ticks) | A follower caps `commit_index` at its own `lastIndex()` instead of the last entry the RPC delivered, so a deposed leader's uncommitted suffix gets committed and applied on an empty heartbeat. Passes once the follower commits only what the leader confirmed. |
| `truncate-evict-hang.json` | HANGS (watchdog kill; never reaches tick 10k) | After `truncateAfter` a conflict, the ring's counters and cursors disagree; `evictOldest` unmaps live entries via stale headers and zeroes `entry_count` with bytes still resident, so `append`'s evict loop spins forever. Passes (converges or fails honestly) once truncation reclaims ring space correctly. |
