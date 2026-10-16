# Regression seeds

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. Every seed here passes today, and reaches a bug
whose fix landed: undo the fix and the seed fails. CI runs them all and
expects them green; one failing is a regression. Known-failing scenarios live
in `../pins/`.

Each seed was recorded by undoing its fix locally, swarming until a seed
failed, and checking the same seed passes with the fix in. A seed that drifts
off its bug after an unrelated change (the interleaving shifts) is re-recorded
the same way; the row names the bug, not the seed.

| Seed | Fails when | What it guards |
|---|---|---|
| `eviction-wedge.json` (small ring) | the Raft log has no catch-up source below its ring | A follower whose gap fell below the leader's ring: `getRange` returned 0 and the leader sent it empty appends forever. Fixed by letting the log read below the ring from the durable log (the sim disk here). Undone, the run hangs: every append answer prompts another empty append. |
| `stale-suffix-commit.json` | a follower caps `commit_index` at its own `lastIndex()` instead of the last entry the RPC verified | A heartbeat committed a deposed leader's stale suffix. Undone: `state_machine_safety` at tick 9935. |
| `truncate-evict-hang.json` (small ring) | `truncateAfter` leaves the ring's write cursor where it was | Stale headers stayed in the byte stream `evictOldest` walks; it unmapped live entries and `append`'s evict loop could never make room. Undone, eviction's assertion fails in `append`. |

The same cases also live on as unit tests in `src/raft/log.zig`,
`src/raft/node.zig` and `src/storage/ual/ual.zig`.
