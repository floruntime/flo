# Regression seeds

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. Every seed here passes today, and reaches a bug
whose fix landed: undo the fix and the seed fails. CI runs them all and
expects them green; one failing is a regression. Known-failing scenarios live
in `../pins/`.

Each seed was recorded by undoing its fix locally, swarming until a seed
failed, and checking the same seed passes with the fix in. The row names the
bug, not the seed.

**A seed, or a pin, holds only on the random stream it was recorded on.** Every
message a node sends draws from that stream, so any change to what Raft sends,
or when, moves every scenario. A change like that re-checks each seed with its
fix undone (and each pin as still failing), and re-records any that no longer
reach their bug.

| Seed | Fails when | What it guards |
|---|---|---|
| `eviction-wedge.json` (small ring) | the Raft log has no catch-up source below its ring | A follower whose gap fell below the leader's ring: `getRange` returned 0 and the leader sent it empty appends forever. Fixed by letting the log read below the ring from the durable log (the sim disk here). Undone, the run hangs: every append answer prompts another empty append. |
| `stale-suffix-commit.json` | a follower caps `commit_index` at its own `lastIndex()` instead of the last entry the RPC verified | A heartbeat committed a deposed leader's stale suffix. Undone: `state_machine_safety`. |
| `stale-ack-chains.json` (`--byzantine` seed 67) | a leader frees a peer for its next batch on any ack, not only the one for its latest batch | Each late or duplicated ack started another stream of batches to the peer, and the streams never merged while it lagged; duplicated messages grew them until the run hung. Undone, the run hangs. |
| `stale-member-wedge.json` (seed 19), `stale-member-wedge-small-ring.json` (small-ring seed 73), `stale-member-wedge-membership.json` (`--membership` seed 4) | a member takes no committed config on another member's word (#278) | A member that missed the config admitting the current leader dropped that leader's frames as a stranger's and never caught up; where its vote was needed, no leader was elected. Undone (asks still sent, answers dropped), each ends in convergence failure. |
| `truncate-evict-hang.json` (small ring) | `truncateAfter` leaves the ring's write cursor where it was | Stale headers stayed in the byte stream `evictOldest` walks; it unmapped live entries and `append`'s evict loop could never make room. Undone, eviction's assertion fails in `append`. |

The same cases also live on as unit tests in `src/raft/log.zig`,
`src/raft/node.zig` and `src/storage/ual/ual.zig`.

The pins #278 had while it was open weren't moved here: the fix's own
messages shift the simulator's random stream, and on the new stream those
seeds no longer reach the bug even with the fix undone. A seed guards a fix
only if it fails without it on the stream the fix runs on, so the seeds above
were recorded that way.
