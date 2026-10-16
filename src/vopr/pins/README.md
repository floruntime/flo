# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not. Every pin fails today, on a bug whose fix has not
landed; CI checks that it still does.

| Pin | Expected today | Why it exists |
|---|---|---|
| `stale-member-wedge.json` (seed 4) | fails: convergence, no leader | A member that missed the config admitting the current leader drops that leader's frames as a stranger's, so it never catches up. Here the leader needs its vote, so no leader is ever elected. The swarm's seed 4, skipped in CI's swarm until the fix lands ([#278](https://github.com/floruntime/flo/issues/278)). |
| `stale-member-wedge-wipes.json` (`--wipes` seed 3) | fails: convergence | The same bug under lost disks ([#278](https://github.com/floruntime/flo/issues/278)). The `--wipes` swarm's seed 3, skipped in CI likewise. |

A pin is re-recorded from a fresh swarm when an unrelated fix shifts the interleaving
enough that its seed no longer reaches the bug (`eviction-wedge.json` moved from seed
10 to small-ring seed 1 when the node started drawing its own timer jitter); a row
describes the bug, not the seed.

A pin whose fix lands goes green; it is then re-recorded as a regression seed
in `../seeds/`, which CI expects to pass.
