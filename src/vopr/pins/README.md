# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not. Every pin fails today, on a bug whose fix has not
landed; CI checks that it still does.

| Pin | Expected today | Why it exists |
|---|---|---|
| `stale-member-wedge.json` (seed 73) | fails: convergence | A member that falls behind across a membership change drops the new leader's frames as a stranger's and never catches up ([#278](https://github.com/floruntime/flo/issues/278)). |
| `stale-member-wedge-membership.json` (`--membership` seed 19) | fails: convergence | The same bug where the membership changes often ([#278](https://github.com/floruntime/flo/issues/278)). |

A pin is re-recorded from a fresh swarm when an unrelated fix shifts the interleaving
enough that its seed no longer reaches the bug (`eviction-wedge.json` moved from seed
10 to small-ring seed 1 when the node started drawing its own timer jitter); a row
describes the bug, not the seed.

A pin whose fix lands goes green; it is then re-recorded as a regression seed
in `../seeds/`, which CI expects to pass.
