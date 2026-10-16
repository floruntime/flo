# Pinned scenarios

Each file is a `Scenario` as written by `writeJson`, replayable exactly with
`vopr --scenario-in=<file>`. A pin survives changes to `fromSeed`'s sampling,
which a bare seed does not. Every pin fails today, on a bug whose fix has not
landed; CI checks that it still does.

| Pin | Expected today | Why it exists |
|---|---|---|
| (none) | | |

A pin is re-recorded from a fresh swarm when an unrelated fix shifts the interleaving
enough that its seed no longer reaches the bug (`eviction-wedge.json` moved from seed
10 to small-ring seed 1 when the node started drawing its own timer jitter); a row
describes the bug, not the seed.

A pin whose fix lands goes green; it is then re-recorded as a regression seed
in `../seeds/`, which CI expects to pass.
