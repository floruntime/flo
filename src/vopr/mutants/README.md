# Mutant scenarios

Each row plants one `families.Mutant` into a pinned scenario. The run must
fail with the invariant named; CI checks that it does, and that the
scenario passes with no mutant. A row that goes green means the check
stopped catching its bug.

| Scenario | Mutant | Fails with |
|---|---|---|
| `actions.json` | `deliver_ignores_due` | `action_rule` |
| `actions.json` | `expire_ignores_deadline` | `action_rule` |
| `actions.json` | `expire_matches_delivery` | `action_rule` |
| `actions.json` | `settle_ignores_token` | `action_rule` |
| `actions.json` | `release_counts_nothing` | `action_rule` |
| `actions.json` | `settle_keeps_request` | `action_rule` |
| `actions.json` | `action_node_clock` | `state_checksum` |

`extend_ignores_held_since` has no row: a hold reaches its 12 h ceiling in
no simulated run, so `families.zig`'s tests catch it on one replica.

A scenario is re-recorded from a fresh swarm (`vopr --iterations=N --actions
--family-mutant=M`) when a change to the workload stops it reaching a bug.
