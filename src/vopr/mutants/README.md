# Mutant scenarios

Each row plants one `families.Mutant` into a pinned scenario. The run must
fail with the invariant named, saying the text given; CI checks that it
does, and that the scenario passes with no mutant. A row that goes green
means the check stopped catching its bug.

| Scenario | Mutant | Fails with | Says |
|---|---|---|---|
| `actions.json` | `deliver_ignores_due` | `action_rule` | `a run was delivered before it was due` |
| `actions.json` | `expire_ignores_deadline` | `action_rule` | `an expire ended a lease before its deadline` |
| `actions.json` | `expire_matches_delivery` | `action_rule` | `an expire ended a lease none of its items named the deadline of` |
| `actions.json` | `settle_ignores_token` | `action_rule` | `a settle that names no live delivery completed a run` |
| `actions.json` | `release_counts_nothing` | `action_rule` | `a run's failures don't count the deliveries that failed` |
| `actions.json` | `settle_keeps_request` | `action_rule` | `a request record names a delivery that isn't live` |
| `actions.json` | `action_node_clock` | `state_checksum` | `action.` |
| `txn-limits.json` | `kv_if_absent_holds` | `applied_integrity` | `decided a put's condition` |
| `txn-limits.json` | `kv_batch_conditions_ignored` | `applied_integrity` | `a condition that can't hold` |
| `txn-limits.json` | `kv_refuses_over_own_limit` | `state_checksum` | `kv.value` |
| `txn-limits.json` | `kv_refuses_large` | `applied_integrity` | `none of whose ops can be refused` |
| `txn-limits.json` | `kv_stops_batch_short` | `applied_integrity` | `without reaching its last op` |
| `txn-limits.json` | `leader_ignores_limit` | `txn_gate` | `a leader proposed a batch above its own transaction limit` |
| `catching-up.json` | `ns_ungated` | `ns_stale_id` | `was refused for an id another create took` |
| `catching-up.json` | `ns_ready_on_commit` | `ns_stale_id` | `was refused for an id another create took` |
| `catching-up-alloc.json` | `ns_keeps_alloc` | `ns_dense_id` | `took an id past the next free one` |

Mutants with no row, so CI's check that every mutant is accounted for
passes:

- `extend_ignores_held_since`: a hold reaches its 12 h ceiling in no
  simulated run, so `families.zig`'s tests catch it on one replica.
- `node_clock`, `local_refusal`, `map_order`, `live_only_effect`,
  `kv_replay_skips_batches`, `kv_refused_commits`, `kv_condition_local`: `simulator.zig`'s tests
  plant them in fixed runs.

A scenario is re-recorded from a fresh swarm (`vopr --iterations=N --actions
--family-mutant=M`, `--catching-up` for the namespace mutants, or
`--txn-limits` for the limit mutants) when a
change to the workload stops it reaching a bug.
