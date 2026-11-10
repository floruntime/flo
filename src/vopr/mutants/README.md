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
| `queues.json` | `queue_settle_ignores_token` | `queue_rule` | `a report that names no live delivery removed a message` |
| `queues.json` | `queue_release_removes` | `queue_rule` | `a fail removed its message` |
| `queues.json` | `queue_expire_ignores_deadline` | `queue_rule` | `an expire ended a lease before its deadline` |
| `queues.json` | `queue_deliver_wrong_order` | `queue_rule` | `a deliver didn't take the first due messages` |
| `queues.json` | `queue_deliver_short` | `queue_rule` | `a deliver took fewer messages than its count and budget allow` |
| `queues.json` | `queue_expire_first_only` | `queue_rule` | `an expire left a live lease it named past its deadline` |
| `queues.json` | `queue_report_changes_unnamed` | `queue_rule` | `a report changed a message it didn't name` |
| `queues.json` | `queue_node_clock` | `queue_rule` | `an enqueued message isn't ready at its delay` |
| `queues.json` | `queue_cap_unchecked` | `queue_rule` | `an enqueue past its caps applied` |
| `queues.json` | `queue_cap_tightened` | `queue_rule` | `an enqueue within its caps was refused` |
| `queues.json` | `queue_settle_keeps_bytes` | `queue_rule` | `a queue's held bytes aren't what its messages hold` |
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

## Queue rules and what proves them

Each rule `queue_family.zig` checks, next to the mutant above that shows it
fires, or the queue machine's own test (`src/queue/machine.zig`) that pins
the behaviour where no rewrite reaches it. A rule with neither would stand
out here.

| Rule | Proved by |
|---|---|
| A deliver leases the first due ready messages in (priority, due, seq) order | `queue_deliver_wrong_order` |
| A deliver takes exactly as many as its count, the due ones and its byte budget allow (the first always) | `queue_deliver_short` |
| A deliver leases only ready, due messages, to its holder and token, for its lease from its stamp | machine test "a deliver leases the first due ready messages, by priority then due time then seq" |
| A report that doesn't hold a message's live delivery leaves it as it was | `queue_settle_ignores_token` |
| A report, a deliver or an expire leaves every message it doesn't name or take as it was | `queue_report_changes_unnamed` |
| A fail readies its messages again after the delay, or dead-letters them; it never removes them | `queue_release_removes` |
| An expire ends only a lease it names, past its deadline | `queue_expire_ignores_deadline` |
| An expire ends every live lease it names past its deadline | `queue_expire_first_only` |
| Entries apply at their own stamp, so every replica agrees | `queue_node_clock` |
| Deliveries and failures never go down; a message is dead exactly at its attempts cap or when failed dead, counted dead once | machine tests "a message released until its attempts are spent is dead", "a message whose leases keep running out is dead once its attempts are spent", "a release to dead letters skips the attempts left" |
| Every ready, leased and dead message has its key, no ready or dead key is left over, and a queue's counts are what it holds | machine tests above, each checking its queue's counts |
| An extend moves a lease only with half of it or less left, to a full lease from its stamp, never past the hold ceiling | machine test "an extend moves a lease out once half of it is gone, and the old key goes"; the rule restates the delivery model's own (`contract/delivery.zig` `extend`), so it guards the queue machine calling it, not the formula |
| A purge leaves its queue empty | machine test "the same name in two namespaces is two queues, and a purge empties one" |
| An enqueue is refused exactly when its queue's messages or held bytes, or its shard's, would pass the caps it carries; one that applies leaves them within those caps | `queue_cap_unchecked` (applies past them), `queue_cap_tightened` (refused within them) |
| A queue's held bytes are what its messages hold (payload, error text and the per-message overhead), and the shard's are every queue's | `queue_settle_keeps_bytes`; machine tests "the shard's held bytes count every queue, and a settle or purge gives them back", "a message's error text is held, replaced as it fails again, and given back with it" |

A scenario is re-recorded from a fresh swarm (`vopr --iterations=N --actions
--family-mutant=M`, `--queues` for the queue mutants, `--catching-up` for the namespace mutants, or
`--txn-limits` for the limit mutants) when a
change to the workload stops it reaching a bug.
