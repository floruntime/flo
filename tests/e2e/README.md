# End-to-end tests

Each test spawns real `zig-out/bin/flo` servers. The full suite takes about
11 minutes of test time and loads a machine hard, so run only the groups your
change touches locally, and leave the full suite to CI.

The fault files (`apply_crash`, `halt`, `repair_skip`, `salvage`,
`snapshot_window`) instead spawn `zig-out/test/flo-faults` — a second `flo`
compiled with the fault drive the shipped binary never carries. `test-e2e`
builds it automatically, so any `test-e2e` run pays one extra full `flo`
compile the first time `flo`'s sources change; zig caches by content, so a
warm rebuild is free.

`zig build test` runs the whole suite — unit, stdx, integration and e2e — so
it is minutes, not a quick check, and loads the machine. On macOS it is not
total either: the Linux-only cluster e2e tests skip here; they run in CI, or
locally via `scripts/linux/run.sh`. Locally, prefer `zig build test-unit` plus
the targeted `test-e2e -Dtest-filter` below.

## Run only what you touched

Tests are named `e2e/<group>: …`, and `-Dtest-filter` keeps the tests whose
name contains a substring. Repeat it to run several groups:

```sh
zig build test-e2e -Dtest-filter="e2e/kv:" --summary all
zig build test-e2e -Dtest-filter="e2e/kv/cluster:" -Dtest-filter="e2e/cluster:" --summary all
```

- A filter is a plain substring. `|` is refused, and a filter that matches no
  test fails the build, so a typo can't pass by running nothing.
- Read the `N/N tests passed` count. It should be the number of tests you
  meant to run.

The groups, by size: `stream` `kv` `ts` `workflow` `processing` `dashboard`
`queue` `worker` `namespace` `cluster` `metrics` `kv/cluster` `action`
`scoping` `http` `server` …. List them all with
`grep -rhoE '^test "e2e/[^:]+:' tests/e2e | sort | uniq -c | sort -rn`.

## Shards

CI runs the suite as four shards, `zig build test-e2e -De2e-shard=i/4`. Each
shard runs whole files, balanced by the per-file times in `shard_times.tsv`.
A file that isn't listed there yet weighs 0.9 s per test. CI's `test-e2e`
job fails unless the shards together ran every test, and skipped only what it
expects.

Each run's job summary lists the slowest tests and each shard's time. When the
shards drift apart, refresh the times from a run:

```sh
scripts/e2e-shard-times.sh <run-id>
```

With `FLO_TEST_TIMES=<path>` set, the e2e runner (`helpers/test_runner.zig`)
appends `<ms>\t<test name>` for each test it runs.
