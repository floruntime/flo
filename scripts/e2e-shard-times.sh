#!/bin/sh
# Rewrite tests/e2e/shard_times.tsv, the per-file times the e2e shards are
# balanced on, from a CI run's test-time artifacts. Refresh it when the
# e2e job summary shows the shards' times drifting apart.
#
#   scripts/e2e-shard-times.sh <run-id>
set -eu
[ $# -eq 1 ] || { echo "usage: $0 <run-id>" >&2; exit 2; }
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
gh run download "$1" -R floruntime/flo -p 'e2e-test-times*' -D "$tmp"
{
  printf '# <ms>\t<file>: e2e time per file from CI run %s (scripts/e2e-shard-times.sh).\n' "$1"
  # A test is named <file, dots for slashes>.test.<name>. The generated
  # root and the sentinel run in every shard, so they aren't balanced.
  find "$tmp" -name '*.tsv' -exec cat {} + |
    awk -F'\t' '$2 ~ /\.test\./ && $2 !~ /^(all_tests|test_root_sentinel)\./ { f = $2; sub(/\.test\..*/, "", f); t[f] += $1 } END { for (f in t) printf "%d\t%s\n", t[f], f }' |
    sort -t "$(printf '\t')" -k2
} > tests/e2e/shard_times.tsv
echo "wrote tests/e2e/shard_times.tsv ($(grep -vc '^#' tests/e2e/shard_times.tsv) files)"
