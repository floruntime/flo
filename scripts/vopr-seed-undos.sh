#!/usr/bin/env bash
# Each regression seed guards a fix only if it fails with that fix undone.
# src/vopr/seeds/undos.tsv names each seed's undo (a plant in src/plant.zig) and
# what it fails with: the first invariant it names, or `crash`, and text its output
# must also show, so a seed that moved onto another bug doesn't pass for this one.
#
#   scripts/vopr-seed-undos.sh check [plant]
#       Build with each undo, and check its seeds fail as the table says.
#   scripts/vopr-seed-undos.sh compile
#       Only check each undo still compiles.
#   scripts/vopr-seed-undos.sh record <seed file> [iterations]
#       Swarm with the seed's undo until a seed fails as the table says, check
#       it passes without the undo, and write that scenario over the seed file.
#
# Run from the repository root. Env: ZIG (default zig), SEED_TIMEOUT (default 60).
set -uo pipefail

ZIG=${ZIG:-zig}
SEED_TIMEOUT=${SEED_TIMEOUT:-60}
# Far past any run that gets anywhere (at most about 12k here), and short of how far
# the undos that go round without getting anywhere run on.
MAX_DELIVERED=200000
SEEDS=src/vopr/seeds
TABLE=$SEEDS/undos.tsv
WORK=$(mktemp -d "${TMPDIR:-/tmp}/vopr-undos.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

rows() { grep -v '^#' "$TABLE"; }

# Build the VOPR with a plant; its path is $WORK/<plant>/bin/vopr.
build() {
  local plant=$1
  [ -x "$WORK/$plant/bin/vopr" ] && return 0
  "$ZIG" build vopr-build -Dvopr-plant="$plant" --release=safe --prefix "$WORK/$plant" > "$WORK/build.log" 2>&1 || {
    grep -E ' error:' "$WORK/build.log" | head -5; echo "could not build with $plant" >&2; return 1; }
}

# How a run ended: ok, hang, crash, or the first invariant it names.
outcome() {
  local log=$1 rc=$2
  if [ "$rc" = 0 ]; then echo ok; return; fi
  if [ "$rc" = 124 ] || [ "$rc" = 137 ]; then echo hang; return; fi
  local inv
  inv=$(grep -aoE '^ +\[[a-z_]+\]' "$log" | head -1 | tr -d ' []')
  echo "${inv:-crash}"
}

run() { # binary, then its arguments; prints the outcome, and leaves the output in run.log
  local bin=$1; shift
  timeout "$SEED_TIMEOUT" "$bin" "$@" --max-delivered="$MAX_DELIVERED" > "$WORK/run.log" 2>&1
  outcome "$WORK/run.log" $?
}

# Every plant in src/plant.zig has a row.
plants_listed() {
  local plant bad=0
  for plant in $(sed -n '/pub const Plant = enum {/,/^};/p' src/plant.zig | grep -oE '^    [a-z_]+,' | tr -d ' ,' | grep -v '^none$'); do
    rows | cut -f2 | grep -qxF "$plant" || { echo "::error::plant $plant has no row in $TABLE"; bad=1; }
  done
  return $bad
}

check() {
  local only=${1:-} bad=0 seen=0 f plant flag values want says started got
  plants_listed || bad=1
  # Every seed file has a row, and every row a file.
  for f in "$SEEDS"/*.json; do
    rows | cut -f1 | grep -qxF "$(basename "$f")" || { echo "::error::$(basename "$f") has no undo in $TABLE"; bad=1; }
  done
  while IFS=$'\t' read -r f plant flag values want says; do
    [ -e "$SEEDS/$f" ] || { echo "::error::$TABLE names $f, which isn't there"; bad=1; continue; }
    [ -z "$only" ] || [ "$only" = "$plant" ] || continue
    seen=1
    build "$plant" || { bad=1; continue; }
    started=$SECONDS
    got=$(run "$WORK/$plant/bin/vopr" --scenario-in="$SEEDS/$f")
    if [ "$got" = "$want" ] && grep -qaE -- "$says" "$WORK/run.log"; then
      echo "ok    $f, $plant: $got ($((SECONDS - started)) s)"
    else
      echo "::error::$f with $plant ended $got, not $want matching '$says': it no longer guards its fix"; bad=1
    fi
  done < <(rows)
  [ "$seen" = 1 ] || { echo "::error::no row names plant $only"; bad=1; }
  return $bad
}

compile() {
  local plant bad=0
  plants_listed || bad=1
  for plant in $(rows | cut -f2 | sort -u); do
    "$ZIG" build vopr-compile -Dvopr-plant="$plant" > "$WORK/build.log" 2>&1 || {
      grep -E ' error:' "$WORK/build.log" | head -5; echo "::error::$plant doesn't compile"; bad=1; }
  done
  return $bad
}

# The scenario of a seed in a slice, with the table's values set, in $WORK/try.json.
scenario() {
  local seed=$1 flag=$2 values=$3 pair
  timeout "$SEED_TIMEOUT" "$WORK/none/bin/vopr" --seed="$seed" $flag --scenario-out="$WORK/try.json" > /dev/null 2>&1
  [ "$values" = - ] && return 0
  for pair in ${values//,/ }; do
    sed -E "s/\"${pair%%=*}\": [^,]*/\"${pair%%=*}\": ${pair#*=}/" "$WORK/try.json" > "$WORK/try.next"
    mv "$WORK/try.next" "$WORK/try.json"
  done
}

record() {
  local file=$1 iterations=${2:-200} row plant flag values want says seed got
  row=$(rows | awk -F'\t' -v f="$file" '$1 == f') || true
  [ -n "$row" ] || { echo "$file has no row in $TABLE" >&2; return 2; }
  IFS=$'\t' read -r _ plant flag values want says <<< "$row"
  [ "$flag" = - ] && flag=
  build "$plant" || return 1
  build none || return 1
  for seed in $(seq 1 "$iterations"); do
    scenario "$seed" "$flag" "$values"
    got=$(run "$WORK/$plant/bin/vopr" --scenario-in="$WORK/try.json")
    [ "$got" = "$want" ] && grep -qaE -- "$says" "$WORK/run.log" || continue
    got=$(run "$WORK/none/bin/vopr" --scenario-in="$WORK/try.json")
    if [ "$got" != ok ]; then
      echo "seed $seed fails without the undo too ($got): a bug of its own"; continue
    fi
    cp "$WORK/try.json" "$SEEDS/$file"
    echo "$file: seed $seed${flag:+ $flag}, $want with $plant, ok without"
    return 0
  done
  echo "no seed in 1..$iterations${flag:+ of $flag} fails with $plant as $want" >&2
  return 1
}

case "${1:-}" in
  check) shift; check "$@" ;;
  compile) compile ;;
  record) shift; record "$@" ;;
  *) sed -n '2,14p' "$0"; exit 2 ;;
esac
