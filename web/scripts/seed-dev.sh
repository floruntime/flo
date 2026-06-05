#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Flo Console — dev synthetic-data seeder
#
# Boots a fresh single-node flo (data dir under /tmp, NO auth) and populates every
# primitive via the `flo` CLI so the Console UI shows realistic live data — no mock.
#
#   ./web/scripts/seed-dev.sh            # build (if needed), start server, seed
#   FLO=/path/to/flo ./web/scripts/seed-dev.sh
#   KEEP=1 ./web/scripts/seed-dev.sh     # reuse an already-running server (skip start)
#
# Dashboard API → http://localhost:9002/api/v1   (Vite proxies /api → :9002)
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"          # web/scripts -> web -> flo (repo root)
FLO="${FLO:-$REPO_ROOT/zig-out/bin/flo}"
DATA_DIR="${DATA_DIR:-/tmp/flo-dev}"
PORT="${PORT:-9000}"
DASH="http://localhost:$((PORT + 2))"

log() { printf '\033[2m›\033[0m %s\n' "$*"; }

# --- build if missing -------------------------------------------------------
if [[ ! -x "$FLO" ]]; then
  log "building flo (zig build)…"
  (cd "$REPO_ROOT" && zig build)
fi

# --- start server (unless KEEP=1 and already up) ----------------------------
if [[ "${KEEP:-0}" != "1" ]]; then
  log "fresh data dir: $DATA_DIR"
  "$FLO" server stop --data-dir "$DATA_DIR" >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR"
  log "starting flo server on :$PORT (dashboard :$((PORT + 2)), no auth)…"
  "$FLO" server start --port "$PORT" --data-dir "$DATA_DIR" --shards 1 \
    > /tmp/flo-server.log 2>&1 &
fi

log "waiting for dashboard…"
for _ in $(seq 1 40); do
  if curl -fs "$DASH/health" >/dev/null 2>&1; then break; fi
  sleep 0.5
done
curl -fs "$DASH/health" >/dev/null || { echo "server did not come up; see /tmp/flo-server.log"; exit 1; }

ns()   { "$FLO" namespace create "$1" >/dev/null 2>&1 || true; }
kv()   { "$FLO" kv set "$2" "$3" --namespace "$1" ${4:+--ttl $4} >/dev/null 2>&1 || true; }
screate() { "$FLO" stream create "$2" --namespace "$1" ${3:+--partitions $3} >/dev/null 2>&1 || true; }
sapp() { "$FLO" stream append "$2" "$3" --namespace "$1" >/dev/null 2>&1 || true; }
enq()  { "$FLO" queue enqueue "$2" "$3" --namespace "$1" ${4:+--priority $4} >/dev/null 2>&1 || true; }
ts()   { "$FLO" ts write "$2" --tags "$3" --value "$4" --namespace "$1" >/dev/null 2>&1 || true; }
areg() { "$FLO" action register "$2" --namespace "$1" ${3:+--timeout $3} ${4:+--retries $4} >/dev/null 2>&1 || true; }
ainv() { "$FLO" action invoke "$2" "$3" --namespace "$1" --async >/dev/null 2>&1 || true; }
# Worker helpers. NOTE: `worker register <id> <task_types...>` only registers the
# FIRST task type (the CLI arg isn't variadic) → one process per worker.
wreg()  { "$FLO" worker register "$2" "$3" --namespace "$1" >/dev/null 2>&1 || true; }
wdrain(){ "$FLO" worker drain "$2" --namespace "$1" >/dev/null 2>&1 || true; }
# Lease + complete up to $4 tasks of action $3 on worker $2 (ns $1) — real execution
# through the node protocol, so action runs flip pending→completed with durations.
wwork() {
  local i tid out
  for i in $(seq 1 "${4:-3}"); do
    out=$("$FLO" worker await "$3" --worker-id "$2" --namespace "$1" --block 600 2>/dev/null)
    tid=$(printf '%s\n' "$out" | sed -n 's/^Task: //p')
    [ -z "$tid" ] && break
    "$FLO" worker complete "$tid" --worker-id "$2" --action "$3" --result '{"status":"ok"}' --namespace "$1" >/dev/null 2>&1 || true
  done
}
# Lease + fail one task (no retry) → a failed run + a worker fail tally.
wfail() {
  local tid out
  out=$("$FLO" worker await "$3" --worker-id "$2" --namespace "$1" --block 600 2>/dev/null)
  tid=$(printf '%s\n' "$out" | sed -n 's/^Task: //p')
  [ -n "$tid" ] && "$FLO" worker fail "$tid" --worker-id "$2" --action "$3" --error "provider timeout after retries" --namespace "$1" >/dev/null 2>&1 || true
}
# group read with --no-ack: forms a consumer group + advances its last-delivered
# cursor to --limit records (so groups sit at varied positions on the activity bar).
gread() { "$FLO" stream group read "$2" --group "$3" --consumer "$4" --namespace "$1" --no-ack --limit "${5:-9999}" >/dev/null 2>&1 || true; }

# --- namespaces -------------------------------------------------------------
log "namespaces…"
for n in production analytics media staging; do ns "$n"; done

# --- KV ---------------------------------------------------------------------
log "kv keys…"
kv production user:142:profile  '{"name":"Ada Lovelace","plan":"pro","seats":5}'
kv production session:abc:auth  '{"uid":142,"scopes":["read","write"]}' 58
kv production cache:api:users   '{"page":1,"items":[1,2,3,4,5]}' 86400
kv production config:flags      '{"dark_mode":true,"beta_ui":false,"max_conn":450}'
kv production user:88:settings  '{"theme":"calm","lang":"en"}'
kv production rl:api:142:bucket '42'
kv production lock:payment      '"held"' 30
kv analytics  funnel:signup     '{"step":"verify","count":812}'
kv analytics  events:42:rollup  '{"window":"1h","sum":54021}'
kv media      cdn:config        '{"region":"eu","ttl":21600}' 21600
kv staging    db_url            '"postgres://localhost/flo"'
kv staging    feature:dark-mode 'true'

# --- Streams (varied event data → varied payloads + sizes) ------------------
log "streams…"
screate production orders 8
screate production payments 8
screate production user-clicks 32
screate production logs-app 64

OTYPE=(created paid shipped delivered cancelled refunded)
ONOTE=("" "" " gift-wrap" " expedited shipping requested" " flagged for manual review by the fraud team")
apporders() { for i in $(seq "$1" "$2"); do
  sapp production orders "{\"type\":\"${OTYPE[$((i % 6))]}\",\"order_id\":\"ord_$((1000+i))\",\"total\":$((i*37%900)).$(printf %02d $((i%100))),\"items\":$((1+i%5)),\"note\":\"${ONOTE[$((i % 5))]}\"}"
done; }
PTYPE=(authorized captured refunded voided); PM=(card wallet bank_transfer apple_pay)
apppay() { for i in $(seq "$1" "$2"); do
  sapp production payments "{\"type\":\"${PTYPE[$((i%4))]}\",\"amount\":$((i*53%1200)).$(printf %02d $((i%100))),\"method\":\"${PM[$((i%4))]}\",\"currency\":\"USD\"}"
done; }
CTYPE=(click page_view scroll hover add_to_cart); CPATH=(/home /search /product/42 /cart /checkout /account/settings)
appclicks() { for i in $(seq "$1" "$2"); do
  sapp production user-clicks "{\"type\":\"${CTYPE[$((i%5))]}\",\"session\":\"s_$((i*97%9999))\",\"path\":\"${CPATH[$((i%6))]}\",\"dwell_ms\":$((i*89%5000))}"
done; }

# Interleave appends with group reads so each group's last-delivered cursor lands
# at a distinct point (--limit doesn't restrict a group read, so a group catches
# up to the head — reading it BEFORE later appends is what creates real lag).
log "streams + consumer groups…"
apporders 1 16
gread production orders fraud-scan     scanner-1   # furthest behind (consumed up to ~16)
apporders 17 32
gread production orders data-warehouse loader-1    # mid (up to ~32)
apporders 33 48
gread production orders fulfillment    worker-1    # caught up (all 48)
gread production orders fulfillment    worker-2    # 2nd member

apppay 1 20
gread production payments receipt-mailer mailer-1  # behind (up to ~20)
apppay 21 40
gread production payments ledger-writer  ledger-1  # caught up

appclicks 1 32
gread production user-clicks session-rollup roll-1 # behind (up to ~32)
appclicks 33 64
gread production user-clicks realtime-dash  dash-1 # caught up

# High-volume stream — ~1000 records via BATCHED appends (10 records per call →
# ~100 calls instead of 1000, so it's fast). The dashboard unpacks full batches,
# so every record shows. Each batch is one append-time → ~100 distinct timestamps.
log "high-volume stream (events, ~1000 records, batched)…"
screate production events 16
EVT=(login logout purchase view click signup error refund add_to_cart checkout)
REG=(us-east-1 eu-west-1 ap-south-1 sa-east-1)
for batch in $(seq 0 99); do
  vals=()
  for k in $(seq 1 10); do
    i=$((batch*10+k))
    vals+=("{\"id\":$i,\"event\":\"${EVT[$((i%10))]}\",\"region\":\"${REG[$((i%4))]}\",\"user\":\"u_$((i*131%99999))\",\"amount\":$((i*17%1000)).$(printf %02d $((i%100))),\"ok\":$([ $((i%7)) -eq 0 ] && echo false || echo true)}")
  done
  "$FLO" stream append events "${vals[@]}" --namespace production >/dev/null 2>&1 || true
done

# --- Queues -----------------------------------------------------------------
log "queues…"
for i in $(seq 1 30); do
  enq production order-processing    "{\"order_id\":\"ord_$((4200+i))\",\"items\":$((1+i%4))}" $((i%3==0?1:10))
  enq production email-notifications "{\"to\":\"user$i@example.com\",\"template\":\"receipt\"}"
done
for i in $(seq 1 12); do
  enq analytics analytics-events "{\"event\":\"purchase\",\"user\":\"u_$i\"}"
done

# --- Time series ------------------------------------------------------------
log "time series…"
for i in $(seq 1 30); do
  ts production cpu_usage    "host=web-01" "$((40 + i % 50)).$((i%10))"
  ts production cpu_usage    "host=web-02" "$((30 + i % 40)).$((i%10))"
  ts production http_latency "route=/api"  "$((1 + i % 8)).$((i%10))"
  ts analytics events_rate   "kind=click"  "$((100 + i * 7 % 900))"
done

# --- Actions ----------------------------------------------------------------
# Register action types, then fire async invocations to create real run records.
# Without a worker leasing them the runs stay `pending` (honest live state) —
# the dashboard list/detail/runs are driven entirely off these.
log "actions…"
areg production send-email    60000  3
areg production process-order 300000 5
areg production charge-payment 30000 3
areg analytics events-rollup  15000  1
for i in $(seq 1 14); do ainv production send-email    "{\"to\":\"user$i@example.com\",\"template\":\"receipt\"}"; done
for i in $(seq 1 8);  do ainv production process-order  "{\"order_id\":$((1000 + i)),\"total\":$((i * 37))}"; done
for i in $(seq 1 5);  do ainv production charge-payment  "{\"amount\":$((i * 100)),\"currency\":\"USD\"}"; done
for i in $(seq 1 9);  do ainv analytics  events-rollup   "{\"window\":\"5m\",\"batch\":$i}"; done

# --- Workers ----------------------------------------------------------------
# Register action workers (one process each — see wreg note), then lease+execute a
# slice of the pending runs so both Workers (throughput) and Actions (completed
# runs + durations + outputs, populated workers[]) show real data. A worker that
# only registers stays `active`; one is drained for a `draining` state.
log "workers…"
wreg production worker-1 send-email
wreg production worker-2 process-order
wreg production worker-3 charge-payment
wreg analytics  worker-4 events-rollup
wwork production worker-1 send-email     6
wwork production worker-2 process-order  4
wwork production worker-3 charge-payment 2
wwork analytics  worker-4 events-rollup  5
wfail production worker-1 send-email
wdrain production worker-3

# --- Processing -------------------------------------------------------------
# Submit real stream-processing pipelines reading from the seeded streams, so the
# Processing screen shows live jobs (RUNNING, with records_processed climbing).
# One job is stopped to show a STOPPED state.
log "processing…"
PIPE_DIR="$(mktemp -d)"
cat > "$PIPE_DIR/events-filter.yaml" <<'YAML'
kind: Processing
name: events-filter
namespace: production
sources:
  - name: src
    stream:
      name: events
      partitions: all
operators:
  - type: filter
    name: keep-events
    condition: "value_contains:e"
sinks:
  - name: out
    stream:
      name: events-filtered
YAML
cat > "$PIPE_DIR/payments-router.yaml" <<'YAML'
kind: Processing
name: payments-router
namespace: production
sources:
  - name: txns
    stream:
      name: payments
      partitions: all
operators:
  - type: filter
    name: is-payment
    condition: "value_contains:payment"
  - type: keyby
    name: by-method
    key_expression: "$.method"
sinks:
  - name: out
    stream:
      name: payments-routed
YAML
cat > "$PIPE_DIR/orders-shape.yaml" <<'YAML'
kind: Processing
name: orders-shape
namespace: production
sources:
  - name: ord
    stream:
      name: orders
      partitions: all
operators:
  - type: map
    name: shape
    amount: "$.amount"
sinks:
  - name: out
    stream:
      name: orders-shaped
YAML
"$FLO" processing submit "$PIPE_DIR/events-filter.yaml"   --namespace production >/dev/null 2>&1 || true
"$FLO" processing submit "$PIPE_DIR/payments-router.yaml" --namespace production >/dev/null 2>&1 || true
ORDERS_JOB=$("$FLO" processing submit "$PIPE_DIR/orders-shape.yaml" --namespace production 2>/dev/null | sed -n 's/^Job submitted: //p')
sleep 1
# Stop the orders job so the list shows a non-RUNNING state too.
[ -n "$ORDERS_JOB" ] && "$FLO" processing stop "$ORDERS_JOB" --namespace production >/dev/null 2>&1 || true
rm -rf "$PIPE_DIR"

# --- Workflows --------------------------------------------------------------
# Create a workflow definition + start runs. A run completes only when its
# dispatched action is executed by a worker, so we register an `echo` action +
# worker in production and complete some runs (→ completed), leave some
# (→ running/waiting on the action), and start runs in analytics where no echo
# action exists (→ failed). Net: a realistic mix of run states + real history.
log "workflows…"
WF_DIR="$(mktemp -d)"
cat > "$WF_DIR/echo-workflow.yaml" <<'YAML'
kind: Workflow
name: echo-workflow
version: "1.0.0"
start:
  run: "@actions/echo"
  transitions:
    success: flo.Completed
    failure: flo.Failed
YAML
areg production echo
wreg production wf-worker echo
"$FLO" workflow create -f "$WF_DIR/echo-workflow.yaml" --namespace production >/dev/null 2>&1 || true
"$FLO" workflow create -f "$WF_DIR/echo-workflow.yaml" --namespace analytics  >/dev/null 2>&1 || true
for i in $(seq 1 6); do "$FLO" workflow start echo-workflow "{\"msg\":\"hello-$i\"}" --namespace production >/dev/null 2>&1 || true; done
# Complete 4 of the 6 dispatched echo actions → those workflow runs complete;
# the rest stay awaiting the action.
wwork production wf-worker echo 4
# analytics has no echo action/worker → these runs fail (real failure + history).
for i in $(seq 1 2); do "$FLO" workflow start echo-workflow "{\"batch\":$i}" --namespace analytics >/dev/null 2>&1 || true; done
rm -rf "$WF_DIR"

log "done. Dashboard API: $DASH/api/v1   ·   UI: run \`npm run dev\` then open http://localhost:5173"
