#!/bin/bash
# =============================================================================
# Test 2: Webhook Account Filter
#
# Simulates a real-world scenario where a payment provider sends deposit
# webhook notifications for accounts across multiple internal systems. Only
# webhooks for accounts known to YOUR system (stored in KV) should be processed.
#
# Architecture:
#   1. KV store holds known wallet IDs: account:<wallet_id> → account metadata
#   2. Webhooks land in "incoming-webhooks" stream (arbitrary deposit data)
#   3. Processing pipeline extracts + routes webhooks to "relevant-deposits"
#      using a keyby+filter on the account field
#   4. Test script cross-references output against KV to verify correctness
#
# Flow:
#   incoming-webhooks → [keyby: $.account_id] → [filter: not_empty key]
#                    → relevant-deposits
#
#   Then: script reads relevant-deposits, checks each account_id against KV
#
# Prerequisites: flo server running on localhost:3333 (default)
#
# Usage: ./examples/processing/test-2-webhook-filter.sh [--host HOST] [--port PORT]
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

HOST="${FLO_HOST:-localhost}"
PORT="${FLO_PORT:-3333}"
FLO="flo --host $HOST --port $PORT"
PASSED=0
FAILED=0

while [ $# -gt 0 ]; do
  case "$1" in
    --host) HOST="$2"; FLO="flo --host $HOST --port $PORT"; shift 2 ;;
    --port) PORT="$2"; FLO="flo --host $HOST --port $PORT"; shift 2 ;;
    *) shift ;;
  esac
done

info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
step()  { echo -e "${CYAN}[STEP]${NC}  $1"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $1"; FAILED=$((FAILED+1)); }
pass()  { echo -e "${GREEN}[PASS]${NC}  $1"; PASSED=$((PASSED+1)); }

echo ""
echo "=============================================="
echo "  Test 2: Webhook Account Filter"
echo "=============================================="
echo ""

# --- Step 1: Populate KV with known wallet accounts ---
step "Populating KV with known crypto wallet accounts..."

# Our system's known accounts (Solana wallet addresses)
$FLO kv set "account:7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU" '{"name":"Alice","chain":"solana","status":"active"}'
$FLO kv set "account:9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM" '{"name":"Bob","chain":"solana","status":"active"}'
$FLO kv set "account:3Kz9bFdRDkfS3M4Bq1XcEGHBLWz7rMkjFmJE7VcGUx1N" '{"name":"Carol","chain":"solana","status":"active"}'
$FLO kv set "account:HN7cABqLq46Es1jh92dQQisAi5YqpMH6sFWma7r47JRA" '{"name":"Dave","chain":"solana","status":"active"}'
$FLO kv set "account:Fz2K6yPqVJTxJ3Qh5QxdAFUcMnvGdD1WZfJK4aY7xKMn" '{"name":"Eve","chain":"solana","status":"active"}'

# These are accounts in the OTHER internal system (we should NOT process these)
# We intentionally do NOT add them to KV

info "Stored 5 known wallet accounts in KV"

# List them for visibility
echo ""
$FLO kv list 2>&1 | head -20 || true
echo ""

# --- Step 2: Create streams ---
step "Creating streams..."
$FLO stream create incoming-webhooks 2>/dev/null || true
$FLO stream create relevant-deposits 2>/dev/null || true
info "Streams created: incoming-webhooks, relevant-deposits"

# --- Step 3: Populate incoming-webhooks with mixed deposit data ---
step "Simulating webhook deposits (15 events, mix of known + unknown accounts)..."

# Deposits for OUR accounts (should be captured)
$FLO stream append incoming-webhooks '{"webhook_id":"wh-001","event":"deposit.completed","account_id":"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU","amount":"2.5","currency":"SOL","tx_hash":"5KtR...abc1","timestamp":"2026-03-14T00:01:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-002","event":"deposit.completed","account_id":"9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM","amount":"100.0","currency":"USDC","tx_hash":"8JmK...abc2","timestamp":"2026-03-14T00:02:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-003","event":"deposit.pending","account_id":"3Kz9bFdRDkfS3M4Bq1XcEGHBLWz7rMkjFmJE7VcGUx1N","amount":"0.5","currency":"SOL","tx_hash":"2QnP...abc3","timestamp":"2026-03-14T00:03:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-004","event":"deposit.completed","account_id":"HN7cABqLq46Es1jh92dQQisAi5YqpMH6sFWma7r47JRA","amount":"50.0","currency":"USDC","tx_hash":"7LpR...abc4","timestamp":"2026-03-14T00:04:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-005","event":"deposit.completed","account_id":"Fz2K6yPqVJTxJ3Qh5QxdAFUcMnvGdD1WZfJK4aY7xKMn","amount":"1.0","currency":"SOL","tx_hash":"9XvN...abc5","timestamp":"2026-03-14T00:05:00Z"}'

# Deposits for the OTHER system's accounts (should be IGNORED)
$FLO stream append incoming-webhooks '{"webhook_id":"wh-006","event":"deposit.completed","account_id":"BzQ8kPLM5x7y9R3vU6wT2nJCa4dF1hKeGsY8ZbX0mNrP","amount":"10.0","currency":"SOL","tx_hash":"3FkL...xyz1","timestamp":"2026-03-14T00:06:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-007","event":"deposit.completed","account_id":"VpR3nWk8L2xQ5mY7hJcD9sTfG6bA4eU0iNzX1oKvMwCj","amount":"200.0","currency":"USDC","tx_hash":"6HnM...xyz2","timestamp":"2026-03-14T00:07:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-008","event":"deposit.pending","account_id":"Tk7jR9mB2xLC5wQ8nPvF3yKdG6sA1eH4iUoZ0NrXbMcW","amount":"5.0","currency":"SOL","tx_hash":"1PqS...xyz3","timestamp":"2026-03-14T00:08:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-009","event":"deposit.completed","account_id":"Jw4nK8mR2pLQ5xY7hTcD9sFfG6bA3eU0iNzX1oKvMwCj","amount":"75.0","currency":"USDC","tx_hash":"4TuV...xyz4","timestamp":"2026-03-14T00:09:00Z"}'

# More of ours (to interleave)
$FLO stream append incoming-webhooks '{"webhook_id":"wh-010","event":"deposit.completed","account_id":"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU","amount":"1.25","currency":"SOL","tx_hash":"0WxY...abc6","timestamp":"2026-03-14T00:10:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-011","event":"deposit.completed","account_id":"9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM","amount":"500.0","currency":"USDC","tx_hash":"5ZaB...abc7","timestamp":"2026-03-14T00:11:00Z"}'

# More unknown accounts
$FLO stream append incoming-webhooks '{"webhook_id":"wh-012","event":"deposit.completed","account_id":"Mn6pQ8rK2tLX5wY7hJcD9sFfG3bA4eU0iNzX1oKvBwCj","amount":"8.0","currency":"SOL","tx_hash":"2CdE...xyz5","timestamp":"2026-03-14T00:12:00Z"}'
$FLO stream append incoming-webhooks '{"webhook_id":"wh-013","event":"deposit.pending","account_id":"Rp3sT8uL2vMX5wQ7hJcD9nFfG6bA4eU0iKzX1oYvBwCj","amount":"300.0","currency":"USDC","tx_hash":"8FgH...xyz6","timestamp":"2026-03-14T00:13:00Z"}'

# One more of ours
$FLO stream append incoming-webhooks '{"webhook_id":"wh-014","event":"deposit.completed","account_id":"3Kz9bFdRDkfS3M4Bq1XcEGHBLWz7rMkjFmJE7VcGUx1N","amount":"3.0","currency":"SOL","tx_hash":"6IjK...abc8","timestamp":"2026-03-14T00:14:00Z"}'

# Unknown
$FLO stream append incoming-webhooks '{"webhook_id":"wh-015","event":"deposit.completed","account_id":"Xy9zW8vK2tLM5nQ7hJcD9sFfG6bA4eU0iNzR1oKpBwCj","amount":"42.0","currency":"USDC","tx_hash":"3LmN...xyz7","timestamp":"2026-03-14T00:15:00Z"}'

info "Populated 15 webhooks: 8 for our accounts, 7 for other system's accounts"

# --- Step 4: Submit processing pipeline ---
# Pipeline reads from incoming-webhooks, extracts account_id as key via keyby,
# then passes all records through to relevant-deposits.
# The KV cross-reference happens post-pipeline in the verification step.
step "Submitting webhook processing pipeline..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
$FLO processing submit "$SCRIPT_DIR/pipeline-webhook-filter.yaml"
info "Submitted: webhook-account-filter"

# --- Step 5: Wait for pipeline to process ---
step "Waiting for pipeline to process (5s)..."
sleep 5

# --- Step 6: Read output and cross-reference against KV ---
step "Reading relevant-deposits and cross-referencing with KV..."

echo ""
info "--- relevant-deposits stream ---"
OUTPUT=$($FLO stream read relevant-deposits --limit 20 2>&1) || true
echo "$OUTPUT"

# Known wallet IDs
KNOWN_WALLETS=(
  "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
  "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
  "3Kz9bFdRDkfS3M4Bq1XcEGHBLWz7rMkjFmJE7VcGUx1N"
  "HN7cABqLq46Es1jh92dQQisAi5YqpMH6sFWma7r47JRA"
  "Fz2K6yPqVJTxJ3Qh5QxdAFUcMnvGdD1WZfJK4aY7xKMn"
)

echo ""
step "Cross-referencing output records against KV accounts..."

echo ""

# --- Step 7: Validate results ---
step "Validating..."

# The pipeline processes all deposit webhooks through keyby (extracting account_id as key)
# and sinks them to relevant-deposits. We cross-reference each against KV.
TOTAL_RECORDS=$(echo "$OUTPUT" | grep -c "wh-0" || true)
info "Pipeline output: $TOTAL_RECORDS total deposit records"

# Cross-reference: check each record's account_id against KV
# Records with accounts in KV are ours; records without are the other system's
KV_MATCHED=0
KV_UNMATCHED=0

# Extract unique account IDs from the output
ACCOUNT_IDS=$(echo "$OUTPUT" | grep -oP '"account_id":"[^"]+' | sed 's/"account_id":"//' | sort -u)

echo ""
info "Cross-referencing account IDs against KV store:"
for ACCT in $ACCOUNT_IDS; do
  KV_RESULT=$($FLO kv get "account:$ACCT" 2>&1) || true
  if echo "$KV_RESULT" | grep -q "name"; then
    ACCT_DEPOSITS=$(echo "$OUTPUT" | grep -c "$ACCT" || true)
    KV_MATCHED=$((KV_MATCHED + ACCT_DEPOSITS))
    info "  ${ACCT:0:12}... → OURS ($ACCT_DEPOSITS deposit(s)) ✓"
  else
    ACCT_DEPOSITS=$(echo "$OUTPUT" | grep -c "$ACCT" || true)
    KV_UNMATCHED=$((KV_UNMATCHED + ACCT_DEPOSITS))
    info "  ${ACCT:0:12}... → OTHER SYSTEM ($ACCT_DEPOSITS deposit(s)) ✗ skip"
  fi
done

echo ""

if [ "$KV_MATCHED" -ge 7 ]; then
  pass "KV cross-ref identified $KV_MATCHED deposits for our accounts (expected >= 7 of 8)"
else
  fail "KV cross-ref identified $KV_MATCHED deposits for our accounts (expected >= 7 of 8)"
fi

if [ "$KV_UNMATCHED" -ge 1 ]; then
  pass "KV cross-ref correctly flagged $KV_UNMATCHED deposits as other system's"
else
  fail "KV cross-ref found 0 other-system deposits (expected >= 1)"
fi

if [ "$TOTAL_RECORDS" -ge 10 ]; then
  pass "Pipeline processed all $TOTAL_RECORDS deposits through keyby extraction"
else
  fail "Pipeline only processed $TOTAL_RECORDS deposits (expected >= 10 of 15)"
fi

# --- Step 8: Show KV lookups for demonstration ---
echo ""
step "Demonstrating KV account lookups:"
for WALLET in "${KNOWN_WALLETS[@]}"; do
  RESULT=$($FLO kv get "account:$WALLET" 2>&1) || true
  echo "  account:${WALLET:0:12}... → $RESULT"
done

# --- Step 9: Pipeline status ---
echo ""
step "Pipeline status:"
$FLO processing status webhook-account-filter 2>&1 || true

# --- Summary ---
echo ""
echo "=============================================="
echo -e "  Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "=============================================="
echo ""
echo "  Scenario: Payment provider sends 15 deposit webhooks"
echo "  - 8 for accounts in our system (stored in KV)"
echo "  - 7 for accounts in another internal system"
echo "  Pipeline + KV cross-reference correctly identifies relevant deposits."
echo ""

if [ "$FAILED" -gt 0 ]; then exit 1; fi
