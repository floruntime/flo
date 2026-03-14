#!/bin/bash
# =============================================================================
# Test 1: Transaction Type Router
#
# Populates a stream with transactions {id, type}, then runs a processing
# pipeline that filters by type and routes to separate output streams.
#
# Topology:
#   raw-transactions → [filter: type contains "payment"]  → payments-stream
#                     → [filter: type contains "refund"]   → refunds-stream
#                     → [filter: type contains "transfer"] → transfers-stream
#
# Prerequisites: flo server running on localhost:3333 (default)
#
# Usage: ./examples/processing/test-1-txn-router.sh [--host HOST] [--port PORT]
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
echo "  Test 1: Transaction Type Router Pipeline"
echo "=============================================="
echo ""

# --- Step 1: Create streams ---
step "Creating streams..."
$FLO stream create raw-transactions 2>/dev/null || true
$FLO stream create payments-stream 2>/dev/null || true
$FLO stream create refunds-stream 2>/dev/null || true
$FLO stream create transfers-stream 2>/dev/null || true
info "Streams created: raw-transactions, payments-stream, refunds-stream, transfers-stream"

# --- Step 2: Populate raw-transactions with test data ---
step "Populating raw-transactions with 20 sample transactions..."

# Payments
$FLO stream append raw-transactions '{"id":"txn-001","type":"payment_card","amount":49.99,"merchant":"Amazon"}'
$FLO stream append raw-transactions '{"id":"txn-002","type":"payment_crypto","amount":120.00,"merchant":"Coinbase"}'
$FLO stream append raw-transactions '{"id":"txn-003","type":"payment_wire","amount":5000.00,"merchant":"HSBC"}'
$FLO stream append raw-transactions '{"id":"txn-004","type":"payment_card","amount":12.50,"merchant":"Uber"}'
$FLO stream append raw-transactions '{"id":"txn-005","type":"payment_ach","amount":1500.00,"merchant":"Stripe"}'

# Refunds
$FLO stream append raw-transactions '{"id":"txn-006","type":"refund_partial","amount":15.00,"merchant":"Amazon"}'
$FLO stream append raw-transactions '{"id":"txn-007","type":"refund_full","amount":49.99,"merchant":"Nike"}'
$FLO stream append raw-transactions '{"id":"txn-008","type":"refund_chargeback","amount":200.00,"merchant":"Etsy"}'

# Transfers
$FLO stream append raw-transactions '{"id":"txn-009","type":"transfer_internal","amount":500.00,"from":"checking","to":"savings"}'
$FLO stream append raw-transactions '{"id":"txn-010","type":"transfer_external","amount":1000.00,"from":"checking","to":"ext-bank"}'
$FLO stream append raw-transactions '{"id":"txn-011","type":"transfer_p2p","amount":25.00,"from":"alice","to":"bob"}'

# Mixed / other types (should NOT match any pipeline)
$FLO stream append raw-transactions '{"id":"txn-012","type":"fee_monthly","amount":9.99}'
$FLO stream append raw-transactions '{"id":"txn-013","type":"interest_earned","amount":0.45}'
$FLO stream append raw-transactions '{"id":"txn-014","type":"payment_card","amount":75.00,"merchant":"Target"}'
$FLO stream append raw-transactions '{"id":"txn-015","type":"refund_full","amount":75.00,"merchant":"Target"}'
$FLO stream append raw-transactions '{"id":"txn-016","type":"transfer_internal","amount":200.00,"from":"savings","to":"checking"}'
$FLO stream append raw-transactions '{"id":"txn-017","type":"payment_debit","amount":33.00,"merchant":"Starbucks"}'
$FLO stream append raw-transactions '{"id":"txn-018","type":"chargeback_dispute","amount":150.00}'
$FLO stream append raw-transactions '{"id":"txn-019","type":"refund_partial","amount":10.00,"merchant":"Uber"}'
$FLO stream append raw-transactions '{"id":"txn-020","type":"transfer_wire","amount":10000.00,"from":"corporate","to":"payroll"}'

info "Populated 20 transactions (8 payments, 5 refunds, 4 transfers, 3 other)"

# --- Step 3: Submit processing pipelines ---
step "Submitting processing pipelines..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

$FLO processing submit "$SCRIPT_DIR/pipeline-payments.yaml"
info "Submitted: payment-router"

$FLO processing submit "$SCRIPT_DIR/pipeline-refunds.yaml"
info "Submitted: refund-router"

$FLO processing submit "$SCRIPT_DIR/pipeline-transfers.yaml"
info "Submitted: transfer-router"

# --- Step 4: Wait for pipelines to process ---
step "Waiting for pipelines to process (5s)..."
sleep 5

# --- Step 5: Verify results ---
step "Verifying output streams..."

echo ""
info "--- payments-stream ---"
PAYMENTS=$($FLO stream read payments-stream --limit 20 2>&1) || true
echo "$PAYMENTS"
PAYMENT_COUNT=$(echo "$PAYMENTS" | grep -c '"type":"payment' || true)
if [ "$PAYMENT_COUNT" -ge 7 ]; then
  pass "payments-stream has $PAYMENT_COUNT payment records (expected >= 7)"
else
  fail "payments-stream has $PAYMENT_COUNT payment records (expected >= 7)"
fi

echo ""
info "--- refunds-stream ---"
REFUNDS=$($FLO stream read refunds-stream --limit 20 2>&1) || true
echo "$REFUNDS"
REFUND_COUNT=$(echo "$REFUNDS" | grep -c '"type":"refund' || true)
if [ "$REFUND_COUNT" -ge 4 ]; then
  pass "refunds-stream has $REFUND_COUNT refund records (expected >= 4)"
else
  fail "refunds-stream has $REFUND_COUNT refund records (expected >= 4)"
fi

echo ""
info "--- transfers-stream ---"
TRANSFERS=$($FLO stream read transfers-stream --limit 20 2>&1) || true
echo "$TRANSFERS"
TRANSFER_COUNT=$(echo "$TRANSFERS" | grep -c '"type":"transfer' || true)
if [ "$TRANSFER_COUNT" -ge 3 ]; then
  pass "transfers-stream has $TRANSFER_COUNT transfer records (expected >= 3)"
else
  fail "transfers-stream has $TRANSFER_COUNT transfer records (expected >= 3)"
fi

# --- Step 6: Check pipeline status ---
echo ""
step "Pipeline status:"
$FLO processing status payment-router 2>&1 || true
$FLO processing status refund-router 2>&1 || true
$FLO processing status transfer-router 2>&1 || true

# --- Summary ---
echo ""
echo "=============================================="
echo -e "  Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "=============================================="
echo ""

if [ "$FAILED" -gt 0 ]; then exit 1; fi
