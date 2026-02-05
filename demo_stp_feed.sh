#!/bin/bash
#
# Demo script for the Python Exchange Simulator
#
# This demonstrates:
# - Starting the exchange server with order book display
# - Connecting an STP monitor to receive trade notifications
# - Submitting passive orders (market maker)
# - Submitting aggressive orders (that cross the spread and generate trades)
# - Cancelling orders
#
# Mirrors the C++ demo_stp_feed.sh script.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
ORDER_PORT=10000
FEED_PORT=10001
# Use unbuffered Python output to ensure all messages are captured
PYTHON="${PYTHON:-python3} -u"

# Helper to format price (divided by 10000)
format_price() {
    local price=$1
    local dollars=$((price / 10000))
    local cents=$((price % 10000))
    printf "\$%d.%02d" $dollars $((cents / 100))
}

echo "========================================"
echo "  NETWORK EXCHANGE DEMONSTRATION"
echo "  Trading and Order Cancellation"
echo "========================================"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping all processes..."
    jobs -p | xargs -r kill 2>/dev/null || true
    sleep 1
    jobs -p | xargs -r kill -9 2>/dev/null || true
    rm -f /tmp/exchange_output.txt /tmp/stp_output.txt /tmp/maker_output.txt /tmp/trader_output.txt /tmp/cancel_output.txt
}

trap cleanup EXIT

# Check if ports are available
check_port() {
    local port=$1
    if nc -z localhost $port 2>/dev/null; then
        echo "ERROR: Port $port is already in use"
        exit 1
    fi
}

check_port $ORDER_PORT
check_port $FEED_PORT

# Start the exchange server with order book display
echo "Starting Exchange Server..."
$PYTHON exchange_server.py --order-port $ORDER_PORT --feed-port $FEED_PORT --show-book --book-levels 5 > /tmp/exchange_output.txt 2>&1 &
EXCHANGE_PID=$!

# Wait for exchange to start
echo "Waiting for exchange to start..."
for i in {1..10}; do
    if nc -z localhost $ORDER_PORT 2>/dev/null && nc -z localhost $FEED_PORT 2>/dev/null; then
        echo "Exchange is ready!"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "ERROR: Exchange failed to start"
        cat /tmp/exchange_output.txt
        exit 1
    fi
    sleep 1
done

echo ""
echo "Starting STP monitor (to watch trades)..."
$PYTHON stp_client.py --port $FEED_PORT > /tmp/stp_output.txt 2>&1 &
STP_PID=$!
sleep 1

echo ""
echo "========================================"
echo "  PHASE 1: MARKET MAKER POSTS LIQUIDITY"
echo "========================================"
echo ""
echo "Market Maker (Client 1) will post passive limit orders to build the book:"
echo ""
echo "  ORDER 1: SELL  50 @ \$5800.00  (best offer)"
echo "  ORDER 2: BUY  200 @ \$5795.00  (best bid)"
echo "  ORDER 3: SELL 150 @ \$5805.00  (2nd level offer)"
echo "  ORDER 4: BUY  150 @ \$5790.00  (2nd level bid)"
echo ""
echo "Expected book after posting:"
echo "         BID         |         ASK"
echo "   200 @ \$5795.00   |    50 @ \$5800.00"
echo "   150 @ \$5790.00   |   150 @ \$5805.00"
echo ""

# Client 1: Market Maker (provides liquidity and stays connected)
(echo "limit,50,58000000,S"
 echo "limit,200,57950000,B"
 echo "limit,150,58050000,S"
 echo "limit,150,57900000,B"
 sleep 15) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/maker_output.txt 2>&1 &
MAKER_PID=$!

# Wait for orders to be processed
for i in {1..10}; do
    if grep -q "1003" /tmp/maker_output.txt 2>/dev/null; then
        break
    fi
    sleep 0.5
done
sleep 1

echo "--- Market Maker EMS Messages ---"
grep -E "^\[|^Submitted:|^Cancel" /tmp/maker_output.txt 2>/dev/null | grep -v "Receive error" | sed 's/^/  /' || true
echo ""

echo "--- Order Book After Market Maker Posts ---"
# Extract the 4th order book snapshot (after all 4 Market Maker orders)
awk '/┌─────────────────────────────────────────────────┐/{n++} n==4{print; while(getline && !/└/) print; print; exit}' /tmp/exchange_output.txt
echo "  (4 passive orders now resting on the book)"
echo ""

echo "========================================"
echo "  PHASE 2: AGGRESSIVE TRADER CROSSES"
echo "========================================"
echo ""
echo "Aggressive Trader (Client 2) submits orders that cross the spread:"
echo ""
echo "  ORDER 5: MARKET BUY 75"
echo "           -> Lifts 50 from best offer @ \$5800.00 (TRADE)"
echo "           -> Lifts 25 from 2nd offer @ \$5805.00 (TRADE)"
echo ""
echo "  ORDER 6: MARKET SELL 50"
echo "           -> Hits best bid, sells 50 @ \$5795.00 (TRADE)"
echo ""
echo "  ORDER 7: LIMIT BUY 100 @ \$5797.50 (passive, rests on book)"
echo "  ORDER 8: LIMIT SELL 80 @ \$5802.50 (passive, rests on book)"
echo ""

(echo "market,75,B"
 echo "market,50,S"
 echo "limit,100,57975000,B"
 echo "limit,80,58025000,S"
 sleep 2) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/trader_output.txt 2>&1 &
TRADER_PID=$!

# Wait for orders to be processed
for i in {1..10}; do
    if grep -q "1005" /tmp/trader_output.txt 2>/dev/null; then
        break
    fi
    sleep 0.5
done
sleep 1

echo "--- Aggressive Trader EMS Messages ---"
grep -E "^\[|^Submitted:" /tmp/trader_output.txt 2>/dev/null | sed 's/^/  /' || true
echo ""

echo "--- Market Maker EMS Messages (passive fills) ---"
grep -E "^\[FILL|^\[PARTIAL_FILL" /tmp/maker_output.txt 2>/dev/null | sed 's/^/  /' || echo "  (waiting for fills...)"
echo ""

echo "--- STP Feed (trade notifications) ---"
grep -E "^\[STP\]" /tmp/stp_output.txt 2>/dev/null | sed 's/^/  /' || echo "  (no trades yet)"
echo ""

echo "--- Order Book After Aggressive Trader ---"
# Extract the 8th order book snapshot (after Market Maker's 4 + Aggressive Trader's 4)
awk '/┌─────────────────────────────────────────────────┐/{n++} n==8{print; while(getline && !/└/) print; print; exit}' /tmp/exchange_output.txt
echo "  Note: Best offer 5800 fully lifted, 5805 partially lifted (125 remaining)"
echo "        Best bid 5795 partially hit (150 remaining)"
echo "        New orders: BID 100@5797.50, ASK 80@5802.50"
echo ""

echo "========================================"
echo "  PHASE 3: ORDER CANCELLATION"
echo "========================================"
echo ""
echo "Cancel Demo (Client 3) will:"
echo "  1. Submit LIMIT BUY 500 @ \$5780.00"
echo "  2. Receive ACK with order_id"
echo "  3. Send CANCEL request"
echo "  4. Receive CANCEL_ACK"
echo ""

# Submit order, wait for ACK, then cancel
# Order IDs: 1000-1003 (Market Maker), 1004-1005 (Trader limits), 1006 (this order)
(echo "limit,500,57800000,B"
 sleep 1
 echo "cancel,1006"
 sleep 1) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/cancel_output.txt 2>&1

echo "--- Cancel Demo EMS Messages ---"
grep -E "^\[|^Submitted:|^Cancel" /tmp/cancel_output.txt 2>/dev/null | sed 's/^/  /' || true
echo ""

echo "--- Order Book After Cancel (order 1006 removed) ---"
# Extract the 10th order book snapshot (after cancel - should match snapshot 8)
awk '/┌─────────────────────────────────────────────────┐/{n++} n==10{print; while(getline && !/└/) print; print; exit}' /tmp/exchange_output.txt
echo "  (Order 1006 BID 500@5780 was added then cancelled - book unchanged)"
echo ""

# Let everything settle and wait for market maker to receive all fills
echo "Waiting for all fill notifications..."
sleep 3

# Give Market Maker time to receive fills from aggressive trader
# The fills happen when the aggressive trader's orders cross the book
for i in {1..10}; do
    if grep -q "FILL" /tmp/maker_output.txt 2>/dev/null; then
        break
    fi
    sleep 0.5
done
sleep 1

# Wait for market maker process to finish
wait $MAKER_PID 2>/dev/null || true

echo "========================================"
echo "  RESULTS SUMMARY"
echo "========================================"
echo ""

echo "=== TRADES EXECUTED (from STP Feed) ==="
if grep -q "TRADE" /tmp/stp_output.txt 2>/dev/null; then
    grep "TRADE" /tmp/stp_output.txt | while read line; do
        echo "  $line"
    done
else
    echo "  (No trades recorded)"
fi
echo ""

echo "=== MARKET MAKER (Client 1) - Final State ==="
echo "This client posted passive orders. When Aggressive Trader's orders"
echo "crossed the spread, Market Maker's orders were filled (see STP Feed above)."
echo ""
grep -E "^\[|^Submitted:|^Cancel" /tmp/maker_output.txt 2>/dev/null | grep -v "Receive error" | sed 's/^/  /' || true
echo ""

echo "=== AGGRESSIVE TRADER (Client 2) - Final State ==="
echo "This client submitted aggressive orders that crossed the spread."
echo "Market orders filled immediately; limit orders rested on book:"
echo ""
grep -E "^\[|^Submitted:|^Cancel" /tmp/trader_output.txt 2>/dev/null | sed 's/^/  /' || true
echo ""

echo "=== CANCEL DEMO (Client 3) - Final State ==="
echo "This client submitted an order, then cancelled it:"
echo ""
grep -E "^\[|^Submitted:|^Cancel" /tmp/cancel_output.txt 2>/dev/null | sed 's/^/  /' || true
echo ""

echo "========================================"
echo "  DEMONSTRATION COMPLETE"
echo "========================================"
echo ""
echo "What happened:"
echo "  1. Market Maker posted 4 passive orders (2 bids, 2 offers)"
echo "  2. Aggressive Trader's MARKET BUY 75 executed against Market Maker's offers"
echo "     - Trade 1: 50 @ \$5800.00"
echo "     - Trade 2: 25 @ \$5805.00"
echo "  3. Aggressive Trader's MARKET SELL 50 executed against Market Maker's bid"
echo "     - Trade 3: 50 @ \$5795.00"
echo "  4. Aggressive Trader's limit orders rested passively on book"
echo "  5. Cancel Demo showed order lifecycle: NEW -> ACK -> CANCELLED"
echo ""
echo "Order format:  limit,size,price,side[,ttl]"
echo "               market,size,side"
echo "Cancel format: cancel,order_id"
echo ""
echo "To run interactively (4 terminals):"
echo "  Terminal 1: python3 exchange_server.py --show-book"
echo "  Terminal 2: python3 order_client_with_fsm.py  (Market Maker)"
echo "  Terminal 3: python3 order_client_with_fsm.py  (Trader)"
echo "  Terminal 4: python3 stp_client.py             (Monitor)"
echo "========================================"
