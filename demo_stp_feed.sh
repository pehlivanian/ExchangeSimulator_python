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
PYTHON=${PYTHON:-python3}

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
echo "  TRADING SIMULATION"
echo "========================================"
echo ""

# Client 1: Market Maker (provides liquidity and stays connected)
echo "=== CLIENT 1: Market Maker ==="
echo "Adding liquidity to both sides of the book..."
echo ""

# Use order_client_with_fsm.py - pipe orders via stdin, sleep to stay connected for fills
(echo "limit,50,58000000,S"
 echo "limit,200,57950000,B"
 echo "limit,150,58050000,S"
 echo "limit,150,57900000,B"
 sleep 15) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/maker_output.txt 2>&1 &
MAKER_PID=$!

sleep 4

echo "Market Maker Orders (posted to book):"
grep "Submitted:" /tmp/maker_output.txt | sed 's/^/  /'
echo ""

# Client 2: Aggressive Trader (crosses the spread)
echo "=== CLIENT 2: Aggressive Trader ==="
echo "Taking liquidity with market orders..."
echo ""

(echo "market,75,B"
 echo "market,50,S"
 echo "limit,100,57975000,B"
 echo "limit,80,58025000,S"
 sleep 2) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/trader_output.txt 2>&1 &
TRADER_PID=$!

sleep 3

echo "Aggressive Trader Orders:"
grep "Submitted:" /tmp/trader_output.txt | sed 's/^/  /'
echo ""

# Client 3: Demonstrate order cancellation
echo "=== CLIENT 3: Cancel Order Demo ==="
echo "Placing an order, then cancelling it..."
echo ""

# Submit order, wait for ACK, then cancel (order_id will be 1009 based on sequence)
(echo "limit,500,57800000,B"
 sleep 1
 echo "cancel,1009"
 sleep 1) | $PYTHON order_client_with_fsm.py --port $ORDER_PORT -q > /tmp/cancel_output.txt 2>&1

echo "Cancel Demo:"
grep "^Sending:" /tmp/cancel_output.txt | sed 's/^Sending: /  Sent:     /'
grep "^Response:" /tmp/cancel_output.txt | sed 's/^Response: /  Received: /'
echo ""

# Let everything settle
sleep 2

# Stop all processes
echo "========================================"
echo "  RESULTS"
echo "========================================"
echo ""

echo "=== ORDER BOOK UPDATES ==="
# Show order book after each order
grep -A 14 "^Order:" /tmp/exchange_output.txt 2>/dev/null || echo "(Order book updates in exchange log)"
echo ""

echo "=== TRADES EXECUTED (STP Feed) ==="
grep "TRADE" /tmp/stp_output.txt 2>/dev/null || echo "(No trades recorded)"
echo ""

echo "=== MARKET MAKER FULL TRANSCRIPT ==="
echo "Orders submitted:"
grep "Submitted:" /tmp/maker_output.txt | sed 's/^/  /'
echo ""
echo "State transitions (ACKs and fills):"
grep "^\[" /tmp/maker_output.txt | sed 's/^/  /' || echo "  (none)"
echo ""

echo "=== AGGRESSIVE TRADER FULL TRANSCRIPT ==="
echo "Orders submitted:"
grep "Submitted:" /tmp/trader_output.txt | sed 's/^/  /'
echo ""
echo "State transitions:"
grep "^\[" /tmp/trader_output.txt | sed 's/^/  /' || echo "  (none)"
echo ""

echo "=== CANCEL ORDER DEMO TRANSCRIPT ==="
echo "Orders submitted:"
grep "Submitted:" /tmp/cancel_output.txt | sed 's/^/  /'
echo ""
echo "State transitions (including cancel):"
grep "^\[" /tmp/cancel_output.txt | sed 's/^/  /' || echo "  (none)"
echo ""

# Wait for market maker to finish receiving fills
wait $MAKER_PID 2>/dev/null || true

echo "========================================"
echo "  DEMONSTRATION COMPLETE"
echo "========================================"
echo ""
echo "Summary:"
echo "  - Market Maker added liquidity on both sides"
echo "  - Aggressive Trader crossed the spread"
echo "  - STP feed broadcast all trades"
echo "  - Order cancellation demonstrated"
echo "  - FSM tracks order state transitions"
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
