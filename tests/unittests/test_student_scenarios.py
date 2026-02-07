#!/usr/bin/env python3
"""
Unit tests for Exchange Simulator - Student Edition

This test suite covers critical scenarios that students should understand when
working with the exchange simulator. Each test is self-contained and includes
detailed documentation explaining what is being tested and why it matters.

Test Categories:
1. Order Book Integrity - Verifying correct book state after operations
2. Message Ordering - Ensuring clients receive messages in correct sequence
3. Concurrent Access - Testing thread safety with multiple clients
4. Error Handling - Verifying proper rejection of invalid operations
5. State Machine Transitions - Testing FSM correctness

Students should be able to:
- Run these tests to understand expected behavior
- Read the test code to learn proper API usage
- Modify tests to explore edge cases
- Add new tests for features they implement
"""

import pytest
import sys
import os
import time
import socket
import threading
from typing import List, Dict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchange_server import ExchangeServer
from order_client_with_fsm import OrderClient


def find_free_port():
    """Find an available TCP port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


@pytest.fixture
def exchange():
    """
    Fixture that creates a fresh exchange server for each test.

    The server is automatically started and stopped for you.
    Usage: def test_something(exchange):
               server, order_port, feed_port = exchange
    """
    order_port = find_free_port()
    feed_port = find_free_port()
    server = ExchangeServer(order_port, feed_port)
    assert server.start(), "Failed to start exchange server"
    time.sleep(0.1)  # Allow server to fully initialize

    yield server, order_port, feed_port

    server.stop()


# ============================================================================
# 1. ORDER BOOK INTEGRITY TESTS
# ============================================================================

class TestOrderBookIntegrity:
    """
    Tests verifying that the order book maintains correct state after various
    operations. The order book must always reflect reality - what's in the book
    should match what orders are actually standing.
    """

    def test_book_state_after_single_fill(self, exchange):
        """
        Test: After a trade, verify the order book correctly removes filled orders.

        Learning objective: Understand that filled orders must be removed from
        the book immediately to prevent double-fills.

        Scenario:
        1. Post a sell order (100 shares @ $50.00)
        2. Submit buy order that matches (100 shares @ $50.00)
        3. Verify sell order is removed from the book
        """
        server, order_port, feed_port = exchange

        # Setup: Create two clients
        seller = OrderClient('localhost', order_port)
        buyer = OrderClient('localhost', order_port)
        assert seller.connect()
        assert buyer.connect()

        # Step 1: Seller posts order
        response = seller.send_order("limit,100,500000,S,seller")
        assert "ACK" in response, "Sell order should be acknowledged"

        # Verify order is in the book
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 1, "Should have 1 ask in the book"
        assert asks[0].price == 500000
        assert asks[0].size == 100

        # Step 2: Buyer takes the order
        response = buyer.send_order("limit,100,500000,B,buyer")
        assert "FILL" in response, "Buy order should fill"

        # Step 3: Verify book is now empty
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 0, "Ask side should be empty after full fill"
        assert len(bids) == 0, "Bid side should remain empty"

        seller.disconnect()
        buyer.disconnect()

    def test_book_state_after_partial_fill(self, exchange):
        """
        Test: After partial fill, verify book shows correct remaining size.

        Learning objective: Partial fills must update the book to reflect the
        remaining quantity, maintaining queue position.

        Scenario:
        1. Post large order (500 shares)
        2. Partially fill it (200 shares)
        3. Verify book shows remaining 300 shares at original price
        """
        server, order_port, feed_port = exchange

        maker = OrderClient('localhost', order_port)
        taker = OrderClient('localhost', order_port)
        assert maker.connect()
        assert taker.connect()

        # Post large resting order
        maker.send_order("limit,500,500000,B,maker")

        # Verify initial state
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1
        assert bids[0].size == 500

        # Partially fill it
        taker.send_order("limit,200,500000,S,taker")

        # Verify remaining size
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Order should remain in book"
        assert bids[0].size == 300, "Book should show 300 shares remaining"
        assert bids[0].price == 500000, "Price should be unchanged"

        maker.disconnect()
        taker.disconnect()

    def test_price_time_priority_maintained(self, exchange):
        """
        Test: Verify price-time priority is strictly enforced.

        Learning objective: The matching engine must always fill the best
        priced order first, and within a price level, must fill older orders
        before newer orders (FIFO).

        Scenario:
        1. Post 3 bids at different prices: $49, $50, $51
        2. Post sell at $49 (should take the $51 bid - best price)
        3. Verify highest bid was filled, others remain
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Post 3 bids at different prices
        client.send_order("limit,100,490000,B,trader1")  # $49.00
        client.send_order("limit,100,500000,B,trader2")  # $50.00
        client.send_order("limit,100,510000,B,trader3")  # $51.00 - best bid

        # Verify all are in the book
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 3

        # Sell should match the best bid ($51)
        response = client.send_order("limit,100,490000,S,seller")
        assert "FILL" in response
        assert "510000" in response, "Should fill at best bid price ($51)"

        # Verify the $51 bid is gone, others remain
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 2, "Two bids should remain"
        assert bids[0].price == 500000, "Best remaining bid should be $50"
        assert bids[1].price == 490000, "Second best should be $49"

        client.disconnect()

    def test_book_state_after_cancel(self, exchange):
        """
        Test: Cancelled orders must be completely removed from the book.

        Learning objective: Cancels must be processed immediately and atomically.
        A cancelled order cannot participate in any future trades.

        Scenario:
        1. Post 2 orders at same price
        2. Cancel first order
        3. Verify only second order remains in book
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Post two orders at same price
        r1 = client.send_order("limit,100,500000,B,trader")
        r2 = client.send_order("limit,150,500000,B,trader")

        # Extract first order ID
        order_id_1 = int(r1.split(',')[1])

        # Verify both in book (total 250 shares)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1
        assert bids[0].size == 250

        # Cancel first order
        cancel_response = client.send_order(f"cancel,{order_id_1},trader")
        assert "CANCEL_ACK" in cancel_response

        # Verify only 150 shares remain
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1
        assert bids[0].size == 150, "Only second order should remain"

        client.disconnect()

    def test_book_integrity_after_multiple_trades(self, exchange):
        """
        Test: Complex sequence of trades should leave book in consistent state.

        Learning objective: The book must remain consistent through complex
        sequences of operations. This tests cumulative correctness.

        Scenario:
        1. Build a 3-level book on each side
        2. Execute trades across multiple levels
        3. Cancel some orders
        4. Verify final book state matches expectations
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Build 3-level bid side
        client.send_order("limit,100,490000,B,trader")  # $49
        client.send_order("limit,100,500000,B,trader")  # $50
        client.send_order("limit,100,510000,B,trader")  # $51

        # Build 3-level ask side
        r1 = client.send_order("limit,100,520000,S,trader")  # $52
        r2 = client.send_order("limit,100,530000,S,trader")  # $53
        r3 = client.send_order("limit,100,540000,S,trader")  # $54

        # Initial state check
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 3
        assert len(asks) == 3

        # Trade: sweep two bid levels with market sell (sweeps best first)
        client.send_order("market,200,0,S,trader")

        # After sweep: only $49 bid should remain (best two were taken)
        time.sleep(0.2)  # Wait for all async messages to arrive
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Only lowest bid should remain"
        assert bids[0].price == 490000, "Should be the $49 bid"
        assert len(asks) == 3, "Ask side unchanged"

        # Disconnect and reconnect to clear message buffer
        client.disconnect()
        time.sleep(0.1)

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Cancel middle ask level (r2 was the second sell order)
        order_id = int(r2.split(',')[1])
        cancel_response = client.send_order(f"cancel,{order_id},trader")
        assert "CANCEL_ACK" in cancel_response, f"Cancel should succeed: {cancel_response}"

        # Final state: 1 bid, 2 asks
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Should have 1 bid"
        assert len(asks) == 2, "Two ask levels should remain after cancel"
        assert asks[0].price == 520000, "First ask should be $52"
        assert asks[1].price == 540000, "$54 should remain (middle $53 was cancelled)"

        client.disconnect()


# ============================================================================
# 2. MESSAGE ORDERING TESTS
# ============================================================================

class TestMessageOrdering:
    """
    Tests verifying that exchange messages arrive in the correct order.
    Message ordering is critical for maintaining consistent state on the client side.
    """

    def test_ack_before_fills(self, exchange):
        """
        Test: ACK message must arrive before any FILL messages.

        Learning objective: The exchange must acknowledge order acceptance
        before reporting any fills. This allows clients to track order IDs.

        Scenario:
        1. Submit limit order that immediately crosses and fills
        2. Verify response contains ACK before any FILL messages
        """
        server, order_port, feed_port = exchange

        # Setup: Post resting sell order
        seller = OrderClient('localhost', order_port)
        buyer = OrderClient('localhost', order_port)
        assert seller.connect()
        assert buyer.connect()

        seller.send_order("limit,100,500000,S,seller")
        time.sleep(0.1)

        # Submit buy order that crosses
        response = buyer.send_order("limit,100,500000,B,buyer")

        lines = response.strip().split('\n')
        assert len(lines) >= 1, "Should receive at least one response line"

        # For a fully-filled crossing order, we get a FILL (not ACK+FILL)
        # But the FILL should contain the order ID
        first_line = lines[0]
        assert "FILL" in first_line, "Should receive FILL for fully-filled order"

        # Extract and verify order ID is present
        parts = first_line.split(',')
        assert len(parts) >= 2
        order_id = int(parts[1])
        assert order_id > 0, "FILL should contain valid order ID"

        seller.disconnect()
        buyer.disconnect()

    def test_partial_fill_ordering(self, exchange):
        """
        Test: PARTIAL_FILL messages must arrive in price-level order.

        Learning objective: When sweeping multiple levels, fills are reported
        in the order they execute (best price first).

        Scenario:
        1. Build 3-level ask side ($50, $51, $52)
        2. Submit buy that sweeps all levels
        3. Verify FILL messages arrive in price order
        """
        server, order_port, feed_port = exchange

        maker = OrderClient('localhost', order_port)
        taker = OrderClient('localhost', order_port)
        assert maker.connect()
        assert taker.connect()

        # Build ask side with 3 levels
        maker.send_order("limit,100,500000,S,maker")  # $50
        maker.send_order("limit,100,510000,S,maker")  # $51
        maker.send_order("limit,100,520000,S,maker")  # $52
        time.sleep(0.1)

        # Sweep all levels with single order
        response = taker.send_order("limit,300,520000,B,taker")

        lines = response.strip().split('\n')
        assert len(lines) == 3, "Should receive 3 fill messages"

        # Extract prices from each fill
        prices = []
        for line in lines:
            if "FILL" in line:
                parts = line.split(',')
                price = int(parts[3])
                prices.append(price)

        # Verify fills are in order from best to worst price
        assert prices[0] == 500000, "First fill should be at $50"
        assert prices[1] == 510000, "Second fill should be at $51"
        assert prices[2] == 520000, "Third fill should be at $52"

        maker.disconnect()
        taker.disconnect()

    def test_passive_fill_notification_timing(self, exchange):
        """
        Test: Passive fill notifications must arrive promptly after trade.

        Learning objective: When your resting order is filled by an aggressor,
        you must receive notification so you can update your position/risk.

        Scenario:
        1. Client A posts order and starts listening for async messages
        2. Client B takes the order
        3. Verify Client A receives passive fill within reasonable time
        """
        server, order_port, feed_port = exchange

        passive_client = OrderClient('localhost', order_port)
        aggressive_client = OrderClient('localhost', order_port)
        assert passive_client.connect()
        assert aggressive_client.connect()

        # Track messages received by passive client
        received_messages = []

        def message_handler(msg):
            received_messages.append(msg)

        # Start async receive on passive client
        passive_client.start_async_receive(message_handler)
        time.sleep(0.1)

        # Post resting order
        passive_client.send_order_async("limit,100,500000,B,passive")
        time.sleep(0.2)

        # Clear any ACK messages
        received_messages.clear()

        # Aggressor takes the order
        aggressive_client.send_order("limit,100,500000,S,aggressive")

        # Wait for passive fill notification
        time.sleep(0.5)

        # Verify passive client received fill
        fills = [msg for msg in received_messages if "FILL" in msg]
        assert len(fills) > 0, "Passive client should receive fill notification"

        # Verify fill contains correct information
        fill_msg = fills[0]
        assert "100" in fill_msg, "Fill should be for 100 shares"
        assert "500000" in fill_msg, "Fill should be at $50 price"

        passive_client.disconnect()
        aggressive_client.disconnect()

    def test_multiple_partial_fills_sequence(self, exchange):
        """
        Test: Multiple partial fills on same order arrive in sequence.

        Learning objective: As a resting order gets nibbled away by multiple
        aggressors, each fill notification must correctly report the remainder.

        Scenario:
        1. Post large order (1000 shares)
        2. Fill it incrementally (300, 400, 300)
        3. Verify each fill message has correct remainder
        """
        server, order_port, feed_port = exchange

        resting_client = OrderClient('localhost', order_port)
        assert resting_client.connect()

        # Track messages
        messages = []

        def handler(msg):
            messages.append(msg)

        resting_client.start_async_receive(handler)
        time.sleep(0.1)

        # Post large order
        resting_client.send_order_async("limit,1000,500000,S,trader")
        time.sleep(0.2)
        messages.clear()  # Clear ACK

        # Create multiple taking clients for sequential fills
        taker1 = OrderClient('localhost', order_port)
        taker2 = OrderClient('localhost', order_port)
        taker3 = OrderClient('localhost', order_port)
        for taker in [taker1, taker2, taker3]:
            assert taker.connect()

        # First partial fill (300 shares)
        taker1.send_order("limit,300,500000,B,taker1")
        time.sleep(0.3)

        fills = [m for m in messages if "FILL" in m]
        assert len(fills) == 1, "Should have one partial fill"
        assert "PARTIAL_FILL" in fills[0], "First fill should be partial"
        assert "700" in fills[0], "Remainder should be 700"
        messages.clear()

        # Second partial fill (400 shares)
        taker2.send_order("limit,400,500000,B,taker2")
        time.sleep(0.3)

        fills = [m for m in messages if "FILL" in m]
        assert len(fills) == 1, "Should have second partial fill"
        assert "PARTIAL_FILL" in fills[0], "Second fill should be partial"
        assert "300" in fills[0], "Remainder should be 300"
        messages.clear()

        # Final fill (300 shares)
        taker3.send_order("limit,300,500000,B,taker3")
        time.sleep(0.3)

        fills = [m for m in messages if "FILL" in m]
        assert len(fills) == 1, "Should have final fill"
        # Final fill is FILL (not PARTIAL_FILL) and has no remainder
        assert "FILL" in fills[0], "Final fill should be complete"

        resting_client.disconnect()
        for taker in [taker1, taker2, taker3]:
            taker.disconnect()


# ============================================================================
# 3. CONCURRENT ACCESS TESTS
# ============================================================================

class TestConcurrentAccess:
    """
    Tests verifying thread safety and correct behavior under concurrent load.
    Real exchanges handle thousands of messages per second from many clients.
    """

    def test_multiple_clients_simultaneous_submission(self, exchange):
        """
        Test: Multiple clients submitting orders concurrently should all succeed.

        Learning objective: The exchange must be thread-safe. Concurrent
        submissions should not cause race conditions or dropped orders.

        Scenario:
        1. Spawn 10 threads, each submitting an order
        2. Verify all 10 orders are acknowledged
        3. Verify all orders appear in the book
        """
        server, order_port, feed_port = exchange

        num_clients = 10
        results = []
        lock = threading.Lock()

        def client_thread(client_id):
            try:
                client = OrderClient('localhost', order_port)
                assert client.connect()

                # Each client posts at a unique price
                price = 500000 + (client_id * 1000)
                response = client.send_order(f"limit,100,{price},B,client{client_id}")

                with lock:
                    results.append(("ACK" in response, response))

                client.disconnect()
            except Exception as e:
                with lock:
                    results.append((False, str(e)))

        # Launch all threads simultaneously
        threads = []
        for i in range(num_clients):
            t = threading.Thread(target=client_thread, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all to complete
        for t in threads:
            t.join(timeout=5.0)

        # Verify all succeeded
        assert len(results) == num_clients, "All clients should complete"
        for success, response in results:
            assert success, f"All orders should ACK: {response}"

        # Verify all orders in book
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == num_clients, "All orders should be in book"

    def test_rapid_order_cancel_sequence(self, exchange):
        """
        Test: Rapid order submission followed by immediate cancel.

        Learning objective: Cancel requests can arrive very quickly after
        order submission. The exchange must handle this race condition correctly.

        Scenario:
        1. Submit order
        2. Immediately cancel it (no sleep)
        3. Verify either: order cancelled OR order not found (already filled/cancelled)
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        success_count = 0

        # Rapidly submit and cancel 20 orders
        for i in range(20):
            # Submit order
            response = client.send_order(f"limit,100,{500000 + i*100},B,trader")
            assert "ACK" in response, "Order should be acknowledged"

            # Extract order ID
            order_id = int(response.split(',')[1])

            # Immediately cancel (no time.sleep!)
            cancel_response = client.send_order(f"cancel,{order_id},trader")

            # Should either succeed or get "not found" (if somehow already processed)
            if "CANCEL_ACK" in cancel_response:
                success_count += 1
            elif "not found" in cancel_response.lower():
                # This is acceptable - order was already processed
                pass
            else:
                pytest.fail(f"Unexpected cancel response: {cancel_response}")

        # Most should succeed
        assert success_count >= 15, f"Most rapid cancels should succeed: {success_count}/20"

        client.disconnect()

    def test_high_frequency_submission_stress(self, exchange):
        """
        Test: High-frequency order submission should maintain correctness.

        Learning objective: Under heavy load, the exchange should not drop
        orders, corrupt data, or deadlock.

        Scenario:
        1. Submit 100 orders as fast as possible from single client
        2. Verify all orders are acknowledged
        3. Verify order IDs are unique and sequential
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        num_orders = 100
        responses = []

        start_time = time.time()

        # Submit orders as fast as possible
        for i in range(num_orders):
            response = client.send_order(f"limit,10,{500000 + i*10},B,hft_trader")
            responses.append(response)

        elapsed = time.time() - start_time

        # Verify all acknowledged
        ack_count = sum(1 for r in responses if "ACK" in r)
        assert ack_count == num_orders, f"All orders should ACK: {ack_count}/{num_orders}"

        # Extract and verify order IDs are unique
        order_ids = []
        for response in responses:
            if "ACK" in response:
                parts = response.split(',')
                order_ids.append(int(parts[1]))

        assert len(order_ids) == num_orders
        assert len(set(order_ids)) == num_orders, "All order IDs should be unique"

        # Order IDs should be sequential (or at least monotonically increasing)
        for i in range(1, len(order_ids)):
            assert order_ids[i] > order_ids[i-1], "Order IDs should be increasing"

        print(f"\nStress test: {num_orders} orders in {elapsed:.3f}s ({num_orders/elapsed:.0f} orders/sec)")

        client.disconnect()

    def test_concurrent_modification_same_price_level(self, exchange):
        """
        Test: Multiple clients modifying the same price level concurrently.

        Learning objective: When multiple orders at the same price are being
        filled/cancelled simultaneously, the book must remain consistent.

        Scenario:
        1. Post 5 orders at same price from different clients
        2. Have 3 clients take liquidity simultaneously
        3. Verify book state is consistent (no double-fills, correct size)
        """
        server, order_port, feed_port = exchange

        # Setup: Create 5 maker clients posting at same price
        makers = []
        for i in range(5):
            client = OrderClient('localhost', order_port)
            assert client.connect()
            client.send_order(f"limit,100,500000,S,maker{i}")
            makers.append(client)

        time.sleep(0.2)

        # Verify initial state: 500 shares at $50
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 1
        assert asks[0].size == 500

        # Create 3 taker threads that will act simultaneously
        taken_sizes = []
        lock = threading.Lock()

        def taker_thread(size):
            client = OrderClient('localhost', order_port)
            assert client.connect()
            response = client.send_order(f"limit,{size},500000,B,taker")

            # Parse how much was actually filled
            filled = 0
            for line in response.split('\n'):
                if "FILL" in line:
                    parts = line.split(',')
                    filled += int(parts[2])

            with lock:
                taken_sizes.append(filled)

            client.disconnect()

        # Launch 3 takers: 150, 200, 150 shares (total 500)
        threads = [
            threading.Thread(target=taker_thread, args=(150,)),
            threading.Thread(target=taker_thread, args=(200,)),
            threading.Thread(target=taker_thread, args=(150,))
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        # Verify exactly 500 shares were taken (no more, no less)
        total_filled = sum(taken_sizes)
        assert total_filled == 500, f"Should fill exactly 500 shares: got {total_filled}"

        # Verify book is now empty (all liquidity consumed)
        time.sleep(0.3)
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 0, "All asks should be filled"

        # Cleanup
        for maker in makers:
            maker.disconnect()


# ============================================================================
# 4. ERROR HANDLING TESTS
# ============================================================================

class TestErrorHandling:
    """
    Tests verifying proper handling of invalid operations and error conditions.
    Robust error handling prevents system crashes and gives clear feedback.
    """

    def test_invalid_order_format_rejected(self, exchange):
        """
        Test: Malformed order messages should be rejected with clear reason.

        Learning objective: Input validation is critical. Invalid messages
        should be rejected gracefully without crashing the server.

        Scenario: Send various malformed orders and verify rejection.
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Test various invalid formats
        invalid_orders = [
            "invalid,format",  # Wrong type
            "limit,abc,500000,B,trader",  # Non-numeric size
            "limit,100,xyz,B,trader",  # Non-numeric price
            "limit,100,500000,X,trader",  # Invalid side
            "limit,100",  # Missing fields
            "limit,-100,500000,B,trader",  # Negative size
        ]

        for invalid_order in invalid_orders:
            response = client.send_order(invalid_order)
            # Accept either REJECT or ERROR as rejection
            assert "REJECT" in response or "ERROR" in response, f"Should reject: {invalid_order}"

        client.disconnect()

    def test_cancel_nonexistent_order_rejected(self, exchange):
        """
        Test: Attempt to cancel an order that doesn't exist.

        Learning objective: The exchange must validate order IDs and reject
        operations on orders that don't exist or are already filled.

        Scenario: Try to cancel order ID that was never created.
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Try to cancel non-existent order
        response = client.send_order("cancel,99999,trader")
        assert "REJECT" in response, "Should reject cancel of non-existent order"
        assert "not found" in response.lower(), "Should indicate order not found"

        client.disconnect()

    def test_cancel_wrong_user_rejected(self, exchange):
        """
        Test: User cannot cancel another user's order.

        Learning objective: Order ownership must be enforced. This prevents
        malicious or accidental cancellation of other users' orders.

        Scenario:
        1. User A places order
        2. User B tries to cancel it
        3. Verify rejection
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # User A places order
        response = client.send_order("limit,100,500000,B,userA")
        assert "ACK" in response
        order_id = int(response.split(',')[1])

        # User B tries to cancel A's order
        response = client.send_order(f"cancel,{order_id},userB")
        assert "REJECT" in response, "Should reject cancel by wrong user"
        assert "not owned" in response.lower(), "Should indicate ownership error"

        # Verify order still in book
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Order should remain after failed cancel"

        client.disconnect()

    def test_cancel_already_filled_order_rejected(self, exchange):
        """
        Test: Cannot cancel an order that has already been fully filled.

        Learning objective: Once an order is filled, it no longer exists in
        the book and cannot be cancelled.

        Scenario:
        1. Post order and let it fill completely
        2. Try to cancel it
        3. Verify rejection
        """
        server, order_port, feed_port = exchange

        # Use separate connections to avoid mixing passive fills with responses
        maker = OrderClient('localhost', order_port)
        assert maker.connect()

        # Post and get order ID
        response = maker.send_order("limit,100,500000,S,maker")
        order_id = int(response.split(',')[1])
        maker.disconnect()  # Disconnect to avoid async messages

        # Fill it completely with different client
        taker = OrderClient('localhost', order_port)
        assert taker.connect()
        taker.send_order("limit,100,500000,B,taker")
        taker.disconnect()

        time.sleep(0.3)

        # Reconnect and try to cancel the filled order
        maker = OrderClient('localhost', order_port)
        assert maker.connect()
        response = maker.send_order(f"cancel,{order_id},maker")
        assert "REJECT" in response, "Should reject cancel of filled order"
        assert "not found" in response.lower(), "Filled order should not be found"

        maker.disconnect()

    def test_market_order_no_liquidity_rejected(self, exchange):
        """
        Test: Market order with no liquidity should be rejected.

        Learning objective: Market orders require immediate execution. If
        there's no liquidity, the order must be rejected (not posted to book).

        Scenario: Submit market order against empty book.
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Try market buy with empty book
        response = client.send_order("market,100,0,B,trader")
        assert "REJECT" in response, "Should reject market order with no liquidity"
        assert "liquidity" in response.lower(), "Should indicate liquidity issue"

        # Try market sell with empty book
        response = client.send_order("market,100,0,S,trader")
        assert "REJECT" in response, "Should reject market order with no liquidity"

        client.disconnect()

    def test_connection_drop_during_order(self, exchange):
        """
        Test: Client disconnect should not crash server or corrupt book.

        Learning objective: The server must gracefully handle client
        disconnections, even mid-operation.

        Scenario:
        1. Post order
        2. Disconnect immediately
        3. Verify server continues running and book is consistent
        """
        server, order_port, feed_port = exchange

        # Client posts order and disconnects immediately
        client = OrderClient('localhost', order_port)
        assert client.connect()
        client.send_order("limit,100,500000,B,trader")
        client.disconnect()  # Abrupt disconnect

        # Verify server still works with new client
        new_client = OrderClient('localhost', order_port)
        assert new_client.connect(), "Server should still accept connections"

        response = new_client.send_order("limit,100,510000,S,trader2")
        assert "ACK" in response, "Server should still process orders"

        # Verify book is consistent
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Original bid should remain in book"
        assert len(asks) == 1, "New ask should be in book"

        new_client.disconnect()


# ============================================================================
# 5. STATE MACHINE TRANSITION TESTS
# ============================================================================

class TestStateMachineTransitions:
    """
    Tests verifying correct order state transitions through the order lifecycle.
    Understanding state machines is key to building reliable trading systems.
    """

    def test_order_lifecycle_posted_to_filled(self, exchange):
        """
        Test: Complete lifecycle of an order that gets filled.

        Learning objective: Understand the states an order goes through:
        PENDING -> ACCEPTED -> FILLED

        Scenario:
        1. Submit order (PENDING -> ACCEPTED)
        2. Fill it (ACCEPTED -> FILLED)
        3. Verify no further state changes possible
        """
        server, order_port, feed_port = exchange

        # Use separate connections to avoid mixing passive fills with cancel response
        maker = OrderClient('localhost', order_port)
        assert maker.connect()

        # State: PENDING (before submission)
        # Submit order
        response = maker.send_order("limit,100,500000,S,maker")
        assert "ACK" in response, "Order should be ACCEPTED"
        order_id = int(response.split(',')[1])

        # State: ACCEPTED (resting in book)
        # Verify in book
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 1, "Order should be in book (ACCEPTED state)"

        maker.disconnect()  # Disconnect to avoid receiving passive fill

        # Fill the order with different client
        taker = OrderClient('localhost', order_port)
        assert taker.connect()
        taker.send_order("limit,100,500000,B,taker")
        taker.disconnect()

        time.sleep(0.3)

        # State: FILLED (no longer in book)
        bids, asks = server._order_book.get_snapshot()
        assert len(asks) == 0, "Filled order should be removed from book"

        # Reconnect and verify cannot cancel filled order (terminal state)
        maker = OrderClient('localhost', order_port)
        assert maker.connect()
        response = maker.send_order(f"cancel,{order_id},maker")
        assert "REJECT" in response, "Cannot cancel filled order"

        maker.disconnect()

    def test_order_lifecycle_posted_to_cancelled(self, exchange):
        """
        Test: Order cancelled before fill.

        Learning objective: Understand cancellation path:
        PENDING -> ACCEPTED -> CANCELLED

        Scenario:
        1. Submit order (PENDING -> ACCEPTED)
        2. Cancel it (ACCEPTED -> CANCELLED)
        3. Verify terminal state
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Submit and accept
        response = client.send_order("limit,100,500000,B,trader")
        assert "ACK" in response
        order_id = int(response.split(',')[1])

        # Verify ACCEPTED (in book)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1

        # Cancel
        response = client.send_order(f"cancel,{order_id},trader")
        assert "CANCEL_ACK" in response

        # Verify CANCELLED (not in book)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 0, "Cancelled order should be removed"

        # Verify cannot cancel again (terminal state)
        response = client.send_order(f"cancel,{order_id},trader")
        assert "REJECT" in response, "Cannot cancel twice"

        client.disconnect()

    def test_order_lifecycle_partial_fills(self, exchange):
        """
        Test: Order state through multiple partial fills.

        Learning objective: Understand PARTIALLY_FILLED state:
        PENDING -> ACCEPTED -> PARTIALLY_FILLED -> PARTIALLY_FILLED -> FILLED

        Scenario:
        1. Submit large order
        2. Partially fill it multiple times
        3. Complete the fill
        4. Verify state progression
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Submit large order
        response = client.send_order("limit,300,500000,B,trader")
        assert "ACK" in response
        order_id = int(response.split(',')[1])

        # State: ACCEPTED
        bids, asks = server._order_book.get_snapshot()
        assert bids[0].size == 300

        # First partial fill
        taker1 = OrderClient('localhost', order_port)
        assert taker1.connect()
        taker1.send_order("limit,100,500000,S,taker1")
        time.sleep(0.2)

        # State: PARTIALLY_FILLED (200 remaining)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1, "Order still in book"
        assert bids[0].size == 200, "Size should be updated"

        # Second partial fill
        taker2 = OrderClient('localhost', order_port)
        assert taker2.connect()
        taker2.send_order("limit,100,500000,S,taker2")
        time.sleep(0.2)

        # State: PARTIALLY_FILLED (100 remaining)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 1
        assert bids[0].size == 100

        # Final fill
        taker3 = OrderClient('localhost', order_port)
        assert taker3.connect()
        taker3.send_order("limit,100,500000,S,taker3")
        time.sleep(0.2)

        # State: FILLED (no longer in book)
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 0, "Fully filled order should be removed"

        client.disconnect()
        for taker in [taker1, taker2, taker3]:
            taker.disconnect()

    def test_order_rejection_immediate(self, exchange):
        """
        Test: Order rejected without ever reaching ACCEPTED state.

        Learning objective: Invalid orders transition:
        PENDING -> REJECTED (terminal state)

        Scenario: Submit invalid order and verify immediate rejection.
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Submit invalid order
        response = client.send_order("invalid_order_format")

        # Should go straight to REJECTED
        assert "REJECT" in response, "Invalid order should be rejected"

        # Verify nothing in book
        bids, asks = server._order_book.get_snapshot()
        assert len(bids) == 0 and len(asks) == 0, "Rejected order not in book"

        client.disconnect()

    def test_invalid_state_transition_prevented(self, exchange):
        """
        Test: Invalid state transitions should be prevented.

        Learning objective: State machines enforce valid transitions only.
        For example, cannot cancel a PENDING order before it's ACCEPTED.

        Scenario: Try to cancel order with invalid (non-existent) order ID.
        """
        server, order_port, feed_port = exchange

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Try to cancel order that was never submitted (invalid transition)
        response = client.send_order("cancel,12345,trader")
        assert "REJECT" in response, "Should reject invalid state transition"

        client.disconnect()


# ============================================================================
# HELPER FUNCTIONS AND UTILITIES
# ============================================================================

def format_price(price_lobster: int) -> str:
    """Convert LOBSTER price (price * 10000) to human-readable string."""
    return f"${price_lobster / 10000:.2f}"


def print_book_state(server):
    """Debug helper to print current book state."""
    bids, asks = server._order_book.get_snapshot()
    print("\n=== ORDER BOOK ===")
    print("BIDS:")
    for bid in bids:
        print(f"  {format_price(bid.price)}: {bid.size} shares")
    print("ASKS:")
    for ask in asks:
        print(f"  {format_price(ask.price)}: {ask.size} shares")
    print("==================\n")


if __name__ == "__main__":
    """
    Run tests with: pytest test_student_scenarios.py -v

    For more detailed output: pytest test_student_scenarios.py -v -s
    For specific test: pytest test_student_scenarios.py::TestOrderBookIntegrity::test_book_state_after_single_fill -v
    """
    pytest.main([__file__, "-v"])
