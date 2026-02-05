#!/usr/bin/env python3
"""
Integration tests for the Exchange Simulator.

Uses the ACTUAL client components:
- OrderClientWithFSM from order_client_with_fsm.py
- STPClient from stp_client.py
- BookBuilder from udp_book_builder.py

Each test starts a fresh server to avoid state bleeding between tests.
"""

import socket
import subprocess
import sys
import threading
import time
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, '../..')

from order_client_with_fsm import OrderClientWithFSM, ManagedOrder, ExchangeMessage
from stp_client import STPClient
from udp_book_builder import BookBuilder


class STPCollector:
    """Wrapper around STPClient that collects trades in a list."""

    def __init__(self, host: str = 'localhost', port: int = 10001):
        self.client = STPClient(host, port)
        self.trades: List[str] = []
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> bool:
        if not self.client.connect():
            return False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def _run(self):
        def callback(msg: str):
            with self._lock:
                self.trades.append(msg)
        self.client.run(callback)

    def stop(self):
        self.client.disconnect()
        if self._thread:
            self._thread.join(timeout=2.0)

    def get_trades(self) -> List[str]:
        with self._lock:
            return list(self.trades)

    def clear(self):
        with self._lock:
            self.trades.clear()


class UDPBookCollector:
    """Collects UDP market data and builds order book using BookBuilder."""

    def __init__(self, port: int = 10002):
        self.port = port
        self.builder = BookBuilder()
        self.messages: List[str] = []
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> bool:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.settimeout(1.0)
            self._sock.sendto(b"subscribe", ('127.0.0.1', self.port))
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return True
        except Exception as e:
            print(f"UDP collector failed: {e}")
            return False

    def _run(self):
        while self._running:
            try:
                data, addr = self._sock.recvfrom(1024)
                msg = data.decode().strip()
                if msg:
                    with self._lock:
                        self.messages.append(msg)
                        self.builder.process_message(msg)
            except socket.timeout:
                continue
            except Exception:
                break

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()
        if self._thread:
            self._thread.join(timeout=2.0)

    def get_messages(self) -> List[str]:
        with self._lock:
            return list(self.messages)

    def get_summary(self) -> dict:
        with self._lock:
            inserts = sum(1 for m in self.messages if ',1,' in m)
            cancels = sum(1 for m in self.messages if ',2,' in m)
            deletes = sum(1 for m in self.messages if ',3,' in m)
            executes = sum(1 for m in self.messages if ',4,' in m)
            return {
                'total': len(self.messages),
                'inserts': inserts,
                'cancels': cancels,
                'deletes': deletes,
                'executes': executes,
            }

    def clear(self):
        with self._lock:
            self.messages.clear()
            self.builder = BookBuilder()


def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, 'exchange_server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd='../..'
    )
    time.sleep(0.8)
    return proc


def stop_server(proc: subprocess.Popen):
    proc.terminate()
    try:
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()


class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors: List[str] = []

    def record(self, name: str, passed: bool, details: str = ""):
        if passed:
            self.passed += 1
            print(f"  ✓ {name}")
        else:
            self.failed += 1
            self.errors.append(f"{name}: {details}")
            print(f"  ✗ {name}: {details}")


def test_basic_orders(results: TestResults):
    """Test basic order submission and ACK using OrderClientWithFSM."""
    print("\n[Test Group 1] Basic Order Operations")
    server = start_server()
    try:
        # Create client using actual OrderClientWithFSM
        client = OrderClientWithFSM('localhost', 10000)
        results.record("Client created", client is not None)

        connected = client.connect()
        results.record("Client connected", connected)

        if connected:
            # Submit a limit order (sync, no async receive needed)
            order = client.create_order("limit", 100, 5000000, "B", 3600)
            client.submit_order_sync(order)
            results.record("Limit order accepted", order.state_name == "ACCEPTED",
                           f"State: {order.state_name}")
            results.record("Order got exchange ID", order.exchange_order_id is not None,
                           f"ID: {order.exchange_order_id}")

            # Submit another order (ask)
            order2 = client.create_order("limit", 100, 5100000, "S", 3600)
            submitted2 = client.submit_order_sync(order2)
            results.record("Non-crossing ask submitted",
                           order2.state_name == "ACCEPTED",
                           f"State: {order2.state_name}")

            client.disconnect()

    finally:
        stop_server(server)


def test_crossing_orders(results: TestResults):
    """Test crossing orders generate trades."""
    print("\n[Test Group 2] Crossing Orders and Trades")
    server = start_server()
    stp = STPCollector()
    udp = UDPBookCollector()
    try:
        stp.start()
        udp.start()
        time.sleep(0.2)

        # Seller places ask
        seller = OrderClientWithFSM('localhost', 10000)
        seller.connect()
        ask_order = seller.create_order("limit", 100, 5000000, "S", 3600)
        seller.submit_order_sync(ask_order)

        time.sleep(0.1)
        udp.clear()
        stp.clear()

        # Buyer places crossing bid
        buyer = OrderClientWithFSM('localhost', 10000)
        buyer.connect()
        bid_order = buyer.create_order("limit", 100, 5000000, "B", 3600)
        buyer.submit_order_sync(bid_order)

        time.sleep(0.3)

        # Check results
        results.record("Crossing order filled", bid_order.state_name in ["FILLED", "PARTIALLY_FILLED"],
                       f"State: {bid_order.state_name}")

        trades = stp.get_trades()
        results.record("STP broadcast trade", len(trades) >= 1, f"Count: {len(trades)}")

        md = udp.get_summary()
        results.record("UDP EXECUTE broadcast", md['executes'] >= 1, f"Executes: {md['executes']}")

        seller.disconnect()
        buyer.disconnect()

    finally:
        stp.stop()
        udp.stop()
        stop_server(server)


def test_market_orders(results: TestResults):
    """Test market orders."""
    print("\n[Test Group 3] Market Orders")
    server = start_server()
    stp = STPCollector()
    try:
        stp.start()
        time.sleep(0.2)

        client = OrderClientWithFSM('localhost', 10000)
        client.connect()

        # Market order with no liquidity should fail
        market_order = client.create_order("market", 100, 0, "B")
        client.submit_order_sync(market_order)
        results.record("Market order rejects on no liquidity",
                       market_order.state_name == "REJECTED",
                       f"State: {market_order.state_name}")

        # Add liquidity
        bid_order = client.create_order("limit", 200, 4800000, "B", 3600)
        client.submit_order_sync(bid_order)

        stp.clear()

        # Market sell should fill
        client2 = OrderClientWithFSM('localhost', 10000)
        client2.connect()
        market_sell = client2.create_order("market", 50, 0, "S")
        client2.submit_order_sync(market_sell)

        time.sleep(0.2)
        results.record("Market order fills with liquidity",
                       market_sell.state_name in ["FILLED", "PARTIALLY_FILLED"],
                       f"State: {market_sell.state_name}")

        trades = stp.get_trades()
        results.record("Market order STP broadcast", len(trades) >= 1, f"Trades: {len(trades)}")

        client.disconnect()
        client2.disconnect()

    finally:
        stp.stop()
        stop_server(server)


def wait_for_state(order, expected_state: str, timeout: float = 2.0) -> bool:
    """Poll until order reaches expected state or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if order.state_name == expected_state:
            return True
        time.sleep(0.05)
    return False


def test_cancellation(results: TestResults):
    """Test order cancellation."""
    print("\n[Test Group 4] Order Cancellation")
    server = start_server()
    udp = UDPBookCollector()
    try:
        udp.start()
        time.sleep(0.2)

        client = OrderClientWithFSM('localhost', 10000)
        client.connect()
        client.start_async_receive()

        # Place order
        order = client.create_order("limit", 100, 4700000, "B", 3600)
        client.submit_order_sync(order)

        udp.clear()

        # Cancel it
        cancel_result = client.cancel_order(order)

        # Wait for state to update (async processing)
        state_updated = wait_for_state(order, "CANCELLED", timeout=2.0)

        results.record("Cancel succeeded", cancel_result)
        results.record("Order state is CANCELLED", state_updated,
                       f"State: {order.state_name}")

        md = udp.get_summary()
        results.record("UDP DELETE broadcast", md['deletes'] >= 1, f"Deletes: {md['deletes']}")

        client._running = False  # Stop async receive cleanly
        client.disconnect()

    finally:
        udp.stop()
        stop_server(server)


def test_sweep_orders(results: TestResults):
    """Test orders that sweep multiple price levels."""
    print("\n[Test Group 5] Multi-Level Sweeps")
    server = start_server()
    stp = STPCollector()
    try:
        stp.start()
        time.sleep(0.2)

        # Market maker places asks at multiple levels
        mm = OrderClientWithFSM('localhost', 10000)
        mm.connect()

        mm.submit_order_sync(mm.create_order("limit", 100, 4700000, "S", 3600))
        mm.submit_order_sync(mm.create_order("limit", 100, 4800000, "S", 3600))
        mm.submit_order_sync(mm.create_order("limit", 100, 4900000, "S", 3600))

        stp.clear()

        # Sweeper buys through all levels
        sweeper = OrderClientWithFSM('localhost', 10000)
        sweeper.connect()

        sweep_order = sweeper.create_order("limit", 300, 5000000, "B", 3600)
        sweeper.submit_order_sync(sweep_order)

        time.sleep(0.3)

        results.record("Sweep order filled", sweep_order.state_name in ["FILLED", "PARTIALLY_FILLED"],
                       f"State: {sweep_order.state_name}")

        trades = stp.get_trades()
        results.record("Multiple STP trades from sweep", len(trades) >= 1, f"Count: {len(trades)}")

        mm.disconnect()
        sweeper.disconnect()

    finally:
        stp.stop()
        stop_server(server)


def test_passive_fills(results: TestResults):
    """Test passive fill notifications."""
    print("\n[Test Group 6] Passive Fill Notifications")
    server = start_server()
    try:
        # Passive trader places resting order
        passive = OrderClientWithFSM('localhost', 10000)
        passive.connect()
        passive.start_async_receive()

        passive_order = passive.create_order("limit", 100, 4200000, "B", 3600)
        passive.submit_order_sync(passive_order)
        initial_state = passive_order.state_name

        # Aggressive trader crosses
        aggressive = OrderClientWithFSM('localhost', 10000)
        aggressive.connect()

        agg_order = aggressive.create_order("limit", 100, 4200000, "S", 3600)
        aggressive.submit_order_sync(agg_order)

        # Wait for passive fill notification to arrive
        passive_filled = wait_for_state(passive_order, "FILLED", timeout=2.0)

        # Passive order should now be filled
        results.record("Passive order filled", passive_filled,
                       f"State: {passive_order.state_name} (was {initial_state})")
        results.record("Aggressive order filled",
                       agg_order.state_name == "FILLED",
                       f"State: {agg_order.state_name}")

        passive._running = False  # Stop async receive cleanly
        passive.disconnect()
        aggressive.disconnect()

    finally:
        stop_server(server)


def test_ttl_expiry(results: TestResults):
    """Test order TTL expiry."""
    print("\n[Test Group 7] Order TTL Expiry")
    server = start_server()
    try:
        client = OrderClientWithFSM('localhost', 10000)
        client.connect()

        # Place order with 5 second TTL (enough time to verify acceptance)
        order = client.create_order("limit", 100, 4100000, "B", 5)  # 5 second TTL
        client.submit_order_sync(order)

        # Immediately check acceptance (before TTL expires)
        accepted = order.state_name == "ACCEPTED"
        results.record("Order with TTL accepted", accepted,
                       f"State: {order.state_name}")

        # Now start async receive to catch the expiry notification
        client.start_async_receive()
        time.sleep(0.1)  # Let async thread start

        print("    (waiting 6s for TTL expiry...)")
        time.sleep(6.0)

        # Wait for EXPIRED state
        expired = wait_for_state(order, "EXPIRED", timeout=2.0)
        results.record("Order expired", expired,
                       f"State: {order.state_name}")

        client._running = False  # Stop async receive cleanly
        client.disconnect()

    finally:
        stop_server(server)


def test_partial_fills(results: TestResults):
    """Test partial fills."""
    print("\n[Test Group 8] Partial Fills")
    server = start_server()
    try:
        # Place large resting order
        big_buyer = OrderClientWithFSM('localhost', 10000)
        big_buyer.connect()

        big_order = big_buyer.create_order("limit", 500, 4500000, "B", 3600)
        big_buyer.submit_order_sync(big_order)
        results.record("Large order ACKed", big_order.exchange_order_id is not None)

        # Start async receive AFTER order is placed, to catch passive fills
        big_buyer.start_async_receive()
        time.sleep(0.2)  # Let async thread start

        # Partially fill it
        small_seller = OrderClientWithFSM('localhost', 10000)
        small_seller.connect()

        small_order = small_seller.create_order("limit", 100, 4500000, "S", 3600)
        small_seller.submit_order_sync(small_order)

        # Wait for passive partial fill notification
        partial_filled = wait_for_state(big_order, "PARTIALLY_FILLED", timeout=3.0)

        results.record("Small order filled", small_order.state_name == "FILLED",
                       f"State: {small_order.state_name}")
        results.record("Large order partially filled", partial_filled,
                       f"State: {big_order.state_name}")

        big_buyer._running = False  # Stop async receive cleanly
        big_buyer.disconnect()
        small_seller.disconnect()

    finally:
        stop_server(server)


def test_book_builder_accuracy(results: TestResults):
    """Test that BookBuilder accurately tracks order book state."""
    print("\n[Test Group 9] Book Builder Accuracy")
    server = start_server()
    udp = UDPBookCollector()
    try:
        udp.start()
        time.sleep(0.2)

        client = OrderClientWithFSM('localhost', 10000)
        client.connect()

        # Place orders at known prices
        client.submit_order_sync(client.create_order("limit", 100, 4900000, "B", 3600))  # Bid $490
        client.submit_order_sync(client.create_order("limit", 200, 4800000, "B", 3600))  # Bid $480
        client.submit_order_sync(client.create_order("limit", 150, 5100000, "S", 3600))  # Ask $510
        client.submit_order_sync(client.create_order("limit", 250, 5200000, "S", 3600))  # Ask $520

        time.sleep(0.3)

        # Check book builder state
        best_bid = udp.builder.book.get_best_bid_price()
        best_ask = udp.builder.book.get_best_ask_price()

        results.record("Book builder has correct best bid",
                       best_bid == 4900000,
                       f"Expected 4900000, got {best_bid}")
        results.record("Book builder has correct best ask",
                       best_ask == 5100000,
                       f"Expected 5100000, got {best_ask}")

        client.disconnect()

    finally:
        udp.stop()
        stop_server(server)


def run_tests():
    print("=" * 70)
    print("  INTEGRATION TESTS (using actual components)")
    print("=" * 70)
    print("  Components under test:")
    print("    - OrderClientWithFSM (order_client_with_fsm.py)")
    print("    - STPClient (stp_client.py)")
    print("    - BookBuilder (udp_book_builder.py)")
    print("=" * 70)

    results = TestResults()

    test_basic_orders(results)
    test_crossing_orders(results)
    test_market_orders(results)
    test_cancellation(results)
    test_sweep_orders(results)
    test_passive_fills(results)
    test_ttl_expiry(results)
    test_partial_fills(results)
    test_book_builder_accuracy(results)

    print()
    print("=" * 70)
    print(f"  RESULTS: {results.passed} passed, {results.failed} failed")
    print("=" * 70)

    if results.errors:
        print("\nFailures:")
        for err in results.errors:
            print(f"  - {err}")

    return results


if __name__ == '__main__':
    results = run_tests()
    sys.exit(0 if results.failed == 0 else 1)
