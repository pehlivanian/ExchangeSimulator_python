#!/usr/bin/env python3
"""
Multi-client scenario integration tests.

Uses the ACTUAL client components:
- OrderClientWithFSM from order_client_with_fsm.py
- STPClient from stp_client.py
- BookBuilder from udp_book_builder.py

Simulates realistic trading scenarios with multiple clients interacting.
"""

import socket
import subprocess
import sys
import threading
import time
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, '../..')

from order_client_with_fsm import OrderClientWithFSM, ManagedOrder
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
    time.sleep(1.0)
    return proc


def stop_server(proc: subprocess.Popen):
    proc.terminate()
    try:
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()


class ScenarioResults:
    def __init__(self):
        self.checks: List[tuple] = []

    def check(self, name: str, condition: bool, details: str = ""):
        self.checks.append((name, condition, details))
        status = "✓" if condition else "✗"
        print(f"    {status} {name}" + (f": {details}" if details and not condition else ""))

    def summary(self) -> tuple:
        passed = sum(1 for _, p, _ in self.checks if p)
        failed = sum(1 for _, p, _ in self.checks if not p)
        return passed, failed


def scenario_two_traders_crossing(results: ScenarioResults):
    """
    Scenario: Two traders, Alice and Bob, trade with each other.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Two Traders Crossing")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = UDPBookCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        # Alice connects and places bid
        alice = OrderClientWithFSM('localhost', 10000)
        alice.connect()
        alice.start_async_receive()

        # Bob connects
        bob = OrderClientWithFSM('localhost', 10000)
        bob.connect()
        bob.start_async_receive()

        print("\n  Step 1: Alice places bid 100 shares @ $50")
        alice_order = alice.create_order("limit", 100, 500000, "B", 3600)
        alice.submit_order_sync(alice_order)
        print(f"    Alice order state: {alice_order.state_name}")
        results.check("Alice bid accepted", alice_order.state_name == "ACCEPTED")

        time.sleep(0.2)

        print("\n  Step 2: Bob places ask 100 shares @ $50 (crosses)")
        bob_order = bob.create_order("limit", 100, 500000, "S", 3600)
        bob.submit_order_sync(bob_order)
        print(f"    Bob order state: {bob_order.state_name}")
        results.check("Bob ask filled", bob_order.state_name == "FILLED")

        time.sleep(0.5)

        print("\n  Step 3: Verify Alice got fill (passive fill notification)")
        print(f"    Alice order state: {alice_order.state_name}")
        results.check("Alice received fill", alice_order.state_name == "FILLED")

        print("\n  Step 4: Verify STP broadcast")
        trades = stp.get_trades()
        print(f"    STP trades: {trades}")
        results.check("STP broadcast trade", len(trades) >= 1)

        print("\n  Step 5: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data INSERT broadcast", md_summary['inserts'] >= 1)
        results.check("Market data EXECUTE broadcast", md_summary['executes'] >= 1)

        alice.disconnect()
        bob.disconnect()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_market_maker_and_takers(results: ScenarioResults):
    """
    Scenario: One market maker provides liquidity, two takers hit it.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Market Maker with Multiple Takers")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = UDPBookCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        # Market maker
        mm = OrderClientWithFSM('localhost', 10000)
        mm.connect()
        mm.start_async_receive()

        # Takers
        taker1 = OrderClientWithFSM('localhost', 10000)
        taker1.connect()
        taker1.start_async_receive()

        taker2 = OrderClientWithFSM('localhost', 10000)
        taker2.connect()
        taker2.start_async_receive()

        print("\n  Step 1: Market Maker places quotes")
        mm_orders = []
        mm_orders.append(mm.create_order("limit", 100, 490000, "B", 3600))  # Bid $49
        mm_orders.append(mm.create_order("limit", 100, 495000, "B", 3600))  # Bid $49.50
        mm_orders.append(mm.create_order("limit", 100, 505000, "S", 3600))  # Ask $50.50
        mm_orders.append(mm.create_order("limit", 100, 510000, "S", 3600))  # Ask $51

        for order in mm_orders:
            mm.submit_order_sync(order)

        accepted_count = sum(1 for o in mm_orders if o.state_name == "ACCEPTED")
        print(f"    MM placed {accepted_count} orders")
        results.check("MM orders placed", accepted_count == 4)

        time.sleep(0.2)

        print("\n  Step 2: Taker1 buys 150 shares (sweeps 2 ask levels)")
        taker1_order = taker1.create_order("limit", 150, 520000, "B", 3600)
        taker1.submit_order_sync(taker1_order)
        print(f"    Taker1 order state: {taker1_order.state_name}")
        results.check("Taker1 gets fill", taker1_order.state_name == "FILLED")

        time.sleep(0.3)

        print("\n  Step 3: Taker2 sells 50 shares (hits best bid)")
        taker2_order = taker2.create_order("limit", 50, 490000, "S", 3600)
        taker2.submit_order_sync(taker2_order)
        print(f"    Taker2 order state: {taker2_order.state_name}")
        results.check("Taker2 gets fill", taker2_order.state_name == "FILLED")

        time.sleep(0.5)

        print("\n  Step 4: Verify Market Maker got passive fills")
        filled_mm_orders = sum(1 for o in mm_orders if o.state_name in ["FILLED", "PARTIALLY_FILLED"])
        print(f"    MM orders filled/partial: {filled_mm_orders}")
        results.check("MM received passive fills", filled_mm_orders >= 2)

        print("\n  Step 5: Verify STP has all trades")
        trades = stp.get_trades()
        print(f"    STP trade count: {len(trades)}")
        results.check("STP has multiple trades", len(trades) >= 2)

        print("\n  Step 6: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data has INSERTs", md_summary['inserts'] >= 4)
        results.check("Market data has EXECUTEs", md_summary['executes'] >= 2)

        mm.disconnect()
        taker1.disconnect()
        taker2.disconnect()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_order_lifecycle(results: ScenarioResults):
    """
    Scenario: Full order lifecycle - place, partial fill, cancel remainder.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Full Order Lifecycle")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = UDPBookCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        trader1 = OrderClientWithFSM('localhost', 10000)
        trader1.connect()
        trader1.start_async_receive()

        trader2 = OrderClientWithFSM('localhost', 10000)
        trader2.connect()
        trader2.start_async_receive()

        print("\n  Step 1: Trader1 places large bid (500 shares @ $45)")
        big_order = trader1.create_order("limit", 500, 450000, "B", 3600)
        trader1.submit_order_sync(big_order)
        print(f"    State: {big_order.state_name}")
        results.check("Large order accepted", big_order.state_name == "ACCEPTED")

        time.sleep(0.2)

        print("\n  Step 2: Trader2 sells 100 shares (partial fill)")
        small_order = trader2.create_order("limit", 100, 450000, "S", 3600)
        trader2.submit_order_sync(small_order)
        print(f"    Trader2 state: {small_order.state_name}")
        results.check("Trader2 fill", small_order.state_name == "FILLED")

        time.sleep(0.3)

        print("\n  Step 3: Verify Trader1 got partial fill notification")
        print(f"    Trader1 state: {big_order.state_name}")
        results.check("Trader1 partial fill", big_order.state_name == "PARTIALLY_FILLED")

        print("\n  Step 4: Trader1 cancels remainder")
        cancel_result = trader1.cancel_order(big_order)
        time.sleep(0.2)
        print(f"    Cancel result: {cancel_result}, State: {big_order.state_name}")
        results.check("Cancel succeeded", big_order.state_name == "CANCELLED")

        print("\n  Step 5: Verify market data lifecycle")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Has INSERT", md_summary['inserts'] >= 1)
        results.check("Has EXECUTE", md_summary['executes'] >= 1)
        results.check("Has DELETE", md_summary['deletes'] >= 1)

        trader1.disconnect()
        trader2.disconnect()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_multiple_stp_subscribers(results: ScenarioResults):
    """
    Scenario: Multiple STP subscribers all receive same trades.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Multiple STP Subscribers")
    print("=" * 70)

    server = start_server()
    stp1 = STPCollector()
    stp2 = STPCollector()
    stp3 = STPCollector()

    try:
        stp1.start()
        stp2.start()
        stp3.start()
        time.sleep(0.3)

        alice = OrderClientWithFSM('localhost', 10000)
        alice.connect()
        alice.start_async_receive()

        bob = OrderClientWithFSM('localhost', 10000)
        bob.connect()
        bob.start_async_receive()

        print("\n  Step 1: Execute a trade")
        alice.submit_order_sync(alice.create_order("limit", 100, 500000, "B", 3600))
        time.sleep(0.1)
        bob.submit_order_sync(bob.create_order("limit", 100, 500000, "S", 3600))
        time.sleep(0.5)

        print("\n  Step 2: Verify all STP subscribers received trade")
        trades1 = stp1.get_trades()
        trades2 = stp2.get_trades()
        trades3 = stp3.get_trades()

        print(f"    STP1: {len(trades1)} trades")
        print(f"    STP2: {len(trades2)} trades")
        print(f"    STP3: {len(trades3)} trades")

        results.check("STP1 received trade", len(trades1) >= 1)
        results.check("STP2 received trade", len(trades2) >= 1)
        results.check("STP3 received trade", len(trades3) >= 1)
        results.check("All subscribers got same count",
                      len(trades1) == len(trades2) == len(trades3))

        alice.disconnect()
        bob.disconnect()

    finally:
        stp1.stop()
        stp2.stop()
        stp3.stop()
        stop_server(server)


def scenario_rapid_trading(results: ScenarioResults):
    """
    Scenario: Rapid-fire trading to stress test.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Rapid Trading Stress Test")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = UDPBookCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        buyer = OrderClientWithFSM('localhost', 10000)
        buyer.connect()
        buyer.start_async_receive()

        seller = OrderClientWithFSM('localhost', 10000)
        seller.connect()
        seller.start_async_receive()

        num_trades = 20
        print(f"\n  Step 1: Execute {num_trades} rapid trades")

        filled_count = 0
        for i in range(num_trades):
            price = 500000 + (i * 1000)

            # Seller posts, buyer takes
            sell_order = seller.create_order("limit", 10, price, "S", 3600)
            seller.submit_order_sync(sell_order)

            buy_order = buyer.create_order("limit", 10, price, "B", 3600)
            buyer.submit_order_sync(buy_order)

            if buy_order.state_name == "FILLED":
                filled_count += 1

        time.sleep(1.0)

        print(f"    Buyer fills: {filled_count}")
        results.check("All buyer orders filled", filled_count == num_trades)

        print("\n  Step 2: Verify STP trade count")
        trades = stp.get_trades()
        print(f"    STP trades: {len(trades)}")
        results.check("STP has all trades", len(trades) >= num_trades)

        print("\n  Step 3: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data INSERTs", md_summary['inserts'] >= num_trades)
        results.check("Market data EXECUTEs", md_summary['executes'] >= num_trades)

        buyer.disconnect()
        seller.disconnect()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_book_builder_consistency(results: ScenarioResults):
    """
    Scenario: Verify BookBuilder tracks order book accurately through a sequence.
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Book Builder Consistency")
    print("=" * 70)

    server = start_server()
    mkt = UDPBookCollector()

    try:
        mkt.start()
        time.sleep(0.3)

        client = OrderClientWithFSM('localhost', 10000)
        client.connect()
        client.start_async_receive()

        print("\n  Step 1: Build initial book")
        orders = []
        orders.append(client.create_order("limit", 100, 4900000, "B", 3600))  # Bid $490
        orders.append(client.create_order("limit", 200, 4800000, "B", 3600))  # Bid $480
        orders.append(client.create_order("limit", 150, 5100000, "S", 3600))  # Ask $510
        orders.append(client.create_order("limit", 250, 5200000, "S", 3600))  # Ask $520

        for order in orders:
            client.submit_order_sync(order)

        time.sleep(0.3)

        best_bid = mkt.builder.book.get_best_bid_price()
        best_ask = mkt.builder.book.get_best_ask_price()
        print(f"    Book builder: bid={best_bid}, ask={best_ask}")
        results.check("Initial best bid correct", best_bid == 4900000)
        results.check("Initial best ask correct", best_ask == 5100000)

        print("\n  Step 2: Execute trade (take best ask)")
        buy_order = client.create_order("limit", 150, 5100000, "B", 3600)
        client.submit_order_sync(buy_order)

        time.sleep(0.3)

        new_best_ask = mkt.builder.book.get_best_ask_price()
        print(f"    New best ask: {new_best_ask}")
        results.check("Best ask updated after trade", new_best_ask == 5200000)

        print("\n  Step 3: Cancel order (best bid)")
        cancel_result = client.cancel_order(orders[0])  # Cancel best bid
        time.sleep(0.3)

        new_best_bid = mkt.builder.book.get_best_bid_price()
        print(f"    New best bid: {new_best_bid}")
        results.check("Best bid updated after cancel", new_best_bid == 4800000)

        client.disconnect()

    finally:
        mkt.stop()
        stop_server(server)


def run_scenarios():
    print("=" * 70)
    print("  MULTI-CLIENT SCENARIO TESTS (using actual components)")
    print("=" * 70)
    print("  Components under test:")
    print("    - OrderClientWithFSM (order_client_with_fsm.py)")
    print("    - STPClient (stp_client.py)")
    print("    - BookBuilder (udp_book_builder.py)")
    print("=" * 70)

    results = ScenarioResults()

    scenario_two_traders_crossing(results)
    scenario_market_maker_and_takers(results)
    scenario_order_lifecycle(results)
    scenario_multiple_stp_subscribers(results)
    scenario_rapid_trading(results)
    scenario_book_builder_consistency(results)

    passed, failed = results.summary()

    print("\n" + "=" * 70)
    print(f"  FINAL RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)

    if failed > 0:
        print("\nFailed checks:")
        for name, ok, details in results.checks:
            if not ok:
                print(f"  - {name}: {details}")

    return failed == 0


if __name__ == '__main__':
    success = run_scenarios()
    sys.exit(0 if success else 1)
