#!/usr/bin/env python3
"""
Multi-client scenario integration tests.

Simulates realistic trading scenarios with multiple clients interacting,
verifying that all components work together correctly:
- Order clients receive their own fills
- STP broadcasts all trades to all subscribers
- UDP market data broadcasts all order book changes
"""

import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set


class OrderClient:
    """A test order client that tracks its orders and fills."""

    def __init__(self, name: str):
        self.name = name
        self.sock: Optional[socket.socket] = None
        self.orders: Dict[int, dict] = {}  # order_id -> order info
        self.fills: List[str] = []
        self._async_sock: Optional[socket.socket] = None
        self._running = False
        self._listener_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def connect(self, port: int = 10000) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0)
            self.sock.connect(('127.0.0.1', port))
            self._running = True
            self._listener_thread = threading.Thread(target=self._listen_for_async, daemon=True)
            self._listener_thread.start()
            return True
        except Exception as e:
            print(f"  {self.name}: Connection failed: {e}")
            return False

    def _listen_for_async(self):
        """Listen for async notifications (passive fills, expiry)."""
        while self._running:
            try:
                # Use select to check if data available without blocking send_order
                import select
                ready, _, _ = select.select([self.sock], [], [], 0.1)
                if ready and self._running:
                    with self._lock:
                        self.sock.setblocking(False)
                        try:
                            data = self.sock.recv(4096, socket.MSG_PEEK)
                            if data:
                                # Check if this looks like an async message
                                preview = data.decode()
                                if any(x in preview for x in ['FILL', 'PARTIAL_FILL', 'EXPIRED']):
                                    # Actually read it
                                    data = self.sock.recv(4096)
                                    for line in data.decode().strip().split('\n'):
                                        if line:
                                            self.fills.append(line)
                        except BlockingIOError:
                            pass
                        finally:
                            self.sock.setblocking(True)
                            self.sock.settimeout(5.0)
            except Exception:
                if self._running:
                    time.sleep(0.05)

    def send_order(self, order: str) -> str:
        if not self.sock:
            return "ERROR: Not connected"
        try:
            with self._lock:
                self.sock.setblocking(True)
                self.sock.settimeout(5.0)
                self.sock.sendall((order + '\n').encode())
                response = self.sock.recv(4096).decode().strip()

            # Track the order if ACKed
            if response.startswith('ACK'):
                parts = response.split(',')
                order_id = int(parts[1])
                self.orders[order_id] = {'order': order, 'response': response}
            elif 'FILL' in response:
                self.fills.append(response)

            return response
        except Exception as e:
            return f"ERROR: {e}"

    def close(self):
        self._running = False
        if self._listener_thread:
            self._listener_thread.join(timeout=1.0)
        if self.sock:
            self.sock.close()
            self.sock = None


class STPCollector:
    """Collects all STP trade broadcasts."""

    def __init__(self, port: int = 10001):
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.trades: List[str] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(1.0)
            self.sock.connect(('127.0.0.1', self.port))
            self._running = True
            self._thread = threading.Thread(target=self._listen, daemon=True)
            self._thread.start()
            return True
        except Exception as e:
            print(f"  STP collector failed: {e}")
            return False

    def _listen(self):
        buffer = ""
        while self._running:
            try:
                data = self.sock.recv(1024).decode()
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        with self._lock:
                            self.trades.append(line.strip())
            except socket.timeout:
                continue
            except Exception:
                break

    def stop(self):
        self._running = False
        if self.sock:
            self.sock.close()
        if self._thread:
            self._thread.join(timeout=2.0)

    def get_trades(self) -> List[str]:
        with self._lock:
            return list(self.trades)


class MarketDataCollector:
    """Collects all UDP market data broadcasts."""

    def __init__(self, port: int = 10002):
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.messages: List[str] = []
        self.inserts: List[str] = []
        self.executes: List[str] = []
        self.deletes: List[str] = []
        self.cancels: List[str] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(1.0)
            self.sock.sendto(b"subscribe", ('127.0.0.1', self.port))
            self._running = True
            self._thread = threading.Thread(target=self._listen, daemon=True)
            self._thread.start()
            return True
        except Exception as e:
            print(f"  Market data collector failed: {e}")
            return False

    def _listen(self):
        while self._running:
            try:
                data, addr = self.sock.recvfrom(1024)
                msg = data.decode().strip()
                if msg:
                    with self._lock:
                        self.messages.append(msg)
                        # Parse message type (field index 1)
                        parts = msg.split(',')
                        if len(parts) >= 2:
                            msg_type = parts[1]
                            if msg_type == '1':
                                self.inserts.append(msg)
                            elif msg_type == '2':
                                self.cancels.append(msg)
                            elif msg_type == '3':
                                self.deletes.append(msg)
                            elif msg_type == '4':
                                self.executes.append(msg)
            except socket.timeout:
                continue
            except Exception:
                break

    def stop(self):
        self._running = False
        if self.sock:
            self.sock.close()
        if self._thread:
            self._thread.join(timeout=2.0)

    def get_summary(self) -> dict:
        with self._lock:
            return {
                'total': len(self.messages),
                'inserts': len(self.inserts),
                'cancels': len(self.cancels),
                'deletes': len(self.deletes),
                'executes': len(self.executes),
            }


def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, 'exchange_server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
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
        self.checks: List[tuple] = []  # (name, passed, details)

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

    1. Alice places a bid
    2. Bob places an ask that crosses Alice's bid
    3. Verify both get fill notifications
    4. Verify STP broadcasts the trade
    5. Verify market data shows INSERT then EXECUTE
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Two Traders Crossing")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = MarketDataCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        alice = OrderClient(name="Alice")
        bob = OrderClient(name="Bob")

        alice.connect()
        bob.connect()
        time.sleep(0.2)

        print("\n  Step 1: Alice places bid 100 shares @ $50")
        alice_response = alice.send_order("limit,100,500000,B,alice")
        print(f"    Alice response: {alice_response}")
        results.check("Alice bid ACKed", alice_response.startswith("ACK"))

        time.sleep(0.2)

        print("\n  Step 2: Bob places ask 100 shares @ $50 (crosses)")
        bob_response = bob.send_order("limit,100,500000,S,bob")
        print(f"    Bob response: {bob_response}")
        results.check("Bob ask filled", "FILL" in bob_response)

        time.sleep(0.5)

        print("\n  Step 3: Verify Alice got passive fill notification")
        print(f"    Alice fills: {alice.fills}")
        results.check("Alice received fill notification",
                      any("FILL" in f for f in alice.fills),
                      f"Fills: {alice.fills}")

        print("\n  Step 4: Verify STP broadcast")
        trades = stp.get_trades()
        print(f"    STP trades: {trades}")
        results.check("STP broadcast trade", len(trades) >= 1, f"Count: {len(trades)}")

        print("\n  Step 5: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data INSERT broadcast", md_summary['inserts'] >= 1)
        results.check("Market data EXECUTE broadcast", md_summary['executes'] >= 1)

        alice.close()
        bob.close()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_market_maker_and_takers(results: ScenarioResults):
    """
    Scenario: One market maker provides liquidity, two takers hit it.

    1. MM places bids and asks at multiple levels
    2. Taker1 buys (hits asks)
    3. Taker2 sells (hits bids)
    4. Verify all trades on STP
    5. Verify each participant gets correct fills
    6. Verify market data shows all activity
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Market Maker with Multiple Takers")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = MarketDataCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        mm = OrderClient(name="MarketMaker")
        taker1 = OrderClient(name="Taker1")
        taker2 = OrderClient(name="Taker2")

        mm.connect()
        taker1.connect()
        taker2.connect()
        time.sleep(0.2)

        print("\n  Step 1: Market Maker places quotes")
        # Bids
        mm.send_order("limit,100,490000,B,mm")  # $49.00
        mm.send_order("limit,100,495000,B,mm")  # $49.50
        # Asks
        mm.send_order("limit,100,505000,S,mm")  # $50.50
        mm.send_order("limit,100,510000,S,mm")  # $51.00
        print(f"    MM placed {len(mm.orders)} orders")
        results.check("MM orders placed", len(mm.orders) == 4, f"Count: {len(mm.orders)}")

        time.sleep(0.2)
        initial_inserts = mkt.get_summary()['inserts']

        print("\n  Step 2: Taker1 buys 150 shares (sweeps 2 ask levels)")
        taker1_response = taker1.send_order("limit,150,520000,B,taker1")
        print(f"    Taker1 response: {taker1_response}")
        results.check("Taker1 gets fill", "FILL" in taker1_response)

        time.sleep(0.3)

        print("\n  Step 3: Taker2 sells 50 shares (hits best bid)")
        taker2_response = taker2.send_order("limit,50,490000,S,taker2")
        print(f"    Taker2 response: {taker2_response}")
        results.check("Taker2 gets fill", "FILL" in taker2_response)

        time.sleep(0.5)

        print("\n  Step 4: Verify Market Maker got passive fills")
        print(f"    MM fills: {mm.fills}")
        # MM should have received fills for the crossed orders
        results.check("MM received passive fills", len(mm.fills) >= 2,
                      f"Fill count: {len(mm.fills)}")

        print("\n  Step 5: Verify STP has all trades")
        trades = stp.get_trades()
        print(f"    STP trade count: {len(trades)}")
        # Taker1 swept 2 levels (100+50), Taker2 hit 1 level - should be multiple trades
        results.check("STP has multiple trades", len(trades) >= 2, f"Trades: {len(trades)}")

        print("\n  Step 6: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data has INSERTs", md_summary['inserts'] >= initial_inserts)
        results.check("Market data has EXECUTEs", md_summary['executes'] >= 2)

        mm.close()
        taker1.close()
        taker2.close()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_order_lifecycle(results: ScenarioResults):
    """
    Scenario: Full order lifecycle - place, partial fill, cancel remainder.

    1. Trader places large order
    2. Another trader partially fills it
    3. First trader cancels remainder
    4. Verify partial fill notification
    5. Verify cancel ACK
    6. Verify market data shows INSERT, EXECUTE, DELETE
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Full Order Lifecycle")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = MarketDataCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        trader1 = OrderClient(name="Trader1")
        trader2 = OrderClient(name="Trader2")

        trader1.connect()
        trader2.connect()
        time.sleep(0.2)

        print("\n  Step 1: Trader1 places large bid (500 shares @ $45)")
        response = trader1.send_order("limit,500,450000,B,trader1")
        print(f"    Response: {response}")
        order_id = int(response.split(',')[1])
        results.check("Large order ACKed", response.startswith("ACK"))

        time.sleep(0.2)

        print("\n  Step 2: Trader2 sells 100 shares (partial fill)")
        response = trader2.send_order("limit,100,450000,S,trader2")
        print(f"    Response: {response}")
        results.check("Trader2 fill", "FILL" in response)

        time.sleep(0.3)

        print("\n  Step 3: Verify Trader1 got partial fill notification")
        print(f"    Trader1 fills: {trader1.fills}")
        has_partial = any("PARTIAL_FILL" in f or "FILL" in f for f in trader1.fills)
        results.check("Trader1 partial fill notification", has_partial)

        print("\n  Step 4: Trader1 cancels remainder")
        cancel_response = trader1.send_order(f"cancel,{order_id},trader1")
        print(f"    Cancel response: {cancel_response}")
        results.check("Cancel ACKed", "CANCEL_ACK" in cancel_response)

        time.sleep(0.3)

        print("\n  Step 5: Verify market data lifecycle")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Has INSERT", md_summary['inserts'] >= 1)
        results.check("Has EXECUTE", md_summary['executes'] >= 1)
        results.check("Has DELETE", md_summary['deletes'] >= 1)

        trader1.close()
        trader2.close()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def scenario_multiple_stp_subscribers(results: ScenarioResults):
    """
    Scenario: Multiple STP subscribers all receive same trades.

    1. Start 3 STP subscribers
    2. Execute a trade
    3. Verify all 3 subscribers received the trade
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

        alice = OrderClient(name="Alice")
        bob = OrderClient(name="Bob")
        alice.connect()
        bob.connect()
        time.sleep(0.2)

        print("\n  Step 1: Execute a trade")
        alice.send_order("limit,100,500000,B,alice")
        time.sleep(0.1)
        bob.send_order("limit,100,500000,S,bob")
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
                      len(trades1) == len(trades2) == len(trades3),
                      f"Counts: {len(trades1)}, {len(trades2)}, {len(trades3)}")

        alice.close()
        bob.close()

    finally:
        stp1.stop()
        stp2.stop()
        stp3.stop()
        stop_server(server)


def scenario_rapid_trading(results: ScenarioResults):
    """
    Scenario: Rapid-fire trading to stress test.

    1. Two traders rapidly exchange orders
    2. Verify trade counts match
    3. Verify no messages lost
    """
    print("\n" + "=" * 70)
    print("SCENARIO: Rapid Trading Stress Test")
    print("=" * 70)

    server = start_server()
    stp = STPCollector()
    mkt = MarketDataCollector()

    try:
        stp.start()
        mkt.start()
        time.sleep(0.3)

        buyer = OrderClient(name="Buyer")
        seller = OrderClient(name="Seller")
        buyer.connect()
        seller.connect()
        time.sleep(0.2)

        num_trades = 20
        print(f"\n  Step 1: Execute {num_trades} rapid trades")

        buyer_fills = 0
        seller_fills = 0

        for i in range(num_trades):
            price = 500000 + (i * 1000)  # Varying prices

            # Seller posts, buyer takes
            seller.send_order(f"limit,10,{price},S,seller")
            response = buyer.send_order(f"limit,10,{price},B,buyer")
            if "FILL" in response:
                buyer_fills += 1

        time.sleep(1.0)  # Let everything settle

        print(f"    Buyer immediate fills: {buyer_fills}")
        print(f"    Seller passive fills: {len(seller.fills)}")

        results.check("All buyer orders filled", buyer_fills == num_trades,
                      f"Expected {num_trades}, got {buyer_fills}")

        print("\n  Step 2: Verify STP trade count")
        trades = stp.get_trades()
        print(f"    STP trades: {len(trades)}")
        results.check("STP has all trades", len(trades) >= num_trades,
                      f"Expected >= {num_trades}, got {len(trades)}")

        print("\n  Step 3: Verify market data")
        md_summary = mkt.get_summary()
        print(f"    Market data: {md_summary}")
        results.check("Market data INSERTs", md_summary['inserts'] >= num_trades)
        results.check("Market data EXECUTEs", md_summary['executes'] >= num_trades)

        buyer.close()
        seller.close()

    finally:
        stp.stop()
        mkt.stop()
        stop_server(server)


def run_scenarios():
    print("=" * 70)
    print("  MULTI-CLIENT SCENARIO INTEGRATION TESTS")
    print("=" * 70)

    results = ScenarioResults()

    scenario_two_traders_crossing(results)
    scenario_market_maker_and_takers(results)
    scenario_order_lifecycle(results)
    scenario_multiple_stp_subscribers(results)
    scenario_rapid_trading(results)

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
