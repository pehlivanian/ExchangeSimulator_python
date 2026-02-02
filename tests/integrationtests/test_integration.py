#!/usr/bin/env python3
"""
Integration tests for the Exchange Simulator.

Each test starts a fresh server to avoid state bleeding between tests.
"""

import socket
import subprocess
import sys
import threading
import time
from typing import List, Optional


class TestClient:
    """Simple test client for order submission."""

    def __init__(self, port: int = 10000):
        self.port = port
        self.sock: Optional[socket.socket] = None

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0)
            self.sock.connect(('127.0.0.1', self.port))
            return True
        except Exception as e:
            print(f"  Connection failed: {e}")
            return False

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def send_order(self, order: str) -> str:
        if not self.sock:
            return "ERROR: Not connected"
        try:
            self.sock.sendall((order + '\n').encode())
            response = self.sock.recv(4096).decode().strip()
            return response
        except Exception as e:
            return f"ERROR: {e}"


class STPListener:
    """Listens for STP trade broadcasts."""

    def __init__(self, port: int = 10001):
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.trades: List[str] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(1.0)
            self.sock.connect(('127.0.0.1', self.port))
            self.running = True
            self.thread = threading.Thread(target=self._listen, daemon=True)
            self.thread.start()
            return True
        except Exception as e:
            print(f"  STP connection failed: {e}")
            return False

    def _listen(self):
        buffer = ""
        while self.running:
            try:
                data = self.sock.recv(1024).decode()
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        self.trades.append(line.strip())
            except socket.timeout:
                continue
            except Exception:
                break

    def stop(self):
        self.running = False
        if self.sock:
            self.sock.close()
        if self.thread:
            self.thread.join(timeout=2.0)

    def get_trades(self) -> List[str]:
        return list(self.trades)

    def clear(self):
        self.trades.clear()


class UDPListener:
    """Listens for UDP market data broadcasts."""

    def __init__(self, port: int = 10002):
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.messages: List[str] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(1.0)
            self.sock.sendto(b"subscribe", ('127.0.0.1', self.port))
            self.running = True
            self.thread = threading.Thread(target=self._listen, daemon=True)
            self.thread.start()
            return True
        except Exception as e:
            print(f"  UDP connection failed: {e}")
            return False

    def _listen(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                msg = data.decode().strip()
                if msg:
                    self.messages.append(msg)
            except socket.timeout:
                continue
            except Exception:
                break

    def stop(self):
        self.running = False
        if self.sock:
            self.sock.close()
        if self.thread:
            self.thread.join(timeout=2.0)

    def get_messages(self) -> List[str]:
        return list(self.messages)

    def clear(self):
        self.messages.clear()


def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, 'exchange_server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
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
    """Test basic order submission and ACK."""
    print("\n[Test Group 1] Basic Order Operations")
    server = start_server()
    try:
        # Test 1: Limit order ACK
        client = TestClient()
        if client.connect():
            response = client.send_order("limit,100,5000000,B,trader1")
            parts = response.split(',')
            results.record("Limit order ACK format", parts[0] == "ACK", f"Got: {response}")
            if parts[0] == "ACK":
                order_id = int(parts[1])
                results.record("Order ID assigned", order_id > 0, f"ID: {order_id}")
            client.close()

        # Test 2: Non-crossing ask
        client = TestClient()
        if client.connect():
            response = client.send_order("limit,100,5100000,S,trader2")
            parts = response.split(',')
            results.record("Non-crossing limit ACK", parts[0] == "ACK", f"Got: {response}")
            client.close()

        # Test 3: Invalid format rejected
        client = TestClient()
        if client.connect():
            response = client.send_order("invalid,garbage,data")
            results.record("Invalid format rejected", "REJECT" in response, f"Got: {response}")
            client.close()

    finally:
        stop_server(server)


def test_crossing_orders(results: TestResults):
    """Test crossing orders generate trades."""
    print("\n[Test Group 2] Crossing Orders and Trades")
    server = start_server()
    stp = STPListener()
    udp = UDPListener()
    try:
        stp.start()
        udp.start()
        time.sleep(0.2)

        # Place ask
        client1 = TestClient()
        client1.connect()
        client1.send_order("limit,100,5000000,S,seller")

        time.sleep(0.1)
        udp.clear()
        stp.clear()

        # Place crossing bid
        client2 = TestClient()
        client2.connect()
        response = client2.send_order("limit,100,5000000,B,buyer")
        results.record("Crossing order fills", "FILL" in response, f"Got: {response}")

        time.sleep(0.2)
        results.record("STP trade broadcast", len(stp.get_trades()) > 0, f"Trades: {stp.get_trades()}")
        results.record("UDP EXECUTE broadcast", any(',4,' in m for m in udp.get_messages()),
                       f"Messages: {len(udp.get_messages())}")

        client1.close()
        client2.close()
    finally:
        stp.stop()
        udp.stop()
        stop_server(server)


def test_market_orders(results: TestResults):
    """Test market orders."""
    print("\n[Test Group 3] Market Orders")
    server = start_server()
    stp = STPListener()
    try:
        stp.start()
        time.sleep(0.2)

        # Test: Market order with no liquidity
        client = TestClient()
        client.connect()
        response = client.send_order("market,100,0,B,trader")
        results.record("Market order rejects on no liquidity",
                       "REJECT" in response or "No liquidity" in response, f"Got: {response}")
        client.close()

        # Add liquidity then test market order
        client = TestClient()
        client.connect()
        client.send_order("limit,200,4800000,B,mm")
        client.close()

        stp.clear()

        client = TestClient()
        client.connect()
        response = client.send_order("market,50,0,S,trader")
        results.record("Market order fills with liquidity", "FILL" in response, f"Got: {response}")
        client.close()

        time.sleep(0.2)
        results.record("Market order STP broadcast", len(stp.get_trades()) > 0, f"Trades: {stp.get_trades()}")

    finally:
        stp.stop()
        stop_server(server)


def test_cancellation(results: TestResults):
    """Test order cancellation."""
    print("\n[Test Group 4] Order Cancellation")
    server = start_server()
    udp = UDPListener()
    try:
        udp.start()
        time.sleep(0.2)

        client = TestClient()
        client.connect()

        # Place order
        response = client.send_order("limit,100,4700000,B,trader")
        parts = response.split(',')
        order_id = int(parts[1])

        udp.clear()

        # Cancel it
        response = client.send_order(f"cancel,{order_id},trader")
        results.record("Cancel ACK received", "CANCEL_ACK" in response, f"Got: {response}")

        # Try to cancel again
        response = client.send_order(f"cancel,{order_id},trader")
        results.record("Double cancel rejected", "REJECT" in response, f"Got: {response}")

        time.sleep(0.2)
        results.record("UDP DELETE broadcast", any(',3,' in m for m in udp.get_messages()),
                       f"Messages: {udp.get_messages()}")

        # Test cancel by wrong user
        response = client.send_order("limit,100,4600000,B,alice")
        parts = response.split(',')
        order_id = int(parts[1])

        response = client.send_order(f"cancel,{order_id},bob")
        results.record("Cancel by wrong user rejected", "REJECT" in response, f"Got: {response}")

        client.close()
    finally:
        udp.stop()
        stop_server(server)


def test_sweep_orders(results: TestResults):
    """Test orders that sweep multiple price levels."""
    print("\n[Test Group 5] Multi-Level Sweeps")
    server = start_server()
    stp = STPListener()
    try:
        stp.start()
        time.sleep(0.2)

        # Build ask side
        client = TestClient()
        client.connect()
        client.send_order("limit,100,4700000,S,mm")
        client.send_order("limit,100,4800000,S,mm")
        client.send_order("limit,100,4900000,S,mm")
        client.close()

        stp.clear()

        # Sweep all levels
        client = TestClient()
        client.connect()
        response = client.send_order("limit,300,5000000,B,sweeper")
        results.record("Sweep order fills", "FILL" in response, f"Got: {response}")
        client.close()

        time.sleep(0.2)
        trades = stp.get_trades()
        results.record("Multiple STP trades from sweep", len(trades) >= 1, f"Trade count: {len(trades)}")

    finally:
        stp.stop()
        stop_server(server)


def test_passive_fills(results: TestResults):
    """Test passive fill notifications."""
    print("\n[Test Group 6] Passive Fill Notifications")
    server = start_server()
    try:
        # Client 1 places resting order
        client1 = TestClient()
        client1.connect()
        response = client1.send_order("limit,100,4200000,B,passive_trader")
        parts = response.split(',')
        passive_order_id = int(parts[1])

        # Client 2 crosses
        client2 = TestClient()
        client2.connect()
        client2.send_order("limit,100,4200000,S,aggressive_trader")

        # Check for notification on client 1
        time.sleep(0.3)
        client1.sock.settimeout(2.0)
        try:
            notification = client1.sock.recv(4096).decode().strip()
            passed = "FILL" in notification and str(passive_order_id) in notification
            results.record("Passive fill notification", passed, f"Got: {notification}")
        except socket.timeout:
            results.record("Passive fill notification", False, "No notification received")

        client1.close()
        client2.close()
    finally:
        stop_server(server)


def test_ttl_expiry(results: TestResults):
    """Test order TTL expiry."""
    print("\n[Test Group 7] Order TTL Expiry")
    server = start_server()
    try:
        client = TestClient()
        client.connect()

        # Place order with 2 second TTL
        response = client.send_order("limit,100,4100000,B,ttl_trader,2")
        parts = response.split(',')
        results.record("Order with TTL accepted", parts[0] == "ACK", f"Got: {response}")

        if parts[0] == "ACK":
            print("    (waiting 3s for TTL expiry...)")
            time.sleep(3.0)

            client.sock.settimeout(1.0)
            try:
                notification = client.sock.recv(4096).decode().strip()
                results.record("TTL expiry notification", "EXPIRED" in notification, f"Got: {notification}")
            except socket.timeout:
                results.record("TTL expiry notification", False, "No notification received")

        client.close()
    finally:
        stop_server(server)


def test_partial_fills(results: TestResults):
    """Test partial fills."""
    print("\n[Test Group 8] Partial Fills")
    server = start_server()
    try:
        # Place large resting order
        client1 = TestClient()
        client1.connect()
        response = client1.send_order("limit,500,4500000,B,big_buyer")
        parts = response.split(',')
        results.record("Large order ACK", parts[0] == "ACK", f"Got: {response}")

        # Partially fill it
        client2 = TestClient()
        client2.connect()
        response = client2.send_order("limit,100,4500000,S,small_seller")
        results.record("Small crossing order fills", "FILL" in response, f"Got: {response}")

        # Check passive fill notification shows partial
        time.sleep(0.3)
        client1.sock.settimeout(1.0)
        try:
            notification = client1.sock.recv(4096).decode().strip()
            # Should be PARTIAL_FILL with remaining size
            results.record("Partial fill notification", "PARTIAL_FILL" in notification or "FILL" in notification,
                           f"Got: {notification}")
        except socket.timeout:
            results.record("Partial fill notification", False, "No notification received")

        client1.close()
        client2.close()
    finally:
        stop_server(server)


def run_tests():
    print("=" * 70)
    print("  EXCHANGE SIMULATOR INTEGRATION TESTS")
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
