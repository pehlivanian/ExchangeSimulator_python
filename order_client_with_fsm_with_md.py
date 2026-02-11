#!/usr/bin/env python3
"""
Order Client with FSM + Market Data - Extends OrderClientWithFSM with a live
market data feed so that student-implemented order types (pegged orders,
pro-rata execution, etc.) can react to BBO and depth-of-book changes.

Architecture
------------
    ┌─────────────────────────────────────────────┐
    │        OrderClientWithFSMAndMarketData      │
    │                                             │
    │  ┌──────────────┐   ┌─────────────────────┐ │
    │  │ TCP order    │   │ UDP market data     │ │
    │  │ connection   │   │ subscription        │ │
    │  │ (inherited)  │   │ (new)               │ │
    │  └──────────────┘   └─────────────────────┘ │
    │          │                    │             │
    │          ▼                    ▼             │
    │   OrderClientWithFSM    BookBuilder         │
    │   (order lifecycle)     (book from feed)    │
    │                              │              │
    │                    ┌─────────┴──────────┐   │
    │                    │ on_bbo_change()    │   │
    │                    │ on_market_data()   │   │
    │                    │   (student hooks)  │   │
    │                    └───────────────────-┘   │
    └─────────────────────────────────────────────┘

Usage
-----
    # Subclass and override the hooks:

    class MyPeggedOrderClient(OrderClientWithFSMAndMarketData):

        def on_bbo_change(self, old_bbo, new_bbo):
            # TODO: Student implements pegged order logic here
            pass

    client = MyPeggedOrderClient(
        host='localhost', order_port=10000, md_port=10002
    )
    client.connect()
    client.start()
"""

import argparse
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from order_client_with_fsm import (
    OrderClientWithFSM, ManagedOrder, ExchangeMessage,
)
from order_book import OrderBook, PriceLevel
from udp_book_builder import BookBuilder
from messages import EventLOBSTER, EventType, DEFAULT_TTL_SECONDS


# ---------------------------------------------------------------------------
# BBO snapshot — simple container students can inspect
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BBO:
    """Best Bid and Offer snapshot."""
    bid_price: Optional[int] = None   # LOBSTER format (price * 10000)
    bid_size: Optional[int] = None
    ask_price: Optional[int] = None
    ask_size: Optional[int] = None

    @property
    def mid_price(self) -> Optional[float]:
        """Mid-price in LOBSTER integer format, or None if either side empty."""
        if self.bid_price is not None and self.ask_price is not None:
            return (self.bid_price + self.ask_price) / 2.0
        return None

    @property
    def spread(self) -> Optional[int]:
        """Spread in LOBSTER integer format, or None if either side empty."""
        if self.bid_price is not None and self.ask_price is not None:
            return self.ask_price - self.bid_price
        return None

    def __str__(self) -> str:
        def _fmt(p):
            return f"${p / 10000:.2f}" if p is not None else "---"
        bp = _fmt(self.bid_price)
        ap = _fmt(self.ask_price)
        bs = self.bid_size if self.bid_size is not None else "---"
        as_ = self.ask_size if self.ask_size is not None else "---"
        spread = f"${self.spread / 10000:.2f}" if self.spread is not None else "---"
        return f"BBO({bp} x {bs}  /  {ap} x {as_}  spread={spread})"


# ---------------------------------------------------------------------------
# Main client class
# ---------------------------------------------------------------------------

class OrderClientWithFSMAndMarketData(OrderClientWithFSM):
    """
    OrderClientWithFSM extended with a UDP market data feed.

    Subscribes to the exchange's UDP market data, reconstructs the order book
    locally via BookBuilder, and exposes:

        - get_bbo()              → current BBO snapshot
        - get_bid_levels(depth)  → list of PriceLevel for bids
        - get_ask_levels(depth)  → list of PriceLevel for asks
        - get_snapshot(depth)    → (bids, asks) tuple of PriceLevel lists

    Override these hooks in your subclass:

        - on_bbo_change(old_bbo, new_bbo)   → called when BBO changes
        - on_market_data(event_line)         → called on every market data msg
    """

    def __init__(self, host: str = 'localhost',
                 order_port: int = 10000,
                 md_port: int = 10002):
        super().__init__(host, order_port)

        self._md_host = host
        self._md_port = md_port
        self._md_socket: Optional[socket.socket] = None
        self._md_thread: Optional[threading.Thread] = None
        self._md_running = False

        # Book builder from udp_book_builder — reconstructs the book
        self._builder = BookBuilder()
        self._builder_lock = threading.Lock()

        # Cached BBO for change detection
        self._last_bbo = BBO()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Connect TCP for orders. Market data starts with start()."""
        return super().connect()

    def start(self) -> None:
        """Start both the async order receive thread and the market data thread."""
        self.start_async_receive()
        self._start_market_data()

    def stop(self) -> None:
        """Stop market data and disconnect."""
        self._stop_market_data()
        self.disconnect()

    # ------------------------------------------------------------------
    # Market data subscription
    # ------------------------------------------------------------------

    def _start_market_data(self) -> None:
        """Subscribe to UDP market data and start the receive thread."""
        self._md_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._md_socket.settimeout(1.0)

        # Send subscription request — any message subscribes us
        self._md_socket.sendto(b"subscribe", (self._md_host, self._md_port))

        self._md_running = True
        self._md_thread = threading.Thread(
            target=self._md_receive_loop, daemon=True, name="md-recv"
        )
        self._md_thread.start()

    def _stop_market_data(self) -> None:
        """Stop the market data thread."""
        self._md_running = False
        if self._md_thread:
            self._md_thread.join(timeout=2.0)
            self._md_thread = None
        if self._md_socket:
            try:
                self._md_socket.close()
            except Exception:
                pass
            self._md_socket = None

    def _md_receive_loop(self) -> None:
        """Background thread: receive UDP market data and update the book."""
        while self._md_running:
            try:
                data, _ = self._md_socket.recvfrom(4096)
                line = data.decode('utf-8').strip()
                if not line:
                    continue

                # Let the BookBuilder process the LOBSTER message
                with self._builder_lock:
                    self._builder.process_message(line)

                # Notify subclass of raw market data
                self.on_market_data(line)

                # Check for BBO change
                new_bbo = self._snapshot_bbo()
                if new_bbo != self._last_bbo:
                    old_bbo = self._last_bbo
                    self._last_bbo = new_bbo
                    self.on_bbo_change(old_bbo, new_bbo)

            except socket.timeout:
                continue
            except OSError:
                if self._md_running:
                    break
            except Exception as e:
                if self._md_running:
                    print(f"[MD] Error: {e}", file=sys.stderr)
                break

    def _snapshot_bbo(self) -> BBO:
        """Take a BBO snapshot from the current book state."""
        with self._builder_lock:
            book = self._builder.book
            return BBO(
                bid_price=book.get_best_bid_price(),
                bid_size=book.get_best_bid_size(),
                ask_price=book.get_best_ask_price(),
                ask_size=book.get_best_ask_size(),
            )

    # ------------------------------------------------------------------
    # Market data getters — call these from your strategy
    # ------------------------------------------------------------------

    def get_bbo(self) -> BBO:
        """Get current Best Bid and Offer."""
        return self._snapshot_bbo()

    def get_bid_levels(self, depth: int = 5) -> List[PriceLevel]:
        """
        Get bid price levels (best first).

        Args:
            depth: Number of price levels to return (1-based).

        Returns:
            List of PriceLevel(price, size, order_count).
        """
        with self._builder_lock:
            levels = self._builder.book.bids.get_book()
        return levels[:depth]

    def get_ask_levels(self, depth: int = 5) -> List[PriceLevel]:
        """
        Get ask price levels (best first).

        Args:
            depth: Number of price levels to return (1-based).

        Returns:
            List of PriceLevel(price, size, order_count).
        """
        with self._builder_lock:
            levels = self._builder.book.asks.get_book()
        return levels[:depth]

    def get_snapshot(self, depth: int = 5) -> Tuple[List[PriceLevel], List[PriceLevel]]:
        """
        Get a depth-of-book snapshot.

        Args:
            depth: Number of price levels per side.

        Returns:
            (bid_levels, ask_levels) — each a list of PriceLevel.
        """
        with self._builder_lock:
            bids, asks = self._builder.book.get_snapshot()
        return bids[:depth], asks[:depth]

    # ------------------------------------------------------------------
    # Student hooks — override these in your subclass
    # ------------------------------------------------------------------

    def on_bbo_change(self, old_bbo: BBO, new_bbo: BBO) -> None:
        """
        Called whenever the Best Bid or Offer changes.

        Override this to implement reactive order types (e.g., pegged orders).

        Args:
            old_bbo: Previous BBO snapshot.
            new_bbo: Current BBO snapshot.
        """
        pass  # Student implements

    def on_market_data(self, lobster_line: str) -> None:
        """
        Called on every incoming market data message (LOBSTER format).

        Override this if you need to react to individual book events
        (inserts, deletes, executions) beyond BBO changes.

        Format: "Time,Type,OrderID,Size,Price,Direction"
        """
        pass  # Student implements


# ======================================================================
# ASSIGNMENT STUBS — Students subclass and fill in the logic
# ======================================================================

class PeggedOrderClient(OrderClientWithFSMAndMarketData):
    """
    Assignment 1: Pegged Order
    --------------------------
    Implement a pegged order that tracks the BBO.

    A pegged buy order should always rest at the current best bid price.
    A pegged sell order should always rest at the current best ask price.

    When the BBO changes:
      1. Cancel the existing order
      2. Re-submit at the new best price
      3. Handle edge cases (empty book, order already filled, etc.)

    Hints:
      - Use self.cancel_order(order) to cancel
      - Use self.create_order() and self.submit_order_sync() to place new orders
      - self._pegged_order tracks your current live order
      - Check order.is_live before cancelling
      - Consider: what if your cancel is too slow and you get filled?
    """

    def __init__(self, host='localhost', order_port=10000, md_port=10002,
                 side: str = 'B', size: int = 100):
        super().__init__(host, order_port, md_port)
        self._peg_side = side.upper()
        self._peg_size = size
        self._pegged_order: Optional[ManagedOrder] = None

    def on_bbo_change(self, old_bbo: BBO, new_bbo: BBO) -> None:
        """
        TODO: Student implements pegged order logic here.

        When the BBO changes, you should:
          1. Determine the new target price based on self._peg_side
             - For a buy peg: target_price = new_bbo.bid_price
             - For a sell peg: target_price = new_bbo.ask_price
          2. If we have an existing live order at a different price, cancel it
          3. Submit a new limit order at the target price
          4. Store the new order in self._pegged_order

        Edge cases to handle:
          - new_bbo has no bid or no ask (empty side)
          - cancel fails (order may have been filled)
          - order already at the correct price (no action needed)
        """
        pass  # TODO: Student implements this


class ProRataClient(OrderClientWithFSMAndMarketData):
    """
    Assignment 2: Pro-Rata Execution in a Pro-Rata Market
    ------------------------------------------------------
    Execute N shares by distributing orders across price levels
    proportionally to the size available at each level.

    In a pro-rata matching market, your fill probability at a price level
    is proportional to your order size relative to the total size at that
    level. To maximize fill probability, you should distribute your order
    across multiple levels weighted by the liquidity at each level.

    Example:
      You want to BUY 1000 shares. The ask side shows:
        Level 1: $50.01 x 5000 shares
        Level 2: $50.02 x 3000 shares
        Level 3: $50.03 x 2000 shares

      Total liquidity = 10,000.  You want 1000 shares (10%).
      Pro-rata allocation:
        Level 1: 1000 * (5000/10000) = 500 shares @ $50.01
        Level 2: 1000 * (3000/10000) = 300 shares @ $50.02
        Level 3: 1000 * (2000/10000) = 200 shares @ $50.03

    Hints:
      - Use self.get_ask_levels(depth) for buying or
            self.get_bid_levels(depth) for selling
      - Each PriceLevel has .price, .size, .order_count
      - Submit separate limit orders at each price level
      - Handle rounding — make sure total shares == N exactly
      - Consider: what if total available liquidity < N?
    """

    def execute_pro_rata(self, side: str, total_size: int,
                         depth: int = 5) -> List[ManagedOrder]:
        """
        TODO: Student implements pro-rata order distribution here.

        Args:
            side: 'B' (buy) or 'S' (sell)
            total_size: Total number of shares to execute
            depth: Number of price levels to distribute across

        Returns:
            List of ManagedOrder objects that were submitted.

        Steps:
          1. Get the relevant price levels:
             - Buying: use self.get_ask_levels(depth)
             - Selling: use self.get_bid_levels(depth)
          2. Calculate total available liquidity across levels
          3. For each level, allocate: level_shares = total_size * (level_size / total_liquidity)
          4. Round to integers; fix any rounding remainder
          5. Submit a limit order at each level
          6. Return the list of submitted orders
        """
        orders = []
        # TODO: Student implements this
        return orders


# ======================================================================
# Interactive demo / main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Order Client with FSM + Market Data'
    )
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--order-port', type=int, default=10000)
    parser.add_argument('--md-port', type=int, default=10002)
    parser.add_argument('-q', '--quiet', action='store_true')
    args = parser.parse_args()

    # --- Use a simple demo subclass that prints BBO changes ---

    class DemoClient(OrderClientWithFSMAndMarketData):
        def on_bbo_change(self, old_bbo, new_bbo):
            print(f"[BBO] {new_bbo}")

        def on_market_data(self, lobster_line):
            pass  # quiet — override to see raw messages

    client = DemoClient(
        host=args.host,
        order_port=args.order_port,
        md_port=args.md_port,
    )

    def on_order_message(order: ManagedOrder, msg: ExchangeMessage):
        if msg.msg_type in ("FILL", "PARTIAL_FILL") and msg.size and msg.price:
            side_str = "BUY" if order.side == 'B' else "SELL"
            price_str = f"${msg.price / 10000:.2f}"
            remainder = f", {msg.remainder_size} remaining" if msg.remainder_size else ""
            print(f"[{msg.msg_type}] Order {order.exchange_order_id}: "
                  f"{side_str} {msg.size} @ {price_str}{remainder}")
        elif msg.msg_type == "ACK" and msg.size and msg.price:
            side_str = "BUY" if order.side == 'B' else "SELL"
            price_str = f"${msg.price / 10000:.2f}"
            print(f"[{msg.msg_type}] Order {order.exchange_order_id}: "
                  f"{side_str} {msg.size} @ {price_str} ACCEPTED")
        else:
            print(f"[{msg.msg_type}] Order {order.exchange_order_id}: {order.state_name}")

    client.set_message_callback(on_order_message)

    if not client.connect():
        sys.exit(1)

    client.start()

    HELP = """
============================================================
  ORDER CLIENT WITH FSM + MARKET DATA
============================================================

LIMIT ORDER:  limit,size,price,side[,ttl]
MARKET ORDER: market,size,side
CANCEL:       cancel,order_id
STATE:        state,order_id
LIST:         orders
BBO:          bbo
BOOK:         book[,depth]

Price format: price * 10000 (e.g., $500.00 = 5000000)
============================================================
"""

    if not args.quiet:
        print(HELP)

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            parts = line.split(',')
            cmd = parts[0].lower()

            try:
                if cmd == "limit" and len(parts) >= 4:
                    size, price, side = int(parts[1]), int(parts[2]), parts[3].upper()
                    ttl = int(parts[4]) if len(parts) > 4 else DEFAULT_TTL_SECONDS
                    order = client.create_order("limit", size, price, side, ttl)
                    if client.submit_order_sync(order):
                        print(f"Submitted: {order}")
                    else:
                        print(f"Failed: {order.error_message}")

                elif cmd == "market" and len(parts) >= 3:
                    size, side = int(parts[1]), parts[2].upper()
                    order = client.create_order("market", size, 0, side)
                    if client.submit_order_sync(order):
                        print(f"Submitted: {order}")
                    else:
                        print(f"Failed: {order.error_message}")

                elif cmd == "cancel" and len(parts) >= 2:
                    order = client.get_order(int(parts[1]))
                    if order and client.cancel_order(order):
                        print("Cancel sent")
                    else:
                        print("Cancel failed")

                elif cmd == "state" and len(parts) >= 2:
                    order = client.get_order(int(parts[1]))
                    if order:
                        print(f"{order}\n  History: "
                              f"{[(s.name, e.name) for s, e in order.state_history]}")
                    else:
                        print("Not found")

                elif cmd == "orders":
                    for oid, order in client.get_all_orders().items():
                        print(f"  {oid}: {order.state_name}")

                elif cmd == "bbo":
                    print(client.get_bbo())

                elif cmd == "book":
                    depth = int(parts[1]) if len(parts) > 1 else 5
                    bids, asks = client.get_snapshot(depth)
                    print("\n  BIDS:")
                    for lvl in bids:
                        print(f"    ${lvl.price / 10000:.2f}  x {lvl.size:>8}  "
                              f"({lvl.order_count} orders)")
                    print("  ASKS:")
                    for lvl in asks:
                        print(f"    ${lvl.price / 10000:.2f}  x {lvl.size:>8}  "
                              f"({lvl.order_count} orders)")
                    if not bids and not asks:
                        print("  (empty book)")
                    print()

                else:
                    print(f"Unknown: {line}")

            except (ValueError, IndexError) as e:
                print(f"Error: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        client.stop()


if __name__ == '__main__':
    main()
