#!/usr/bin/env python3
"""
Comprehensive integration tests for complex trading scenarios.

Tests multi-client interactions with various trading patterns including:
- Multi-level order sweeps
- Partial fill tracking
- Market order edge cases
- Cross-client passive fills
- Self-trade scenarios
- Rapid-fire order sequences
- Cancel and fill race conditions
"""

import pytest
import sys
import os
import time
import socket
from typing import List, Tuple, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchange_server import ExchangeServer
from order_client_with_fsm import OrderClientWithFSM, ManagedOrder, ExchangeMessage


def find_free_port():
    """Find a free port to use for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


@dataclass
class FillRecord:
    """Record of a fill received by a client."""
    order_id: int
    size: int
    price: int
    remainder: Optional[int] = None
    is_partial: bool = False


class TrackingClient:
    """
    Wrapper around OrderClientWithFSM that tracks all fills and messages.

    This provides comprehensive tracking for testing complex scenarios where
    we need to verify both sync responses and async passive fills.
    """

    def __init__(self, name: str, host: str, port: int):
        self.name = name
        self.client = OrderClientWithFSM(host, port)
        self.fills: List[FillRecord] = []
        self.all_messages: List[Tuple[ManagedOrder, ExchangeMessage]] = []
        self.client.set_message_callback(self._on_message)

    def _on_message(self, order: ManagedOrder, msg: ExchangeMessage):
        """Callback for all messages received."""
        self.all_messages.append((order, msg))

        if msg.msg_type in ("FILL", "PARTIAL_FILL") and msg.size and msg.price:
            fill = FillRecord(
                order_id=msg.order_id or 0,
                size=msg.size,
                price=msg.price,
                remainder=msg.remainder_size,
                is_partial=(msg.msg_type == "PARTIAL_FILL")
            )
            self.fills.append(fill)

    def connect(self) -> bool:
        """Connect to exchange."""
        return self.client.connect()

    def disconnect(self) -> None:
        """Disconnect from exchange."""
        self.client.disconnect()

    def start_async_receive(self) -> None:
        """Start async message reception."""
        self.client.start_async_receive()

    def create_order(self, order_type: str, size: int, price: int, side: str,
                     ttl: int = 3600) -> ManagedOrder:
        """Create a new managed order."""
        return self.client.create_order(order_type, size, price, side, ttl)

    def submit_order(self, order: ManagedOrder) -> bool:
        """Submit order without waiting."""
        return self.client.submit_order(order)

    def submit_order_sync(self, order: ManagedOrder, timeout: float = 5.0) -> bool:
        """Submit order and wait for initial response."""
        return self.client.submit_order_sync(order, timeout)

    def cancel_order(self, order: ManagedOrder, user: str = "client") -> bool:
        """Cancel an order."""
        return self.client.cancel_order(order, user)

    def clear_fills(self) -> None:
        """Clear recorded fills."""
        self.fills.clear()

    def get_total_filled_size(self, order_id: int) -> int:
        """Get total size filled for a specific order."""
        return sum(f.size for f in self.fills if f.order_id == order_id)

    def get_fill_prices(self, order_id: int) -> List[int]:
        """Get all fill prices for a specific order."""
        return [f.price for f in self.fills if f.order_id == order_id]

    def get_fill_count(self, order_id: int) -> int:
        """Get number of fills for a specific order."""
        return sum(1 for f in self.fills if f.order_id == order_id)


@pytest.fixture
def exchange_server():
    """Fixture that creates and manages an exchange server for tests."""
    order_port = find_free_port()
    feed_port = find_free_port()
    server = ExchangeServer(order_port, feed_port, market_data_port=0)
    assert server.start(), "Failed to start exchange server"
    time.sleep(0.1)

    yield server, order_port, feed_port

    server.stop()


class TestMultiLevelSweeps:
    """Test scenarios where orders sweep through multiple price levels."""

    def test_market_order_sweeps_multiple_levels(self, exchange_server):
        """
        Test market order that sweeps through multiple price levels.

        Scenario:
        1. Client 1 posts orders at 3 different price levels (ask side)
        2. Client 2 sends large market buy that sweeps all levels
        3. Verify Client 2 gets fills at each price level
        4. Verify Client 1 receives passive fill notifications for all orders
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Maker", "localhost", order_port)
        client2 = TrackingClient("Taker", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Client 1 posts offers at multiple levels
        order1 = client1.create_order("limit", 100, 500000, "S")  # $50.00
        order2 = client1.create_order("limit", 150, 505000, "S")  # $50.50
        order3 = client1.create_order("limit", 200, 510000, "S")  # $51.00

        assert client1.submit_order_sync(order1)
        assert client1.submit_order_sync(order2)
        assert client1.submit_order_sync(order3)

        assert order1.state_name == "ACCEPTED"
        assert order2.state_name == "ACCEPTED"
        assert order3.state_name == "ACCEPTED"

        time.sleep(0.2)
        client1.clear_fills()

        # Client 2 sends market order that sweeps all levels (450 total available)
        market_order = client2.create_order("market", 450, 0, "B")
        assert client2.submit_order_sync(market_order, timeout=2.0)

        time.sleep(0.5)

        # Verify Client 2 received fills at each price level
        assert len(client2.fills) >= 3, f"Expected 3 fills, got {len(client2.fills)}"

        fill_prices = [f.price for f in client2.fills]
        assert 500000 in fill_prices, "Should fill at $50.00"
        assert 505000 in fill_prices, "Should fill at $50.50"
        assert 510000 in fill_prices, "Should fill at $51.00"

        # Verify total filled size
        total_filled = sum(f.size for f in client2.fills)
        assert total_filled == 450, f"Expected 450 total filled, got {total_filled}"

        # Verify Client 1 received passive fills for all orders
        time.sleep(0.3)
        assert len(client1.fills) >= 3, f"Expected 3 passive fills, got {len(client1.fills)}"

        # Check that all of Client 1's orders are filled
        assert order1.state_name == "FILLED"
        assert order2.state_name == "FILLED"
        assert order3.state_name == "FILLED"

        client1.disconnect()
        client2.disconnect()

    def test_limit_order_sweeps_partial_levels(self, exchange_server):
        """
        Test limit order that sweeps and partially fills final level.

        Scenario:
        1. Client 1 posts 3 ask levels: 100@50, 100@51, 100@52
        2. Client 2 sends buy limit for 250@52 (sweeps first 2, partial on 3rd)
        3. Verify correct fills at each level with partial on last
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Maker", "localhost", order_port)
        client2 = TrackingClient("Taker", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Post 3 levels
        orders = [
            client1.create_order("limit", 100, 500000, "S"),
            client1.create_order("limit", 100, 510000, "S"),
            client1.create_order("limit", 100, 520000, "S"),
        ]

        for order in orders:
            assert client1.submit_order_sync(order)
            assert order.state_name == "ACCEPTED"

        time.sleep(0.2)
        client1.clear_fills()

        # Sweep with limit order
        sweep_order = client2.create_order("limit", 250, 520000, "B")
        assert client2.submit_order_sync(sweep_order)

        time.sleep(0.5)

        # Verify 3 fills for taker (2 full, 1 partial)
        assert len(client2.fills) == 3

        # First two should be full fills (100 each)
        assert client2.fills[0].size == 100
        assert client2.fills[1].size == 100
        # Third should be partial (50)
        assert client2.fills[2].size == 50

        # Check maker received passive fills
        assert len(client1.fills) == 3
        assert orders[0].state_name == "FILLED"
        assert orders[1].state_name == "FILLED"
        assert orders[2].state_name == "PARTIALLY_FILLED"

        client1.disconnect()
        client2.disconnect()


class TestPartialFillScenarios:
    """Test partial fill tracking and remainder calculations."""

    def test_large_resting_order_multiple_small_fills(self, exchange_server):
        """
        Test large order filled incrementally by multiple small orders.

        Scenario:
        1. Client 1 posts large bid (1000 shares)
        2. Client 2 sells 200 shares (partial fill #1)
        3. Client 2 sells 300 shares (partial fill #2)
        4. Client 2 sells 500 shares (final fill)
        5. Verify remainder tracking after each fill
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Buyer", "localhost", order_port)
        client2 = TrackingClient("Seller", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Large resting order
        large_order = client1.create_order("limit", 1000, 500000, "B")
        assert client1.submit_order_sync(large_order)
        assert large_order.state_name == "ACCEPTED"

        time.sleep(0.2)
        client1.clear_fills()

        # First small fill (200 shares)
        sell1 = client2.create_order("limit", 200, 500000, "S")
        assert client2.submit_order_sync(sell1)
        time.sleep(0.3)

        assert len(client1.fills) == 1
        assert client1.fills[0].size == 200
        assert client1.fills[0].is_partial
        assert client1.fills[0].remainder == 800
        assert large_order.state_name == "PARTIALLY_FILLED"

        # Second small fill (300 shares)
        sell2 = client2.create_order("limit", 300, 500000, "S")
        assert client2.submit_order_sync(sell2)
        time.sleep(0.3)

        assert len(client1.fills) == 2
        assert client1.fills[1].size == 300
        assert client1.fills[1].is_partial
        assert client1.fills[1].remainder == 500
        assert large_order.state_name == "PARTIALLY_FILLED"

        # Final fill (500 shares)
        sell3 = client2.create_order("limit", 500, 500000, "S")
        assert client2.submit_order_sync(sell3)
        time.sleep(0.3)

        assert len(client1.fills) == 3
        assert client1.fills[2].size == 500
        assert not client1.fills[2].is_partial
        assert client1.fills[2].remainder is None
        assert large_order.state_name == "FILLED"

        client1.disconnect()
        client2.disconnect()

    def test_partial_fill_tracking_across_price_levels(self, exchange_server):
        """
        Test partial fill with remainder when crossing multiple levels.

        Scenario:
        1. Client 1 posts 3 small offers
        2. Client 2 sends large buy that partially fills after sweeping all
        3. Verify remainder is posted at the limit price
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Maker", "localhost", order_port)
        client2 = TrackingClient("Taker", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Post small offers
        for i in range(3):
            order = client1.create_order("limit", 50, 500000 + i*1000, "S")
            assert client1.submit_order_sync(order)

        time.sleep(0.2)

        # Large buy that sweeps all and has remainder
        large_buy = client2.create_order("limit", 300, 510000, "B")
        assert client2.submit_order_sync(large_buy)

        time.sleep(0.5)

        # Should have 3 fills (one per level swept) plus ACK for remainder
        # The order should be partially filled and remainder should be posted
        assert len(client2.fills) == 3
        total_filled = sum(f.size for f in client2.fills)
        assert total_filled == 150  # 50 * 3

        # Check that remainder is posted in book
        # Last fill should show remainder of 150
        assert client2.fills[-1].remainder == 150

        client1.disconnect()
        client2.disconnect()


class TestMarketOrderEdgeCases:
    """Test market order behavior in various edge cases."""

    def test_market_order_empty_book_rejection(self, exchange_server):
        """
        Test market order against empty book is rejected.
        """
        server, order_port, feed_port = exchange_server

        client = TrackingClient("Trader", "localhost", order_port)
        assert client.connect()
        client.start_async_receive()

        market_order = client.create_order("market", 100, 0, "B")
        assert client.submit_order_sync(market_order)

        time.sleep(0.2)

        # Should be rejected due to no liquidity
        assert market_order.state_name == "REJECTED"
        assert len(client.fills) == 0

        client.disconnect()

    def test_market_order_partial_fill_insufficient_liquidity(self, exchange_server):
        """
        Test market order that partially fills due to insufficient liquidity.

        Scenario:
        1. Post 100 shares on ask side
        2. Send market buy for 200 shares
        3. Verify only 100 fills, 100 remainder reported as unfilled
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Seller", "localhost", order_port)
        client2 = TrackingClient("Buyer", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Post limited liquidity
        offer = client1.create_order("limit", 100, 500000, "S")
        assert client1.submit_order_sync(offer)

        time.sleep(0.2)

        # Market order exceeds available liquidity
        market_buy = client2.create_order("market", 200, 0, "B")
        assert client2.submit_order_sync(market_buy)

        time.sleep(0.3)

        # Should have one full fill plus partial fill message
        assert len(client2.fills) >= 1
        total_filled = sum(f.size for f in client2.fills)
        assert total_filled == 100

        # Check if partial fill message indicates remainder
        partial_fills = [f for f in client2.fills if f.is_partial]
        if partial_fills:
            assert partial_fills[0].remainder == 100

        client1.disconnect()
        client2.disconnect()

    def test_market_order_sweeps_to_last_share(self, exchange_server):
        """
        Test market order that exactly consumes all available liquidity.
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Maker", "localhost", order_port)
        client2 = TrackingClient("Taker", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Post exact liquidity across multiple levels
        orders = [
            client1.create_order("limit", 100, 500000, "S"),
            client1.create_order("limit", 100, 501000, "S"),
            client1.create_order("limit", 100, 502000, "S"),
        ]

        for order in orders:
            assert client1.submit_order_sync(order)

        time.sleep(0.2)

        # Market order for exact total
        market_buy = client2.create_order("market", 300, 0, "B")
        assert client2.submit_order_sync(market_buy)

        time.sleep(0.5)

        # Should have 3 fills, no remainder
        assert len(client2.fills) == 3
        total = sum(f.size for f in client2.fills)
        assert total == 300

        # Last fill should not be partial
        assert not client2.fills[-1].is_partial

        client1.disconnect()
        client2.disconnect()


class TestCrossClientPassiveFills:
    """Test passive fill notifications across clients."""

    def test_simple_cross_client_passive_fill(self, exchange_server):
        """
        Test basic passive fill notification.

        Scenario:
        1. Client 1 posts bid
        2. Client 2 hits the bid
        3. Verify Client 1 receives async passive fill
        4. Verify Client 2 receives sync fill response
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Passive", "localhost", order_port)
        client2 = TrackingClient("Aggressor", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Client 1 posts passive order
        passive_order = client1.create_order("limit", 100, 500000, "B")
        assert client1.submit_order_sync(passive_order)
        assert passive_order.state_name == "ACCEPTED"

        time.sleep(0.2)
        client1.clear_fills()

        # Client 2 aggresses
        aggressive_order = client2.create_order("limit", 100, 500000, "S")
        assert client2.submit_order_sync(aggressive_order)

        time.sleep(0.5)

        # Verify both clients received fills
        assert len(client1.fills) == 1, "Passive client should receive fill"
        assert len(client2.fills) == 1, "Aggressive client should receive fill"

        # Both should be for same size and price
        assert client1.fills[0].size == 100
        assert client2.fills[0].size == 100
        assert client1.fills[0].price == 500000
        assert client2.fills[0].price == 500000

        # Both orders should be filled
        assert passive_order.state_name == "FILLED"
        assert aggressive_order.state_name == "FILLED"

        client1.disconnect()
        client2.disconnect()

    def test_multiple_passive_fills_same_order(self, exchange_server):
        """
        Test single passive order filled by multiple aggressors.

        Scenario:
        1. Client 1 posts large order
        2. Client 2 takes 30%
        3. Client 3 takes 50%
        4. Client 2 takes remaining 20%
        5. Verify Client 1 receives all 3 passive fills
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Passive", "localhost", order_port)
        client2 = TrackingClient("Aggressor1", "localhost", order_port)
        client3 = TrackingClient("Aggressor2", "localhost", order_port)

        for client in [client1, client2, client3]:
            assert client.connect()
            client.start_async_receive()

        # Large passive order
        passive_order = client1.create_order("limit", 1000, 500000, "S")
        assert client1.submit_order_sync(passive_order)

        time.sleep(0.2)
        client1.clear_fills()

        # First aggressor (300 shares)
        agg1 = client2.create_order("limit", 300, 500000, "B")
        assert client2.submit_order_sync(agg1)
        time.sleep(0.3)

        assert len(client1.fills) == 1
        assert client1.fills[0].size == 300
        assert client1.fills[0].remainder == 700

        # Second aggressor (500 shares)
        agg2 = client3.create_order("limit", 500, 500000, "B")
        assert client3.submit_order_sync(agg2)
        time.sleep(0.3)

        assert len(client1.fills) == 2
        assert client1.fills[1].size == 500
        assert client1.fills[1].remainder == 200

        # First aggressor returns (200 shares)
        agg3 = client2.create_order("limit", 200, 500000, "B")
        assert client2.submit_order_sync(agg3)
        time.sleep(0.3)

        assert len(client1.fills) == 3
        assert client1.fills[2].size == 200
        assert not client1.fills[2].is_partial
        assert passive_order.state_name == "FILLED"

        for client in [client1, client2, client3]:
            client.disconnect()


class TestSelfTrade:
    """Test self-trade scenarios (same client on both sides)."""

    def test_self_trade_buy_then_sell(self, exchange_server):
        """
        Test client posting buy then crossing with sell.

        Scenario:
        1. Client posts bid
        2. Client posts crossing ask
        3. Verify client receives both active (sell) and passive (bid) fills
        """
        server, order_port, feed_port = exchange_server

        client = TrackingClient("SelfTrader", "localhost", order_port)
        assert client.connect()
        client.start_async_receive()

        # Post bid
        buy_order = client.create_order("limit", 100, 500000, "B")
        assert client.submit_order_sync(buy_order)
        assert buy_order.state_name == "ACCEPTED"

        time.sleep(0.2)
        client.clear_fills()

        # Post crossing sell
        sell_order = client.create_order("limit", 100, 500000, "S")
        assert client.submit_order_sync(sell_order)

        time.sleep(0.5)

        # Should receive 2 fills: one for active (sell), one for passive (buy)
        assert len(client.fills) >= 2, f"Expected 2 fills (self-trade), got {len(client.fills)}"

        # Both orders should be filled
        assert buy_order.state_name == "FILLED"
        assert sell_order.state_name == "FILLED"

        # Should have fills with both order IDs
        order_ids = set(f.order_id for f in client.fills)
        assert buy_order.exchange_order_id in order_ids
        assert sell_order.exchange_order_id in order_ids

        client.disconnect()

    def test_self_trade_partial_match(self, exchange_server):
        """
        Test self-trade with partial fill.

        Scenario:
        1. Client posts large bid (500 shares)
        2. Client posts smaller crossing sell (200 shares)
        3. Verify bid gets partial fill, sell gets full fill
        """
        server, order_port, feed_port = exchange_server

        client = TrackingClient("SelfTrader", "localhost", order_port)
        assert client.connect()
        client.start_async_receive()

        # Large bid
        buy_order = client.create_order("limit", 500, 500000, "B")
        assert client.submit_order_sync(buy_order)

        time.sleep(0.2)
        client.clear_fills()

        # Smaller sell
        sell_order = client.create_order("limit", 200, 500000, "S")
        assert client.submit_order_sync(sell_order)

        time.sleep(0.5)

        # Sell should be fully filled
        assert sell_order.state_name == "FILLED"

        # Buy should be partially filled
        assert buy_order.state_name == "PARTIALLY_FILLED"

        # Should have 2 fills total
        assert len(client.fills) >= 2

        client.disconnect()


class TestRapidFireSequences:
    """Test rapid-fire order submission and fill handling."""

    def test_rapid_sequential_orders(self, exchange_server):
        """
        Test multiple orders submitted in quick succession.

        Scenario:
        1. Client 1 posts 10 offers quickly
        2. Client 2 takes all 10 quickly
        3. Verify all fills reported correctly to both clients
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Maker", "localhost", order_port)
        client2 = TrackingClient("Taker", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Rapidly post 10 offers
        num_orders = 10
        maker_orders = []
        for i in range(num_orders):
            order = client1.create_order("limit", 50, 500000 + i*100, "S")
            assert client1.submit_order_sync(order, timeout=1.0)
            maker_orders.append(order)

        time.sleep(0.3)
        client1.clear_fills()

        # Rapidly take all offers
        taker_orders = []
        for i in range(num_orders):
            order = client2.create_order("limit", 50, 500000 + i*100, "B")
            assert client2.submit_order_sync(order, timeout=1.0)
            taker_orders.append(order)

        time.sleep(1.0)

        # Verify all taker orders filled
        filled_takers = sum(1 for o in taker_orders if o.state_name == "FILLED")
        assert filled_takers == num_orders, f"Expected {num_orders} filled takers, got {filled_takers}"

        # Verify all maker orders filled (passive)
        filled_makers = sum(1 for o in maker_orders if o.state_name == "FILLED")
        assert filled_makers == num_orders, f"Expected {num_orders} filled makers, got {filled_makers}"

        # Verify fill counts
        assert len(client1.fills) == num_orders, f"Maker should have {num_orders} fills"
        assert len(client2.fills) == num_orders, f"Taker should have {num_orders} fills"

        client1.disconnect()
        client2.disconnect()

    def test_rapid_alternating_sides(self, exchange_server):
        """
        Test rapid alternating buy and sell orders.

        Scenario:
        1. Rapidly alternate posting bids and asks that cross
        2. Verify all trades execute correctly
        """
        server, order_port, feed_port = exchange_server

        client = TrackingClient("Trader", "localhost", order_port)
        assert client.connect()
        client.start_async_receive()

        orders = []
        num_pairs = 5

        for i in range(num_pairs):
            # Post bid
            bid = client.create_order("limit", 50, 500000, "B")
            assert client.submit_order_sync(bid, timeout=1.0)
            orders.append(bid)

            # Immediately post crossing ask
            ask = client.create_order("limit", 50, 500000, "S")
            assert client.submit_order_sync(ask, timeout=1.0)
            orders.append(ask)

            time.sleep(0.1)

        time.sleep(0.5)

        # All orders should be filled (self-trades)
        filled_count = sum(1 for o in orders if o.state_name == "FILLED")
        assert filled_count == num_pairs * 2, f"Expected {num_pairs * 2} filled, got {filled_count}"

        client.disconnect()


class TestCancelFillRaceConditions:
    """Test race conditions between cancels and fills."""

    def test_cancel_after_partial_fill(self, exchange_server):
        """
        Test canceling remainder after partial fill.

        Scenario:
        1. Post large order (1000 shares)
        2. Partially fill it (300 shares)
        3. Cancel the remainder (700 shares)
        4. Verify fill and cancel sizes are correct
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Trader", "localhost", order_port)
        client2 = TrackingClient("Counter", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Large order
        large_order = client1.create_order("limit", 1000, 500000, "B")
        assert client1.submit_order_sync(large_order)

        time.sleep(0.2)
        client1.clear_fills()

        # Partial fill
        partial = client2.create_order("limit", 300, 500000, "S")
        assert client2.submit_order_sync(partial)

        time.sleep(0.5)

        # Verify partial fill
        assert len(client1.fills) == 1
        assert client1.fills[0].size == 300
        assert client1.fills[0].remainder == 700
        assert large_order.state_name == "PARTIALLY_FILLED"

        # Cancel remainder
        assert client1.cancel_order(large_order)

        time.sleep(0.3)

        # Order should now be cancelled
        assert large_order.state_name == "CANCELLED"

        # Verify no more fills after cancel
        assert len(client1.fills) == 1

        client1.disconnect()
        client2.disconnect()

    def test_multiple_partial_fills_then_cancel(self, exchange_server):
        """
        Test multiple partial fills followed by cancel.

        Scenario:
        1. Post order for 1000 shares
        2. Fill 200 shares
        3. Fill 300 shares
        4. Cancel remaining 500 shares
        5. Verify correct tracking throughout
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Trader", "localhost", order_port)
        client2 = TrackingClient("Counter", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Large order
        large_order = client1.create_order("limit", 1000, 500000, "S")
        assert client1.submit_order_sync(large_order)

        time.sleep(0.2)
        client1.clear_fills()

        # First partial
        assert client2.submit_order_sync(client2.create_order("limit", 200, 500000, "B"))
        time.sleep(0.3)

        assert len(client1.fills) == 1
        assert client1.fills[0].size == 200
        assert client1.fills[0].remainder == 800

        # Second partial
        assert client2.submit_order_sync(client2.create_order("limit", 300, 500000, "B"))
        time.sleep(0.3)

        assert len(client1.fills) == 2
        assert client1.fills[1].size == 300
        assert client1.fills[1].remainder == 500

        # Cancel remainder
        assert client1.cancel_order(large_order)
        time.sleep(0.3)

        assert large_order.state_name == "CANCELLED"

        # Total filled should be 500
        total_filled = sum(f.size for f in client1.fills)
        assert total_filled == 500

        client1.disconnect()
        client2.disconnect()

    def test_cancel_then_try_to_fill(self, exchange_server):
        """
        Test that cancelled order cannot be filled.

        Scenario:
        1. Post order
        2. Cancel it immediately
        3. Try to fill it
        4. Verify fill attempt fails (order not in book)
        """
        server, order_port, feed_port = exchange_server

        client1 = TrackingClient("Trader", "localhost", order_port)
        client2 = TrackingClient("Counter", "localhost", order_port)

        assert client1.connect()
        assert client2.connect()

        client1.start_async_receive()
        client2.start_async_receive()

        # Post and immediately cancel
        order = client1.create_order("limit", 100, 500000, "B")
        assert client1.submit_order_sync(order)
        time.sleep(0.1)

        assert client1.cancel_order(order)
        time.sleep(0.3)

        assert order.state_name == "CANCELLED"

        # Try to fill cancelled order
        counter = client2.create_order("limit", 100, 500000, "S")
        assert client2.submit_order_sync(counter)
        time.sleep(0.3)

        # Counter order should be posted (not filled)
        assert counter.state_name == "ACCEPTED"

        # Client 1 should have no fills
        assert len(client1.fills) == 0

        client1.disconnect()
        client2.disconnect()


class TestComplexMultiClientScenarios:
    """Test complex scenarios with multiple clients and interactions."""

    def test_three_way_trade(self, exchange_server):
        """
        Test scenario with three clients trading.

        Scenario:
        1. Client A posts bid @ 50
        2. Client B posts offer @ 51
        3. Client C sweeps both with limit @ 51
        """
        server, order_port, feed_port = exchange_server

        clientA = TrackingClient("BidSide", "localhost", order_port)
        clientB = TrackingClient("AskSide", "localhost", order_port)
        clientC = TrackingClient("Sweeper", "localhost", order_port)

        for client in [clientA, clientB, clientC]:
            assert client.connect()
            client.start_async_receive()

        # Set up book
        bid = clientA.create_order("limit", 100, 500000, "B")
        ask = clientB.create_order("limit", 100, 510000, "S")

        assert clientA.submit_order_sync(bid)
        assert clientB.submit_order_sync(ask)

        time.sleep(0.2)
        clientA.clear_fills()
        clientB.clear_fills()

        # Client C sweeps the ask (but not the bid)
        sweep = clientC.create_order("limit", 100, 510000, "B")
        assert clientC.submit_order_sync(sweep)

        time.sleep(0.5)

        # Client B should be filled (passive)
        assert len(clientB.fills) == 1
        assert ask.state_name == "FILLED"

        # Client C should be filled (active)
        assert len(clientC.fills) == 1
        assert sweep.state_name == "FILLED"

        # Client A should have no fills (not hit)
        assert len(clientA.fills) == 0
        assert bid.state_name == "ACCEPTED"

        for client in [clientA, clientB, clientC]:
            client.disconnect()

    def test_market_maker_scenario(self, exchange_server):
        """
        Test realistic market maker scenario.

        Scenario:
        1. MM posts tight two-sided market
        2. Trader A lifts the offer
        3. MM requotes
        4. Trader B hits the bid
        5. Verify MM receives both fills and can requote
        """
        server, order_port, feed_port = exchange_server

        mm = TrackingClient("MarketMaker", "localhost", order_port)
        traderA = TrackingClient("TraderA", "localhost", order_port)
        traderB = TrackingClient("TraderB", "localhost", order_port)

        for client in [mm, traderA, traderB]:
            assert client.connect()
            client.start_async_receive()

        # MM posts two-sided market
        bid1 = mm.create_order("limit", 100, 495000, "B")  # 49.50
        ask1 = mm.create_order("limit", 100, 505000, "S")  # 50.50

        assert mm.submit_order_sync(bid1)
        assert mm.submit_order_sync(ask1)

        time.sleep(0.2)
        mm.clear_fills()

        # Trader A lifts offer
        assert traderA.submit_order_sync(traderA.create_order("limit", 100, 505000, "B"))
        time.sleep(0.3)

        assert len(mm.fills) == 1
        assert ask1.state_name == "FILLED"

        # MM requotes ask side
        ask2 = mm.create_order("limit", 100, 505000, "S")
        assert mm.submit_order_sync(ask2)
        time.sleep(0.2)

        mm.clear_fills()

        # Trader B hits bid
        assert traderB.submit_order_sync(traderB.create_order("limit", 100, 495000, "S"))
        time.sleep(0.3)

        assert len(mm.fills) == 1
        assert bid1.state_name == "FILLED"

        # MM should have 2 total trades (one on each side)
        for client in [mm, traderA, traderB]:
            client.disconnect()
