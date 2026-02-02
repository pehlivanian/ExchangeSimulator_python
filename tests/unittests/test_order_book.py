"""Tests for order book operations."""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from order_book import OrderBook, OrderBookSide, PriceLevel
from messages import EventLOBSTER, EventType, Order


class TestOrderBookSide:
    """Tests for a single side of the order book."""

    def test_insert_single_order(self):
        side = OrderBookSide(is_bid=True)
        order = Order(
            seq_num=1, side='B', time=1.0, order_id=1000,
            price=50000000, size=100, order_type=EventType.INSERT
        )
        ack = side.insert(order)
        assert ack.acked is True
        assert ack.order_id == 1000
        assert len(side) == 1

    def test_insert_duplicate_rejected(self):
        side = OrderBookSide(is_bid=True)
        order = Order(
            seq_num=1, side='B', time=1.0, order_id=1000,
            price=50000000, size=100, order_type=EventType.INSERT
        )
        side.insert(order)
        ack = side.insert(order)
        assert ack.acked is False
        assert "Duplicate" in ack.reason_rejected

    def test_bbo_single_order(self):
        side = OrderBookSide(is_bid=True)
        order = Order(
            seq_num=1, side='B', time=1.0, order_id=1000,
            price=50000000, size=100, order_type=EventType.INSERT
        )
        side.insert(order)
        assert side.get_bbo_price() == 50000000
        assert side.get_bbo_size() == 100

    def test_bid_price_priority(self):
        """Higher price should be at top for bids."""
        side = OrderBookSide(is_bid=True)
        side.insert(Order(seq_num=1, side='B', time=1.0, order_id=1000,
                         price=49000000, size=100, order_type=EventType.INSERT))
        side.insert(Order(seq_num=2, side='B', time=2.0, order_id=1001,
                         price=50000000, size=50, order_type=EventType.INSERT))
        side.insert(Order(seq_num=3, side='B', time=3.0, order_id=1002,
                         price=48000000, size=75, order_type=EventType.INSERT))

        assert side.get_bbo_price() == 50000000
        assert side.get_bbo_size() == 50

    def test_ask_price_priority(self):
        """Lower price should be at top for asks."""
        side = OrderBookSide(is_bid=False)
        side.insert(Order(seq_num=1, side='S', time=1.0, order_id=1000,
                         price=51000000, size=100, order_type=EventType.INSERT))
        side.insert(Order(seq_num=2, side='S', time=2.0, order_id=1001,
                         price=50000000, size=50, order_type=EventType.INSERT))
        side.insert(Order(seq_num=3, side='S', time=3.0, order_id=1002,
                         price=52000000, size=75, order_type=EventType.INSERT))

        assert side.get_bbo_price() == 50000000
        assert side.get_bbo_size() == 50

    def test_time_priority_same_price(self):
        """Earlier order should be at top when prices are equal."""
        side = OrderBookSide(is_bid=True)
        side.insert(Order(seq_num=2, side='B', time=2.0, order_id=1001,
                         price=50000000, size=50, order_type=EventType.INSERT))
        side.insert(Order(seq_num=1, side='B', time=1.0, order_id=1000,
                         price=50000000, size=100, order_type=EventType.INSERT))

        bbo = side.get_bbo()
        assert bbo.order_id == 1000  # Earlier order first

    def test_remove_order(self):
        side = OrderBookSide(is_bid=True)
        side.insert(Order(seq_num=1, side='B', time=1.0, order_id=1000,
                         price=50000000, size=100, order_type=EventType.INSERT))
        removed = side.remove(1000)
        assert removed is not None
        assert removed.order_id == 1000
        assert len(side) == 0

    def test_remove_nonexistent(self):
        side = OrderBookSide(is_bid=True)
        removed = side.remove(9999)
        assert removed is None

    def test_bbo_size_aggregation(self):
        """BBO size should sum all orders at best price."""
        side = OrderBookSide(is_bid=True)
        side.insert(Order(seq_num=1, side='B', time=1.0, order_id=1000,
                         price=50000000, size=100, order_type=EventType.INSERT))
        side.insert(Order(seq_num=2, side='B', time=2.0, order_id=1001,
                         price=50000000, size=50, order_type=EventType.INSERT))
        side.insert(Order(seq_num=3, side='B', time=3.0, order_id=1002,
                         price=49000000, size=200, order_type=EventType.INSERT))

        assert side.get_bbo_price() == 50000000
        assert side.get_bbo_size() == 150  # 100 + 50

    def test_get_book(self):
        """Get all price levels."""
        side = OrderBookSide(is_bid=True)
        side.insert(Order(seq_num=1, side='B', time=1.0, order_id=1000,
                         price=50000000, size=100, order_type=EventType.INSERT))
        side.insert(Order(seq_num=2, side='B', time=2.0, order_id=1001,
                         price=50000000, size=50, order_type=EventType.INSERT))
        side.insert(Order(seq_num=3, side='B', time=3.0, order_id=1002,
                         price=49000000, size=200, order_type=EventType.INSERT))

        levels = side.get_book()
        assert len(levels) == 2
        assert levels[0].price == 50000000
        assert levels[0].size == 150
        assert levels[0].order_count == 2
        assert levels[1].price == 49000000
        assert levels[1].size == 200
        assert levels[1].order_count == 1

    def test_empty_book(self):
        side = OrderBookSide(is_bid=True)
        assert side.get_bbo() is None
        assert side.get_bbo_price() is None
        assert side.get_bbo_size() is None
        assert len(side.get_book()) == 0


class TestOrderBook:
    """Tests for the full order book."""

    def test_insert_bid(self):
        book = OrderBook()
        event = EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        )
        ack, trades = book.process_event(event)
        assert ack.acked is True
        assert trades is None
        assert book.get_best_bid_price() == 50000000

    def test_insert_ask(self):
        book = OrderBook()
        event = EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=51000000, direction='S'
        )
        ack, trades = book.process_event(event)
        assert ack.acked is True
        assert trades is None
        assert book.get_best_ask_price() == 51000000

    def test_no_crossing_when_spread_exists(self):
        book = OrderBook()
        # Insert bid at 50
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Insert ask at 51 (no crossing)
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=51000000, direction='S'
        ))
        assert trades is None
        assert book.get_best_bid_price() == 50000000
        assert book.get_best_ask_price() == 51000000

    def test_crossing_generates_trade(self):
        book = OrderBook()
        # Insert ask at 50
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='S'
        ))
        # Insert bid at 50 (crosses)
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=50000000, direction='B'
        ))
        assert trades is not None
        assert len(trades) == 2  # standing trade + taking trade
        assert trades[0].size == 50
        assert trades[0].price == 50000000
        assert trades[0].order_id == 1000  # standing order

    def test_crossing_bid_into_ask(self):
        book = OrderBook()
        # Insert ask at 50
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='S'
        ))
        # Insert bid at 51 (crosses at 50)
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=51000000, direction='B'
        ))
        assert trades is not None
        assert trades[0].price == 50000000  # Trade at standing order's price

    def test_crossing_ask_into_bid(self):
        book = OrderBook()
        # Insert bid at 50
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Insert ask at 49 (crosses at 50)
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=49000000, direction='S'
        ))
        assert trades is not None
        assert trades[0].price == 50000000  # Trade at standing order's price

    def test_full_fill_removes_order(self):
        book = OrderBook()
        # Insert ask at 50
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='S'
        ))
        # Insert bid that fully fills the ask
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=50000000, direction='B'
        ))
        assert book.get_best_ask_price() is None

    def test_partial_fill_reduces_size(self):
        book = OrderBook()
        # Insert ask at 50 for 100
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='S'
        ))
        # Insert bid for 30 (partial)
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=30, price=50000000, direction='B'
        ))
        assert book.get_best_ask_price() == 50000000
        assert book.get_best_ask_size() == 70

    def test_crossing_multiple_price_levels(self):
        book = OrderBook()
        # Insert asks at multiple levels
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=50, price=50000000, direction='S'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=51000000, direction='S'
        ))
        # Insert bid that crosses both levels
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=3, time=3.0, event_type=EventType.INSERT,
            order_id=1002, size=75, price=52000000, direction='B'
        ))
        # Should trade 50 at 50, then 25 at 51
        assert trades is not None
        assert len(trades) == 4  # 2 trades * 2 sides each
        # First standing trade
        assert trades[0].size == 50
        assert trades[0].price == 50000000
        # Second standing trade
        assert trades[2].size == 25
        assert trades[2].price == 51000000

    def test_execute_event(self):
        book = OrderBook()
        # Insert bid
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Execute against the bid
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.EXECUTE,
            order_id=0, size=50, price=50000000, direction='B'
        ))
        assert ack.acked is True
        assert trades is not None
        assert len(trades) == 1
        assert trades[0].size == 50
        assert book.get_best_bid_size() == 50

    def test_execute_no_liquidity(self):
        book = OrderBook()
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.EXECUTE,
            order_id=0, size=50, price=50000000, direction='B'
        ))
        assert ack.acked is False
        assert "No liquidity" in ack.reason_rejected

    def test_delete_event(self):
        book = OrderBook()
        # Insert bid
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Delete it
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.DELETE,
            order_id=1000, size=0, price=50000000, direction='B'
        ))
        assert ack.acked is True
        assert book.get_best_bid_price() is None

    def test_cancel_event_partial(self):
        book = OrderBook()
        # Insert bid
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Cancel 30 shares
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.CANCEL,
            order_id=1000, size=30, price=50000000, direction='B'
        ))
        assert ack.acked is True
        assert book.get_best_bid_size() == 70

    def test_snapshot(self):
        book = OrderBook()
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=51000000, direction='S'
        ))
        bids, asks = book.get_snapshot()
        assert len(bids) == 1
        assert len(asks) == 1
        assert bids[0].price == 50000000
        assert asks[0].price == 51000000


class TestOrderBookEdgeCases:
    """Edge case tests for the order book."""

    def test_self_trade_prevention(self):
        """Orders from same side shouldn't trade with each other."""
        book = OrderBook()
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='B'
        ))
        # Another bid shouldn't trade
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=50000000, direction='B'
        ))
        assert trades is None
        assert book.get_best_bid_size() == 150

    def test_large_order_sweeps_book(self):
        """Large order that exceeds available liquidity."""
        book = OrderBook()
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=50, price=50000000, direction='S'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=50, price=51000000, direction='S'
        ))
        # Bid for 200 but only 100 available
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=3, time=3.0, event_type=EventType.INSERT,
            order_id=1002, size=200, price=52000000, direction='B'
        ))
        assert trades is not None
        # Should have 2 fills (50 each)
        total_filled = sum(t.size for t in trades[::2])  # Every other is standing
        assert total_filled == 100
        # Remainder should be posted
        assert book.get_best_bid_size() == 100
        assert book.get_best_bid_price() == 52000000

    def test_zero_spread(self):
        """Bid and ask at same price."""
        book = OrderBook()
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1000, size=100, price=50000000, direction='S'
        ))
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=50000000, direction='B'
        ))
        assert trades is not None
        assert book.get_best_bid_price() is None
        assert book.get_best_ask_price() is None
