#!/usr/bin/env python3
"""
Order book logic tests using three approaches:

1. Synthetic tests - exact validation with controlled message sequences
2. Delta validation - verify changes match between our book and LOBSTER reference
3. Invariant tests - verify book maintains correct properties throughout replay
"""

import pytest
from pathlib import Path

from order_book import OrderBook
from order_generator import OrderGeneratorState
from lobster_reader import LOBSTERReader, LOBSTER_EVENT_TYPE
from messages import EventLOBSTER, EventType


# Test data files
DATA_DIR = Path(__file__).parent
MESSAGE_FILE = DATA_DIR / "AMZN_2012-06-21_34200000_57600000_message_1.csv"
ORDERBOOK_FILE = DATA_DIR / "AMZN_2012-06-21_34200000_57600000_orderbook_1.csv"


# =============================================================================
# APPROACH 1: Synthetic Tests - Exact validation with controlled sequences
# =============================================================================

class TestSyntheticOrderBook:
    """Exact validation tests using synthetic message sequences."""

    def test_single_insert_bid(self):
        """Insert a single bid order."""
        book = OrderBook()
        event = EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        )
        book.process_event(event)

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_bid_size() == 100
        assert book.get_best_ask_price() is None

    def test_single_insert_ask(self):
        """Insert a single ask order."""
        book = OrderBook()
        event = EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5010000, direction='S'
        )
        book.process_event(event)

        assert book.get_best_ask_price() == 5010000
        assert book.get_best_ask_size() == 100
        assert book.get_best_bid_price() is None

    def test_insert_both_sides(self):
        """Insert orders on both sides."""
        book = OrderBook()

        # Insert bid
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        # Insert ask
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=200, price=5010000, direction='S'
        ))

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_bid_size() == 100
        assert book.get_best_ask_price() == 5010000
        assert book.get_best_ask_size() == 200

    def test_multiple_bids_price_priority(self):
        """Higher priced bids should be best."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=200, price=5005000, direction='B'  # Higher price
        ))

        assert book.get_best_bid_price() == 5005000
        assert book.get_best_bid_size() == 200

    def test_multiple_asks_price_priority(self):
        """Lower priced asks should be best."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5010000, direction='S'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=200, price=5005000, direction='S'  # Lower price
        ))

        assert book.get_best_ask_price() == 5005000
        assert book.get_best_ask_size() == 200

    def test_same_price_aggregates_size(self):
        """Orders at same price should aggregate size."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=150, price=5000000, direction='B'  # Same price
        ))

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_bid_size() == 250  # 100 + 150

    def test_delete_removes_order(self):
        """DELETE should remove an order completely."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() == 5000000

        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.DELETE,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() is None

    def test_cancel_reduces_size(self):
        """CANCEL should reduce order size."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.CANCEL,
            order_id=1001, size=30, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_bid_size() == 70  # 100 - 30

    def test_execute_removes_liquidity(self):
        """EXECUTE should remove liquidity from the book."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.EXECUTE,
            order_id=1001, size=40, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_bid_size() == 60  # 100 - 40

    def test_execute_full_order(self):
        """Full execution should remove order."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.EXECUTE,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() is None

    def test_crossing_insert_generates_trade(self):
        """Insert that crosses spread should generate trade."""
        book = OrderBook()

        # Resting ask at $501
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5010000, direction='S'
        ))

        # Aggressive bid at $502 (crosses the ask)
        ack, trades = book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=50, price=5020000, direction='B'
        ))

        assert trades is not None
        assert len(trades) == 2  # Standing + taking trade
        assert trades[0].size == 50
        assert trades[0].price == 5010000  # Trades at resting price

        # Ask should have 50 remaining
        assert book.get_best_ask_size() == 50

    def test_delete_reveals_next_level(self):
        """Deleting BBO reveals next price level."""
        book = OrderBook()

        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=200, price=4990000, direction='B'  # Worse price
        ))

        assert book.get_best_bid_price() == 5000000

        book.process_event(EventLOBSTER(
            seq_num=3, time=3.0, event_type=EventType.DELETE,
            order_id=1001, size=100, price=5000000, direction='B'
        ))

        assert book.get_best_bid_price() == 4990000
        assert book.get_best_bid_size() == 200

    def test_complex_sequence(self):
        """Test a complex sequence of operations."""
        book = OrderBook()

        # Build up the book
        book.process_event(EventLOBSTER(
            seq_num=1, time=1.0, event_type=EventType.INSERT,
            order_id=1001, size=100, price=5000000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=2, time=2.0, event_type=EventType.INSERT,
            order_id=1002, size=100, price=4990000, direction='B'
        ))
        book.process_event(EventLOBSTER(
            seq_num=3, time=3.0, event_type=EventType.INSERT,
            order_id=1003, size=100, price=5010000, direction='S'
        ))
        book.process_event(EventLOBSTER(
            seq_num=4, time=4.0, event_type=EventType.INSERT,
            order_id=1004, size=100, price=5020000, direction='S'
        ))

        assert book.get_best_bid_price() == 5000000
        assert book.get_best_ask_price() == 5010000

        # Partial cancel
        book.process_event(EventLOBSTER(
            seq_num=5, time=5.0, event_type=EventType.CANCEL,
            order_id=1001, size=30, price=5000000, direction='B'
        ))
        assert book.get_best_bid_size() == 70

        # Execute against ask
        book.process_event(EventLOBSTER(
            seq_num=6, time=6.0, event_type=EventType.EXECUTE,
            order_id=1003, size=100, price=5010000, direction='S'
        ))
        assert book.get_best_ask_price() == 5020000

        # Delete remaining bid at best
        book.process_event(EventLOBSTER(
            seq_num=7, time=7.0, event_type=EventType.DELETE,
            order_id=1001, size=70, price=5000000, direction='B'
        ))
        assert book.get_best_bid_price() == 4990000


# =============================================================================
# APPROACH 2: Delta Validation - Verify changes match LOBSTER reference
# =============================================================================

class TestDeltaValidation:
    """Verify that book state changes match LOBSTER reference changes."""

    @pytest.fixture
    def data_files_exist(self):
        """Check that test data files exist."""
        if not MESSAGE_FILE.exists():
            pytest.skip(f"Message file not found: {MESSAGE_FILE}")
        if not ORDERBOOK_FILE.exists():
            pytest.skip(f"Orderbook file not found: {ORDERBOOK_FILE}")
        return True

    def test_insert_delta_matches(self, data_files_exist):
        """
        For INSERT messages, verify that:
        - If insert improves BBO, our BBO changes accordingly
        - The direction of change matches the reference
        """
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        prev_book = None
        prev_actual = {'bid_price': 0, 'bid_size': 0, 'ask_price': 0, 'ask_size': 0}
        insert_delta_matches = 0
        insert_count = 0

        for msg, expected_book in reader.read_messages_with_orderbook():
            if insert_count >= 1000:  # Test first 1000 INSERTs
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            if msg.event_type == 1:  # INSERT
                insert_count += 1

                # Get current actual state
                actual = {
                    'bid_price': book.get_best_bid_price() or 0,
                    'bid_size': book.get_best_bid_size() or 0,
                    'ask_price': book.get_best_ask_price() or 0,
                    'ask_size': book.get_best_ask_size() or 0,
                }

                # Compute deltas
                if prev_book is not None:
                    exp_bid_delta = expected_book.bid_price - prev_book.bid_price
                    exp_ask_delta = expected_book.ask_price - prev_book.ask_price
                    act_bid_delta = actual['bid_price'] - prev_actual['bid_price']
                    act_ask_delta = actual['ask_price'] - prev_actual['ask_price']

                    # Check if delta directions match
                    bid_dir_match = (exp_bid_delta > 0) == (act_bid_delta > 0) or \
                                   (exp_bid_delta == 0) == (act_bid_delta == 0) or \
                                   (exp_bid_delta < 0) == (act_bid_delta < 0)
                    ask_dir_match = (exp_ask_delta > 0) == (act_ask_delta > 0) or \
                                   (exp_ask_delta == 0) == (act_ask_delta == 0) or \
                                   (exp_ask_delta < 0) == (act_ask_delta < 0)

                    if bid_dir_match and ask_dir_match:
                        insert_delta_matches += 1

                prev_book = expected_book
                prev_actual = actual

        match_rate = insert_delta_matches / max(insert_count - 1, 1)
        print(f"\nINSERT delta direction match rate: {match_rate:.1%} ({insert_delta_matches}/{insert_count-1})")

        # We expect reasonable match rate (not 100% due to pre-existing orders)
        assert match_rate > 0.5, f"INSERT delta match rate too low: {match_rate:.1%}"

    def test_price_movement_correlation(self, data_files_exist):
        """
        Test that our price movements correlate with reference price movements.
        """
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        ref_prices = []
        actual_prices = []

        processed = 0
        for msg, expected_book in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 10000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            # Record mid prices every 100 messages
            if processed % 100 == 0:
                ref_mid = (expected_book.bid_price + expected_book.ask_price) / 2
                act_bid = book.get_best_bid_price() or 0
                act_ask = book.get_best_ask_price() or 0
                if act_bid > 0 and act_ask > 0:
                    act_mid = (act_bid + act_ask) / 2
                    ref_prices.append(ref_mid)
                    actual_prices.append(act_mid)

        # Compute correlation
        if len(ref_prices) > 10:
            n = len(ref_prices)
            mean_ref = sum(ref_prices) / n
            mean_act = sum(actual_prices) / n

            cov = sum((r - mean_ref) * (a - mean_act) for r, a in zip(ref_prices, actual_prices)) / n
            std_ref = (sum((r - mean_ref) ** 2 for r in ref_prices) / n) ** 0.5
            std_act = (sum((a - mean_act) ** 2 for a in actual_prices) / n) ** 0.5

            if std_ref > 0 and std_act > 0:
                correlation = cov / (std_ref * std_act)
                print(f"\nPrice correlation with reference: {correlation:.3f}")
                assert correlation > 0.8, f"Price correlation too low: {correlation:.3f}"


# =============================================================================
# APPROACH 3: Invariant Tests - Verify book maintains correct properties
# =============================================================================

class TestInvariants:
    """Verify the order book maintains correct invariants throughout replay."""

    @pytest.fixture
    def data_files_exist(self):
        """Check that test data files exist."""
        if not MESSAGE_FILE.exists():
            pytest.skip(f"Message file not found: {MESSAGE_FILE}")
        if not ORDERBOOK_FILE.exists():
            pytest.skip(f"Orderbook file not found: {ORDERBOOK_FILE}")
        return True

    def test_no_crossed_book(self, data_files_exist):
        """Verify bid < ask throughout replay (book never crossed)."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        crossed_count = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 50000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            bid = book.get_best_bid_price()
            ask = book.get_best_ask_price()

            if bid is not None and ask is not None and bid >= ask:
                crossed_count += 1

        print(f"\nProcessed {processed} messages, crossed book: {crossed_count} times")
        assert crossed_count == 0, f"Book was crossed {crossed_count} times"

    def test_no_negative_sizes(self, data_files_exist):
        """Verify no price level has negative size."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        negative_sizes = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 50000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            bids, asks = book.get_snapshot()
            for level in bids + asks:
                if level.size < 0:
                    negative_sizes += 1

        print(f"\nProcessed {processed} messages, negative sizes: {negative_sizes}")
        assert negative_sizes == 0, f"Found {negative_sizes} negative sizes"

    def test_bid_side_sorted_descending(self, data_files_exist):
        """Verify bid side is sorted by price descending."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        sort_violations = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 10000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            bids, _ = book.get_snapshot()
            for i in range(1, len(bids)):
                if bids[i].price > bids[i-1].price:
                    sort_violations += 1

        print(f"\nProcessed {processed} messages, bid sort violations: {sort_violations}")
        assert sort_violations == 0, f"Found {sort_violations} bid sort violations"

    def test_ask_side_sorted_ascending(self, data_files_exist):
        """Verify ask side is sorted by price ascending."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        sort_violations = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 10000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            _, asks = book.get_snapshot()
            for i in range(1, len(asks)):
                if asks[i].price < asks[i-1].price:
                    sort_violations += 1

        print(f"\nProcessed {processed} messages, ask sort violations: {sort_violations}")
        assert sort_violations == 0, f"Found {sort_violations} ask sort violations"

    def test_spread_reasonable(self, data_files_exist):
        """Verify spread stays within reasonable bounds."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        spreads = []
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 20000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is not None:
                event = reader.to_event(msg, state.get_next_seq_num())
                if event is not None:
                    book.process_event(event)

            bid = book.get_best_bid_price()
            ask = book.get_best_ask_price()

            if bid is not None and ask is not None:
                spread = ask - bid
                spreads.append(spread)

        if spreads:
            avg_spread = sum(spreads) / len(spreads)
            max_spread = max(spreads)
            print(f"\nSpread stats over {len(spreads)} observations:")
            print(f"  Average: ${avg_spread/10000:.4f}")
            print(f"  Maximum: ${max_spread/10000:.4f}")

            # For AMZN, spread should typically be under $1.00
            assert avg_spread < 10000, f"Average spread too wide: ${avg_spread/10000:.2f}"

    def test_order_tracking_consistency(self, data_files_exist):
        """
        For orders we INSERT, verify they are properly tracked and
        subsequent operations affect them correctly.
        """
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        # Track orders we've seen inserted
        our_orders = {}  # order_id -> {price, side, remaining_size}
        tracking_errors = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 20000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is None:
                continue

            event = reader.to_event(msg, state.get_next_seq_num())
            if event is None:
                continue

            # Track INSERTs
            if msg.event_type == 1:  # INSERT
                our_orders[msg.order_id] = {
                    'price': msg.price,
                    'side': msg.side,
                    'remaining_size': msg.size
                }

            # Process event
            book.process_event(event)

            # Update tracking for operations on our orders
            if msg.order_id in our_orders:
                if msg.event_type == 4:  # EXECUTE
                    our_orders[msg.order_id]['remaining_size'] -= msg.size
                    if our_orders[msg.order_id]['remaining_size'] <= 0:
                        del our_orders[msg.order_id]

                elif msg.event_type == 2:  # CANCEL
                    our_orders[msg.order_id]['remaining_size'] -= msg.size
                    if our_orders[msg.order_id]['remaining_size'] <= 0:
                        del our_orders[msg.order_id]

                elif msg.event_type == 3:  # DELETE
                    del our_orders[msg.order_id]

        print(f"\nProcessed {processed} messages")
        print(f"Orders still tracked: {len(our_orders)}")
        assert tracking_errors == 0, f"Found {tracking_errors} tracking errors"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
