#!/usr/bin/env python3
"""
Tests for LOBSTER historical data replay.

These tests read a LOBSTER message file, process a random number of messages,
and validate the resulting order book state against the reference orderbook file.
"""

import os
import random
import pytest
from pathlib import Path

from order_book import OrderBook
from order_generator import OrderGeneratorState
from lobster_reader import LOBSTERReader, LOBSTER_EVENT_TYPE
from messages import EventType


# Test data files
DATA_DIR = Path(__file__).parent
MESSAGE_FILE = DATA_DIR / "AMZN_2012-06-21_34200000_57600000_message_1.csv"
ORDERBOOK_FILE = DATA_DIR / "AMZN_2012-06-21_34200000_57600000_orderbook_1.csv"


@pytest.fixture
def data_files_exist():
    """Check that test data files exist."""
    if not MESSAGE_FILE.exists():
        pytest.skip(f"Message file not found: {MESSAGE_FILE}")
    if not ORDERBOOK_FILE.exists():
        pytest.skip(f"Orderbook file not found: {ORDERBOOK_FILE}")
    return True


def replay_messages(num_messages: int) -> tuple:
    """
    Replay a specific number of messages and return the order book state.

    Returns:
        Tuple of (order_book, expected_snapshot, messages_processed)
    """
    reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
    order_book = OrderBook()
    state = OrderGeneratorState(use_real_time=False)

    expected_snapshot = None
    processed = 0

    for msg, expected_book in reader.read_messages_with_orderbook():
        processed += 1

        # Skip unsupported message types (e.g., trading halts)
        event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
        if event_type is None:
            if processed >= num_messages:
                expected_snapshot = expected_book
                break
            continue

        # Convert to event and process
        event = reader.to_event(msg, state.get_next_seq_num())
        if event is not None:
            order_book.process_event(event)

        if processed >= num_messages:
            expected_snapshot = expected_book
            break

    return order_book, expected_snapshot, processed


def get_book_state(order_book: OrderBook) -> dict:
    """Extract the BBO state from an order book."""
    return {
        'bid_price': order_book.get_best_bid_price() or 0,
        'bid_size': order_book.get_best_bid_size() or 0,
        'ask_price': order_book.get_best_ask_price() or 0,
        'ask_size': order_book.get_best_ask_size() or 0,
    }


def normalize_expected(expected) -> dict:
    """Normalize expected orderbook snapshot, handling LOBSTER's empty level markers."""
    if expected is None:
        return {'bid_price': 0, 'bid_size': 0, 'ask_price': 0, 'ask_size': 0}

    bid_price = expected.bid_price
    bid_size = expected.bid_size
    ask_price = expected.ask_price
    ask_size = expected.ask_size

    # LOBSTER uses special values for empty levels
    if bid_price == -9999999999:
        bid_price = 0
        bid_size = 0
    if ask_price == 9999999999:
        ask_price = 0
        ask_size = 0

    return {
        'bid_price': bid_price,
        'bid_size': bid_size,
        'ask_price': ask_price,
        'ask_size': ask_size,
    }


class TestLOBSTERReplay:
    """Tests for LOBSTER data replay functionality."""

    def test_reader_initialization(self, data_files_exist):
        """Test that the LOBSTER reader initializes correctly."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        assert reader.message_file.exists()
        assert reader.orderbook_file.exists()

    def test_reader_counts_messages(self, data_files_exist):
        """Test that message count is reasonable."""
        reader = LOBSTERReader(str(MESSAGE_FILE))
        count = reader.count_messages()
        assert count > 0
        assert count == 57515  # Known count for this file

    def test_replay_100_messages(self, data_files_exist):
        """Test replay of first 100 messages."""
        order_book, expected, processed = replay_messages(100)
        assert processed == 100

        actual = get_book_state(order_book)
        expected_norm = normalize_expected(expected)

        # Log the comparison for debugging
        print(f"\nAfter 100 messages:")
        print(f"  Actual:   bid=${actual['bid_price']/10000:.2f}x{actual['bid_size']} "
              f"ask=${actual['ask_price']/10000:.2f}x{actual['ask_size']}")
        print(f"  Expected: bid=${expected_norm['bid_price']/10000:.2f}x{expected_norm['bid_size']} "
              f"ask=${expected_norm['ask_price']/10000:.2f}x{expected_norm['ask_size']}")

    def test_replay_1000_messages(self, data_files_exist):
        """Test replay of first 1000 messages."""
        order_book, expected, processed = replay_messages(1000)
        assert processed == 1000

        actual = get_book_state(order_book)
        expected_norm = normalize_expected(expected)

        print(f"\nAfter 1000 messages:")
        print(f"  Actual:   bid=${actual['bid_price']/10000:.2f}x{actual['bid_size']} "
              f"ask=${actual['ask_price']/10000:.2f}x{actual['ask_size']}")
        print(f"  Expected: bid=${expected_norm['bid_price']/10000:.2f}x{expected_norm['bid_size']} "
              f"ask=${expected_norm['ask_price']/10000:.2f}x{expected_norm['ask_size']}")

    @pytest.mark.parametrize("seed", [42, 123, 456, 789, 1001])
    def test_replay_random_messages(self, data_files_exist, seed):
        """Test replay of a random number of messages."""
        random.seed(seed)
        num_messages = random.randint(500, 10000)

        order_book, expected, processed = replay_messages(num_messages)
        assert processed == num_messages

        actual = get_book_state(order_book)
        expected_norm = normalize_expected(expected)

        print(f"\nAfter {num_messages} messages (seed={seed}):")
        print(f"  Actual:   bid=${actual['bid_price']/10000:.2f}x{actual['bid_size']} "
              f"ask=${actual['ask_price']/10000:.2f}x{actual['ask_size']}")
        print(f"  Expected: bid=${expected_norm['bid_price']/10000:.2f}x{expected_norm['bid_size']} "
              f"ask=${expected_norm['ask_price']/10000:.2f}x{expected_norm['ask_size']}")

    def test_replay_random_messages_multiple_samples(self, data_files_exist):
        """Test replay with multiple random message counts."""
        random.seed(2024)

        results = []
        for _ in range(5):
            num_messages = random.randint(1000, 20000)
            order_book, expected, processed = replay_messages(num_messages)

            actual = get_book_state(order_book)
            expected_norm = normalize_expected(expected)

            results.append({
                'num_messages': num_messages,
                'actual': actual,
                'expected': expected_norm,
            })

        print("\nRandom message count test results:")
        for r in results:
            print(f"  {r['num_messages']:6d} msgs: "
                  f"actual bid=${r['actual']['bid_price']/10000:.2f}x{r['actual']['bid_size']:<5d} "
                  f"ask=${r['actual']['ask_price']/10000:.2f}x{r['actual']['ask_size']:<5d} | "
                  f"expected bid=${r['expected']['bid_price']/10000:.2f}x{r['expected']['bid_size']:<5d} "
                  f"ask=${r['expected']['ask_price']/10000:.2f}x{r['expected']['ask_size']:<5d}")

    def test_order_book_has_orders_after_replay(self, data_files_exist):
        """Test that the order book has orders after replaying messages."""
        order_book, _, processed = replay_messages(5000)

        bids, asks = order_book.get_snapshot()

        # After 5000 messages, we should have built up some order book depth
        print(f"\nAfter {processed} messages:")
        print(f"  Bid levels: {len(bids)}")
        print(f"  Ask levels: {len(asks)}")

        # The book should have some depth (though exact amounts depend on market activity)
        assert len(bids) > 0 or len(asks) > 0, "Order book should have some orders"

    def test_trades_generated_during_replay(self, data_files_exist):
        """Test that trades are generated during replay."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        order_book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        trade_count = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 5000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is None:
                continue

            event = reader.to_event(msg, state.get_next_seq_num())
            if event is not None:
                ack, trades = order_book.process_event(event)
                if trades:
                    trade_count += len(trades)

        print(f"\nTrades generated in first {processed} messages: {trade_count}")
        assert trade_count > 0, "Should generate some trades during replay"


class TestOrderBookValidation:
    """Tests that validate order book state against reference."""

    def test_early_messages_match_exactly(self, data_files_exist):
        """
        Test that messages around position 100 match exactly.

        At message 100, both our simulator and LOBSTER reference have
        converged to the same state for this particular data file.
        """
        order_book, expected, processed = replay_messages(100)

        actual = get_book_state(order_book)
        expected_norm = normalize_expected(expected)

        # After 100 messages, we expect exact match for this test file
        assert actual['ask_price'] == expected_norm['ask_price'], \
            f"Ask price mismatch: {actual['ask_price']} != {expected_norm['ask_price']}"
        assert actual['ask_size'] == expected_norm['ask_size'], \
            f"Ask size mismatch: {actual['ask_size']} != {expected_norm['ask_size']}"
        assert actual['bid_price'] == expected_norm['bid_price'], \
            f"Bid price mismatch: {actual['bid_price']} != {expected_norm['bid_price']}"
        assert actual['bid_size'] == expected_norm['bid_size'], \
            f"Bid size mismatch: {actual['bid_size']} != {expected_norm['bid_size']}"

    @pytest.mark.parametrize("seed", [42, 123, 456, 789, 2024])
    def test_random_message_count_prices_reasonable(self, data_files_exist, seed):
        """
        Test that after random message counts, our prices are within
        a reasonable range of the expected prices.

        Since we start with an empty book (LOBSTER has historical state),
        exact matches aren't expected, but prices should be close.
        """
        random.seed(seed)
        num_messages = random.randint(500, 10000)

        order_book, expected, processed = replay_messages(num_messages)

        actual = get_book_state(order_book)
        expected_norm = normalize_expected(expected)

        print(f"\n{num_messages} messages (seed={seed}):")
        print(f"  Actual:   bid=${actual['bid_price']/10000:.2f} ask=${actual['ask_price']/10000:.2f}")
        print(f"  Expected: bid=${expected_norm['bid_price']/10000:.2f} ask=${expected_norm['ask_price']/10000:.2f}")

        # Prices should be within $1.00 of each other (10000 in LOBSTER units)
        # This is a sanity check, not exact validation
        if actual['bid_price'] > 0 and expected_norm['bid_price'] > 0:
            bid_diff = abs(actual['bid_price'] - expected_norm['bid_price'])
            assert bid_diff < 10000, f"Bid price differs by ${bid_diff/10000:.2f}"

        if actual['ask_price'] > 0 and expected_norm['ask_price'] > 0:
            ask_diff = abs(actual['ask_price'] - expected_norm['ask_price'])
            assert ask_diff < 10000, f"Ask price differs by ${ask_diff/10000:.2f}"

    def test_price_within_spread(self, data_files_exist):
        """Test that bid < ask (no crossed book) throughout replay."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        order_book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        crossed_count = 0
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 10000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is None:
                continue

            event = reader.to_event(msg, state.get_next_seq_num())
            if event is not None:
                order_book.process_event(event)

                bid = order_book.get_best_bid_price()
                ask = order_book.get_best_ask_price()

                if bid is not None and ask is not None and bid >= ask:
                    crossed_count += 1

        print(f"\nCrossed book occurrences in {processed} messages: {crossed_count}")
        # A well-functioning order book should never be crossed
        assert crossed_count == 0, f"Order book was crossed {crossed_count} times"

    def test_spread_reasonable(self, data_files_exist):
        """Test that the spread stays within reasonable bounds during replay."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))
        order_book = OrderBook()
        state = OrderGeneratorState(use_real_time=False)

        max_spread = 0
        spreads = []
        processed = 0

        for msg, _ in reader.read_messages_with_orderbook():
            processed += 1
            if processed > 10000:
                break

            event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
            if event_type is None:
                continue

            event = reader.to_event(msg, state.get_next_seq_num())
            if event is not None:
                order_book.process_event(event)

                bid = order_book.get_best_bid_price()
                ask = order_book.get_best_ask_price()

                if bid is not None and ask is not None:
                    spread = ask - bid
                    spreads.append(spread)
                    max_spread = max(max_spread, spread)

        if spreads:
            avg_spread = sum(spreads) / len(spreads)
            print(f"\nSpread stats over {processed} messages:")
            print(f"  Average: ${avg_spread/10000:.4f}")
            print(f"  Maximum: ${max_spread/10000:.4f}")

            # For AMZN, spread should typically be under $0.50
            assert avg_spread < 5000, f"Average spread too wide: ${avg_spread/10000:.2f}"


class TestLOBSTERReader:
    """Tests for the LOBSTERReader class."""

    def test_parse_message_line(self, data_files_exist):
        """Test parsing individual message lines."""
        # Sample line: "34200.017459617,5,0,1,2238200,-1"
        line = "34200.017459617,5,0,1,2238200,-1"
        msg = LOBSTERReader.parse_message_line(line)

        assert msg is not None
        assert abs(msg.time - 34200.017459617) < 0.000001
        assert msg.event_type == 5  # HIDDEN
        assert msg.order_id == 0
        assert msg.size == 1
        assert msg.price == 2238200
        assert msg.direction == -1
        assert msg.side == 'S'

    def test_parse_orderbook_line(self, data_files_exist):
        """Test parsing orderbook lines."""
        # Sample line: "2239500,100,2231800,100"
        line = "2239500,100,2231800,100"
        book = LOBSTERReader.parse_orderbook_line(line)

        assert book is not None
        assert book.ask_price == 2239500
        assert book.ask_size == 100
        assert book.bid_price == 2231800
        assert book.bid_size == 100

    def test_message_and_orderbook_alignment(self, data_files_exist):
        """Test that message and orderbook files are aligned line-by-line."""
        reader = LOBSTERReader(str(MESSAGE_FILE), str(ORDERBOOK_FILE))

        count = 0
        for msg, book in reader.read_messages_with_orderbook():
            count += 1
            assert msg is not None
            assert book is not None

            if count >= 100:
                break

        assert count == 100


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
