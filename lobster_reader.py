#!/usr/bin/env python3
"""
LOBSTER data file reader for historical market data replay.

Reads LOBSTER format message and orderbook files for replaying historical
market data through the exchange simulator.

LOBSTER Message File Format (6 columns):
    1. Time: Seconds after midnight (with decimal precision)
    2. Type: 1=new limit order, 2=partial cancel, 3=full delete,
             4=visible execution, 5=hidden execution, 7=trading halt
    3. Order ID: Unique order reference number
    4. Size: Number of shares
    5. Price: Dollar price * 10000
    6. Direction: -1=Sell, 1=Buy

LOBSTER Orderbook File Format (4 columns per level):
    For each level: Ask Price, Ask Size, Bid Price, Bid Size
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, List, Optional, Tuple

try:
    from .messages import EventLOBSTER, EventType
except ImportError:
    from messages import EventLOBSTER, EventType


# LOBSTER event type mapping
LOBSTER_EVENT_TYPE = {
    1: EventType.INSERT,    # New limit order
    2: EventType.CANCEL,    # Partial cancellation
    3: EventType.DELETE,    # Full deletion
    4: EventType.EXECUTE,   # Visible execution
    5: EventType.HIDDEN,    # Hidden execution
    7: None,                # Trading halt (we'll skip these)
}


@dataclass
class LOBSTERMessage:
    """A parsed LOBSTER message."""
    time: float
    event_type: int
    order_id: int
    size: int
    price: int
    direction: int  # 1=Buy, -1=Sell

    @property
    def side(self) -> str:
        """Convert direction to side character."""
        return 'B' if self.direction == 1 else 'S'

    @property
    def is_trading_halt(self) -> bool:
        """Check if this is a trading halt message."""
        return self.event_type == 7


@dataclass
class LOBSTEROrderbookSnapshot:
    """A parsed LOBSTER orderbook snapshot (single level for validation)."""
    ask_price: int
    ask_size: int
    bid_price: int
    bid_size: int


class LOBSTERReader:
    """
    Reader for LOBSTER data files.

    Can read message files and orderbook files, either individually or together
    for validation during replay.
    """

    def __init__(self, message_file: str, orderbook_file: Optional[str] = None):
        """
        Initialize the reader.

        Args:
            message_file: Path to LOBSTER message CSV file
            orderbook_file: Optional path to LOBSTER orderbook CSV file for validation
        """
        self.message_file = Path(message_file)
        self.orderbook_file = Path(orderbook_file) if orderbook_file else None

        if not self.message_file.exists():
            raise FileNotFoundError(f"Message file not found: {message_file}")

        if self.orderbook_file and not self.orderbook_file.exists():
            raise FileNotFoundError(f"Orderbook file not found: {orderbook_file}")

    @staticmethod
    def parse_message_line(line: str) -> Optional[LOBSTERMessage]:
        """Parse a single line from a LOBSTER message file."""
        line = line.strip()
        if not line:
            return None

        parts = line.split(',')
        if len(parts) != 6:
            return None

        try:
            return LOBSTERMessage(
                time=float(parts[0]),
                event_type=int(parts[1]),
                order_id=int(parts[2]),
                size=int(parts[3]),
                price=int(parts[4]),
                direction=int(parts[5])
            )
        except (ValueError, IndexError):
            return None

    @staticmethod
    def parse_orderbook_line(line: str) -> Optional[LOBSTEROrderbookSnapshot]:
        """Parse a single line from a LOBSTER orderbook file (first level only)."""
        line = line.strip()
        if not line:
            return None

        parts = line.split(',')
        if len(parts) < 4:
            return None

        try:
            return LOBSTEROrderbookSnapshot(
                ask_price=int(parts[0]),
                ask_size=int(parts[1]),
                bid_price=int(parts[2]),
                bid_size=int(parts[3])
            )
        except (ValueError, IndexError):
            return None

    def read_messages(self) -> Generator[LOBSTERMessage, None, None]:
        """
        Read all messages from the message file.

        Yields:
            LOBSTERMessage objects
        """
        with open(self.message_file, 'r') as f:
            for line in f:
                msg = self.parse_message_line(line)
                if msg is not None:
                    yield msg

    def read_messages_with_orderbook(self) -> Generator[Tuple[LOBSTERMessage, Optional[LOBSTEROrderbookSnapshot]], None, None]:
        """
        Read messages paired with corresponding orderbook snapshots.

        Yields:
            Tuples of (LOBSTERMessage, LOBSTEROrderbookSnapshot or None)
        """
        if not self.orderbook_file:
            for msg in self.read_messages():
                yield msg, None
            return

        with open(self.message_file, 'r') as msg_file, \
             open(self.orderbook_file, 'r') as book_file:

            for msg_line, book_line in zip(msg_file, book_file):
                msg = self.parse_message_line(msg_line)
                book = self.parse_orderbook_line(book_line)

                if msg is not None:
                    yield msg, book

    def to_event(self, msg: LOBSTERMessage, seq_num: int) -> Optional[EventLOBSTER]:
        """
        Convert a LOBSTERMessage to an EventLOBSTER.

        Args:
            msg: The LOBSTER message
            seq_num: Sequence number to assign

        Returns:
            EventLOBSTER or None if the message type is not supported
        """
        event_type = LOBSTER_EVENT_TYPE.get(msg.event_type)
        if event_type is None:
            return None

        return EventLOBSTER(
            seq_num=seq_num,
            time=msg.time,
            event_type=event_type,
            order_id=msg.order_id,
            size=msg.size,
            price=msg.price,
            direction=msg.side
        )

    def count_messages(self) -> int:
        """Count total messages in the file."""
        count = 0
        with open(self.message_file, 'r') as f:
            for _ in f:
                count += 1
        return count


def find_lobster_files(data_dir: str, ticker: str = None) -> List[Tuple[str, str]]:
    """
    Find LOBSTER message and orderbook file pairs in a directory.

    Args:
        data_dir: Directory to search
        ticker: Optional ticker symbol to filter by

    Returns:
        List of (message_file, orderbook_file) path tuples
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return []

    pairs = []

    for msg_file in data_path.glob("*_message_*.csv"):
        # Derive orderbook filename from message filename
        book_file = msg_file.name.replace("_message_", "_orderbook_")
        book_path = msg_file.parent / book_file

        if ticker and not msg_file.name.startswith(ticker):
            continue

        if book_path.exists():
            pairs.append((str(msg_file), str(book_path)))
        else:
            pairs.append((str(msg_file), None))

    return sorted(pairs)


def format_time(seconds_after_midnight: float) -> str:
    """Convert seconds after midnight to HH:MM:SS.mmm format."""
    hours = int(seconds_after_midnight // 3600)
    minutes = int((seconds_after_midnight % 3600) // 60)
    seconds = seconds_after_midnight % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"


def format_price(price: int) -> str:
    """Convert LOBSTER price (price * 10000) to dollar format."""
    return f"${price / 10000:.2f}"


if __name__ == '__main__':
    # Simple test/demo
    import sys

    if len(sys.argv) < 2:
        print("Usage: python lobster_reader.py <message_file> [orderbook_file]")
        sys.exit(1)

    msg_file = sys.argv[1]
    book_file = sys.argv[2] if len(sys.argv) > 2 else None

    reader = LOBSTERReader(msg_file, book_file)

    print(f"Reading: {msg_file}")
    if book_file:
        print(f"With orderbook: {book_file}")
    print()

    count = 0
    for msg, book in reader.read_messages_with_orderbook():
        count += 1
        if count <= 10:
            event_names = {1: "INSERT", 2: "CANCEL", 3: "DELETE", 4: "EXECUTE", 5: "HIDDEN", 7: "HALT"}
            event_name = event_names.get(msg.event_type, "UNKNOWN")
            side = "BUY" if msg.direction == 1 else "SELL"

            print(f"{count:6d}: {format_time(msg.time)} {event_name:8s} "
                  f"id={msg.order_id:12d} size={msg.size:6d} "
                  f"price={format_price(msg.price):>10s} {side}")

            if book:
                print(f"        Book: bid={format_price(book.bid_price)}x{book.bid_size} "
                      f"ask={format_price(book.ask_price)}x{book.ask_size}")

    print(f"\nTotal messages: {count}")
