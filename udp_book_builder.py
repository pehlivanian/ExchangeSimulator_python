#!/usr/bin/env python3
"""
UDP Market Data Client with Order Book Building.

Subscribes to market data feed, builds order book from LOBSTER format messages,
and displays the book state.

Usage:
    python udp_book_builder.py [--port PORT] [--levels N] [--update-every N]
    python udp_book_builder.py --plot-after 10000 --save-plot bid_ask.pdf
"""

import argparse
import socket
import sys
import time
from typing import List, Optional, Tuple

from order_book import OrderBook
from messages import EventLOBSTER, EventType


class BookBuilder:
    """Builds and maintains an order book from LOBSTER format market data."""

    # LOBSTER event type mapping
    EVENT_TYPES = {
        1: EventType.INSERT,
        2: EventType.CANCEL,
        3: EventType.DELETE,
        4: EventType.EXECUTE,
        5: EventType.HIDDEN,
    }

    def __init__(self, record_prices: bool = False):
        self.book = OrderBook()
        self.message_count = 0
        self.trade_count = 0
        self._seq_num = 0

        # Price recording for plotting
        self._record_prices = record_prices
        self.timestamps: List[float] = []
        self.bid_prices: List[Optional[float]] = []
        self.ask_prices: List[Optional[float]] = []

    def process_message(self, lobster_line: str) -> bool:
        """
        Process a LOBSTER format message and update the book.

        Format: Time,Type,OrderID,Size,Price,Direction
        Returns True if message was processed successfully.
        """
        try:
            parts = lobster_line.strip().split(',')
            if len(parts) != 6:
                return False

            time_val = float(parts[0])
            event_type_int = int(parts[1])
            order_id = int(parts[2])
            size = int(parts[3])
            price = int(parts[4])
            direction = int(parts[5])

            # Skip hidden orders
            if event_type_int == 5:
                return True

            # Convert direction to side
            side = 'B' if direction == 1 else 'S'

            # Get event type
            event_type = self.EVENT_TYPES.get(event_type_int)
            if event_type is None:
                return False

            # Create event
            self._seq_num += 1
            event = EventLOBSTER(
                seq_num=self._seq_num,
                time=time_val,
                event_type=event_type,
                order_id=order_id,
                size=size,
                price=price,
                direction=side
            )

            # Process event
            ack, trades = self.book.process_event(event)

            self.message_count += 1
            if trades:
                self.trade_count += len(trades)

            # Record bid/ask prices if enabled
            if self._record_prices:
                bid = self.book.get_best_bid_price()
                ask = self.book.get_best_ask_price()
                self.timestamps.append(time_val)
                self.bid_prices.append(bid / 10000.0 if bid else None)
                self.ask_prices.append(ask / 10000.0 if ask else None)

            return True

        except (ValueError, IndexError) as e:
            return False

    def get_book_display(self, levels: int = 10) -> str:
        """Get a formatted string representation of the order book."""
        bids, asks = self.book.get_snapshot()

        lines = []
        lines.append("")
        lines.append("┌─────────────────────────────────────────────────┐")
        lines.append("│              ORDER BOOK                         │")
        lines.append("├─────────────────────────────────────────────────┤")
        lines.append("│      BIDS (Buy)      │      ASKS (Sell)         │")
        lines.append("│   Price    │  Size   │   Price    │  Size      │")
        lines.append("├──────────────────────┼──────────────────────────┤")

        max_rows = max(min(len(bids), levels), min(len(asks), levels))
        if max_rows == 0:
            lines.append("│         (empty)      │         (empty)          │")
        else:
            for i in range(max_rows):
                line = "│ "

                # Bid side
                if i < len(bids):
                    bid_price = bids[i].price / 10000.0
                    bid_size = bids[i].size
                    line += f"{bid_price:9.2f} │ {bid_size:7d} │ "
                else:
                    line += "          │         │ "

                # Ask side
                if i < len(asks):
                    ask_price = asks[i].price / 10000.0
                    ask_size = asks[i].size
                    line += f"{ask_price:9.2f} │ {ask_size:7d}    │"
                else:
                    line += "          │            │"

                lines.append(line)

        lines.append("└─────────────────────────────────────────────────┘")

        # Add stats
        bid_price = self.book.get_best_bid_price()
        ask_price = self.book.get_best_ask_price()
        if bid_price and ask_price:
            spread = (ask_price - bid_price) / 10000.0
            mid = (bid_price + ask_price) / 2 / 10000.0
            lines.append(f"  Spread: ${spread:.2f}  Mid: ${mid:.2f}")

        lines.append(f"  Messages: {self.message_count}  Trades: {self.trade_count}")

        return "\n".join(lines)

    def save_plot(self, filename: str) -> bool:
        """
        Save a plot of bid/ask prices over time to a PDF file.

        Args:
            filename: Output PDF filename

        Returns:
            True if plot was saved successfully, False otherwise.
        """
        if not self.timestamps:
            print("No data to plot.")
            return False

        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            from datetime import datetime, timedelta
        except ImportError:
            print("Error: matplotlib is required for plotting. Install with: pip install matplotlib")
            return False

        # Convert timestamps (seconds after midnight) to datetime for better axis labels
        base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        times = [base_date + timedelta(seconds=t) for t in self.timestamps]

        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot bid and ask prices
        ax.plot(times, self.bid_prices, 'b-', label='Bid', linewidth=0.5, alpha=0.8)
        ax.plot(times, self.ask_prices, 'r-', label='Ask', linewidth=0.5, alpha=0.8)

        # Formatting
        ax.set_xlabel('Time')
        ax.set_ylabel('Price ($)')
        ax.set_title(f'Bid/Ask Prices ({self.message_count:,} messages)')
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)

        # Format x-axis as time
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()

        # Tight layout
        plt.tight_layout()

        # Save to PDF
        plt.savefig(filename, format='pdf', dpi=150)
        plt.close()

        print(f"Plot saved to {filename}")
        return True


def clear_screen():
    """Clear the terminal screen."""
    print("\033[2J\033[H", end="")


def main():
    parser = argparse.ArgumentParser(
        description='UDP Market Data Client with Order Book Building'
    )
    parser.add_argument('--port', type=int, default=10002,
                        help='UDP port to subscribe to (default: 10002)')
    parser.add_argument('--levels', type=int, default=10,
                        help='Number of price levels to display (default: 10)')
    parser.add_argument('--update-every', type=int, default=1,
                        help='Update display every N messages (default: 1)')
    parser.add_argument('--no-clear', action='store_true',
                        help='Do not clear screen between updates')
    parser.add_argument('--save-plot', type=str, metavar='FILENAME',
                        help='Save bid/ask plot to PDF file')
    parser.add_argument('--plot-after', type=int, default=0, metavar='N',
                        help='Save plot after N messages (0=at end only)')
    args = parser.parse_args()

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5.0)

    # Send subscription message
    server_addr = ('127.0.0.1', args.port)
    sock.sendto(b"subscribe", server_addr)

    print(f"Subscribed to market data on port {args.port}")
    print(f"Building order book from LOBSTER format messages...")
    print(f"Display levels: {args.levels}, Update every: {args.update_every} messages")
    if args.save_plot:
        print(f"Will save plot to: {args.save_plot}")
        if args.plot_after > 0:
            print(f"Plot will be saved after {args.plot_after} messages")
    print("Press Ctrl+C to stop.")
    print()

    # Enable price recording if plotting is requested
    record_prices = args.save_plot is not None
    builder = BookBuilder(record_prices=record_prices)
    last_display_count = 0
    plot_saved = False

    try:
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                msg = data.decode('utf-8').strip()

                if builder.process_message(msg):
                    # Update display
                    if builder.message_count - last_display_count >= args.update_every:
                        if not args.no_clear:
                            clear_screen()
                        print(builder.get_book_display(args.levels))
                        last_display_count = builder.message_count

                    # Save plot if threshold reached
                    if (args.save_plot and args.plot_after > 0 and
                            not plot_saved and builder.message_count >= args.plot_after):
                        builder.save_plot(args.save_plot)
                        plot_saved = True

            except socket.timeout:
                print("(waiting for data...)")
                continue

    except KeyboardInterrupt:
        print(f"\n\nFinal state after {builder.message_count} messages:")
        print(builder.get_book_display(args.levels))
    finally:
        # Save plot if requested and not already saved
        if args.save_plot and not plot_saved:
            builder.save_plot(args.save_plot)
        sock.close()


if __name__ == '__main__':
    main()
