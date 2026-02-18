#!/usr/bin/env python3
"""
UDP Market Data Client with Order Book Building.

Subscribes to market data feed, builds order book from LOBSTER format messages,
and displays the book state.

Usage:
    python udp_book_builder.py [--port PORT] [--levels N] [--update-every N]
    python udp_book_builder.py --plot-after 10000 --save-plot bid_ask.pdf
    python udp_book_builder.py --diagnostics report.json
"""

import argparse
import collections
import json
import queue
import socket
import statistics
import threading
import time
from typing import Dict, List, Optional, Tuple

from order_book import OrderBook
from messages import EventLOBSTER, EventType


# ---------------------------------------------------------------------------
# DiagnosticsCollector — runs on a background thread
# ---------------------------------------------------------------------------

class DiagnosticsCollector:
    """Collects order book diagnostics from a lock-free queue.

    The main (hot-path) thread enqueues lightweight snapshot tuples via
    ``feed()``.  The background thread pulls from the queue and accumulates
    statistics.  On ``stop()`` the thread drains the queue and writes a
    JSON report.
    """

    def __init__(self, output_file: str):
        self._output_file = output_file
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

        # --- accumulators ---
        self.msg_type_counts: Dict[str, int] = collections.defaultdict(int)
        self._spreads: List[float] = []
        self._book_snapshots: List[dict] = []

        # timing
        self._start_wall = None
        self._end_wall = None
        self._first_ts = None
        self._last_ts = None
        self._event_count = 0

    # --- public API (called from main thread) ---

    def start(self):
        self._start_wall = time.time()
        self._thread.start()

    def feed(self, timestamp: float, event_type_str: str, price: int,
             size: int, side: str, bid_price: Optional[int],
             ask_price: Optional[int], bid_size: Optional[int],
             ask_size: Optional[int],
             bids_snapshot: List[Tuple[int, int]],
             asks_snapshot: List[Tuple[int, int]]):
        """Enqueue a snapshot — O(1), non-blocking."""
        self._queue.put_nowait((
            timestamp, event_type_str, price, size, side,
            bid_price, ask_price, bid_size, ask_size,
            bids_snapshot, asks_snapshot,
        ))

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=10)
        self._end_wall = time.time()
        self._write_report()

    # --- background thread ---

    def _run(self):
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.1)
                self._process(item)
            except queue.Empty:
                continue
        # drain remaining
        while True:
            try:
                item = self._queue.get_nowait()
                self._process(item)
            except queue.Empty:
                break

    def _process(self, item):
        (timestamp, event_type_str, price, size, side,
         bid_price, ask_price, bid_size, ask_size,
         bids_snap, asks_snap) = item

        self._event_count += 1
        if self._first_ts is None:
            self._first_ts = timestamp
        self._last_ts = timestamp

        # message type counts
        self.msg_type_counts[event_type_str] += 1

        # spread when BBO is valid
        if bid_price is not None and ask_price is not None:
            spread = (ask_price - bid_price) / 10000.0
            self._spreads.append(spread)

        # book shape snapshot (sample every 500 events to bound memory)
        if self._event_count % 500 == 0 and (bids_snap or asks_snap):
            self._book_snapshots.append({
                "bids": [(p / 10000.0, s) for p, s in bids_snap],
                "asks": [(p / 10000.0, s) for p, s in asks_snap],
            })

    # --- helpers ---

    def _avg_book_shape(self) -> Optional[dict]:
        """Average depth by level across all sampled snapshots."""
        if not self._book_snapshots:
            return None
        max_bid_levels = max(len(s["bids"]) for s in self._book_snapshots)
        max_ask_levels = max(len(s["asks"]) for s in self._book_snapshots)
        n = len(self._book_snapshots)

        avg_bids = []
        for lvl in range(max_bid_levels):
            sizes = [s["bids"][lvl][1] for s in self._book_snapshots
                     if lvl < len(s["bids"])]
            avg_bids.append({"level": lvl, "avg_size": sum(sizes) / len(sizes),
                             "sample_count": len(sizes)})

        avg_asks = []
        for lvl in range(max_ask_levels):
            sizes = [s["asks"][lvl][1] for s in self._book_snapshots
                     if lvl < len(s["asks"])]
            avg_asks.append({"level": lvl, "avg_size": sum(sizes) / len(sizes),
                             "sample_count": len(sizes)})

        return {"bids": avg_bids, "asks": avg_asks, "num_snapshots": n}

    # --- report generation ---

    def _build_report(self) -> dict:
        spread_stats: dict = {}
        if self._spreads:
            spread_stats = {
                "mean": statistics.mean(self._spreads),
                "median": statistics.median(self._spreads),
                "stdev": statistics.stdev(self._spreads) if len(self._spreads) > 1 else 0.0,
                "min": min(self._spreads),
                "max": max(self._spreads),
                "count": len(self._spreads),
            }

        duration_wall = (self._end_wall - self._start_wall) if self._start_wall and self._end_wall else 0
        duration_data = (self._last_ts - self._first_ts) if self._first_ts is not None and self._last_ts is not None else 0

        return {
            "meta": {
                "wall_clock_seconds": round(duration_wall, 3),
                "data_time_span_seconds": round(duration_data, 3),
                "total_events": self._event_count,
            },
            "message_type_breakdown": dict(self.msg_type_counts),
            "spread_statistics": spread_stats,
            "average_book_shape": self._avg_book_shape(),
        }

    def _write_report(self):
        report = self._build_report()
        with open(self._output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        self._print_summary(report)

    @staticmethod
    def _print_summary(r: dict):
        meta = r["meta"]
        print("\n" + "=" * 60)
        print("  ORDER BOOK DIAGNOSTICS REPORT")
        print("=" * 60)
        print(f"  Wall clock:       {meta['wall_clock_seconds']:.1f}s")
        print(f"  Data time span:   {meta['data_time_span_seconds']:.1f}s")
        print(f"  Total events:     {meta['total_events']:,}")

        print("\n  Message type breakdown:")
        for k, v in r["message_type_breakdown"].items():
            print(f"    {k:12s}  {v:>8,}")

        ss = r["spread_statistics"]
        if ss:
            print(f"\n  Spread: mean=${ss['mean']:.4f}  "
                  f"median=${ss['median']:.4f}  "
                  f"std=${ss['stdev']:.4f}  "
                  f"[${ss['min']:.4f}, ${ss['max']:.4f}]")

        shape = r["average_book_shape"]
        if shape:
            print(f"\n  Avg book shape ({shape['num_snapshots']} snapshots):")
            for lvl in shape["bids"][:5]:
                print(f"    bid L{lvl['level']}: avg_size={lvl['avg_size']:.0f}")
            for lvl in shape["asks"][:5]:
                print(f"    ask L{lvl['level']}: avg_size={lvl['avg_size']:.0f}")

        print("=" * 60)

    def plot_book_shape(self, filename: str, max_levels: int = 0) -> bool:
        """Plot average book shape (depth by level) as a horizontal bar chart.

        Args:
            filename: Output file (PDF/PNG).
            max_levels: Maximum levels to plot (0 = all).
        """
        report = self._build_report()
        shape = report.get("average_book_shape")
        if not shape:
            print("No book shape data to plot (need at least 500 events).")
            return False

        try:
            import matplotlib.pyplot as plt
            import matplotlib.ticker as ticker
        except ImportError:
            print("Error: matplotlib is required for plotting. "
                  "Install with: pip install matplotlib")
            return False

        bid_levels = shape["bids"]
        ask_levels = shape["asks"]

        total_levels = max(len(bid_levels), len(ask_levels))
        if total_levels == 0:
            print("No book shape levels to plot.")
            return False

        if max_levels > 0:
            bid_levels = bid_levels[:max_levels]
            ask_levels = ask_levels[:max_levels]

        max_levels_plot = max(len(bid_levels), len(ask_levels))

        fig, ax = plt.subplots(figsize=(10, max(4, max_levels_plot * 0.4)))

        # Bids extend left (negative x), asks extend right (positive x)
        y_positions = list(range(max_levels_plot))
        bid_sizes = [bid_levels[i]["avg_size"] if i < len(bid_levels) else 0
                     for i in range(max_levels_plot)]
        ask_sizes = [ask_levels[i]["avg_size"] if i < len(ask_levels) else 0
                     for i in range(max_levels_plot)]

        ax.barh(y_positions, [-s for s in bid_sizes], color='#2196F3',
                alpha=0.8, label='Bids')
        ax.barh(y_positions, ask_sizes, color='#F44336',
                alpha=0.8, label='Asks')

        ax.set_ylabel('Level (0 = best)')
        ax.set_xlabel('Average Size')
        ax.set_title(f'Average Book Shape ({shape["num_snapshots"]} snapshots)')
        ax.set_yticks(y_positions)
        ax.set_yticklabels([f'L{i}' for i in y_positions])
        ax.invert_yaxis()
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3, axis='x')

        # Make x-axis labels absolute values
        ax.xaxis.set_major_formatter(
            ticker.FuncFormatter(lambda x, _: f'{abs(x):.0f}'))

        plt.tight_layout()
        plt.savefig(filename, dpi=150)
        plt.close()

        print(f"Book shape plot saved to {filename}")
        return True


# ---------------------------------------------------------------------------
# BookBuilder (unchanged except for process_message return value)
# ---------------------------------------------------------------------------

class BookBuilder:
    """Builds and maintains an order book from LOBSTER format market data."""

    EVENT_TYPES = {
        1: EventType.INSERT,
        2: EventType.CANCEL,
        3: EventType.DELETE,
        4: EventType.EXECUTE,
        5: EventType.HIDDEN,
    }

    EVENT_TYPE_NAMES = {
        1: 'INSERT',
        2: 'CANCEL',
        3: 'DELETE',
        4: 'EXECUTE',
        5: 'HIDDEN',
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

    def seed_from_orderbook_file(self, orderbook_file: str) -> int:
        """
        Seed the order book from a LOBSTER orderbook snapshot file.

        LOBSTER orderbook format (CSV, no header):
        ask_price_1, ask_size_1, bid_price_1, bid_size_1, ask_price_2, ...

        Returns:
            Number of price levels seeded
        """
        try:
            with open(orderbook_file, 'r') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return 0

                parts = first_line.split(',')
                levels_seeded = 0

                for i in range(0, len(parts) - 3, 4):
                    ask_price = int(parts[i])
                    ask_size = int(parts[i + 1])
                    bid_price = int(parts[i + 2])
                    bid_size = int(parts[i + 3])

                    self.book.seed_from_snapshot(
                        bid_price, bid_size, ask_price, ask_size, time=0.0
                    )
                    levels_seeded += 1

                return levels_seeded

        except Exception as e:
            print(f"Warning: Could not seed order book from {orderbook_file}: {e}")
            return 0

    def process_message(self, lobster_line: str) -> Optional[Tuple]:
        """
        Process a LOBSTER format message and update the book.

        Format: Time,Type,OrderID,Size,Price,Direction

        Returns:
            A tuple (timestamp, event_type_str, price, size, side,
                     bid_price, ask_price, bid_size, ask_size,
                     bids_snapshot, asks_snapshot) on success,
            or None on failure.
        """
        try:
            parts = lobster_line.strip().split(',')
            if len(parts) != 6:
                return None

            time_val = float(parts[0])
            event_type_int = int(parts[1])
            order_id = int(parts[2])
            size = int(parts[3])
            price = int(parts[4])
            direction = int(parts[5])

            # Skip hidden orders
            if event_type_int == 5:
                return ()  # truthy empty tuple — processed but nothing to diagnose

            side = 'B' if direction == 1 else 'S'

            event_type = self.EVENT_TYPES.get(event_type_int)
            if event_type is None:
                return None

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

            # Build lightweight snapshot for diagnostics
            bid_p = self.book.get_best_bid_price()
            ask_p = self.book.get_best_ask_price()
            bid_s = self.book.get_best_bid_size()
            ask_s = self.book.get_best_ask_size()

            bids, asks = self.book.get_snapshot()
            bids_snap = [(lvl.price, lvl.size) for lvl in bids]
            asks_snap = [(lvl.price, lvl.size) for lvl in asks]

            event_name = self.EVENT_TYPE_NAMES.get(event_type_int, 'UNKNOWN')

            return (time_val, event_name, price, size, side,
                    bid_p, ask_p, bid_s, ask_s, bids_snap, asks_snap)

        except (ValueError, IndexError):
            return None

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

                if i < len(bids):
                    bid_price = bids[i].price / 10000.0
                    bid_size = bids[i].size
                    line += f"{bid_price:9.2f} │ {bid_size:7d} │ "
                else:
                    line += "          │         │ "

                if i < len(asks):
                    ask_price = asks[i].price / 10000.0
                    ask_size = asks[i].size
                    line += f"{ask_price:9.2f} │ {ask_size:7d}    │"
                else:
                    line += "          │            │"

                lines.append(line)

        lines.append("└─────────────────────────────────────────────────┘")

        bid_price = self.book.get_best_bid_price()
        ask_price = self.book.get_best_ask_price()
        if bid_price and ask_price:
            spread = (ask_price - bid_price) / 10000.0
            mid = (bid_price + ask_price) / 2 / 10000.0
            lines.append(f"  Spread: ${spread:.2f}  Mid: ${mid:.2f}")

        lines.append(f"  Messages: {self.message_count}  Trades: {self.trade_count}")

        return "\n".join(lines)

    def save_plot(self, filename: str) -> bool:
        """Save a plot of bid/ask prices over time to a PDF file."""
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

        base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        times = [base_date + timedelta(seconds=t) for t in self.timestamps]

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(times, self.bid_prices, 'b-', label='Bid', linewidth=0.5, alpha=0.8)
        ax.plot(times, self.ask_prices, 'r-', label='Ask', linewidth=0.5, alpha=0.8)

        ax.set_xlabel('Time')
        ax.set_ylabel('Price ($)')
        ax.set_title(f'Bid/Ask Prices ({self.message_count:,} messages)')
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()

        plt.tight_layout()
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
    parser.add_argument('--orderbook', type=str, metavar='FILE',
                        help='LOBSTER orderbook file to seed initial book state')
    parser.add_argument('--diagnostics', type=str, metavar='FILE',
                        help='Enable microstructure diagnostics; write JSON report to FILE on exit')
    parser.add_argument('--plot-book-shape', type=str, metavar='FILE',
                        help='Save average book shape plot to FILE (requires --diagnostics)')
    parser.add_argument('--max-shape-levels', type=int, default=0, metavar='N',
                        help='Max levels in book shape plot (0=all, default: 0)')
    args = parser.parse_args()

    if args.plot_book_shape and not args.diagnostics:
        parser.error('--plot-book-shape requires --diagnostics')

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5.0)

    server_addr = ('127.0.0.1', args.port)
    sock.sendto(b"subscribe", server_addr)

    print(f"Subscribed to market data on port {args.port}")
    print(f"Building order book from LOBSTER format messages...")
    print(f"Display levels: {args.levels}, Update every: {args.update_every} messages")
    if args.save_plot:
        print(f"Will save plot to: {args.save_plot}")
        if args.plot_after > 0:
            print(f"Plot will be saved after {args.plot_after} messages")
    if args.diagnostics:
        print(f"Diagnostics enabled — report will be written to: {args.diagnostics}")
    print("Press Ctrl+C to stop.")
    print()

    record_prices = args.save_plot is not None
    builder = BookBuilder(record_prices=record_prices)

    # Seed order book if provided
    if args.orderbook:
        levels = builder.seed_from_orderbook_file(args.orderbook)
        if levels > 0:
            print(f"Seeded order book with {levels} price level(s) from {args.orderbook}")
            if record_prices:
                bid = builder.book.get_best_bid_price()
                ask = builder.book.get_best_ask_price()
                builder.timestamps.append(0.0)
                builder.bid_prices.append(bid / 10000.0 if bid else None)
                builder.ask_prices.append(ask / 10000.0 if ask else None)
        else:
            print(f"Warning: No levels seeded from {args.orderbook}")

    # Start diagnostics collector if requested
    diag: Optional[DiagnosticsCollector] = None
    if args.diagnostics:
        diag = DiagnosticsCollector(args.diagnostics)
        diag.start()

    last_display_count = 0
    plot_saved = False

    try:
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                msg = data.decode('utf-8').strip()

                result = builder.process_message(msg)
                if result is not None:
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

                    # Feed diagnostics (non-blocking enqueue)
                    if diag and len(result) > 0:
                        diag.feed(*result)

            except socket.timeout:
                print("(waiting for data...)")
                continue

    except KeyboardInterrupt:
        print(f"\n\nFinal state after {builder.message_count} messages:")
        print(builder.get_book_display(args.levels))
    finally:
        if args.save_plot and not plot_saved:
            builder.save_plot(args.save_plot)
        if diag:
            diag.stop()
            print(f"Diagnostics report written to: {args.diagnostics}")
            if args.plot_book_shape:
                diag.plot_book_shape(args.plot_book_shape,
                                     max_levels=args.max_shape_levels)
        sock.close()


if __name__ == '__main__':
    main()
