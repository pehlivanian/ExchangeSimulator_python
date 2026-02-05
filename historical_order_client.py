#!/usr/bin/env python3
"""
Historical Order Client - Replays LOBSTER data file as orders to the exchange.

Reads a LOBSTER message file and converts each message to the appropriate
order command for the exchange:
  - INSERT (type 1) -> limit order
  - CANCEL (type 2) -> cancel (partial cancel becomes full cancel + new order)
  - DELETE (type 3) -> cancel
  - EXECUTE (type 4) -> cancel (liquidity taken, remove from book)
  - HIDDEN (type 5) -> cancel (hidden execution, remove from book)

Usage:
    python historical_order_client.py <message_file> [--throttle MICROSECONDS]
"""

import argparse
import socket
import sys
import time
from typing import Dict, Optional, Tuple

try:
    from .lobster_reader import LOBSTERReader, LOBSTERMessage, format_time, format_price
except ImportError:
    from lobster_reader import LOBSTERReader, LOBSTERMessage, format_time, format_price


class HistoricalOrderClient:
    """
    Client that replays LOBSTER historical data as orders to the exchange.

    Maintains a mapping of LOBSTER order IDs to exchange order IDs since
    the exchange assigns its own order IDs.
    """

    def __init__(self, host: str, port: int, verbose_errors: bool = False):
        self.host = host
        self.port = port
        self._socket: Optional[socket.socket] = None
        self._buffer = ""
        self._verbose_errors = verbose_errors

        # Track LOBSTER order_id -> (exchange_order_id, size, price, side)
        self._order_map: Dict[int, Tuple[int, int, int, str]] = {}

        # Statistics
        self.orders_sent = 0
        self.cancels_sent = 0
        self.errors = 0
        self.skipped = 0

        # Error breakdown
        self.insert_errors = 0
        self.cancel_errors = 0
        self.execute_errors = 0

        # Immediate fills (orders that matched on insert)
        self.immediate_fills = 0

    def connect(self) -> bool:
        """Connect to the exchange. Returns True on success."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            self._socket.settimeout(5.0)
            return True
        except Exception as e:
            print(f"Failed to connect: {e}", file=sys.stderr)
            return False

    def disconnect(self) -> None:
        """Disconnect from the exchange."""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def _send_and_receive(self, message: str) -> str:
        """Send a message and receive the response."""
        if not self._socket:
            return "ERROR,Not connected"

        try:
            if not message.endswith('\n'):
                message += '\n'
            self._socket.sendall(message.encode('utf-8'))

            # Receive responses - need to handle multiple responses
            # (e.g., ACK followed by FILL for immediate matches)
            self._socket.settimeout(5.0)
            data = self._socket.recv(4096)
            self._buffer += data.decode('utf-8')

            # Try to drain any additional buffered data (non-blocking)
            self._socket.setblocking(False)
            try:
                while True:
                    more_data = self._socket.recv(4096)
                    if not more_data:
                        break
                    self._buffer += more_data.decode('utf-8')
            except BlockingIOError:
                pass  # No more data available
            finally:
                self._socket.setblocking(True)

            # Extract all complete lines and return them together
            lines = []
            while '\n' in self._buffer:
                line, self._buffer = self._buffer.split('\n', 1)
                line = line.strip()
                if line:
                    lines.append(line)

            return '\n'.join(lines) if lines else ""

        except socket.timeout:
            return "ERROR,Timeout"
        except Exception as e:
            return f"ERROR,{e}"

    def _parse_ack_response(self, response: str) -> Tuple[Optional[int], bool]:
        """
        Parse an ACK or FILL response to extract the exchange order ID.

        Returns:
            Tuple of (order_id or None, was_fill)
        """
        # Handle multi-line responses (ACK followed by FILL, etc.)
        lines = response.strip().split('\n')
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 2:
                msg_type = parts[0]
                # ACK,order_id,size,price - order is resting
                if msg_type == 'ACK':
                    try:
                        return int(parts[1]), False
                    except ValueError:
                        pass
                # FILL,order_id,size,price - order was fully filled immediately
                elif msg_type == 'FILL':
                    try:
                        return int(parts[1]), True
                    except ValueError:
                        pass
                # PARTIAL_FILL,order_id,filled_size,price,remaining - partial fill
                elif msg_type == 'PARTIAL_FILL':
                    try:
                        return int(parts[1]), False  # Still resting with remaining
                    except ValueError:
                        pass
        return None, False

    def process_insert(self, msg: LOBSTERMessage) -> bool:
        """Process an INSERT event (new limit order)."""
        side = 'B' if msg.direction == 1 else 'S'
        # Use a single consistent user for all orders
        user = "lobster_replay"

        order_str = f"limit,{msg.size},{msg.price},{side},{user}"
        response = self._send_and_receive(order_str)

        exchange_order_id, was_fill = self._parse_ack_response(response)
        if exchange_order_id is not None:
            if was_fill:
                # Order was immediately filled - don't track it
                self.orders_sent += 1
                self.immediate_fills += 1
            else:
                # Order is resting - track it
                self._order_map[msg.order_id] = (exchange_order_id, msg.size, msg.price, side)
                self.orders_sent += 1
            return True
        else:
            self.errors += 1
            self.insert_errors += 1
            if self._verbose_errors:
                print(f"  INSERT error: {order_str} -> {response}")
            return False

    def process_cancel(self, msg: LOBSTERMessage) -> bool:
        """
        Process a CANCEL event (partial cancellation).

        LOBSTER partial cancel reduces order size. We implement as:
        1. Cancel the existing order
        2. Submit a new order with reduced size
        """
        if msg.order_id not in self._order_map:
            self.skipped += 1
            return False

        exchange_order_id, old_size, price, side = self._order_map[msg.order_id]
        user = "lobster_replay"

        # Cancel the existing order
        cancel_str = f"cancel,{exchange_order_id},{user}"
        response = self._send_and_receive(cancel_str)

        if not response.startswith('CANCEL_ACK'):
            self.errors += 1
            self.cancel_errors += 1
            if self._verbose_errors:
                print(f"  CANCEL(partial) error: {cancel_str} -> {response}")
            # Order may have been filled - remove from tracking
            del self._order_map[msg.order_id]
            return False

        self.cancels_sent += 1

        # Calculate new size (old_size - cancelled_amount)
        # In LOBSTER, the 'size' field in CANCEL is the amount being cancelled
        new_size = old_size - msg.size

        if new_size > 0:
            # Submit new order with remaining size
            order_str = f"limit,{new_size},{price},{side},{user}"
            response = self._send_and_receive(order_str)

            new_exchange_id, was_fill = self._parse_ack_response(response)
            if new_exchange_id is not None:
                if was_fill:
                    # Immediately filled - don't track
                    del self._order_map[msg.order_id]
                    self.immediate_fills += 1
                else:
                    self._order_map[msg.order_id] = (new_exchange_id, new_size, price, side)
                self.orders_sent += 1
            else:
                # Order gone from our tracking
                del self._order_map[msg.order_id]
                self.errors += 1
        else:
            # Order fully cancelled
            del self._order_map[msg.order_id]

        return True

    def process_delete(self, msg: LOBSTERMessage) -> bool:
        """Process a DELETE event (full order deletion)."""
        if msg.order_id not in self._order_map:
            self.skipped += 1
            return False

        exchange_order_id, _, _, _ = self._order_map[msg.order_id]
        user = "lobster_replay"

        cancel_str = f"cancel,{exchange_order_id},{user}"
        response = self._send_and_receive(cancel_str)

        if response.startswith('CANCEL_ACK'):
            del self._order_map[msg.order_id]
            self.cancels_sent += 1
            return True
        else:
            self.errors += 1
            self.cancel_errors += 1
            if self._verbose_errors:
                print(f"  DELETE error: {cancel_str} -> {response}")
            del self._order_map[msg.order_id]
            return False

    def process_execute(self, msg: LOBSTERMessage) -> bool:
        """
        Process an EXECUTE event (visible execution).

        The order was executed against, removing liquidity.
        We send a cancel to remove the liquidity from our book.
        """
        if msg.order_id not in self._order_map:
            self.skipped += 1
            return False

        exchange_order_id, old_size, price, side = self._order_map[msg.order_id]
        user = "lobster_replay"

        # Cancel to remove the executed quantity
        cancel_str = f"cancel,{exchange_order_id},{user}"
        response = self._send_and_receive(cancel_str)

        if not response.startswith('CANCEL_ACK'):
            self.errors += 1
            self.execute_errors += 1
            if self._verbose_errors:
                print(f"  EXECUTE error: {cancel_str} -> {response}")
            # Order was likely already filled - remove from tracking
            del self._order_map[msg.order_id]
            return False

        self.cancels_sent += 1

        # Calculate remaining size after execution
        new_size = old_size - msg.size

        if new_size > 0:
            # Re-submit with remaining size
            order_str = f"limit,{new_size},{price},{side},{user}"
            response = self._send_and_receive(order_str)

            new_exchange_id, was_fill = self._parse_ack_response(response)
            if new_exchange_id is not None:
                if was_fill:
                    # Immediately filled - don't track
                    del self._order_map[msg.order_id]
                    self.immediate_fills += 1
                else:
                    self._order_map[msg.order_id] = (new_exchange_id, new_size, price, side)
                self.orders_sent += 1
            else:
                del self._order_map[msg.order_id]
                self.errors += 1
        else:
            del self._order_map[msg.order_id]

        return True

    def process_hidden(self, msg: LOBSTERMessage) -> bool:
        """
        Process a HIDDEN execution event.

        Same as EXECUTE - liquidity was taken from hidden order.
        """
        return self.process_execute(msg)

    def process_message(self, msg: LOBSTERMessage) -> bool:
        """
        Process a single LOBSTER message.

        Returns True if processed successfully, False otherwise.
        """
        if msg.event_type == 1:  # INSERT
            return self.process_insert(msg)
        elif msg.event_type == 2:  # CANCEL (partial)
            return self.process_cancel(msg)
        elif msg.event_type == 3:  # DELETE (full)
            return self.process_delete(msg)
        elif msg.event_type == 4:  # EXECUTE
            return self.process_execute(msg)
        elif msg.event_type == 5:  # HIDDEN
            return self.process_hidden(msg)
        elif msg.event_type == 7:  # TRADING HALT
            self.skipped += 1
            return False
        else:
            self.skipped += 1
            return False


def main():
    parser = argparse.ArgumentParser(
        description='Historical Order Client - Replay LOBSTER data as orders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
LOBSTER Event Types:
  1 = INSERT   -> Submit limit order
  2 = CANCEL   -> Cancel + resubmit with reduced size
  3 = DELETE   -> Cancel order
  4 = EXECUTE  -> Cancel (liquidity taken)
  5 = HIDDEN   -> Cancel (hidden execution)
  7 = HALT     -> Skip

Example:
  python historical_order_client.py data/AMZN_message.csv --throttle 100
"""
    )
    parser.add_argument('message_file', help='LOBSTER message file to replay')
    parser.add_argument('--host', default='localhost',
                        help='Exchange host (default: localhost)')
    parser.add_argument('--port', type=int, default=10000,
                        help='Exchange port (default: 10000)')
    parser.add_argument('--throttle', type=int, default=0, metavar='MICROSECONDS',
                        help='Microseconds to sleep between messages (0=no throttle)')
    parser.add_argument('--max-messages', type=int, default=0, metavar='N',
                        help='Stop after N messages (0=process all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print each message as it is processed')
    parser.add_argument('--verbose-errors', action='store_true',
                        help='Print detailed error messages')
    parser.add_argument('--print-every', type=int, default=0, metavar='N',
                        help='Print progress every N messages')

    args = parser.parse_args()

    # Calculate throttle in seconds
    throttle_s = args.throttle / 1_000_000.0 if args.throttle > 0 else 0

    # Open LOBSTER file
    try:
        reader = LOBSTERReader(args.message_file)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Connect to exchange
    client = HistoricalOrderClient(args.host, args.port, verbose_errors=args.verbose_errors)
    if not client.connect():
        sys.exit(1)

    print("=" * 70)
    print("  HISTORICAL ORDER CLIENT")
    print("=" * 70)
    print(f"  Message file: {args.message_file}")
    print(f"  Exchange:     {args.host}:{args.port}")
    if args.throttle > 0:
        print(f"  Throttle:     {args.throttle} us between messages")
    if args.max_messages > 0:
        print(f"  Max messages: {args.max_messages}")
    print("=" * 70)
    print()

    event_names = {1: "INSERT", 2: "CANCEL", 3: "DELETE", 4: "EXECUTE", 5: "HIDDEN", 7: "HALT"}
    message_count = 0
    start_time = time.time()

    try:
        for msg in reader.read_messages():
            message_count += 1

            if args.max_messages > 0 and message_count > args.max_messages:
                break

            if args.verbose:
                event_name = event_names.get(msg.event_type, "UNKNOWN")
                side = "BUY" if msg.direction == 1 else "SELL"
                print(f"[{message_count:7d}] {format_time(msg.time)} {event_name:8s} "
                      f"id={msg.order_id:12d} size={msg.size:6d} "
                      f"price={format_price(msg.price):>10s} {side}")

            client.process_message(msg)

            if args.print_every > 0 and message_count % args.print_every == 0:
                elapsed = time.time() - start_time
                rate = message_count / elapsed if elapsed > 0 else 0
                print(f"  Processed {message_count} messages "
                      f"({rate:.0f} msg/s, {client.orders_sent} orders, "
                      f"{client.cancels_sent} cancels, {client.errors} errors)")

            if throttle_s > 0:
                time.sleep(throttle_s)

    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        client.disconnect()

    # Final summary
    elapsed = time.time() - start_time
    rate = message_count / elapsed if elapsed > 0 else 0

    print()
    print("=" * 70)
    print("  REPLAY COMPLETE")
    print("=" * 70)
    print(f"  Messages processed: {message_count}")
    print(f"  Orders sent:        {client.orders_sent}")
    print(f"  Immediate fills:    {client.immediate_fills}")
    print(f"  Cancels sent:       {client.cancels_sent}")
    print(f"  Errors:             {client.errors}")
    if client.errors > 0:
        print(f"    - Insert errors:  {client.insert_errors}")
        print(f"    - Cancel errors:  {client.cancel_errors}")
        print(f"    - Execute errors: {client.execute_errors}")
    print(f"  Skipped:            {client.skipped}")
    print(f"  Elapsed time:       {elapsed:.2f}s ({rate:.0f} msg/s)")
    print(f"  Active orders:      {len(client._order_map)}")
    print("=" * 70)


if __name__ == '__main__':
    main()
