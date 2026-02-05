#!/usr/bin/env python3
"""
Order Client - Sends orders to the exchange and receives responses.

Runs in async mode to properly handle passive fill notifications.

Mirrors the C++ order_client_async.cpp implementation.
"""

import argparse
import socket
import sys
import threading
import time


class OrderClient:
    """
    Client for sending orders to the exchange.

    Supports staying connected to receive async fill notifications.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._socket = None
        self._running = False
        self._receive_thread = None
        self._buffer = ""

    def connect(self) -> bool:
        """Connect to the exchange. Returns True on success."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Failed to connect: {e}", file=sys.stderr)
            return False

    def disconnect(self) -> None:
        """Disconnect from the exchange."""
        self._running = False
        if self._receive_thread:
            self._receive_thread.join(timeout=2.0)
            self._receive_thread = None

        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def send_order(self, order: str) -> str:
        """
        Send an order and wait for the response (synchronous).

        Args:
            order: Order string in format "type,size,price,side,user"

        Returns:
            Response string from the exchange (may contain multiple lines
            if async messages were buffered)
        """
        if not self._socket:
            return "ERROR,Not connected"

        try:
            # Send order
            if not order.endswith('\n'):
                order += '\n'
            self._socket.sendall(order.encode('utf-8'))

            # Receive response - first blocking read
            self._socket.settimeout(5.0)
            data = self._socket.recv(4096)
            self._buffer += data.decode('utf-8')

            # Drain any additional buffered data (non-blocking)
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

            # Extract complete lines from buffer
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

    def start_async_receive(self, callback=None) -> None:
        """
        Start receiving messages asynchronously.

        Args:
            callback: Optional function to call with each received message.
                     If None, messages are printed to stdout.
        """
        self._running = True
        self._receive_thread = threading.Thread(
            target=self._receive_loop,
            args=(callback,),
            daemon=True
        )
        self._receive_thread.start()

    def _receive_loop(self, callback) -> None:
        """Receive loop for async messages."""
        self._socket.setblocking(False)

        while self._running:
            try:
                data = self._socket.recv(4096)
                if not data:
                    break

                self._buffer += data.decode('utf-8')

                # Process complete lines
                while '\n' in self._buffer:
                    line, self._buffer = self._buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        if callback:
                            callback(line)
                        else:
                            # Format messages for display
                            formatted = self._format_message(line)
                            print(f"[ASYNC] {formatted}", flush=True)

    def _format_message(self, line: str) -> str:
        """Format a message for display, converting prices to dollars."""
        parts = line.split(',')
        if not parts:
            return line
        msg_type = parts[0]
        try:
            if msg_type == 'FILL' and len(parts) >= 4:
                order_id = parts[1]
                size = int(parts[2])
                price = int(parts[3])
                price_str = f"${price / 10000:.2f}"
                return f"FILL Order {order_id}: {size} @ {price_str}"
            elif msg_type == 'PARTIAL_FILL' and len(parts) >= 5:
                order_id = parts[1]
                size = int(parts[2])
                price = int(parts[3])
                remainder = int(parts[4])
                price_str = f"${price / 10000:.2f}"
                return f"PARTIAL_FILL Order {order_id}: {size} @ {price_str}, {remainder} remaining"
            elif msg_type == 'ACK' and len(parts) >= 4:
                order_id = parts[1]
                size = int(parts[2])
                price = int(parts[3])
                price_str = f"${price / 10000:.2f}"
                return f"ACK Order {order_id}: {size} @ {price_str}"
            elif msg_type == 'CANCEL_ACK' and len(parts) >= 3:
                order_id = parts[1]
                size = int(parts[2])
                return f"CANCEL_ACK Order {order_id}: {size} cancelled"
        except (ValueError, IndexError):
            pass
        return line

            except BlockingIOError:
                time.sleep(0.01)  # 10ms
            except Exception as e:
                if self._running:
                    print(f"Receive error: {e}", file=sys.stderr)
                break

    def send_order_async(self, order: str) -> bool:
        """
        Send an order without waiting for response (async mode).

        Response will come through the async receive callback.
        """
        if not self._socket:
            return False

        try:
            if not order.endswith('\n'):
                order += '\n'
            self._socket.sendall(order.encode('utf-8'))
            return True
        except Exception as e:
            print(f"Send error: {e}", file=sys.stderr)
            return False

    def send_cancel(self, order_id: int, user: str) -> str:
        """
        Send a cancel request for an existing order (synchronous).

        Args:
            order_id: The order ID to cancel
            user: The user who placed the order

        Returns:
            Response string from the exchange
        """
        cancel_order = f"cancel,{order_id},{user}"
        return self.send_order(cancel_order)

    def send_cancel_async(self, order_id: int, user: str) -> bool:
        """
        Send a cancel request without waiting for response (async mode).

        Args:
            order_id: The order ID to cancel
            user: The user who placed the order

        Returns:
            True if sent successfully, False otherwise
        """
        cancel_order = f"cancel,{order_id},{user}"
        return self.send_order_async(cancel_order)


ORDER_HELP = """
============================================================
  ORDER CLIENT - Supported Order Types
============================================================

LIMIT ORDER - Post a passive order to the book
  Format:  limit,size,price,side,user[,ttl]
  Example: limit,100,50000000,B,trader1    (Buy 100 @ $5000.00, TTL=1 hour)
  Example: limit,50,50500000,S,trader1,60  (Sell 50 @ $5050.00, TTL=60 sec)

MARKET ORDER - Execute immediately against the book
  Format:  market,size,0,side,user
  Example: market,100,0,B,trader1          (Market buy 100 shares)
  Example: market,50,0,S,trader1           (Market sell 50 shares)

CANCEL ORDER - Cancel a resting order
  Format:  cancel,order_id,user
  Example: cancel,1000,trader1             (Cancel order #1000)

------------------------------------------------------------
Price format: price * 10000 (e.g., $5000.00 = 50000000)
Side: B = Buy, S = Sell
TTL: Time-to-live in seconds (default: 3600 = 1 hour)
============================================================
"""


def main():
    parser = argparse.ArgumentParser(
        description='Order Client - Send orders to the exchange',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=ORDER_HELP
    )
    parser.add_argument('--host', default='localhost',
                        help='Exchange host (default: localhost)')
    parser.add_argument('--port', type=int, default=10000,
                        help='Exchange port (default: 10000)')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Suppress help message in interactive mode')
    parser.add_argument('orders', nargs='*',
                        help='Orders to send (format: type,size,price,side,user)')
    args = parser.parse_args()

    client = OrderClient(args.host, args.port)

    if not client.connect():
        sys.exit(1)

    try:
        # Start async receive thread
        client.start_async_receive()

        # Send command-line orders
        for order in args.orders:
            print(f"Sending: {order}")
            client.send_order_async(order)
            time.sleep(0.1)  # Small delay between orders

        # Interactive mode - read from stdin while receiving
        if not args.orders:
            if not args.quiet:
                print(ORDER_HELP)
        print("Enter orders (Ctrl+C to quit):")
        try:
            for line in sys.stdin:
                line = line.strip()
                if line:
                    print(f"Sending: {line}")
                    client.send_order_async(line)
        except KeyboardInterrupt:
            pass

    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
