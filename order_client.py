#!/usr/bin/env python3
"""
Order Client - Sends orders to the exchange and receives responses.

Supports both synchronous (send order, wait for response) and asynchronous
(stay connected to receive passive fill notifications) modes.

Mirrors the C++ order_client_example.cpp and order_client_async.cpp implementations.
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
            Response string from the exchange
        """
        if not self._socket:
            return "ERROR,Not connected"

        try:
            # Send order
            if not order.endswith('\n'):
                order += '\n'
            self._socket.sendall(order.encode('utf-8'))

            # Receive response
            self._socket.settimeout(5.0)
            data = self._socket.recv(4096)
            return data.decode('utf-8').strip()

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
                            print(f"[ASYNC] {line}", flush=True)

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


ORDER_HELP = """
============================================================
  ORDER CLIENT - Supported Order Types
============================================================

LIMIT ORDER - Post a passive order to the book
  Format:  limit,size,price,side,user
  Example: limit,100,50000000,B,trader1    (Buy 100 @ $5000.00)
  Example: limit,50,50500000,S,trader1     (Sell 50 @ $5050.00)

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
    parser.add_argument('--async', dest='async_mode', action='store_true',
                        help='Stay connected for async messages')
    parser.add_argument('--stay', type=float, default=0,
                        help='Stay connected for N seconds after sending orders')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Suppress help message in interactive mode')
    parser.add_argument('orders', nargs='*',
                        help='Orders to send (format: type,size,price,side,user)')
    args = parser.parse_args()

    client = OrderClient(args.host, args.port)

    if not client.connect():
        sys.exit(1)

    try:
        if args.async_mode or args.stay > 0:
            # Async mode - start receive thread first
            client.start_async_receive()

            # Send orders
            for order in args.orders:
                print(f"Sending: {order}")
                client.send_order_async(order)
                time.sleep(0.1)  # Small delay between orders

            if args.stay > 0:
                print(f"Staying connected for {args.stay} seconds...")
                time.sleep(args.stay)
            elif args.async_mode:
                # Stay connected until interrupted
                print("Connected. Press Ctrl+C to disconnect.")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
        else:
            # Synchronous mode
            for order in args.orders:
                print(f"Sending: {order}")
                response = client.send_order(order)
                # Print each line of the response separately
                for line in response.split('\n'):
                    if line.strip():
                        print(f"Response: {line}")

            # If reading from stdin
            if not args.orders:
                if not args.quiet:
                    print(ORDER_HELP)
                print("Enter orders (Ctrl+D to quit):")
                for line in sys.stdin:
                    line = line.strip()
                    if line:
                        response = client.send_order(line)
                        print(f"Response: {response}")

    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
