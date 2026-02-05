#!/usr/bin/env python3
"""
STP Client - Receives trade notifications from the exchange.

Connects to the STP feed and prints all received trade messages.
Mirrors the C++ stp_client_example.cpp implementation.
"""

import argparse
import socket
import sys


class STPClient:
    """Client for receiving STP (trade) feed from the exchange."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._socket = None
        self._running = False

    def connect(self) -> bool:
        """Connect to the STP feed. Returns True on success."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Failed to connect: {e}", file=sys.stderr)
            return False

    def disconnect(self) -> None:
        """Disconnect from the STP feed."""
        self._running = False
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def run(self, callback=None) -> None:
        """
        Run the receive loop (blocks until disconnected).

        Args:
            callback: Optional function to call with each trade message.
                     If None, messages are printed to stdout.
        """
        if not self._socket:
            return

        self._running = True
        buffer = ""

        while self._running:
            try:
                data = self._socket.recv(4096)
                if not data:
                    print("Server disconnected", file=sys.stderr)
                    break

                buffer += data.decode('utf-8')

                # Process complete lines
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        if callback:
                            callback(line)
                        else:
                            # Format TRADE messages with readable price
                            formatted = self._format_message(line)
                            print(f"[STP] {formatted}", flush=True)

            except Exception as e:
                if self._running:
                    print(f"Receive error: {e}", file=sys.stderr)
                break

    def _format_message(self, line: str) -> str:
        """Format a message for display, converting prices to dollars."""
        parts = line.split(',')
        if len(parts) >= 4 and parts[0] == 'TRADE':
            try:
                size = int(parts[1])
                price = int(parts[2])
                side = parts[3]
                price_str = f"${price / 10000:.2f}"
                return f"TRADE {side} {size} @ {price_str}"
            except (ValueError, IndexError):
                pass
        return line


def main():
    parser = argparse.ArgumentParser(description='STP Feed Client')
    parser.add_argument('--host', default='localhost',
                        help='Exchange host (default: localhost)')
    parser.add_argument('--port', type=int, default=10001,
                        help='STP feed port (default: 10001)')
    args = parser.parse_args()

    client = STPClient(args.host, args.port)

    if not client.connect():
        sys.exit(1)

    print(f"Connected to STP feed at {args.host}:{args.port}")
    print("Waiting for trades... (Ctrl+C to quit)")

    try:
        client.run()
    except KeyboardInterrupt:
        print("\nDisconnecting...")
    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
