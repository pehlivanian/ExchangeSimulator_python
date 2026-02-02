#!/usr/bin/env python3
"""
Simple UDP client for receiving market data broadcasts.

Usage:
    python udp_market_data_client.py [port]

The client sends a subscription message and then listens for LOBSTER format updates.
"""

import socket
import sys

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 10002

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5.0)

    # Send subscription message
    server_addr = ('127.0.0.1', port)
    sock.sendto(b"subscribe", server_addr)
    print(f"Subscribed to market data on port {port}")
    print("LOBSTER format: Time,Type,OrderID,Size,Price,Direction")
    print("  Type: 1=INSERT, 2=CANCEL, 3=DELETE, 4=EXECUTE, 5=HIDDEN")
    print("  Direction: 1=Buy, -1=Sell")
    print()
    print("Waiting for market data...")
    print("-" * 70)

    msg_count = 0
    try:
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                msg = data.decode('utf-8').strip()
                msg_count += 1

                # Parse and format the message for display
                parts = msg.split(',')
                if len(parts) == 6:
                    time_s, event_type, order_id, size, price, direction = parts
                    event_names = {'1': 'INSERT', '2': 'CANCEL', '3': 'DELETE',
                                   '4': 'EXECUTE', '5': 'HIDDEN'}
                    event_name = event_names.get(event_type, f'TYPE_{event_type}')
                    side = 'BUY' if direction == '1' else 'SELL'
                    price_dollars = int(price) / 10000
                    print(f"[{msg_count:6d}] {float(time_s):12.3f}s {event_name:8s} "
                          f"id={int(order_id):12d} size={int(size):6d} "
                          f"${price_dollars:>10.2f} {side}")
                else:
                    print(f"[{msg_count:6d}] {msg}")

            except socket.timeout:
                print("(waiting for data...)")
                continue

    except KeyboardInterrupt:
        print(f"\nReceived {msg_count} messages")
    finally:
        sock.close()


if __name__ == '__main__':
    main()
