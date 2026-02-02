#!/usr/bin/env python3
"""
UDP Market Data Server for broadcasting order book updates in LOBSTER format.

LOBSTER message format:
    Time,Type,OrderID,Size,Price,Direction

Where:
    Time: Seconds after midnight (with decimal precision)
    Type: 1=INSERT, 2=CANCEL, 3=DELETE, 4=EXECUTE, 5=HIDDEN
    OrderID: Unique order reference number
    Size: Number of shares
    Price: Dollar price * 10000
    Direction: 1=Buy, -1=Sell
"""

import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional, Set, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class MarketDataMessage:
    """A market data message in LOBSTER format."""
    time: float  # Seconds after midnight
    event_type: int  # 1=INSERT, 2=CANCEL, 3=DELETE, 4=EXECUTE, 5=HIDDEN
    order_id: int
    size: int
    price: int  # Price * 10000
    direction: int  # 1=Buy, -1=Sell

    def serialize(self) -> str:
        """Serialize to LOBSTER CSV format."""
        return f"{self.time:.9f},{self.event_type},{self.order_id},{self.size},{self.price},{self.direction}"

    @staticmethod
    def side_to_direction(side: str) -> int:
        """Convert side character to LOBSTER direction."""
        return 1 if side == 'B' else -1


class UDPMarketDataServer:
    """
    UDP server for broadcasting market data updates.

    Clients can subscribe by sending any message to the server.
    The server tracks subscribers and broadcasts updates to all of them.
    """

    def __init__(self, port: int, broadcast_address: str = ""):
        """
        Initialize the UDP market data server.

        Args:
            port: UDP port to bind to
            broadcast_address: Address to broadcast to (empty = unicast to subscribers)
        """
        self.port = port
        self.broadcast_address = broadcast_address
        self._socket: Optional[socket.socket] = None
        self._subscribers: Set[Tuple[str, int]] = set()
        self._subscribers_lock = threading.Lock()
        self._running = False
        self._listener_thread: Optional[threading.Thread] = None
        self._sequence_num = 0
        self._sequence_lock = threading.Lock()

    def start(self) -> bool:
        """Start the UDP server."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Enable broadcast if broadcast address specified
            if self.broadcast_address:
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            self._socket.bind(('', self.port))
            self._socket.settimeout(1.0)  # For clean shutdown

            self._running = True
            self._listener_thread = threading.Thread(target=self._listen_for_subscribers, daemon=True)
            self._listener_thread.start()

            logger.info(f"UDP Market Data server started on port {self.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start UDP server: {e}")
            return False

    def stop(self) -> None:
        """Stop the UDP server."""
        self._running = False

        if self._listener_thread:
            self._listener_thread.join(timeout=2.0)
            self._listener_thread = None

        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

        logger.info("UDP Market Data server stopped")

    def _listen_for_subscribers(self) -> None:
        """Listen for subscription requests."""
        while self._running:
            try:
                data, addr = self._socket.recvfrom(1024)
                with self._subscribers_lock:
                    if addr not in self._subscribers:
                        self._subscribers.add(addr)
                        logger.info(f"New market data subscriber: {addr}")
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    logger.error(f"Error in subscriber listener: {e}")

    def broadcast(self, message: MarketDataMessage) -> None:
        """Broadcast a market data message to all subscribers."""
        if not self._socket or not self._running:
            return

        data = (message.serialize() + "\n").encode('utf-8')

        if self.broadcast_address:
            # Broadcast mode
            try:
                self._socket.sendto(data, (self.broadcast_address, self.port))
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
        else:
            # Unicast to subscribers
            with self._subscribers_lock:
                dead_subscribers = []
                for addr in self._subscribers:
                    try:
                        self._socket.sendto(data, addr)
                    except Exception:
                        dead_subscribers.append(addr)

                for addr in dead_subscribers:
                    self._subscribers.discard(addr)

    def broadcast_insert(self, time: float, order_id: int, size: int,
                         price: int, side: str) -> None:
        """Broadcast an INSERT message."""
        msg = MarketDataMessage(
            time=time,
            event_type=1,
            order_id=order_id,
            size=size,
            price=price,
            direction=MarketDataMessage.side_to_direction(side)
        )
        self.broadcast(msg)

    def broadcast_cancel(self, time: float, order_id: int, size: int,
                         price: int, side: str) -> None:
        """Broadcast a CANCEL (partial cancellation) message."""
        msg = MarketDataMessage(
            time=time,
            event_type=2,
            order_id=order_id,
            size=size,
            price=price,
            direction=MarketDataMessage.side_to_direction(side)
        )
        self.broadcast(msg)

    def broadcast_delete(self, time: float, order_id: int, size: int,
                         price: int, side: str) -> None:
        """Broadcast a DELETE (full cancellation) message."""
        msg = MarketDataMessage(
            time=time,
            event_type=3,
            order_id=order_id,
            size=size,
            price=price,
            direction=MarketDataMessage.side_to_direction(side)
        )
        self.broadcast(msg)

    def broadcast_execute(self, time: float, order_id: int, size: int,
                          price: int, side: str) -> None:
        """Broadcast an EXECUTE message."""
        msg = MarketDataMessage(
            time=time,
            event_type=4,
            order_id=order_id,
            size=size,
            price=price,
            direction=MarketDataMessage.side_to_direction(side)
        )
        self.broadcast(msg)

    def broadcast_raw(self, lobster_line: str) -> None:
        """Broadcast a raw LOBSTER format line."""
        if not self._socket or not self._running:
            return

        data = (lobster_line.strip() + "\n").encode('utf-8')

        if self.broadcast_address:
            try:
                self._socket.sendto(data, (self.broadcast_address, self.port))
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
        else:
            with self._subscribers_lock:
                for addr in list(self._subscribers):
                    try:
                        self._socket.sendto(data, addr)
                    except Exception:
                        self._subscribers.discard(addr)

    def get_subscriber_count(self) -> int:
        """Get the number of active subscribers."""
        with self._subscribers_lock:
            return len(self._subscribers)


def get_time_of_day() -> float:
    """Get current time as seconds after midnight."""
    now = time.time()
    midnight = time.mktime(time.localtime(now)[:3] + (0, 0, 0) + time.localtime(now)[6:])
    return now - midnight


if __name__ == '__main__':
    print("This module provides UDPMarketDataServer for broadcasting market data.")
    print("It is used internally by exchange_server.py.")
    print()
    print("To receive market data, run:  python udp_book_builder.py")
    print("To start the exchange, run:   python exchange_server.py")
