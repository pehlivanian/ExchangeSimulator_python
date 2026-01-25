"""
Order generation state management.

Mirrors the C++ OrderGeneratorState class.
"""

import time
import threading
from dataclasses import dataclass
from typing import Optional, Dict

try:
    from .messages import EventLOBSTER, EventType
except ImportError:
    from messages import EventLOBSTER, EventType


class OrderGeneratorState:
    """
    Manages state for generating orders and tracking their ownership.

    Thread-safe implementation with locks for concurrent access.
    """

    def __init__(self, use_real_time: bool = True, time_increment: float = 0.001):
        self._seq_num = 0
        self._order_id = 999  # Will start at 1000
        self._timestamp = 0.0
        self._time_increment = time_increment
        self._use_real_time = use_real_time

        # Tracking maps
        self._order_to_user: Dict[int, str] = {}
        self._order_to_connection: Dict[int, int] = {}

        self._lock = threading.Lock()

    def get_next_seq_num(self) -> int:
        """Get the next sequence number (thread-safe)."""
        with self._lock:
            self._seq_num += 1
            return self._seq_num

    def get_next_order_id(self) -> int:
        """Get the next order ID (thread-safe)."""
        with self._lock:
            self._order_id += 1
            return self._order_id

    def get_next_timestamp(self) -> float:
        """Get the next timestamp (thread-safe)."""
        with self._lock:
            if self._use_real_time:
                return time.time()
            else:
                self._timestamp += self._time_increment
                return self._timestamp

    def record_order(self, order_id: int, user: str, connection_id: int) -> None:
        """Record an order's ownership (thread-safe)."""
        with self._lock:
            self._order_to_user[order_id] = user
            self._order_to_connection[order_id] = connection_id

    def get_connection(self, order_id: int) -> Optional[int]:
        """Get the connection ID for an order (thread-safe)."""
        with self._lock:
            return self._order_to_connection.get(order_id)

    def get_user(self, order_id: int) -> Optional[str]:
        """Get the user for an order (thread-safe)."""
        with self._lock:
            return self._order_to_user.get(order_id)

    def remove_order(self, order_id: int) -> None:
        """Remove tracking for an order (thread-safe)."""
        with self._lock:
            self._order_to_user.pop(order_id, None)
            self._order_to_connection.pop(order_id, None)


def create_insert_event(
    state: OrderGeneratorState,
    order_id: int,
    size: int,
    price: int,
    direction: str
) -> EventLOBSTER:
    """Create an INSERT event."""
    return EventLOBSTER(
        seq_num=state.get_next_seq_num(),
        time=state.get_next_timestamp(),
        event_type=EventType.INSERT,
        order_id=order_id,
        size=size,
        price=price,
        direction=direction
    )


def create_execute_event(
    state: OrderGeneratorState,
    size: int,
    price: int,
    direction: str
) -> EventLOBSTER:
    """
    Create an EXECUTE event.

    Note: direction is the side of the standing order being executed,
    not the aggressor side.
    """
    return EventLOBSTER(
        seq_num=state.get_next_seq_num(),
        time=state.get_next_timestamp(),
        event_type=EventType.EXECUTE,
        order_id=0,  # EXECUTE events don't need a specific order ID
        size=size,
        price=price,
        direction=direction
    )


def create_delete_event(
    state: OrderGeneratorState,
    order_id: int,
    price: int,
    direction: str
) -> EventLOBSTER:
    """Create a DELETE event (full cancellation)."""
    return EventLOBSTER(
        seq_num=state.get_next_seq_num(),
        time=state.get_next_timestamp(),
        event_type=EventType.DELETE,
        order_id=order_id,
        size=0,
        price=price,
        direction=direction
    )


def create_cancel_event(
    state: OrderGeneratorState,
    order_id: int,
    size: int,
    price: int,
    direction: str
) -> EventLOBSTER:
    """Create a CANCEL event (partial cancellation)."""
    return EventLOBSTER(
        seq_num=state.get_next_seq_num(),
        time=state.get_next_timestamp(),
        event_type=EventType.CANCEL,
        order_id=order_id,
        size=size,
        price=price,
        direction=direction
    )
