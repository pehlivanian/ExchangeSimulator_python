"""
Message types and parsing utilities for the Exchange Simulator.

Mirrors the C++ message definitions from utils.hpp, exchange_feeds.hpp, and live_order.hpp.
"""

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional
import re


class EventType(IntEnum):
    """LOBSTER event types."""
    INSERT = 1
    CANCEL = 2
    DELETE = 3
    EXECUTE = 4
    HIDDEN = 5


class OrderHandlerMessageType(Enum):
    """Response message types from the order handler."""
    ACK = "ACK"
    FILL = "FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    REJECT = "REJECT"
    CANCEL_ACK = "CANCEL_ACK"


@dataclass
class Order:
    """Internal order representation."""
    seq_num: int
    side: str  # 'B' or 'S'
    time: float
    order_id: int
    price: int  # Price in LOBSTER format (price * 10000)
    size: int
    order_type: int = EventType.INSERT


@dataclass
class Trade:
    """Trade record."""
    seq_num: int
    counter_seq_num: int
    order_id: int
    side: str  # 'B' or 'S'
    price: int
    size: int


@dataclass
class Ack:
    """Order acknowledgment."""
    seq_num: int
    order_id: int
    acked: bool
    reason_rejected: str = ""


@dataclass
class EventLOBSTER:
    """LOBSTER format event."""
    seq_num: int
    time: float
    event_type: int
    order_id: int
    size: int
    price: int
    direction: str  # 'B' or 'S'


@dataclass
class LiveOrder:
    """Order received from a live client."""
    order_type: str  # "market" or "limit"
    size: int
    price: int  # In LOBSTER format
    side: str  # 'B' or 'S'
    user: str


@dataclass
class CancelOrder:
    """Cancel order request from a live client."""
    order_id: int
    user: str


@dataclass
class OrderHandlerMessage:
    """Message sent back to order handler clients."""
    msg_type: OrderHandlerMessageType
    order_id: int = 0
    size: int = 0
    price: int = 0
    remainder_size: int = 0
    reason: str = ""

    def serialize(self) -> str:
        """Serialize to wire format."""
        if self.msg_type == OrderHandlerMessageType.ACK:
            return f"ACK,{self.order_id},{self.size},{self.price}"
        elif self.msg_type == OrderHandlerMessageType.FILL:
            return f"FILL,{self.order_id},{self.size},{self.price}"
        elif self.msg_type == OrderHandlerMessageType.PARTIAL_FILL:
            return f"PARTIAL_FILL,{self.order_id},{self.size},{self.price},{self.remainder_size}"
        elif self.msg_type == OrderHandlerMessageType.REJECT:
            return f"REJECT,{self.reason}"
        elif self.msg_type == OrderHandlerMessageType.CANCEL_ACK:
            return f"CANCEL_ACK,{self.order_id},{self.size}"
        return ""


@dataclass
class STPMessage:
    """Straight-Through Processing trade message."""
    size: int
    price: int
    aggressor_side: str  # 'B' or 'S'

    def serialize(self) -> str:
        """Serialize to wire format."""
        side_str = "BUY" if self.aggressor_side == 'B' else "SELL"
        return f"TRADE,{self.size},{self.price},{side_str}"


def parse_live_order(input_str: str) -> Optional[LiveOrder]:
    """
    Parse a live order string.

    Format: orderType,size,price,side,user
    Example: limit,100,58000000,B,trader1
    """
    input_str = input_str.strip()
    if not input_str:
        return None

    parts = input_str.split(',')
    if len(parts) != 5:
        return None

    order_type = parts[0].strip().lower()
    if order_type not in ("market", "limit"):
        return None

    try:
        size = int(parts[1].strip())
    except ValueError:
        return None

    if size <= 0:
        return None

    try:
        price = int(parts[2].strip())
    except ValueError:
        return None

    side = parts[3].strip().upper()
    if side not in ('B', 'S'):
        return None

    if order_type == "limit" and price <= 0:
        return None

    user = parts[4].strip()
    if not user:
        return None

    return LiveOrder(
        order_type=order_type,
        size=size,
        price=price,
        side=side,
        user=user
    )


def parse_cancel_order(input_str: str) -> Optional[CancelOrder]:
    """
    Parse a cancel order string.

    Format: cancel,order_id,user
    Example: cancel,1000,trader1
    """
    input_str = input_str.strip()
    if not input_str:
        return None

    parts = input_str.split(',')
    if len(parts) != 3:
        return None

    order_type = parts[0].strip().lower()
    if order_type != "cancel":
        return None

    try:
        order_id = int(parts[1].strip())
    except ValueError:
        return None

    user = parts[2].strip()
    if not user:
        return None

    return CancelOrder(order_id=order_id, user=user)


def parse_lobster_line(line: str) -> Optional[EventLOBSTER]:
    """
    Parse a line from a LOBSTER message CSV file.

    Format: Time,EventType,OrderID,Size,Price,Direction
    Direction: 1 = Bid, -1 = Ask
    """
    line = line.strip()
    if not line:
        return None

    parts = line.split(',')
    if len(parts) != 6:
        return None

    try:
        time = float(parts[0])
        event_type = int(parts[1])
        order_id = int(parts[2])
        size = int(parts[3])
        price = int(parts[4])
        direction_int = int(parts[5])
    except ValueError:
        return None

    direction = 'B' if direction_int == 1 else 'S'

    return EventLOBSTER(
        seq_num=0,  # Will be assigned by OrderGeneratorState
        time=time,
        event_type=event_type,
        order_id=order_id,
        size=size,
        price=price,
        direction=direction
    )
