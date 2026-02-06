"""
Python Exchange Simulator

A multi-threaded exchange simulator supporting:
- Order handling (market and limit orders)
- Price-time priority order book
- STP (Straight-Through Processing) trade feed
- Async passive fill notifications
"""

from .messages import (
    EventType,
    OrderHandlerMessageType,
    Order,
    Trade,
    Ack,
    EventLOBSTER,
    LiveOrder,
    OrderHandlerMessage,
    STPMessage,
    parse_live_order,
    parse_lobster_line,
)
from .order_book import OrderBook, OrderBookSide, PriceLevel

# Re-export print_book as a standalone function for convenience
def print_book(order_book: OrderBook, levels: int = 10) -> str:
    """Print a formatted order book snapshot."""
    bids, asks = order_book.get_snapshot()

    lines = []
    lines.append("")
    lines.append("┌─────────────────────────────────────────────────┐")
    lines.append("│              ORDER BOOK SNAPSHOT                │")
    lines.append("├─────────────────────────────────────────────────┤")
    lines.append("│      BIDS (Buy)      │      ASKS (Sell)         │")
    lines.append("│   Price    │  Size   │   Price    │  Size      │")
    lines.append("├──────────────────────┼──────────────────────────┤")

    max_rows = max(min(len(bids), levels), min(len(asks), levels))

    for i in range(max_rows):
        line = "│ "
        if i < len(bids):
            line += f"{bids[i].price / 10000.0:9.2f} │ {bids[i].size:7d} │ "
        else:
            line += "          │         │ "
        if i < len(asks):
            line += f"{asks[i].price / 10000.0:9.2f} │ {asks[i].size:7d}    │"
        else:
            line += "          │            │"
        lines.append(line)

    lines.append("└─────────────────────────────────────────────────┘")
    return "\n".join(lines)
from .order_generator import (
    OrderGeneratorState,
    create_insert_event,
    create_execute_event,
    create_delete_event,
    create_cancel_event,
)
from .tcp_order_handler import TCPOrderHandler
from .tcp_feed_server import TCPFeedServer
from .exchange_server import ExchangeServer
from .order_client_with_fsm import OrderClient, OrderClientWithFSM
from .stp_client import STPClient

__all__ = [
    'EventType',
    'OrderHandlerMessageType',
    'Order',
    'Trade',
    'Ack',
    'EventLOBSTER',
    'LiveOrder',
    'OrderHandlerMessage',
    'STPMessage',
    'parse_live_order',
    'parse_lobster_line',
    'OrderBook',
    'OrderBookSide',
    'PriceLevel',
    'print_book',
    'OrderGeneratorState',
    'create_insert_event',
    'create_execute_event',
    'create_delete_event',
    'create_cancel_event',
    'TCPOrderHandler',
    'TCPFeedServer',
    'ExchangeServer',
    'OrderClient',
    'OrderClientWithFSM',
    'STPClient',
]
