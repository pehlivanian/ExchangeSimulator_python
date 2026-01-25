#!/usr/bin/env python3
"""
Exchange Server - Main entry point for the Python Exchange Simulator.

This server handles live trading with:
- Order handling on one port (receives orders, sends ACK/FILL/REJECT)
- STP feed on another port (broadcasts trades to all subscribers)

Mirrors the C++ exchange_server.cpp implementation.
"""

import argparse
import logging
import signal
import sys
import threading
import time
from typing import Callable, Dict, Optional, TextIO

try:
    from .messages import (
        CancelOrder,
        LiveOrder,
        OrderHandlerMessage,
        OrderHandlerMessageType,
        STPMessage,
        parse_cancel_order,
        parse_live_order,
    )
    from .order_book import OrderBook
    from .order_generator import (
        OrderGeneratorState,
        create_insert_event,
        create_execute_event,
        create_delete_event,
    )
    from .tcp_order_handler import TCPOrderHandler
    from .tcp_feed_server import TCPFeedServer
except ImportError:
    from messages import (
        CancelOrder,
        LiveOrder,
        OrderHandlerMessage,
        OrderHandlerMessageType,
        STPMessage,
        parse_cancel_order,
        parse_live_order,
    )
    from order_book import OrderBook
    from order_generator import (
        OrderGeneratorState,
        create_insert_event,
        create_execute_event,
        create_delete_event,
    )
    from tcp_order_handler import TCPOrderHandler
    from tcp_feed_server import TCPFeedServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExchangeServer:
    """
    Main exchange server coordinating order handling and trade broadcasting.
    """

    def __init__(self, order_port: int, feed_port: int):
        self.order_port = order_port
        self.feed_port = feed_port

        # Core components
        self._order_book = OrderBook()
        self._state = OrderGeneratorState(use_real_time=True)

        # Network components
        self._order_handler: Optional[TCPOrderHandler] = None
        self._feed_server: Optional[TCPFeedServer] = None

        # Order tracking for passive fills and cancellation
        self._order_remaining_size: Dict[int, int] = {}
        self._order_side: Dict[int, str] = {}  # order_id -> 'B' or 'S'
        self._order_price: Dict[int, int] = {}  # order_id -> price
        self._order_remaining_lock = threading.Lock()

        # Shutdown coordination
        self._running = False

        # Post-order callback for logging/display
        self._post_order_callback: Optional[Callable[[str, str], None]] = None

    def set_post_order_callback(self, callback: Callable[[str, str], None]) -> None:
        """
        Set a callback to be called after each order is processed.

        Args:
            callback: Function(order_string, response_string) called after each order
        """
        self._post_order_callback = callback

    def print_book(self, output: TextIO = None, levels: int = 10) -> str:
        """
        Print a formatted order book snapshot.

        Args:
            output: Optional file-like object to write to (defaults to returning string)
            levels: Number of price levels to display

        Returns:
            Formatted string if output is None, otherwise empty string
        """
        bids, asks = self._order_book.get_snapshot()

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

            # Bid side
            if i < len(bids):
                bid_price = bids[i].price / 10000.0
                bid_size = bids[i].size
                line += f"{bid_price:9.2f} │ {bid_size:7d} │ "
            else:
                line += "          │         │ "

            # Ask side
            if i < len(asks):
                ask_price = asks[i].price / 10000.0
                ask_size = asks[i].size
                line += f"{ask_price:9.2f} │ {ask_size:7d}    │"
            else:
                line += "          │            │"

            lines.append(line)

        lines.append("└─────────────────────────────────────────────────┘")

        result = "\n".join(lines)

        if output is not None:
            output.write(result + "\n")
            output.flush()
            return ""
        return result

    def start(self) -> bool:
        """Start the exchange server. Returns True on success."""
        # Start the feed server first
        self._feed_server = TCPFeedServer(self.feed_port)
        if not self._feed_server.start():
            return False

        # Start the order handler
        self._order_handler = TCPOrderHandler(
            port=self.order_port,
            order_callback=self._process_order,
            disconnect_callback=self._on_client_disconnect
        )
        if not self._order_handler.start():
            self._feed_server.stop()
            return False

        self._running = True
        logger.info(f"Exchange server started - Orders: {self.order_port}, Feed: {self.feed_port}")
        return True

    def stop(self) -> None:
        """Stop the exchange server."""
        self._running = False

        if self._order_handler:
            self._order_handler.stop()
            self._order_handler = None

        if self._feed_server:
            self._feed_server.stop()
            self._feed_server = None

        logger.info("Exchange server stopped")

    def run(self) -> None:
        """Run the main loop (blocks until shutdown)."""
        last_log_time = time.time()

        while self._running:
            time.sleep(1.0)

            # Log client counts every 30 seconds
            current_time = time.time()
            if current_time - last_log_time >= 30.0:
                order_clients = self._order_handler.get_client_count() if self._order_handler else 0
                feed_clients = self._feed_server.get_client_count() if self._feed_server else 0
                logger.info(f"Connected clients - Orders: {order_clients}, Feed: {feed_clients}")
                last_log_time = current_time

    def _process_order(self, conn_id: int, order_str: str) -> str:
        """
        Process an incoming order and return the response.

        This is called from the client handler thread.
        """
        # First, try to parse as a cancel order
        cancel_order = parse_cancel_order(order_str)
        if cancel_order is not None:
            response = self._process_cancel_order(conn_id, cancel_order)
            if self._post_order_callback:
                self._post_order_callback(order_str, response)
            return response

        # Parse as a regular order
        live_order = parse_live_order(order_str)
        if live_order is None:
            response = OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.REJECT,
                reason="Invalid order format"
            ).serialize()
            if self._post_order_callback:
                self._post_order_callback(order_str, response)
            return response

        # Process based on order type
        if live_order.order_type == "market":
            response = self._process_market_order(conn_id, live_order)
        else:
            response = self._process_limit_order(conn_id, live_order)

        # Call post-order callback if set
        if self._post_order_callback:
            self._post_order_callback(order_str, response)

        return response

    def _process_market_order(self, conn_id: int, order: LiveOrder) -> str:
        """Process a market order."""
        is_bid = order.side == 'B'
        remainder = order.size
        total_executed = 0
        fill_responses = []  # Track fills at each price level

        # Walk the opposite side until filled or no liquidity
        while remainder > 0:
            if is_bid:
                bbo_price = self._order_book.get_best_ask_price()
                bbo_size = self._order_book.get_best_ask_size()
                exec_direction = 'S'  # Executing against asks
            else:
                bbo_price = self._order_book.get_best_bid_price()
                bbo_size = self._order_book.get_best_bid_size()
                exec_direction = 'B'  # Executing against bids

            if bbo_price is None or bbo_size is None:
                break

            exec_size = min(remainder, bbo_size)

            # Create and process execute event
            exec_event = create_execute_event(
                self._state,
                exec_size,
                bbo_price,
                exec_direction
            )

            ack, trades = self._order_book.process_event(exec_event)

            if trades:
                for trade in trades:
                    # Send passive fill notification to standing order owner
                    self._send_passive_fill(trade)

                    # Broadcast STP message
                    stp_msg = STPMessage(
                        size=trade.size,
                        price=trade.price,
                        aggressor_side=order.side
                    )
                    self._feed_server.broadcast(stp_msg.serialize())

            # Record fill at this price level
            fill_responses.append(
                OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.FILL,
                    order_id=0,
                    size=exec_size,
                    price=bbo_price
                ).serialize()
            )

            total_executed += exec_size
            remainder -= exec_size

        if total_executed == 0:
            return OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.REJECT,
                reason="No liquidity"
            ).serialize()
        elif remainder > 0:
            # Partial fill - market order couldn't be fully filled
            # Add a final message indicating unfilled remainder
            fill_responses.append(
                OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.PARTIAL_FILL,
                    order_id=0,
                    size=total_executed,
                    price=0,
                    remainder_size=remainder
                ).serialize()
            )

        # Return all fills as separate lines
        return '\n'.join(fill_responses)

    def _process_limit_order(self, conn_id: int, order: LiveOrder) -> str:
        """Process a limit order."""
        order_id = self._state.get_next_order_id()

        # Record order ownership
        self._state.record_order(order_id, order.user, conn_id)

        # Track remaining size, side, and price for passive fills and cancellation
        with self._order_remaining_lock:
            self._order_remaining_size[order_id] = order.size
            self._order_side[order_id] = order.side
            self._order_price[order_id] = order.price

        # Create insert event
        insert_event = create_insert_event(
            self._state,
            order_id,
            order.size,
            order.price,
            order.side
        )

        # Process the insert (may result in immediate trades if crossing)
        ack, trades = self._order_book.process_event(insert_event)

        total_executed = 0
        exec_price = order.price

        if trades:
            # Process trades in pairs (standing, taking)
            for i in range(0, len(trades), 2):
                standing_trade = trades[i]
                taking_trade = trades[i + 1] if i + 1 < len(trades) else None

                # Send passive fill notification to standing order owner
                self._send_passive_fill(standing_trade)

                # Broadcast STP message
                stp_msg = STPMessage(
                    size=standing_trade.size,
                    price=standing_trade.price,
                    aggressor_side=order.side
                )
                self._feed_server.broadcast(stp_msg.serialize())

                total_executed += standing_trade.size
                exec_price = standing_trade.price

        # Update tracking for the taking order
        remainder = order.size - total_executed
        with self._order_remaining_lock:
            if remainder <= 0:
                # Fully filled, remove tracking
                self._order_remaining_size.pop(order_id, None)
                self._order_side.pop(order_id, None)
                self._order_price.pop(order_id, None)
                self._state.remove_order(order_id)
            else:
                self._order_remaining_size[order_id] = remainder

        # Generate response
        if total_executed > 0 and remainder > 0:
            # Partial fill - part executed, part posted
            response = OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.PARTIAL_FILL,
                order_id=order_id,
                size=total_executed,
                price=exec_price,
                remainder_size=remainder
            ).serialize()
            # Also send ACK for the posted portion
            ack_msg = OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.ACK,
                order_id=order_id,
                size=remainder,
                price=order.price
            ).serialize()
            return response + '\n' + ack_msg
        elif total_executed > 0:
            # Fully filled
            return OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.FILL,
                order_id=order_id,
                size=total_executed,
                price=exec_price
            ).serialize()
        else:
            # Posted to book (no immediate execution)
            return OrderHandlerMessage(
                msg_type=OrderHandlerMessageType.ACK,
                order_id=order_id,
                size=order.size,
                price=order.price
            ).serialize()

    def _send_passive_fill(self, trade) -> None:
        """Send a passive fill notification to the standing order owner."""
        conn_id = self._state.get_connection(trade.order_id)
        if conn_id is None:
            return

        with self._order_remaining_lock:
            if trade.order_id not in self._order_remaining_size:
                return

            remaining = self._order_remaining_size[trade.order_id]
            remaining -= trade.size

            if remaining <= 0:
                # Fully filled
                msg = OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.FILL,
                    order_id=trade.order_id,
                    size=trade.size,
                    price=trade.price
                )
                self._order_remaining_size.pop(trade.order_id, None)
                self._order_side.pop(trade.order_id, None)
                self._order_price.pop(trade.order_id, None)
                self._state.remove_order(trade.order_id)
            else:
                # Partial fill
                msg = OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.PARTIAL_FILL,
                    order_id=trade.order_id,
                    size=trade.size,
                    price=trade.price,
                    remainder_size=remaining
                )
                self._order_remaining_size[trade.order_id] = remaining

        self._order_handler.send_async_message(conn_id, msg.serialize())

    def _process_cancel_order(self, conn_id: int, cancel: CancelOrder) -> str:
        """Process a cancel order request."""
        order_id = cancel.order_id

        with self._order_remaining_lock:
            # Check if order exists and get its details
            if order_id not in self._order_remaining_size:
                return OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.REJECT,
                    reason=f"Order {order_id} not found"
                ).serialize()

            # Verify the user owns this order
            order_user = self._state.get_user(order_id)
            if order_user != cancel.user:
                return OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.REJECT,
                    reason=f"Order {order_id} not owned by {cancel.user}"
                ).serialize()

            # Get order details for deletion
            remaining_size = self._order_remaining_size[order_id]
            side = self._order_side.get(order_id)
            price = self._order_price.get(order_id)

            if side is None or price is None:
                return OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.REJECT,
                    reason=f"Order {order_id} missing side/price info"
                ).serialize()

            # Create delete event to remove from order book
            delete_event = create_delete_event(
                self._state,
                order_id,
                price,
                side
            )

            # Process the delete
            ack, _ = self._order_book.process_event(delete_event)

            if not ack.acked:
                return OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.REJECT,
                    reason=f"Failed to cancel order {order_id}: {ack.reason_rejected}"
                ).serialize()

            # Remove all tracking
            cancelled_size = self._order_remaining_size.pop(order_id, 0)
            self._order_side.pop(order_id, None)
            self._order_price.pop(order_id, None)
            self._state.remove_order(order_id)

        return OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.CANCEL_ACK,
            order_id=order_id,
            size=cancelled_size
        ).serialize()

    def _on_client_disconnect(self, conn_id: int) -> None:
        """Called when a client disconnects."""
        # Could clean up orders associated with this connection
        # For now, we let standing orders remain in the book
        logger.debug(f"Client {conn_id} disconnected")


def main():
    parser = argparse.ArgumentParser(
        description='Exchange Server - A multi-threaded order matching engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Order formats:
  limit,size,price,side,user    - Limit order (e.g., limit,100,50000000,B,trader1)
  market,size,0,side,user       - Market order (e.g., market,50,0,S,trader1)
  cancel,order_id,user          - Cancel order (e.g., cancel,1000,trader1)

Price format: price * 10000 (e.g., $5000.00 = 50000000)
Side: B=Buy, S=Sell
"""
    )
    parser.add_argument('--order-port', type=int, default=10000,
                        help='Port for order handler (default: 10000)')
    parser.add_argument('--feed-port', type=int, default=10001,
                        help='Port for STP feed (default: 10001)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose output (shows order book after each order)')
    parser.add_argument('--show-book', action='store_true',
                        help='Print order book after each order (same as -v)')
    parser.add_argument('--book-levels', type=int, default=5,
                        help='Number of book levels to display (default: 5)')
    args = parser.parse_args()

    # Verbose mode enables both debug logging and order book display
    show_book = args.verbose or args.show_book

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Print startup banner in verbose mode
    if show_book:
        print("=" * 60)
        print("  EXCHANGE SERVER")
        print("=" * 60)
        print(f"  Order Handler Port: {args.order_port}")
        print(f"  STP Feed Port:      {args.feed_port}")
        print(f"  Book Levels:        {args.book_levels}")
        print("=" * 60)
        print()
        print("Supported order types:")
        print("  limit,size,price,side,user   - Limit order")
        print("  market,size,0,side,user      - Market order")
        print("  cancel,order_id,user         - Cancel order")
        print()
        print("Price format: price * 10000 (e.g., $5000.00 = 50000000)")
        print("Side: B=Buy, S=Sell")
        print("=" * 60)
        print()

    server = ExchangeServer(args.order_port, args.feed_port)

    # Set up post-order callback to print order book
    if show_book:
        def post_order_callback(order_str: str, response: str):
            print("\n" + "-" * 60)
            print(f"Order:    {order_str}")
            print(f"Response: {response}")
            print(server.print_book(levels=args.book_levels))
            sys.stdout.flush()

        server.set_post_order_callback(post_order_callback)

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print()  # Newline after ^C
        logger.info(f"Received signal {signum}, shutting down...")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not server.start():
        logger.error("Failed to start server")
        sys.exit(1)

    if show_book:
        print("Server started. Waiting for orders...")
        print("Press Ctrl+C to stop.")
        print()

    try:
        server.run()
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        server.stop()


if __name__ == '__main__':
    main()
