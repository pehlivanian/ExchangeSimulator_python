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
        create_cancel_event,
    )
    from .tcp_order_handler import TCPOrderHandler
    from .tcp_feed_server import TCPFeedServer
    from .lobster_reader import LOBSTERReader, LOBSTER_EVENT_TYPE, format_time, format_price
    from .udp_market_data import UDPMarketDataServer, MarketDataMessage, get_time_of_day
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
        create_cancel_event,
    )
    from tcp_order_handler import TCPOrderHandler
    from tcp_feed_server import TCPFeedServer
    from lobster_reader import LOBSTERReader, LOBSTER_EVENT_TYPE, format_time, format_price
    from udp_market_data import UDPMarketDataServer, MarketDataMessage, get_time_of_day

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

    def __init__(self, order_port: int, feed_port: int, market_data_port: int = 0):
        self.order_port = order_port
        self.feed_port = feed_port
        self.market_data_port = market_data_port

        # Core components
        self._order_book = OrderBook()
        self._state = OrderGeneratorState(use_real_time=True)

        # Network components
        self._order_handler: Optional[TCPOrderHandler] = None
        self._feed_server: Optional[TCPFeedServer] = None
        self._market_data_server: Optional[UDPMarketDataServer] = None

        # Order tracking for passive fills and cancellation
        self._order_remaining_size: Dict[int, int] = {}
        self._order_side: Dict[int, str] = {}  # order_id -> 'B' or 'S'
        self._order_price: Dict[int, int] = {}  # order_id -> price
        self._order_expiry_time: Dict[int, float] = {}  # order_id -> expiry timestamp
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

        # Start UDP market data server if port specified
        if self.market_data_port > 0:
            self._market_data_server = UDPMarketDataServer(self.market_data_port)
            if not self._market_data_server.start():
                self._feed_server.stop()
                return False

        # Start the order handler
        self._order_handler = TCPOrderHandler(
            port=self.order_port,
            order_callback=self._process_order,
            disconnect_callback=self._on_client_disconnect
        )
        if not self._order_handler.start():
            self._feed_server.stop()
            if self._market_data_server:
                self._market_data_server.stop()
            return False

        self._running = True
        md_info = f", MarketData(UDP): {self.market_data_port}" if self.market_data_port > 0 else ""
        logger.info(f"Exchange server started - Orders: {self.order_port}, Feed: {self.feed_port}{md_info}")
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

        if self._market_data_server:
            self._market_data_server.stop()
            self._market_data_server = None

        logger.info("Exchange server stopped")

    def run(self) -> None:
        """Run the main loop (blocks until shutdown)."""
        last_log_time = time.time()

        while self._running:
            time.sleep(1.0)

            # Check for expired orders every second
            self._check_expired_orders()

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
        current_time = get_time_of_day()

        # Record order ownership
        self._state.record_order(order_id, order.user, conn_id)

        # Track remaining size, side, price, and expiry for passive fills and cancellation
        with self._order_remaining_lock:
            self._order_remaining_size[order_id] = order.size
            self._order_side[order_id] = order.side
            self._order_price[order_id] = order.price
            self._order_expiry_time[order_id] = time.time() + order.ttl

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
        fill_responses = []  # Track individual fills at each price level

        if trades:
            # Process trades in pairs (standing, taking)
            for i in range(0, len(trades), 2):
                standing_trade = trades[i]
                taking_trade = trades[i + 1] if i + 1 < len(trades) else None

                # Send passive fill notification to standing order owner
                self._send_passive_fill(standing_trade)

                # Broadcast STP message (trades)
                stp_msg = STPMessage(
                    size=standing_trade.size,
                    price=standing_trade.price,
                    aggressor_side=order.side
                )
                self._feed_server.broadcast(stp_msg.serialize())

                # Broadcast EXECUTE on UDP market data (standing order executed)
                if self._market_data_server:
                    self._market_data_server.broadcast_execute(
                        time=current_time,
                        order_id=standing_trade.order_id,
                        size=standing_trade.size,
                        price=standing_trade.price,
                        side=standing_trade.side
                    )

                total_executed += standing_trade.size

                # Record this fill for the aggressor's response
                fill_responses.append((standing_trade.size, standing_trade.price))

        # Update tracking for the taking order
        remainder = order.size - total_executed
        with self._order_remaining_lock:
            if remainder <= 0:
                # Fully filled, remove tracking
                self._order_remaining_size.pop(order_id, None)
                self._order_side.pop(order_id, None)
                self._order_price.pop(order_id, None)
                self._order_expiry_time.pop(order_id, None)
                self._state.remove_order(order_id)
            else:
                self._order_remaining_size[order_id] = remainder

        # Broadcast market data for the new order
        if self._market_data_server:
            if remainder > 0:
                # Order rests in book (INSERT for the resting portion)
                self._market_data_server.broadcast_insert(
                    time=current_time,
                    order_id=order_id,
                    size=remainder,
                    price=order.price,
                    side=order.side
                )

        # Generate response - send individual fills at each price level
        if total_executed > 0:
            responses = []
            running_remainder = order.size

            for fill_size, fill_price in fill_responses:
                running_remainder -= fill_size
                if running_remainder > 0:
                    # Partial fill
                    responses.append(OrderHandlerMessage(
                        msg_type=OrderHandlerMessageType.PARTIAL_FILL,
                        order_id=order_id,
                        size=fill_size,
                        price=fill_price,
                        remainder_size=running_remainder
                    ).serialize())
                else:
                    # Final fill (fully filled)
                    responses.append(OrderHandlerMessage(
                        msg_type=OrderHandlerMessageType.FILL,
                        order_id=order_id,
                        size=fill_size,
                        price=fill_price
                    ).serialize())

            # If there's a remainder, also send ACK for the posted portion
            if remainder > 0:
                responses.append(OrderHandlerMessage(
                    msg_type=OrderHandlerMessageType.ACK,
                    order_id=order_id,
                    size=remainder,
                    price=order.price
                ).serialize())

            return '\n'.join(responses)
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
                self._order_expiry_time.pop(trade.order_id, None)
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
        current_time = get_time_of_day()

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
            self._order_expiry_time.pop(order_id, None)
            self._state.remove_order(order_id)

        # Broadcast DELETE on UDP market data
        if self._market_data_server:
            self._market_data_server.broadcast_delete(
                time=current_time,
                order_id=order_id,
                size=cancelled_size,
                price=price,
                side=side
            )

        return OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.CANCEL_ACK,
            order_id=order_id,
            size=cancelled_size
        ).serialize()

    def _check_expired_orders(self) -> None:
        """Check for and expire any orders past their TTL."""
        current_time = time.time()
        expired_orders = []

        with self._order_remaining_lock:
            for order_id, expiry_time in list(self._order_expiry_time.items()):
                if current_time >= expiry_time:
                    expired_orders.append(order_id)

        # Process expirations outside the lock to avoid deadlock
        for order_id in expired_orders:
            self._expire_order(order_id)

    def _expire_order(self, order_id: int) -> None:
        """Expire an order due to TTL."""
        with self._order_remaining_lock:
            # Check if order still exists (might have been filled or cancelled)
            if order_id not in self._order_remaining_size:
                return

            remaining_size = self._order_remaining_size[order_id]
            side = self._order_side.get(order_id)
            price = self._order_price.get(order_id)

            if side is None or price is None:
                return

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
                logger.warning(f"Failed to expire order {order_id}: {ack.reason_rejected}")
                return

            # Remove all tracking
            expired_size = self._order_remaining_size.pop(order_id, 0)
            self._order_side.pop(order_id, None)
            self._order_price.pop(order_id, None)
            self._order_expiry_time.pop(order_id, None)

            # Get connection ID for sending expiry notification
            conn_id = self._state.get_connection(order_id)
            self._state.remove_order(order_id)

        # Send EXPIRED message to client (outside lock)
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.EXPIRED,
            order_id=order_id,
            size=expired_size
        )
        if conn_id is not None:
            self._order_handler.send_async_message(conn_id, msg.serialize())
        logger.debug(f"Order {order_id} expired, size={expired_size}")

        # Call post-order callback to update console display
        if self._post_order_callback:
            self._post_order_callback(f"[TTL EXPIRED] order_id={order_id}", msg.serialize())

    def _on_client_disconnect(self, conn_id: int) -> None:
        """Called when a client disconnects."""
        # Could clean up orders associated with this connection
        # For now, we let standing orders remain in the book
        logger.debug(f"Client {conn_id} disconnected")


class HistoricalReplayServer:
    """
    Historical replay server that replays LOBSTER market data.

    Reads messages from LOBSTER files and processes them through the order book,
    optionally validating against a reference orderbook file.
    Broadcasts order book updates over UDP in LOBSTER format.
    """

    def __init__(self, message_file: str, orderbook_file: Optional[str] = None,
                 market_data_port: int = 0):
        self.message_file = message_file
        self.orderbook_file = orderbook_file
        self.market_data_port = market_data_port

        self._order_book = OrderBook()
        self._state = OrderGeneratorState(use_real_time=False)
        self._market_data_server: Optional[UDPMarketDataServer] = None

        # Statistics
        self._message_count = 0
        self._trade_count = 0
        self._validation_errors = 0
        self._skipped_messages = 0

    def print_book(self, output: TextIO = None, levels: int = 5) -> str:
        """Print a formatted order book snapshot."""
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

    def validate_orderbook(self, expected_bid_price: int, expected_bid_size: int,
                          expected_ask_price: int, expected_ask_size: int) -> bool:
        """
        Validate the current orderbook state against expected values.

        Returns True if the orderbook matches, False otherwise.
        """
        actual_bid_price = self._order_book.get_best_bid_price()
        actual_bid_size = self._order_book.get_best_bid_size()
        actual_ask_price = self._order_book.get_best_ask_price()
        actual_ask_size = self._order_book.get_best_ask_size()

        # Handle None values (empty book side)
        actual_bid_price = actual_bid_price or 0
        actual_bid_size = actual_bid_size or 0
        actual_ask_price = actual_ask_price or 0
        actual_ask_size = actual_ask_size or 0

        # LOBSTER uses special values for empty levels
        if expected_bid_price == -9999999999:
            expected_bid_price = 0
            expected_bid_size = 0
        if expected_ask_price == 9999999999:
            expected_ask_price = 0
            expected_ask_size = 0

        matches = (
            actual_bid_price == expected_bid_price and
            actual_bid_size == expected_bid_size and
            actual_ask_price == expected_ask_price and
            actual_ask_size == expected_ask_size
        )

        return matches

    def run(self, print_every: int = 0, validate: bool = True,
            max_messages: int = 0, verbose: bool = False,
            skip_initial: int = 0, wait_subscribers: int = 0,
            wait_ready: bool = False, throttle_us: int = 0) -> None:
        """
        Run the historical replay.

        Args:
            print_every: Print orderbook every N messages (0 = never)
            validate: Validate against reference orderbook if available
            max_messages: Stop after N messages (0 = process all)
            verbose: Print each message as it's processed
            skip_initial: Skip validation for first N messages (book warmup)
            wait_subscribers: Wait until this many UDP subscribers connect (0 = don't wait)
            wait_ready: Wait for user to press Enter before starting replay
            throttle_us: Microseconds to sleep between UDP sends (0 = no throttle)
        """
        reader = LOBSTERReader(self.message_file, self.orderbook_file)

        # Start UDP market data server if port specified
        if self.market_data_port > 0:
            self._market_data_server = UDPMarketDataServer(self.market_data_port)
            if not self._market_data_server.start():
                logger.error("Failed to start UDP market data server")
                return

        print("=" * 70)
        print("  HISTORICAL REPLAY MODE")
        print("=" * 70)
        print(f"  Message file:   {self.message_file}")
        if self.orderbook_file:
            print(f"  Orderbook file: {self.orderbook_file}")
        if self.market_data_port > 0:
            print(f"  Market data:    UDP port {self.market_data_port}")
        print(f"  Print every:    {print_every if print_every > 0 else 'disabled'}")
        print(f"  Validation:     {'enabled' if validate and self.orderbook_file else 'disabled'}")
        if skip_initial > 0:
            print(f"  Skip initial:   {skip_initial} messages (warmup)")
        if max_messages > 0:
            print(f"  Max messages:   {max_messages}")
        if throttle_us > 0:
            print(f"  Throttle:       {throttle_us} µs between sends")
        print("=" * 70)
        if validate and self.orderbook_file:
            print()
            print("NOTE: LOBSTER data represents updates to an existing order book.")
            print("      Our simulator starts empty, so early validation errors are expected.")
            print("      Use --skip-initial N to skip validation during book warmup.")
        if self.market_data_port > 0:
            print()
            print(f"UDP Market Data broadcasting on port {self.market_data_port}")
            print("Send any UDP packet to subscribe to market data updates.")
        print()

        try:
            # Wait for subscribers if requested
            if self._market_data_server and wait_subscribers > 0:
                print(f"Waiting for {wait_subscribers} subscriber(s)...")
                while self._market_data_server.get_subscriber_count() < wait_subscribers:
                    current = self._market_data_server.get_subscriber_count()
                    print(f"  {current}/{wait_subscribers} subscribers connected", end='\r')
                    time.sleep(0.5)
                print(f"  {wait_subscribers}/{wait_subscribers} subscribers connected")
                print()

            # Wait for user ready signal if requested
            if wait_ready:
                if self._market_data_server:
                    print(f"Subscribers connected: {self._market_data_server.get_subscriber_count()}")
                input("Press Enter to start replay...")
                print()

            self._run_replay(reader, print_every, validate, max_messages, verbose, skip_initial, throttle_us)
        finally:
            # Stop UDP server
            if self._market_data_server:
                self._market_data_server.stop()
                self._market_data_server = None

    def _run_replay(self, reader: LOBSTERReader, print_every: int, validate: bool,
                    max_messages: int, verbose: bool, skip_initial: int,
                    throttle_us: int = 0) -> None:
        """Internal replay loop."""
        throttle_s = throttle_us / 1_000_000.0 if throttle_us > 0 else 0
        for msg, expected_book in reader.read_messages_with_orderbook():
            self._message_count += 1

            # Check message limit
            if max_messages > 0 and self._message_count > max_messages:
                break

            # Skip trading halt messages
            if msg.is_trading_halt:
                self._skipped_messages += 1
                continue

            # Convert to event
            event = reader.to_event(msg, self._state.get_next_seq_num())
            if event is None:
                self._skipped_messages += 1
                continue

            # Verbose output
            if verbose:
                event_names = {1: "INSERT", 2: "CANCEL", 3: "DELETE", 4: "EXECUTE", 5: "HIDDEN"}
                event_name = event_names.get(msg.event_type, "UNKNOWN")
                side = "BUY" if msg.direction == 1 else "SELL"
                print(f"[{self._message_count:7d}] {format_time(msg.time)} {event_name:8s} "
                      f"id={msg.order_id:12d} size={msg.size:6d} "
                      f"price={format_price(msg.price):>10s} {side}")

            # Process the event
            ack, trades = self._order_book.process_event(event)

            if trades:
                self._trade_count += len(trades)

            # Broadcast on UDP market data (raw LOBSTER format line)
            if self._market_data_server:
                lobster_line = f"{msg.time:.9f},{msg.event_type},{msg.order_id},{msg.size},{msg.price},{msg.direction}"
                self._market_data_server.broadcast_raw(lobster_line)
                if throttle_s > 0:
                    time.sleep(throttle_s)

            # Validate against expected orderbook (skip during warmup period)
            if validate and expected_book and self.orderbook_file and self._message_count > skip_initial:
                if not self.validate_orderbook(
                    expected_book.bid_price, expected_book.bid_size,
                    expected_book.ask_price, expected_book.ask_size
                ):
                    self._validation_errors += 1
                    if verbose or self._validation_errors <= 10:
                        actual_bid = self._order_book.get_best_bid_price() or 0
                        actual_bid_sz = self._order_book.get_best_bid_size() or 0
                        actual_ask = self._order_book.get_best_ask_price() or 0
                        actual_ask_sz = self._order_book.get_best_ask_size() or 0
                        print(f"  VALIDATION ERROR at message {self._message_count}:")
                        print(f"    Expected: bid={format_price(expected_book.bid_price)}x{expected_book.bid_size} "
                              f"ask={format_price(expected_book.ask_price)}x{expected_book.ask_size}")
                        print(f"    Actual:   bid={format_price(actual_bid)}x{actual_bid_sz} "
                              f"ask={format_price(actual_ask)}x{actual_ask_sz}")

            # Periodic orderbook printing
            if print_every > 0 and self._message_count % print_every == 0:
                print(f"\n--- After message {self._message_count} ({format_time(msg.time)}) ---")
                print(self.print_book())

        # Final summary
        print()
        print("=" * 70)
        print("  REPLAY COMPLETE")
        print("=" * 70)
        print(f"  Messages processed: {self._message_count}")
        print(f"  Messages skipped:   {self._skipped_messages}")
        print(f"  Trades generated:   {self._trade_count}")
        if validate and self.orderbook_file:
            print(f"  Validation errors:  {self._validation_errors}")
        print()
        print("Final orderbook state:")
        print(self.print_book())
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Exchange Server - A multi-threaded order matching engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Order formats (live mode):
  limit,size,price,side,user[,ttl]  - Limit order with optional TTL in seconds
                                      (e.g., limit,100,50000000,B,trader1)
                                      (e.g., limit,100,50000000,B,trader1,60)
  market,size,0,side,user           - Market order (e.g., market,50,0,S,trader1)
  cancel,order_id,user              - Cancel order (e.g., cancel,1000,trader1)

Price format: price * 10000 (e.g., $5000.00 = 50000000)
Side: B=Buy, S=Sell
TTL: Time-to-live in seconds (default: 3600 = 1 hour)

Historical replay mode:
  --historical <message_file>       - Replay LOBSTER message file
  --orderbook <orderbook_file>      - LOBSTER orderbook file for validation
  --print-every N                   - Print orderbook every N messages
  --max-messages N                  - Stop after N messages
  --no-validate                     - Disable orderbook validation

UDP Market Data (default port 10002):
  --market-data-port PORT           - UDP market data port (0 to disable)
                                      (LOBSTER format: Time,Type,ID,Size,Price,Dir)
  --wait-subscribers N              - Wait for N subscribers before starting
  --wait-ready                      - Wait for Enter key before starting
  --throttle MICROSECONDS           - Microseconds between UDP sends (0=no throttle)
"""
    )
    # Live mode options
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

    # Historical replay mode options
    parser.add_argument('--historical', type=str, metavar='MESSAGE_FILE',
                        help='Run in historical replay mode with LOBSTER message file')
    parser.add_argument('--orderbook', type=str, metavar='ORDERBOOK_FILE',
                        help='LOBSTER orderbook file for validation (optional)')
    parser.add_argument('--print-every', type=int, default=0, metavar='N',
                        help='Print orderbook every N messages (0=disabled)')
    parser.add_argument('--max-messages', type=int, default=0, metavar='N',
                        help='Stop after N messages (0=process all)')
    parser.add_argument('--no-validate', action='store_true',
                        help='Disable orderbook validation')
    parser.add_argument('--skip-initial', type=int, default=0, metavar='N',
                        help='Skip validation for first N messages (warmup period)')
    parser.add_argument('--market-data-port', type=int, default=10002, metavar='PORT',
                        help='UDP port for market data broadcasting (default: 10002, 0=disabled)')
    parser.add_argument('--wait-subscribers', type=int, default=0, metavar='N',
                        help='Wait for N UDP subscribers before starting replay')
    parser.add_argument('--wait-ready', action='store_true',
                        help='Wait for user to press Enter before starting replay')
    parser.add_argument('--throttle', type=int, default=0, metavar='MICROSECONDS',
                        help='Microseconds to sleep between UDP sends (0=no throttle)')

    args = parser.parse_args()

    # Historical replay mode
    if args.historical:
        try:
            server = HistoricalReplayServer(
                message_file=args.historical,
                orderbook_file=args.orderbook,
                market_data_port=args.market_data_port
            )
            server.run(
                print_every=args.print_every,
                validate=not args.no_validate,
                max_messages=args.max_messages,
                verbose=args.verbose,
                skip_initial=args.skip_initial,
                wait_subscribers=args.wait_subscribers,
                wait_ready=args.wait_ready,
                throttle_us=args.throttle
            )
        except FileNotFoundError as e:
            logger.error(str(e))
            sys.exit(1)
        except KeyboardInterrupt:
            print("\nReplay interrupted.")
        return

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
        if args.market_data_port > 0:
            print(f"  Market Data Port:   {args.market_data_port} (UDP)")
        print(f"  Book Levels:        {args.book_levels}")
        print("=" * 60)
        print()
        print("Supported order types:")
        print("  limit,size,price,side,user[,ttl] - Limit order (TTL in seconds, default: 3600)")
        print("  market,size,0,side,user          - Market order")
        print("  cancel,order_id,user             - Cancel order")
        print()
        print("Price format: price * 10000 (e.g., $5000.00 = 50000000)")
        print("Side: B=Buy, S=Sell")
        print("=" * 60)
        print()

    server = ExchangeServer(args.order_port, args.feed_port, args.market_data_port)

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
