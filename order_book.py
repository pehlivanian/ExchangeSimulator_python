"""
Order book implementation for the Exchange Simulator.

This implements a price-time priority order book with separate bid and ask sides.
Mirrors the C++ OrderBook and ForwardListAdaptor implementations.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict
from sortedcontainers import SortedList
import threading

try:
    from .messages import Order, Trade, Ack, EventLOBSTER, EventType
except ImportError:
    from messages import Order, Trade, Ack, EventLOBSTER, EventType


@dataclass
class PriceLevel:
    """Represents a single price level in the order book."""
    price: int
    size: int
    order_count: int


class OrderBookSide:
    """
    One side of the order book (bid or ask).

    Uses a sorted list for price-time priority ordering.
    """

    def __init__(self, is_bid: bool):
        self.is_bid = is_bid
        # For bids: sort by price descending, then time ascending
        # For asks: sort by price ascending, then time ascending
        if is_bid:
            # Negative price for descending order, positive time for ascending
            self._key_func = lambda o: (-o.price, o.time, o.seq_num)
        else:
            # Positive price for ascending order, positive time for ascending
            self._key_func = lambda o: (o.price, o.time, o.seq_num)

        self._orders: SortedList = SortedList(key=self._key_func)
        self._order_map: Dict[int, Order] = {}  # order_id -> Order

    def insert(self, order: Order) -> Ack:
        """Insert an order into this side of the book."""
        if order.order_id in self._order_map:
            return Ack(
                seq_num=order.seq_num,
                order_id=order.order_id,
                acked=False,
                reason_rejected="Duplicate order ID"
            )

        self._orders.add(order)
        self._order_map[order.order_id] = order

        return Ack(
            seq_num=order.seq_num,
            order_id=order.order_id,
            acked=True
        )

    def remove(self, order_id: int) -> Optional[Order]:
        """Remove an order by ID. Returns the removed order or None."""
        if order_id not in self._order_map:
            return None

        order = self._order_map.pop(order_id)
        self._orders.remove(order)
        return order

    def get_bbo(self) -> Optional[Order]:
        """Get the best bid/offer (first order in price-time priority)."""
        if not self._orders:
            return None
        return self._orders[0]

    def get_bbo_price(self) -> Optional[int]:
        """Get the best price."""
        bbo = self.get_bbo()
        return bbo.price if bbo else None

    def get_bbo_size(self) -> Optional[int]:
        """Get the total size at the best price level."""
        bbo = self.get_bbo()
        if not bbo:
            return None

        total = 0
        for order in self._orders:
            if order.price == bbo.price:
                total += order.size
            elif self.is_bid:
                # For bids, once we see a lower price, we're done
                if order.price < bbo.price:
                    break
            else:
                # For asks, once we see a higher price, we're done
                if order.price > bbo.price:
                    break
        return total

    def get_book(self) -> List[PriceLevel]:
        """Get all price levels."""
        if not self._orders:
            return []

        levels = []
        current_price = None
        current_size = 0
        current_count = 0

        for order in self._orders:
            if current_price is None:
                current_price = order.price
                current_size = order.size
                current_count = 1
            elif order.price == current_price:
                current_size += order.size
                current_count += 1
            else:
                levels.append(PriceLevel(current_price, current_size, current_count))
                current_price = order.price
                current_size = order.size
                current_count = 1

        if current_price is not None:
            levels.append(PriceLevel(current_price, current_size, current_count))

        return levels

    def update_order_size(self, order_id: int, new_size: int) -> bool:
        """Update an order's size (for partial fills)."""
        if order_id not in self._order_map:
            return False

        order = self._order_map[order_id]
        # Remove and re-add with new size to maintain sorted order integrity
        self._orders.remove(order)
        order.size = new_size
        self._orders.add(order)
        return True

    def find_order_at_price(self, price: int) -> Optional[Order]:
        """Find any order at the given price level."""
        for order in self._orders:
            if order.price == price:
                return order
            # Early exit based on sort order
            if self.is_bid and order.price < price:
                break
            if not self.is_bid and order.price > price:
                break
        return None

    def reduce_size_at_price(self, price: int, size: int) -> bool:
        """
        Reduce size at a price level, removing orders as needed.
        Used when we don't have the specific order ID but know the price.
        Returns True if any size was reduced.
        """
        remaining_to_reduce = size
        orders_to_remove = []

        for order in self._orders:
            if order.price != price:
                if self.is_bid and order.price < price:
                    break
                if not self.is_bid and order.price > price:
                    break
                continue

            if order.size <= remaining_to_reduce:
                remaining_to_reduce -= order.size
                orders_to_remove.append(order.order_id)
            else:
                # Partial reduction
                self.update_order_size(order.order_id, order.size - remaining_to_reduce)
                remaining_to_reduce = 0
                break

            if remaining_to_reduce == 0:
                break

        for order_id in orders_to_remove:
            self.remove(order_id)

        return remaining_to_reduce < size

    def __len__(self) -> int:
        return len(self._orders)

    def __bool__(self) -> bool:
        return len(self._orders) > 0


class OrderBook:
    """
    Full order book with bid and ask sides.

    Handles order matching and trade generation.
    """

    def __init__(self):
        self.bids = OrderBookSide(is_bid=True)
        self.asks = OrderBookSide(is_bid=False)
        self._lock = threading.Lock()
        self._next_seed_id = -1  # Negative IDs for seeded orders

    def seed_from_snapshot(self, bid_price: int, bid_size: int,
                           ask_price: int, ask_size: int, time: float = 0.0) -> None:
        """
        Seed the order book with initial BBO state from a LOBSTER orderbook snapshot.

        This creates synthetic orders to represent the initial market state.
        Uses negative order IDs to distinguish from real LOBSTER order IDs.

        Args:
            bid_price: Best bid price (LOBSTER format: price * 10000)
            bid_size: Size at best bid
            ask_price: Best ask price
            ask_size: Size at best ask
            time: Timestamp for the orders
        """
        with self._lock:
            # Handle LOBSTER's empty level markers
            if bid_price != -9999999999 and bid_size > 0:
                bid_order = Order(
                    seq_num=self._next_seed_id,
                    side='B',
                    time=time,
                    order_id=self._next_seed_id,
                    price=bid_price,
                    size=bid_size,
                    order_type=EventType.INSERT
                )
                self.bids.insert(bid_order)
                self._next_seed_id -= 1

            if ask_price != 9999999999 and ask_size > 0:
                ask_order = Order(
                    seq_num=self._next_seed_id,
                    side='S',
                    time=time,
                    order_id=self._next_seed_id,
                    price=ask_price,
                    size=ask_size,
                    order_type=EventType.INSERT
                )
                self.asks.insert(ask_order)
                self._next_seed_id -= 1

    def process_event(self, event: EventLOBSTER) -> Tuple[Ack, Optional[List[Trade]]]:
        """
        Process a LOBSTER event and return ack and any resulting trades.

        Returns: (ack, trades) where trades is None if no trades occurred.
        """
        with self._lock:
            if event.event_type == EventType.INSERT:
                return self._process_insert(event)
            elif event.event_type == EventType.EXECUTE:
                return self._process_execute(event)
            elif event.event_type == EventType.CANCEL:
                return self._process_cancel(event)
            elif event.event_type == EventType.DELETE:
                return self._process_delete(event)
            else:
                # Unknown event type
                return Ack(
                    seq_num=event.seq_num,
                    order_id=event.order_id,
                    acked=False,
                    reason_rejected=f"Unknown event type: {event.event_type}"
                ), None

    def _process_insert(self, event: EventLOBSTER) -> Tuple[Ack, Optional[List[Trade]]]:
        """Process an INSERT event - may result in trades if it crosses the spread."""
        order = Order(
            seq_num=event.seq_num,
            side=event.direction,
            time=event.time,
            order_id=event.order_id,
            price=event.price,
            size=event.size,
            order_type=EventType.INSERT
        )

        # Determine which side to insert to and which to match against
        if order.side == 'B':
            insert_side = self.bids
            match_side = self.asks
            crosses = lambda o, bbo: o.price >= bbo.price
        else:
            insert_side = self.asks
            match_side = self.bids
            crosses = lambda o, bbo: o.price <= bbo.price

        trades = []
        remaining_size = order.size

        # Try to match against the opposite side
        while remaining_size > 0:
            bbo = match_side.get_bbo()
            if bbo is None:
                break

            if not crosses(order, bbo):
                break

            # We have a match
            match_size = min(remaining_size, bbo.size)

            # Create trades for both sides
            # Standing order trade (the one being hit)
            standing_trade = Trade(
                seq_num=bbo.seq_num,
                counter_seq_num=order.seq_num,
                order_id=bbo.order_id,
                side=bbo.side,
                price=bbo.price,  # Trade at standing order's price
                size=match_size
            )

            # Taking order trade
            taking_trade = Trade(
                seq_num=order.seq_num,
                counter_seq_num=bbo.seq_num,
                order_id=order.order_id,
                side=order.side,
                price=bbo.price,
                size=match_size
            )

            trades.append(standing_trade)
            trades.append(taking_trade)

            remaining_size -= match_size

            # Update or remove the standing order
            if match_size >= bbo.size:
                match_side.remove(bbo.order_id)
            else:
                match_side.update_order_size(bbo.order_id, bbo.size - match_size)

        # If there's remaining size, insert it into the book
        if remaining_size > 0:
            order.size = remaining_size
            ack = insert_side.insert(order)
        else:
            # Fully filled, ack without inserting
            ack = Ack(
                seq_num=order.seq_num,
                order_id=order.order_id,
                acked=True
            )

        return ack, trades if trades else None

    def _process_execute(self, event: EventLOBSTER) -> Tuple[Ack, Optional[List[Trade]]]:
        """
        Process an EXECUTE event - removes liquidity from the book.

        The direction in EXECUTE is the side of the standing order being executed.
        For LOBSTER replay, we execute at the specified price, finding any order there.
        """
        if event.direction == 'B':
            side = self.bids
        else:
            side = self.asks

        # Try to find the specific order by ID first
        target_order = None
        if event.order_id in side._order_map:
            target_order = side._order_map[event.order_id]
        else:
            # Fall back to finding any order at the execution price
            target_order = side.find_order_at_price(event.price)

        if target_order is None:
            # No liquidity at this price - this can happen with seeded books
            # Try to reduce size at the price level anyway
            if side.reduce_size_at_price(event.price, event.size):
                return Ack(
                    seq_num=event.seq_num,
                    order_id=event.order_id,
                    acked=True
                ), None

            return Ack(
                seq_num=event.seq_num,
                order_id=event.order_id,
                acked=False,
                reason_rejected=f"No liquidity at price {event.price}"
            ), None

        # Execute against the found order
        exec_size = min(event.size, target_order.size)

        # Create trade
        trade = Trade(
            seq_num=target_order.seq_num,
            counter_seq_num=event.seq_num,
            order_id=target_order.order_id,
            side=target_order.side,
            price=target_order.price,
            size=exec_size
        )

        # Update or remove the standing order
        if exec_size >= target_order.size:
            side.remove(target_order.order_id)
        else:
            side.update_order_size(target_order.order_id, target_order.size - exec_size)

        ack = Ack(
            seq_num=event.seq_num,
            order_id=event.order_id,
            acked=True
        )

        return ack, [trade]

    def _process_cancel(self, event: EventLOBSTER) -> Tuple[Ack, Optional[List[Trade]]]:
        """
        Process a CANCEL event - partial cancellation.

        For LOBSTER replay, if the specific order ID isn't found,
        we reduce size at the specified price level.
        """
        if event.direction == 'B':
            side = self.bids
        else:
            side = self.asks

        if event.order_id in side._order_map:
            order = side._order_map[event.order_id]
            new_size = order.size - event.size
            if new_size <= 0:
                side.remove(event.order_id)
            else:
                side.update_order_size(event.order_id, new_size)

            return Ack(
                seq_num=event.seq_num,
                order_id=event.order_id,
                acked=True
            ), None
        else:
            # Fall back to reducing size at the price level
            if side.reduce_size_at_price(event.price, event.size):
                return Ack(
                    seq_num=event.seq_num,
                    order_id=event.order_id,
                    acked=True
                ), None

            return Ack(
                seq_num=event.seq_num,
                order_id=event.order_id,
                acked=False,
                reason_rejected="Order not found"
            ), None

    def _process_delete(self, event: EventLOBSTER) -> Tuple[Ack, Optional[List[Trade]]]:
        """
        Process a DELETE event - full cancellation.

        For LOBSTER replay, if the specific order ID isn't found,
        we try to remove all size at the specified price level.
        """
        if event.direction == 'B':
            side = self.bids
        else:
            side = self.asks

        if side.remove(event.order_id):
            return Ack(
                seq_num=event.seq_num,
                order_id=event.order_id,
                acked=True
            ), None
        else:
            # Fall back to reducing size at the price level
            # For DELETE, we remove the size that was specified in the event
            # (LOBSTER DELETE events have size = remaining size of the order)
            if event.size > 0 and side.reduce_size_at_price(event.price, event.size):
                return Ack(
                    seq_num=event.seq_num,
                    order_id=event.order_id,
                    acked=True
                ), None

            return Ack(
                seq_num=event.seq_num,
                order_id=event.order_id,
                acked=False,
                reason_rejected="Order not found"
            ), None

    def get_best_bid_price(self) -> Optional[int]:
        """Get the best bid price."""
        return self.bids.get_bbo_price()

    def get_best_ask_price(self) -> Optional[int]:
        """Get the best ask price."""
        return self.asks.get_bbo_price()

    def get_best_bid_size(self) -> Optional[int]:
        """Get total size at best bid."""
        return self.bids.get_bbo_size()

    def get_best_ask_size(self) -> Optional[int]:
        """Get total size at best ask."""
        return self.asks.get_bbo_size()

    def get_snapshot(self) -> Tuple[List[PriceLevel], List[PriceLevel]]:
        """Get a snapshot of the order book."""
        with self._lock:
            return self.bids.get_book(), self.asks.get_book()
