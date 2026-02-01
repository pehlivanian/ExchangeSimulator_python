#!/usr/bin/env python3
"""
Order Client with FSM - Enhanced OrderClient that tracks order state via OrderStateMachine.

The OrderClient uses composition to contain an OrderStateMachine. All state transitions
are driven by parsing messages from the exchange server - no direct state manipulation.
"""

import argparse
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from order_fsm_final import OrderStateMachine, OrderState, OrderEvent, InvalidTransitionError
from messages import DEFAULT_TTL_SECONDS


@dataclass
class ExchangeMessage:
    """Parsed exchange server message."""
    msg_type: str
    order_id: Optional[int] = None
    size: Optional[int] = None
    price: Optional[int] = None
    remainder_size: Optional[int] = None
    reason: Optional[str] = None


def parse_exchange_message(message: str) -> Optional[ExchangeMessage]:
    """
    Parse an exchange server response message.

    Message formats:
        ACK,{order_id},{size},{price}
        FILL,{order_id},{size},{price}
        PARTIAL_FILL,{order_id},{size},{price},{remainder_size}
        REJECT,{reason}
        CANCEL_ACK,{order_id},{size}
        EXPIRED,{order_id},{size}
    """
    message = message.strip()
    if not message:
        return None

    parts = message.split(',')
    if not parts:
        return None

    msg_type = parts[0].upper()

    try:
        if msg_type == "ACK" and len(parts) == 4:
            return ExchangeMessage(
                msg_type=msg_type,
                order_id=int(parts[1]),
                size=int(parts[2]),
                price=int(parts[3])
            )
        elif msg_type == "FILL" and len(parts) == 4:
            return ExchangeMessage(
                msg_type=msg_type,
                order_id=int(parts[1]),
                size=int(parts[2]),
                price=int(parts[3])
            )
        elif msg_type == "PARTIAL_FILL" and len(parts) == 5:
            return ExchangeMessage(
                msg_type=msg_type,
                order_id=int(parts[1]),
                size=int(parts[2]),
                price=int(parts[3]),
                remainder_size=int(parts[4])
            )
        elif msg_type == "REJECT" and len(parts) >= 2:
            return ExchangeMessage(
                msg_type=msg_type,
                reason=','.join(parts[1:])
            )
        elif msg_type == "CANCEL_ACK" and len(parts) == 3:
            return ExchangeMessage(
                msg_type=msg_type,
                order_id=int(parts[1]),
                size=int(parts[2])
            )
        elif msg_type == "EXPIRED" and len(parts) == 3:
            return ExchangeMessage(
                msg_type=msg_type,
                order_id=int(parts[1]),
                size=int(parts[2])
            )
        elif msg_type == "ERROR" and len(parts) >= 2:
            return ExchangeMessage(
                msg_type=msg_type,
                reason=','.join(parts[1:])
            )
    except (ValueError, IndexError):
        pass

    return None


# Mapping from exchange message types to FSM events
MESSAGE_TO_EVENT = {
    "ACK": OrderEvent.BROKER_ACK,
    "FILL": OrderEvent.FULL_EXECUTION,
    "PARTIAL_FILL": OrderEvent.EXECUTION,
    "CANCEL_ACK": OrderEvent.CANCEL_ACK,
    "EXPIRED": OrderEvent.TTL_ELAPSED,
    "REJECT": OrderEvent.BROKER_REJECT,
}


class ExchangeOrderStateMachine(OrderStateMachine):
    """
    OrderStateMachine extended with exchange message processing.

    Handles the mapping from exchange message types to FSM events internally,
    so clients don't need to know about FSM implementation details.
    """

    def __init__(self, order_id: Optional[str] = None):
        super().__init__(order_id)
        self._last_error: Optional[str] = None
        self._exchange_order_id: Optional[int] = None

    def process_exchange_message(self, msg: ExchangeMessage) -> bool:
        """
        Process an exchange message and trigger the appropriate FSM event.

        Returns True if the message was processed successfully.
        """
        self._last_error = None

        event = MESSAGE_TO_EVENT.get(msg.msg_type)
        if event is None:
            self._last_error = msg.reason if msg.msg_type == "ERROR" else f"Unknown message: {msg.msg_type}"
            return False

        try:
            self.process_event(event, msg=msg)
        except InvalidTransitionError:
            # Invalid transition is a no-op (e.g., REJECT in unexpected state)
            pass

        return True

    def _on_broker_ack(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_NEW -> ACCEPTED, capture exchange order ID."""
        msg = kwargs.get('msg')
        if msg and msg.order_id is not None:
            self._exchange_order_id = msg.order_id
        super()._on_broker_ack(event, *args, **kwargs)

    @property
    def exchange_order_id(self) -> Optional[int]:
        return self._exchange_order_id

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error


class ManagedOrder:
    """
    An order managed by a state machine.

    Contains an ExchangeOrderStateMachine. State transitions happen only via
    process_message() - no direct state manipulation from outside.
    """

    def __init__(self, order_id: Optional[int] = None,
                 size: int = 0, price: int = 0, side: str = 'B',
                 order_type: str = 'limit', ttl: int = DEFAULT_TTL_SECONDS):
        self._fsm = ExchangeOrderStateMachine(order_id=str(order_id) if order_id else None)
        self._size = size
        self._price = price
        self._side = side
        self._order_type = order_type
        self._ttl = ttl
        self._last_message: Optional[ExchangeMessage] = None

    def process_message(self, msg: ExchangeMessage) -> bool:
        """
        Process an exchange message. FSM handles all state transition logic.
        """
        self._last_message = msg
        return self._fsm.process_exchange_message(msg)

    def submit(self) -> bool:
        """
        Submit the order (local validation step).
        Transitions from NEW to PENDING_VALIDATION.
        """
        try:
            self._fsm.process_event(OrderEvent.SUBMIT)
            return True
        except InvalidTransitionError:
            return False

    def validate(self) -> bool:
        """
        Perform local validation and transition to PENDING_NEW if valid.
        """
        if self._size <= 0:
            self._fsm.process_event(OrderEvent.CHECKS_FAILED, reason="Invalid size")
            return False

        if self._order_type == "limit" and self._price <= 0:
            self._fsm.process_event(OrderEvent.CHECKS_FAILED, reason="Invalid price")
            return False

        if self._side not in ('B', 'S'):
            self._fsm.process_event(OrderEvent.CHECKS_FAILED, reason="Invalid side")
            return False

        try:
            self._fsm.process_event(OrderEvent.CHECKS_PASSED)
            return True
        except InvalidTransitionError:
            return False

    # --- Read-only properties exposing FSM state ---

    @property
    def state(self) -> OrderState:
        return self._fsm.get_current_state()

    @property
    def state_name(self) -> str:
        return self._fsm.get_current_state().name

    @property
    def is_terminal(self) -> bool:
        return self._fsm.is_terminal_state()

    @property
    def is_live(self) -> bool:
        return self._fsm.is_live

    @property
    def state_history(self):
        return self._fsm.get_state_history()

    @property
    def exchange_order_id(self) -> Optional[int]:
        return self._fsm.exchange_order_id

    @property
    def size(self) -> int:
        return self._size

    @property
    def price(self) -> int:
        return self._price

    @property
    def side(self) -> str:
        return self._side

    @property
    def order_type(self) -> str:
        return self._order_type

    @property
    def ttl(self) -> int:
        return self._ttl

    @property
    def last_message(self) -> Optional[ExchangeMessage]:
        return self._last_message

    @property
    def error_message(self) -> Optional[str]:
        return self._fsm.last_error

    def to_order_string(self) -> str:
        """Generate the wire format order string."""
        if self._order_type == "limit":
            return f"limit,{self._size},{self._price},{self._side},client,{self._ttl}"
        else:
            return f"market,{self._size},0,{self._side},client"

    def __repr__(self) -> str:
        return (f"ManagedOrder(state={self.state_name}, "
                f"exchange_id={self.exchange_order_id}, "
                f"size={self._size}, price={self._price}, side={self._side})")


class OrderClientWithFSM:
    """
    Enhanced OrderClient that tracks order state via OrderStateMachine.

    Each order is wrapped in a ManagedOrder that contains an OrderStateMachine.
    Exchange messages are parsed and passed to the appropriate ManagedOrder,
    which handles state transitions internally.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._socket: Optional[socket.socket] = None
        self._running = False
        self._receive_thread: Optional[threading.Thread] = None
        self._buffer = ""

        self._orders: Dict[int, ManagedOrder] = {}
        self._orders_lock = threading.Lock()
        self._pending_order: Optional[ManagedOrder] = None
        self._message_callback: Optional[Callable[[ManagedOrder, ExchangeMessage], None]] = None

    def connect(self) -> bool:
        """Connect to the exchange."""
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

    def set_message_callback(self, callback: Callable[[ManagedOrder, ExchangeMessage], None]) -> None:
        """Set callback for message notifications."""
        self._message_callback = callback

    def create_order(self, order_type: str, size: int, price: int, side: str,
                     ttl: int = DEFAULT_TTL_SECONDS) -> ManagedOrder:
        """Create a new managed order in NEW state."""
        return ManagedOrder(
            order_id=None,
            size=size,
            price=price,
            side=side.upper(),
            order_type=order_type.lower(),
            ttl=ttl
        )

    def submit_order(self, order: ManagedOrder) -> bool:
        """Submit a managed order to the exchange."""
        if not order.submit():
            return False

        if not order.validate():
            return False

        if not self._socket:
            return False

        try:
            order_str = order.to_order_string() + '\n'
            self._pending_order = order
            self._socket.sendall(order_str.encode('utf-8'))
            return True
        except Exception:
            return False

    def submit_order_sync(self, order: ManagedOrder, timeout: float = 5.0) -> bool:
        """Submit order and wait for initial response."""
        if not self.submit_order(order):
            return False

        try:
            self._socket.settimeout(timeout)
            data = self._socket.recv(4096)
            self._buffer += data.decode('utf-8')

            while '\n' in self._buffer:
                line, self._buffer = self._buffer.split('\n', 1)
                line = line.strip()
                if line:
                    self._process_message(line)

            return order.state != OrderState.PENDING_NEW

        except socket.timeout:
            return False
        except Exception:
            return False

    def cancel_order(self, order: ManagedOrder, user: str = "client") -> bool:
        """Send cancel request for a managed order."""
        if order.exchange_order_id is None or not order.is_live or not self._socket:
            return False

        try:
            cancel_str = f"cancel,{order.exchange_order_id},{user}\n"
            self._socket.sendall(cancel_str.encode('utf-8'))
            return True
        except Exception:
            return False

    def get_order(self, exchange_order_id: int) -> Optional[ManagedOrder]:
        """Get order by exchange ID."""
        with self._orders_lock:
            return self._orders.get(exchange_order_id)

    def get_all_orders(self) -> Dict[int, ManagedOrder]:
        """Get all tracked orders."""
        with self._orders_lock:
            return self._orders.copy()

    def start_async_receive(self) -> None:
        """Start receiving messages asynchronously."""
        self._running = True
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._receive_thread.start()

    def _receive_loop(self) -> None:
        """Receive loop for async messages."""
        if not self._socket:
            return

        self._socket.setblocking(False)

        while self._running:
            try:
                data = self._socket.recv(4096)
                if not data:
                    break

                self._buffer += data.decode('utf-8')

                while '\n' in self._buffer:
                    line, self._buffer = self._buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        self._process_message(line)

            except BlockingIOError:
                time.sleep(0.01)
            except Exception as e:
                if self._running:
                    print(f"Receive error: {e}", file=sys.stderr)
                break

    def _process_message(self, message: str) -> None:
        """Process exchange message and route to appropriate order."""
        msg = parse_exchange_message(message)
        if msg is None:
            return

        order: Optional[ManagedOrder] = None

        with self._orders_lock:
            if msg.msg_type == "ACK" and self._pending_order is not None:
                order = self._pending_order
                self._pending_order = None
                if msg.order_id is not None:
                    self._orders[msg.order_id] = order

            elif msg.msg_type == "REJECT" and self._pending_order is not None:
                order = self._pending_order
                self._pending_order = None

            elif msg.msg_type == "FILL" and self._pending_order is not None:
                # Handle immediate full fill for pending order (crossing order)
                order = self._pending_order
                self._pending_order = None
                if msg.order_id is not None:
                    self._orders[msg.order_id] = order
                # Transition through ACK first, then FILL
                order.process_message(ExchangeMessage(
                    msg_type="ACK",
                    order_id=msg.order_id,
                    size=msg.size,
                    price=msg.price
                ))

            elif msg.msg_type == "PARTIAL_FILL" and self._pending_order is not None:
                # Handle immediate partial fill for pending order (crossing order)
                order = self._pending_order
                self._pending_order = None
                if msg.order_id is not None:
                    self._orders[msg.order_id] = order
                # Transition through ACK first, then PARTIAL_FILL
                order.process_message(ExchangeMessage(
                    msg_type="ACK",
                    order_id=msg.order_id,
                    size=msg.size,
                    price=msg.price
                ))

            elif msg.order_id is not None:
                order = self._orders.get(msg.order_id)

        if order is not None:
            order.process_message(msg)
            if self._message_callback:
                self._message_callback(order, msg)


# --- Main / Demo ---

ORDER_HELP = """
============================================================
  ORDER CLIENT WITH FSM
============================================================

LIMIT ORDER:  limit,size,price,side[,ttl]
MARKET ORDER: market,size,side
CANCEL:       cancel,order_id
STATE:        state,order_id
LIST:         orders

Price format: price * 10000 (e.g., $5000.00 = 50000000)
============================================================
"""


def main():
    parser = argparse.ArgumentParser(description='Order Client with FSM')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=10000)
    parser.add_argument('-q', '--quiet', action='store_true')
    args = parser.parse_args()

    client = OrderClientWithFSM(args.host, args.port)

    def on_message(order: ManagedOrder, msg: ExchangeMessage):
        print(f"[{msg.msg_type}] Order {order.exchange_order_id}: {order.state_name}")

    client.set_message_callback(on_message)

    if not client.connect():
        sys.exit(1)

    try:
        client.start_async_receive()

        if not args.quiet:
            print(ORDER_HELP)

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            parts = line.split(',')
            cmd = parts[0].lower()

            try:
                if cmd == "limit" and len(parts) >= 4:
                    size, price, side = int(parts[1]), int(parts[2]), parts[3].upper()
                    ttl = int(parts[4]) if len(parts) > 4 else DEFAULT_TTL_SECONDS
                    order = client.create_order("limit", size, price, side, ttl)
                    if client.submit_order_sync(order):
                        print(f"Submitted: {order}")
                    else:
                        print(f"Failed: {order.error_message}")

                elif cmd == "market" and len(parts) >= 3:
                    size, side = int(parts[1]), parts[2].upper()
                    order = client.create_order("market", size, 0, side)
                    if client.submit_order_sync(order):
                        print(f"Submitted: {order}")
                    else:
                        print(f"Failed: {order.error_message}")

                elif cmd == "cancel" and len(parts) >= 2:
                    order = client.get_order(int(parts[1]))
                    if order and client.cancel_order(order):
                        print(f"Cancel sent")
                    else:
                        print(f"Cancel failed")

                elif cmd == "state" and len(parts) >= 2:
                    order = client.get_order(int(parts[1]))
                    if order:
                        print(f"{order}\n  History: {[(s.name, e.name) for s, e in order.state_history]}")
                    else:
                        print("Not found")

                elif cmd == "orders":
                    for oid, order in client.get_all_orders().items():
                        print(f"  {oid}: {order.state_name}")

                else:
                    print(f"Unknown: {line}")

            except (ValueError, IndexError) as e:
                print(f"Error: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
