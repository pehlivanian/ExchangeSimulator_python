"""
Order State Machine Implementation

A Python implementation of a finite state machine following the architecture
pattern from FSM.hpp, implementing the order lifecycle state diagram.

The only public interface for state transitions is process_event().
All internal state (quantity, filled_quantity, etc.) is managed by the FSM itself.
"""

from enum import Enum, auto
from typing import Callable, Optional
from abc import ABC, abstractmethod


class OrderState(Enum):
    """Order lifecycle states."""
    NEW = 0
    PENDING_VALIDATION = auto()
    VALIDATION_FAILED = auto()
    PENDING_NEW = auto()
    REJECTED = auto()
    ACCEPTED = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELLED = auto()
    EXPIRED = auto()
    NUM_STATES = auto()


class OrderEvent(Enum):
    """Events that trigger state transitions."""
    SUBMIT = 0              # NEW -> PENDING_VALIDATION
    CHECKS_FAILED = auto()  # PENDING_VALIDATION -> VALIDATION_FAILED
    CHECKS_PASSED = auto()  # PENDING_VALIDATION -> PENDING_NEW
    BROKER_REJECT = auto()  # PENDING_NEW -> REJECTED
    BROKER_ACK = auto()     # PENDING_NEW -> ACCEPTED
    EXECUTION = auto()      # ACCEPTED -> PARTIALLY_FILLED, PARTIALLY_FILLED -> PARTIALLY_FILLED
    FULL_EXECUTION = auto() # ACCEPTED/PARTIALLY_FILLED -> FILLED
    CANCEL_ACK = auto()     # ACCEPTED/PARTIALLY_FILLED -> CANCELLED (cancel acknowledged)
    CANCEL_REJ = auto()     # ACCEPTED/PARTIALLY_FILLED -> same state (cancel rejected, self-loop)
    TTL_ELAPSED = auto()    # PENDING_NEW/ACCEPTED/PARTIALLY_FILLED -> EXPIRED
    NUM_EVENTS = auto()


class FSM(ABC):
    """
    Base class for finite state machines.

    Derived classes must:
    1. Define _build_transition_matrix() to return the state/event -> handler mapping
    2. Implement handler methods for each valid transition
    """

    def __init__(self, initial_state: Enum):
        self._current_state: Enum = initial_state
        self._state_history: list[tuple[Enum, Enum]] = []
        self._transition_matrix: dict[tuple[Enum, Enum], Callable] = {}
        self._build_transition_matrix()

    @abstractmethod
    def _build_transition_matrix(self) -> None:
        """
        Populate self._transition_matrix with (state, event) -> handler mappings.
        Must be implemented by derived classes.
        """
        pass

    @abstractmethod
    def _error_handler(self, event: Enum, *args, **kwargs) -> None:
        """Handle invalid state/event combinations."""
        pass

    def process_event(self, event: Enum, *args, **kwargs) -> None:
        """
        Process an event and execute the appropriate transition handler.

        This is the ONLY public interface for state transitions.
        Event-specific data (e.g., execution quantity) is passed via kwargs.
        """
        key = (self._current_state, event)
        handler = self._transition_matrix.get(key, self._error_handler)
        handler(event, *args, **kwargs)

    @property
    def current_state(self) -> Enum:
        """Return the current state (read-only)."""
        return self._current_state

    def get_current_state(self) -> Enum:
        """Return the current state. Alias for current_state property."""
        return self._current_state

    def set_current_state(self, state: Enum) -> None:
        """Set the current state."""
        self._current_state = state

    def record_transition(self, new_state: Enum, event: Enum) -> None:
        """Record state transition in history."""
        self._state_history.append((new_state, event))

    def get_state_history(self) -> list[tuple[Enum, Enum]]:
        """Return a copy of the state transition history."""
        return self._state_history.copy()

    def is_terminal_state(self) -> bool:
        """Check if current state is a terminal state. Override in subclass."""
        return False


class OrderStateMachine(FSM):
    """
    Concrete FSM implementation for order lifecycle management.

    Usage:
        order = OrderStateMachine(order_id="ORD-001", total_quantity=100.0)
        order.process_event(OrderEvent.SUBMIT)
        order.process_event(OrderEvent.CHECKS_PASSED)
        order.process_event(OrderEvent.BROKER_ACK)
        order.process_event(OrderEvent.EXECUTION, quantity=30.0)
        order.process_event(OrderEvent.FULL_EXECUTION, quantity=70.0)

    The only way to change state is through process_event().
    """

    TERMINAL_STATES = frozenset({
        OrderState.VALIDATION_FAILED,
        OrderState.REJECTED,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
    })

    def __init__(self, order_id: Optional[str] = None, total_quantity: float = 0.0):
        """
        Initialize the order state machine.

        Args:
            order_id: Unique identifier for this order
            total_quantity: Total quantity to be filled (optional)
        """
        self._order_id = order_id
        self._total_quantity = total_quantity
        self._filled_quantity: float = 0.0
        self._order_live = False

        super().__init__(OrderState.NEW)

    def _build_transition_matrix(self) -> None:
        """Build the transition matrix mapping (state, event) -> handler."""
        sb = self._on_submit
        vf = self._on_validation_failed
        cp = self._on_checks_passed
        rj = self._on_broker_reject
        ac = self._on_broker_ack
        ex = self._on_execution
        fe = self._on_full_execution
        ca = self._on_cancel_ack
        cr = self._on_cancel_rej
        te = self._on_ttl_elapsed
        er = self._error_handler

        matrix = [
            # SUBMIT  CHK_FAIL  CHK_PASS  BRK_REJ  BRK_ACK  EXEC  FULL_EXEC  CAN_ACK  CAN_REJ  TTL
            [sb,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # NEW
            [er,     vf,       cp,       er,      er,      er,   er,        er,      er,      er],  # PENDING_VALIDATION
            [er,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # VALIDATION_FAILED
            [er,     er,       er,       rj,      ac,      er,   er,        er,      er,      te],  # PENDING_NEW
            [er,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # REJECTED
            [er,     er,       er,       er,      er,      ex,   fe,        ca,      cr,      te],  # ACCEPTED
            [er,     er,       er,       er,      er,      ex,   fe,        ca,      cr,      te],  # PARTIALLY_FILLED
            [er,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # FILLED
            [er,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # CANCELLED
            [er,     er,       er,       er,      er,      er,   er,        er,      er,      er],  # EXPIRED
        ]

        for state in OrderState:
            if state == OrderState.NUM_STATES:
                continue
            for event in OrderEvent:
                if event == OrderEvent.NUM_EVENTS:
                    continue
                handler = matrix[state.value][event.value]
                self._transition_matrix[(state, event)] = handler

    def _error_handler(self, event: OrderEvent, *args, **kwargs) -> None:
        """Handle invalid state/event combinations."""
        raise InvalidTransitionError(
            f"Invalid transition: event {event.name} not allowed in state {self._current_state.name}"
        )

    # --- Transition Handlers ---

    def _on_submit(self, event: OrderEvent, *args, **kwargs) -> None:
        """NEW -> PENDING_VALIDATION"""
        self.set_current_state(OrderState.PENDING_VALIDATION)
        self.record_transition(self._current_state, event)

    def _on_validation_failed(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_VALIDATION -> VALIDATION_FAILED (terminal)"""
        self.set_current_state(OrderState.VALIDATION_FAILED)
        self.record_transition(self._current_state, event)

    def _on_checks_passed(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_VALIDATION -> PENDING_NEW"""
        self.set_current_state(OrderState.PENDING_NEW)
        self.record_transition(self._current_state, event)

    def _on_broker_reject(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_NEW -> REJECTED (terminal)"""
        self.set_current_state(OrderState.REJECTED)
        self.record_transition(self._current_state, event)

    def _on_broker_ack(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_NEW -> ACCEPTED, order becomes live"""
        self.set_current_state(OrderState.ACCEPTED)
        self._order_live = True
        self.record_transition(self._current_state, event)

    def _on_execution(self, event: OrderEvent, *args, quantity: float = 0.0, **kwargs) -> None:
        """ACCEPTED -> PARTIALLY_FILLED, or PARTIALLY_FILLED -> PARTIALLY_FILLED (self-loop)"""
        self._filled_quantity += quantity
        self.set_current_state(OrderState.PARTIALLY_FILLED)
        self.record_transition(self._current_state, event)

    def _on_full_execution(self, event: OrderEvent, *args, quantity: float = 0.0, **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> FILLED (terminal), order no longer live"""
        self._filled_quantity += quantity
        self.set_current_state(OrderState.FILLED)
        self._order_live = False
        self.record_transition(self._current_state, event)

    def _on_cancel_ack(self, event: OrderEvent, *args, **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> CANCELLED (terminal), order no longer live"""
        self.set_current_state(OrderState.CANCELLED)
        self._order_live = False
        self.record_transition(self._current_state, event)

    def _on_cancel_rej(self, event: OrderEvent, *args, **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> same state (self-loop), order stays live"""
        self.record_transition(self._current_state, event)

    def _on_ttl_elapsed(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_NEW/ACCEPTED/PARTIALLY_FILLED -> EXPIRED (terminal), order no longer live"""
        self.set_current_state(OrderState.EXPIRED)
        self._order_live = False
        self.record_transition(self._current_state, event)

    # --- Read-only Public Properties ---

    def is_terminal_state(self) -> bool:
        """Check if the order is in a terminal state."""
        return self._current_state in self.TERMINAL_STATES

    @property
    def state_history(self) -> list[tuple[OrderState, OrderEvent]]:
        """Return a copy of the state transition history."""
        return self._state_history.copy()

    @property
    def order_id(self) -> Optional[str]:
        return self._order_id

    @property
    def filled_quantity(self) -> float:
        return self._filled_quantity

    @filled_quantity.setter
    def filled_quantity(self, value: float) -> None:
        self._filled_quantity = value

    @property
    def total_quantity(self) -> float:
        return self._total_quantity

    @total_quantity.setter
    def total_quantity(self, value: float) -> None:
        self._total_quantity = value

    @property
    def remaining_quantity(self) -> float:
        return self._total_quantity - self._filled_quantity

    @property
    def is_live(self) -> bool:
        return self._order_live

    def __repr__(self) -> str:
        return (f"OrderStateMachine(order_id={self._order_id!r}, "
                f"state={self._current_state.name}, "
                f"filled={self._filled_quantity}/{self._total_quantity})")


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


# --- Test Cases ---

if __name__ == "__main__":
    print("=" * 60)
    print("TEST 1: Successful order lifecycle (full fill path)")
    print("=" * 60)

    order = OrderStateMachine(order_id="ORD-001", total_quantity=100.0)

    assert order.current_state == OrderState.NEW
    assert order.total_quantity == 100.0
    assert order.filled_quantity == 0.0
    assert order.is_live == False
    assert order.is_terminal_state() == False
    print("Initial state: NEW, not live, not terminal, filled=0")

    order.process_event(OrderEvent.SUBMIT)
    assert order.current_state == OrderState.PENDING_VALIDATION
    print("After SUBMIT: PENDING_VALIDATION")

    order.process_event(OrderEvent.CHECKS_PASSED)
    assert order.current_state == OrderState.PENDING_NEW
    print("After CHECKS_PASSED: PENDING_NEW")

    order.process_event(OrderEvent.BROKER_ACK)
    assert order.current_state == OrderState.ACCEPTED
    assert order.is_live == True
    print("After BROKER_ACK: ACCEPTED, is_live=True")

    order.process_event(OrderEvent.EXECUTION, quantity=30.0)
    assert order.current_state == OrderState.PARTIALLY_FILLED
    assert order.filled_quantity == 30.0
    print("After EXECUTION(30): PARTIALLY_FILLED, filled=30")

    order.process_event(OrderEvent.EXECUTION, quantity=25.0)
    assert order.current_state == OrderState.PARTIALLY_FILLED
    assert order.filled_quantity == 55.0
    print("After EXECUTION(25): PARTIALLY_FILLED, filled=55")

    order.process_event(OrderEvent.FULL_EXECUTION, quantity=45.0)
    assert order.current_state == OrderState.FILLED
    assert order.is_terminal_state() == True
    assert order.is_live == False
    assert order.filled_quantity == 100.0
    print("After FULL_EXECUTION(45): FILLED, terminal, filled=100")

    print("\n" + "=" * 60)
    print("TEST 2: Cancel from PARTIALLY_FILLED")
    print("=" * 60)

    order2 = OrderStateMachine(order_id="ORD-002", total_quantity=100.0)
    order2.process_event(OrderEvent.SUBMIT)
    order2.process_event(OrderEvent.CHECKS_PASSED)
    order2.process_event(OrderEvent.BROKER_ACK)
    order2.process_event(OrderEvent.EXECUTION, quantity=40.0)
    assert order2.current_state == OrderState.PARTIALLY_FILLED
    print("After EXECUTION(40): PARTIALLY_FILLED")

    order2.process_event(OrderEvent.CANCEL_REJ)
    assert order2.current_state == OrderState.PARTIALLY_FILLED
    print("After CANCEL_REJ: still PARTIALLY_FILLED (self-loop)")

    order2.process_event(OrderEvent.CANCEL_ACK)
    assert order2.current_state == OrderState.CANCELLED
    assert order2.filled_quantity == 40.0
    print("After CANCEL_ACK: CANCELLED, filled=40 preserved")

    print("\n" + "=" * 60)
    print("TEST 3: TTL expiration")
    print("=" * 60)

    order3 = OrderStateMachine(order_id="ORD-003", total_quantity=100.0)
    order3.process_event(OrderEvent.SUBMIT)
    order3.process_event(OrderEvent.CHECKS_PASSED)
    order3.process_event(OrderEvent.BROKER_ACK)
    order3.process_event(OrderEvent.EXECUTION, quantity=60.0)
    order3.process_event(OrderEvent.TTL_ELAPSED)
    assert order3.current_state == OrderState.EXPIRED
    assert order3.filled_quantity == 60.0
    print("PARTIALLY_FILLED -> EXPIRED, filled=60 preserved")

    print("\n" + "=" * 60)
    print("TEST 4: Validation failure")
    print("=" * 60)

    order4 = OrderStateMachine(order_id="ORD-004", total_quantity=100.0)
    order4.process_event(OrderEvent.SUBMIT)
    order4.process_event(OrderEvent.CHECKS_FAILED)
    assert order4.current_state == OrderState.VALIDATION_FAILED
    assert order4.is_terminal_state() == True
    print("PENDING_VALIDATION -> VALIDATION_FAILED")

    print("\n" + "=" * 60)
    print("TEST 5: Invalid transitions raise errors")
    print("=" * 60)

    order5 = OrderStateMachine(order_id="ORD-005", total_quantity=100.0)
    try:
        order5.process_event(OrderEvent.BROKER_ACK)
        assert False, "Should have raised InvalidTransitionError"
    except InvalidTransitionError:
        print("BROKER_ACK from NEW raises InvalidTransitionError")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)
