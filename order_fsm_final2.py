"""
Order State Machine Implementation

A Python implementation of a finite state machine following the architecture
pattern from FSM.hpp, implementing the order lifecycle state diagram.
"""

from enum import Enum, auto
from typing import Callable, Optional, Any
from abc import ABC, abstractmethod

"""
   All (states, events) have been defined according to the diagram in Order_State_Diagram.pdf. States
   are represented by nodes events by labels on the edges. Your job is fill in the missing logic so
   that the state machines captures the desired flow.
"""


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

    In Python, we use ABC and abstract methods.

    Derived classes must:
    1. Define _build_transition_matrix() to return the state/event -> handler mapping
    2. Implement handler methods for each valid transition
    """

    def __init__(self, initial_state: Enum):
        self._current_state: Enum = initial_state
        self._state_history: list[tuple[OrderState, OrderEvent]] = []
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
        Mirrors FSM::process_event() in the C++ implementation.

        Additional arguments are passed through to the handler, allowing
        handlers to receive event-specific data (e.g., execution quantity).
        """
        key = (self._current_state, event)
        handler = self._transition_matrix.get(key, self._error_handler)
        handler(event, *args, **kwargs)

    def get_current_state(self) -> Enum:
        """Return the current state."""
        return self._current_state

    def record_transition(self, new_state: OrderState, event: OrderEvent) -> None:
        """Record state transition in history."""
        self._state_history.append((new_state, event))

    def set_current_state(self, state: Enum) -> None:
        """Set the current state."""
        self._current_state = state

    def assert_current_state(self, state: Enum) -> None:
        """Assert that the FSM is in the expected state."""
        assert self._current_state == state, \
            f"Expected state {state}, but current state is {self._current_state}"

    def set_state_and_transition(self, state: Enum, event: Enum, *args, **kwargs) -> None:
        """Set state, record state, and process an event."""
        self.set_current_state(state)
        self.record_transition(state, event)
        self.process_event(event, *args, **kwargs)

    def is_terminal_state(self) -> bool:
        """Check if current state is a terminal state. Override in subclass."""
        return False


class OrderStateMachine(FSM):
    """
    Concrete FSM implementation for order lifecycle management.

    Implements the state diagram with states:
    NEW -> PENDING_VALIDATION -> PENDING_NEW -> ACCEPTED -> PARTIALLY_FILLED -> FILLED
    etc.

    With terminal states: VALIDATION_FAILED, REJECTED, FILLED, CANCELLED, EXPIRED
    """

    # Terminal states where no further transitions are valid
    TERMINAL_STATES = frozenset({
        OrderState.VALIDATION_FAILED,
        OrderState.REJECTED,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
    })

    def __init__(self, order_id: Optional[str] = None):
        self._order_id = order_id
        self._filled_quantity: float = 0.0
        self._total_quantity: float = 0.0
        self._order_expired = False
        self._order_live = False

        super().__init__(OrderState.NEW)

    def _build_transition_matrix(self) -> None:
        """
        Build the transition matrix mapping (state, event) -> handler.

        This mirrors the 2D matrix M in the C++ Session class:

                          EVENTS
              SUBMIT  CHK_FAIL  CHK_PASS  BRK_REJ  BRK_ACK  EXEC  FULL_EXEC  CAN_ACK  CAN_REJ  TTL
            +------+--------+--------+-------+-------+-----+---------+--------+--------+-----+
        NEW |  sb  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        P_V |  er  |   vf   |   cp   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        V_F |  er  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        P_N |  er  |   er   |   er   |  rj   |  ac   | er  |   er    |   er   |   er   | te  |
        REJ |  er  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        ACC |  er  |   er   |   er   |  er   |  er   | ex  |   fe    |   ca   |   cr   | te  |
        P_F |  er  |   er   |   er   |  er   |  er   | ex  |   fe    |   ca   |   cr   | te  |
        FIL |  er  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        CAN |  er  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        EXP |  er  |   er   |   er   |  er   |  er   | er  |   er    |   er   |   er   | er  |
        """
        # Define shorthand aliases for handlers (mirrors C++ #define macros)
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

        # Build matrix as list of lists, indexed by [state][event]
        # This directly mirrors the C++ M matrix structure
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

        # Convert 2D matrix to dictionary for O(1) lookup
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

    def _on_execution(self, event: OrderEvent, quantity: float = 0.0, *args, **kwargs) -> None:
        """ACCEPTED -> PARTIALLY_FILLED, or PARTIALLY_FILLED -> PARTIALLY_FILLED (self-loop)"""
        self._filled_quantity += quantity
        self.set_current_state(OrderState.PARTIALLY_FILLED)
        self.record_transition(self._current_state, event)

    def _on_full_execution(self, event: OrderEvent, quantity: float = 0.0, *args, **kwargs) -> None:
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
        # State remains unchanged (self-loop)
        self.record_transition(self._current_state, event)

    def _on_ttl_elapsed(self, event: OrderEvent, *args, **kwargs) -> None:
        """PENDING_NEW/ACCEPTED/PARTIALLY_FILLED -> EXPIRED (terminal), order no longer live"""
        self.set_current_state(OrderState.EXPIRED)
        self._order_live = False
        self._order_expired = True
        self.record_transition(self._current_state, event)

    # --- Public Interface ---

    def is_terminal_state(self) -> bool:
        """Check if the order is in a terminal state."""
        return self._current_state in self.TERMINAL_STATES

    def get_state_history(self) -> list[tuple[OrderState, OrderEvent]]:
        """Return the history of state transitions."""
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
    def is_live(self) -> bool:
        return self._order_live

    def quantity_filled(self) -> float:
        """Return the total quantity filled across all executions."""
        return self._filled_quantity

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

    order = OrderStateMachine(order_id="ORD-001")
    order.total_quantity = 100.0

    order.assert_current_state(OrderState.NEW)
    assert order.total_quantity == 100.0
    assert order.quantity_filled() == 0.0
    assert order.is_live == False
    assert order.is_terminal_state() == False
    print("✓ Initial state: NEW, not live, not terminal, filled=0")

    order.process_event(OrderEvent.SUBMIT)
    order.assert_current_state(OrderState.PENDING_VALIDATION)
    assert order.is_live == False
    print("✓ After SUBMIT: PENDING_VALIDATION")

    order.process_event(OrderEvent.CHECKS_PASSED)
    order.assert_current_state(OrderState.PENDING_NEW)
    assert order.is_live == False
    print("✓ After CHECKS_PASSED: PENDING_NEW")

    order.process_event(OrderEvent.BROKER_ACK)
    order.assert_current_state(OrderState.ACCEPTED)
    assert order.is_live == True
    print("✓ After BROKER_ACK: ACCEPTED, is_live=True")

    order.process_event(OrderEvent.EXECUTION, quantity=30.0)
    order.assert_current_state(OrderState.PARTIALLY_FILLED)
    assert order.is_live == True
    assert order.quantity_filled() == 30.0
    print("✓ After EXECUTION: PARTIALLY_FILLED, is_live=True, filled=30")

    order.process_event(OrderEvent.EXECUTION, quantity=25.0)
    order.assert_current_state(OrderState.PARTIALLY_FILLED)
    assert order.quantity_filled() == 55.0
    print("✓ After EXECUTION (self-loop): still PARTIALLY_FILLED, filled=55")

    order.process_event(OrderEvent.FULL_EXECUTION, quantity=45.0)
    order.assert_current_state(OrderState.FILLED)
    assert order.is_terminal_state() == True
    assert order.is_live == False
    assert order.quantity_filled() == 100.0
    print("✓ After FULL_EXECUTION: FILLED, terminal, is_live=False, filled=100")

    print("\n" + "=" * 60)
    print("TEST 2: Full execution from ACCEPTED")
    print("=" * 60)

    order2 = OrderStateMachine(order_id="ORD-002")
    order2.total_quantity = 50.0
    order2.process_event(OrderEvent.SUBMIT)
    order2.process_event(OrderEvent.CHECKS_PASSED)
    order2.process_event(OrderEvent.BROKER_ACK)
    order2.assert_current_state(OrderState.ACCEPTED)

    order2.process_event(OrderEvent.FULL_EXECUTION, quantity=50.0)
    order2.assert_current_state(OrderState.FILLED)
    assert order2.is_terminal_state() == True
    assert order2.quantity_filled() == 50.0
    print("✓ ACCEPTED -> FILLED via FULL_EXECUTION, filled=50")

    print("\n" + "=" * 60)
    print("TEST 3: Cancel flow (reject then ack)")
    print("=" * 60)

    order3 = OrderStateMachine(order_id="ORD-003")
    order3.total_quantity = 100.0
    order3.process_event(OrderEvent.SUBMIT)
    order3.process_event(OrderEvent.CHECKS_PASSED)
    order3.process_event(OrderEvent.BROKER_ACK)
    order3.assert_current_state(OrderState.ACCEPTED)
    assert order3.is_live == True

    order3.process_event(OrderEvent.CANCEL_REJ)
    order3.assert_current_state(OrderState.ACCEPTED)
    assert order3.is_live == True
    print("✓ After CANCEL_REJ: still ACCEPTED (self-loop)")

    order3.process_event(OrderEvent.CANCEL_ACK)
    order3.assert_current_state(OrderState.CANCELLED)
    assert order3.is_terminal_state() == True
    assert order3.is_live == False
    assert order3.quantity_filled() == 0.0
    print("✓ After CANCEL_ACK: CANCELLED, terminal, is_live=False, filled=0")

    print("\n" + "=" * 60)
    print("TEST 4: Cancel from PARTIALLY_FILLED")
    print("=" * 60)

    order4 = OrderStateMachine(order_id="ORD-004")
    order4.total_quantity = 100.0
    order4.process_event(OrderEvent.SUBMIT)
    order4.process_event(OrderEvent.CHECKS_PASSED)
    order4.process_event(OrderEvent.BROKER_ACK)
    order4.process_event(OrderEvent.EXECUTION, quantity=40.0)
    order4.assert_current_state(OrderState.PARTIALLY_FILLED)
    assert order4.quantity_filled() == 40.0
    print("✓ After EXECUTION: PARTIALLY_FILLED, filled=40")

    order4.process_event(OrderEvent.CANCEL_REJ)
    order4.assert_current_state(OrderState.PARTIALLY_FILLED)
    assert order4.quantity_filled() == 40.0
    print("✓ After CANCEL_REJ: still PARTIALLY_FILLED (self-loop), filled=40")

    order4.process_event(OrderEvent.CANCEL_ACK)
    order4.assert_current_state(OrderState.CANCELLED)
    assert order4.quantity_filled() == 40.0
    print("✓ After CANCEL_ACK: CANCELLED, filled=40 preserved")

    print("\n" + "=" * 60)
    print("TEST 5: TTL expiration from various states")
    print("=" * 60)

    # TTL from PENDING_NEW
    order5a = OrderStateMachine(order_id="ORD-005a")
    order5a.process_event(OrderEvent.SUBMIT)
    order5a.process_event(OrderEvent.CHECKS_PASSED)
    order5a.assert_current_state(OrderState.PENDING_NEW)
    order5a.process_event(OrderEvent.TTL_ELAPSED)
    order5a.assert_current_state(OrderState.EXPIRED)
    assert order5a.is_terminal_state() == True
    print("✓ PENDING_NEW -> EXPIRED via TTL_ELAPSED")

    # TTL from ACCEPTED
    order5b = OrderStateMachine(order_id="ORD-005b")
    order5b.process_event(OrderEvent.SUBMIT)
    order5b.process_event(OrderEvent.CHECKS_PASSED)
    order5b.process_event(OrderEvent.BROKER_ACK)
    order5b.assert_current_state(OrderState.ACCEPTED)
    order5b.process_event(OrderEvent.TTL_ELAPSED)
    order5b.assert_current_state(OrderState.EXPIRED)
    print("✓ ACCEPTED -> EXPIRED via TTL_ELAPSED")

    # TTL from PARTIALLY_FILLED
    order5c = OrderStateMachine(order_id="ORD-005c")
    order5c.total_quantity = 100.0
    order5c.process_event(OrderEvent.SUBMIT)
    order5c.process_event(OrderEvent.CHECKS_PASSED)
    order5c.process_event(OrderEvent.BROKER_ACK)
    order5c.process_event(OrderEvent.EXECUTION, quantity=60.0)
    order5c.assert_current_state(OrderState.PARTIALLY_FILLED)
    assert order5c.quantity_filled() == 60.0
    order5c.process_event(OrderEvent.TTL_ELAPSED)
    order5c.assert_current_state(OrderState.EXPIRED)
    assert order5c.quantity_filled() == 60.0
    print("✓ PARTIALLY_FILLED -> EXPIRED via TTL_ELAPSED, filled=60 preserved")

    print("\n" + "=" * 60)
    print("TEST 6: Validation failure path")
    print("=" * 60)

    order6 = OrderStateMachine(order_id="ORD-006")
    order6.process_event(OrderEvent.SUBMIT)
    order6.assert_current_state(OrderState.PENDING_VALIDATION)

    order6.process_event(OrderEvent.CHECKS_FAILED)
    order6.assert_current_state(OrderState.VALIDATION_FAILED)
    assert order6.is_terminal_state() == True
    assert order6.is_live == False
    print("✓ PENDING_VALIDATION -> VALIDATION_FAILED")

    print("\n" + "=" * 60)
    print("TEST 7: Broker rejection path")
    print("=" * 60)

    order7 = OrderStateMachine(order_id="ORD-007")
    order7.process_event(OrderEvent.SUBMIT)
    order7.process_event(OrderEvent.CHECKS_PASSED)
    order7.assert_current_state(OrderState.PENDING_NEW)

    order7.process_event(OrderEvent.BROKER_REJECT)
    order7.assert_current_state(OrderState.REJECTED)
    assert order7.is_terminal_state() == True
    assert order7.is_live == False
    print("✓ PENDING_NEW -> REJECTED")

    print("\n" + "=" * 60)
    print("TEST 8: Invalid transitions raise errors")
    print("=" * 60)

    order8 = OrderStateMachine(order_id="ORD-008")
    try:
        order8.process_event(OrderEvent.BROKER_ACK)  # Can't ack from NEW
        assert False, "Should have raised InvalidTransitionError"
    except InvalidTransitionError:
        print("✓ BROKER_ACK from NEW raises InvalidTransitionError")

    order8.process_event(OrderEvent.SUBMIT)
    order8.process_event(OrderEvent.CHECKS_PASSED)
    order8.process_event(OrderEvent.BROKER_ACK)
    order8.process_event(OrderEvent.FULL_EXECUTION)
    order8.assert_current_state(OrderState.FILLED)

    try:
        order8.process_event(OrderEvent.CANCEL_ACK)  # Can't cancel from FILLED
        assert False, "Should have raised InvalidTransitionError"
    except InvalidTransitionError:
        print("✓ CANCEL_ACK from FILLED raises InvalidTransitionError")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)
