"""
Order State Machine Implementation with Graphviz DOT Output

A Python implementation of a finite state machine following the architecture
pattern from FSM.hpp, implementing the order lifecycle state diagram.

This version generates Graphviz DOT files to visualize state transitions.
"""

from enum import Enum, auto
from typing import Callable, Optional, TextIO
from abc import ABC, abstractmethod
from pathlib import Path


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
    Base class for finite state machines with optional DOT output.

    Derived classes must:
    1. Define _build_transition_matrix() to return the state/event -> handler mapping
    2. Implement handler methods for each valid transition
    """

    # Override in subclass to define terminal states for DOT styling
    TERMINAL_STATES: frozenset = frozenset()

    def __init__(self, initial_state: Enum, dot_file: Optional[str] = None):
        self._current_state: Enum = initial_state
        self._initial_state: Enum = initial_state
        self._state_history: list[tuple[Enum, Enum, str]] = []  # (state, event, label)
        self._transition_matrix: dict[tuple[Enum, Enum], Callable] = {}

        # DOT output
        self._dot_file_path: Optional[str] = dot_file
        self._dot_file: Optional[TextIO] = None
        self._transition_count: int = 0

        self._build_transition_matrix()

        if dot_file:
            self._init_dot_file()

    @abstractmethod
    def _build_transition_matrix(self) -> None:
        """Populate self._transition_matrix with (state, event) -> handler mappings."""
        pass

    @abstractmethod
    def _error_handler(self, event: Enum, **kwargs) -> None:
        """Handle invalid state/event combinations."""
        pass

    def _get_dot_node_name(self) -> str:
        """Override to provide a name for the DOT graph."""
        return "FSM"

    def _init_dot_file(self) -> None:
        """Initialize the DOT file with header and node definitions."""
        self._dot_file = open(self._dot_file_path, 'w')
        name = self._get_dot_node_name()
        self._dot_file.write(f'digraph {name} {{\n')
        self._dot_file.write('    rankdir=TB;\n')
        self._dot_file.write('    node [shape=box, style="rounded,filled", fillcolor=lightblue];\n')
        self._dot_file.write('    edge [fontsize=10];\n\n')

        # Mark terminal states with different style
        if self.TERMINAL_STATES:
            self._dot_file.write('    // Terminal states (double border, different color)\n')
            for state in self.TERMINAL_STATES:
                self._dot_file.write(
                    f'    {state.name} [shape=doubleoctagon, fillcolor=lightcoral];\n'
                )
            self._dot_file.write('\n')

        # Mark initial state
        self._dot_file.write(f'    // Initial state\n')
        self._dot_file.write(f'    {self._initial_state.name} [fillcolor=lightgreen];\n\n')
        self._dot_file.write('    // Transitions\n')

    def _log_dot_transition(self, from_state: Enum, to_state: Enum,
                            event: Enum, label_extra: str = "") -> None:
        """Log a transition to the DOT file."""
        if not self._dot_file:
            return
        self._transition_count += 1
        label = f"{self._transition_count}: {event.name}"
        if label_extra:
            label += f"\\n{label_extra}"

        # Self-loops need special handling for visibility
        if from_state == to_state:
            self._dot_file.write(
                f'    {from_state.name} -> {to_state.name} '
                f'[label="{label}", color=blue, style=dashed];\n'
            )
        else:
            self._dot_file.write(
                f'    {from_state.name} -> {to_state.name} [label="{label}"];\n'
            )

    def finalize_dot(self) -> Optional[str]:
        """Finalize and close the DOT file. Returns the file path if written."""
        if self._dot_file:
            self._dot_file.write('}\n')
            self._dot_file.close()
            path = self._dot_file_path
            self._dot_file = None
            return path
        return None

    def process_event(self, event: Enum, **kwargs) -> None:
        """
        Process an event and execute the appropriate transition handler.
        This is the ONLY public interface for state transitions.
        """
        key = (self._current_state, event)
        handler = self._transition_matrix.get(key, self._error_handler)
        handler(event, **kwargs)

    @property
    def current_state(self) -> Enum:
        """Return the current state (read-only)."""
        return self._current_state

    def _set_current_state(self, state: Enum) -> None:
        """Internal method to set the current state."""
        self._current_state = state

    def _record_transition(self, from_state: Enum, to_state: Enum,
                           event: Enum, label_extra: str = "") -> None:
        """Record state transition in history and write to DOT file."""
        self._state_history.append((to_state, event, label_extra))
        self._log_dot_transition(from_state, to_state, event, label_extra)

    def is_terminal_state(self) -> bool:
        """Check if current state is a terminal state."""
        return self._current_state in self.TERMINAL_STATES

    def __del__(self):
        """Ensure DOT file is closed on deletion."""
        if self._dot_file:
            self.finalize_dot()


class OrderStateMachine(FSM):
    """
    Concrete FSM implementation for order lifecycle management.

    Usage:
        order = OrderStateMachine(order_id="ORD-001", total_quantity=100.0,
                                  dot_file="order_flow.dot")
        order.process_event(OrderEvent.SUBMIT)
        order.process_event(OrderEvent.EXECUTION, quantity=30.0)
        ...
        order.finalize_dot()  # Write and close the DOT file
    """

    TERMINAL_STATES = frozenset({
        OrderState.VALIDATION_FAILED,
        OrderState.REJECTED,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.EXPIRED,
    })

    def __init__(self, order_id: str, total_quantity: float,
                 dot_file: Optional[str] = None):
        """
        Initialize the order state machine.

        Args:
            order_id: Unique identifier for this order
            total_quantity: Total quantity to be filled
            dot_file: Optional path to write Graphviz DOT output
        """
        self._order_id = order_id
        self._total_quantity = total_quantity
        self._filled_quantity: float = 0.0
        self._order_live = False

        super().__init__(OrderState.NEW, dot_file)

    def _get_dot_node_name(self) -> str:
        """Provide order ID for DOT graph name."""
        # Sanitize order_id for DOT (remove special chars)
        safe_id = ''.join(c if c.isalnum() else '_' for c in self._order_id)
        return f"Order_{safe_id}"

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

    def _error_handler(self, event: OrderEvent, **kwargs) -> None:
        """Handle invalid state/event combinations."""
        raise InvalidTransitionError(
            f"Invalid transition: event {event.name} not allowed in state {self._current_state.name}"
        )

    # --- Transition Handlers ---

    def _on_submit(self, event: OrderEvent, **kwargs) -> None:
        """NEW -> PENDING_VALIDATION"""
        from_state = self._current_state
        self._set_current_state(OrderState.PENDING_VALIDATION)
        self._record_transition(from_state, self._current_state, event)

    def _on_validation_failed(self, event: OrderEvent, reason: str = "", **kwargs) -> None:
        """PENDING_VALIDATION -> VALIDATION_FAILED (terminal)"""
        from_state = self._current_state
        self._set_current_state(OrderState.VALIDATION_FAILED)
        label = reason if reason else ""
        self._record_transition(from_state, self._current_state, event, label)

    def _on_checks_passed(self, event: OrderEvent, **kwargs) -> None:
        """PENDING_VALIDATION -> PENDING_NEW"""
        from_state = self._current_state
        self._set_current_state(OrderState.PENDING_NEW)
        self._record_transition(from_state, self._current_state, event)

    def _on_broker_reject(self, event: OrderEvent, reason: str = "", **kwargs) -> None:
        """PENDING_NEW -> REJECTED (terminal)"""
        from_state = self._current_state
        self._set_current_state(OrderState.REJECTED)
        label = reason if reason else ""
        self._record_transition(from_state, self._current_state, event, label)

    def _on_broker_ack(self, event: OrderEvent, **kwargs) -> None:
        """PENDING_NEW -> ACCEPTED, order becomes live"""
        from_state = self._current_state
        self._set_current_state(OrderState.ACCEPTED)
        self._order_live = True
        self._record_transition(from_state, self._current_state, event, "LIVE")

    def _on_execution(self, event: OrderEvent, quantity: float = 0.0, **kwargs) -> None:
        """ACCEPTED -> PARTIALLY_FILLED, or PARTIALLY_FILLED -> PARTIALLY_FILLED"""
        from_state = self._current_state
        self._filled_quantity += quantity
        self._set_current_state(OrderState.PARTIALLY_FILLED)
        label = f"qty={quantity}\\nfilled={self._filled_quantity}/{self._total_quantity}"
        self._record_transition(from_state, self._current_state, event, label)

    def _on_full_execution(self, event: OrderEvent, quantity: float = 0.0, **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> FILLED (terminal)"""
        from_state = self._current_state
        self._filled_quantity += quantity
        self._set_current_state(OrderState.FILLED)
        self._order_live = False
        label = f"qty={quantity}\\nfilled={self._filled_quantity}/{self._total_quantity}"
        self._record_transition(from_state, self._current_state, event, label)

    def _on_cancel_ack(self, event: OrderEvent, **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> CANCELLED (terminal)"""
        from_state = self._current_state
        self._set_current_state(OrderState.CANCELLED)
        self._order_live = False
        label = f"filled={self._filled_quantity}" if self._filled_quantity > 0 else ""
        self._record_transition(from_state, self._current_state, event, label)

    def _on_cancel_rej(self, event: OrderEvent, reason: str = "", **kwargs) -> None:
        """ACCEPTED/PARTIALLY_FILLED -> same state (self-loop)"""
        from_state = self._current_state
        # State remains unchanged
        label = reason if reason else "rejected"
        self._record_transition(from_state, self._current_state, event, label)

    def _on_ttl_elapsed(self, event: OrderEvent, **kwargs) -> None:
        """PENDING_NEW/ACCEPTED/PARTIALLY_FILLED -> EXPIRED (terminal)"""
        from_state = self._current_state
        self._set_current_state(OrderState.EXPIRED)
        self._order_live = False
        label = f"filled={self._filled_quantity}" if self._filled_quantity > 0 else ""
        self._record_transition(from_state, self._current_state, event, label)

    # --- Read-only Public Properties ---

    @property
    def state_history(self) -> list[tuple[OrderState, OrderEvent, str]]:
        """Return a copy of the state transition history."""
        return self._state_history.copy()

    @property
    def order_id(self) -> str:
        return self._order_id

    @property
    def filled_quantity(self) -> float:
        return self._filled_quantity

    @property
    def total_quantity(self) -> float:
        return self._total_quantity

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


# --- Test Cases with Complex Order Flows ---

if __name__ == "__main__":
    import os

    # Create output directory for DOT files
    dot_dir = Path("dot_output")
    dot_dir.mkdir(exist_ok=True)

    print("=" * 70)
    print("GENERATING DOT FILES FOR COMPLEX ORDER FLOWS")
    print("=" * 70)

    # =========================================================================
    # FLOW 1: Institutional order with multiple partial fills and cancel attempts
    # =========================================================================
    print("\n[FLOW 1] Institutional order: multiple fills, failed cancels, then filled")

    order1 = OrderStateMachine(
        order_id="INST-001",
        total_quantity=10000.0,
        dot_file=str(dot_dir / "flow1_institutional_fill.dot")
    )

    # Standard submission flow
    order1.process_event(OrderEvent.SUBMIT)
    order1.process_event(OrderEvent.CHECKS_PASSED)
    order1.process_event(OrderEvent.BROKER_ACK)

    # Multiple partial executions with cancel attempts in between
    order1.process_event(OrderEvent.EXECUTION, quantity=1000.0)   # 10%
    order1.process_event(OrderEvent.EXECUTION, quantity=1500.0)   # 25%
    order1.process_event(OrderEvent.CANCEL_REJ, reason="market moving")  # Try to cancel
    order1.process_event(OrderEvent.EXECUTION, quantity=2000.0)   # 45%
    order1.process_event(OrderEvent.EXECUTION, quantity=500.0)    # 50%
    order1.process_event(OrderEvent.CANCEL_REJ, reason="order locked")   # Try again
    order1.process_event(OrderEvent.EXECUTION, quantity=1500.0)   # 65%
    order1.process_event(OrderEvent.EXECUTION, quantity=2000.0)   # 85%
    order1.process_event(OrderEvent.EXECUTION, quantity=1000.0)   # 95%
    order1.process_event(OrderEvent.FULL_EXECUTION, quantity=500.0)  # 100%

    assert order1.filled_quantity == 10000.0
    assert order1.is_terminal_state()
    print(f"    Final: {order1}")
    order1.finalize_dot()

    # =========================================================================
    # FLOW 2: Partial fill then successful cancel (common retail scenario)
    # =========================================================================
    print("\n[FLOW 2] Retail order: partial fill then cancelled")

    order2 = OrderStateMachine(
        order_id="RETAIL-002",
        total_quantity=500.0,
        dot_file=str(dot_dir / "flow2_partial_cancel.dot")
    )

    order2.process_event(OrderEvent.SUBMIT)
    order2.process_event(OrderEvent.CHECKS_PASSED)
    order2.process_event(OrderEvent.BROKER_ACK)
    order2.process_event(OrderEvent.EXECUTION, quantity=50.0)    # 10%
    order2.process_event(OrderEvent.EXECUTION, quantity=100.0)   # 30%
    order2.process_event(OrderEvent.CANCEL_REJ, reason="pending execution")  # Rejected
    order2.process_event(OrderEvent.EXECUTION, quantity=25.0)    # 35%
    order2.process_event(OrderEvent.CANCEL_ACK)  # Finally cancelled

    assert order2.filled_quantity == 175.0
    assert order2.current_state == OrderState.CANCELLED
    print(f"    Final: {order2}")
    order2.finalize_dot()

    # =========================================================================
    # FLOW 3: Order expires after partial fills (GTC order at end of day)
    # =========================================================================
    print("\n[FLOW 3] GTC order: partial fills then TTL expiration")

    order3 = OrderStateMachine(
        order_id="GTC-003",
        total_quantity=1000.0,
        dot_file=str(dot_dir / "flow3_ttl_expiration.dot")
    )

    order3.process_event(OrderEvent.SUBMIT)
    order3.process_event(OrderEvent.CHECKS_PASSED)
    order3.process_event(OrderEvent.BROKER_ACK)
    order3.process_event(OrderEvent.EXECUTION, quantity=100.0)
    order3.process_event(OrderEvent.EXECUTION, quantity=150.0)
    order3.process_event(OrderEvent.EXECUTION, quantity=200.0)
    order3.process_event(OrderEvent.CANCEL_REJ, reason="illiquid market")
    order3.process_event(OrderEvent.EXECUTION, quantity=50.0)
    order3.process_event(OrderEvent.TTL_ELAPSED)  # End of trading day

    assert order3.filled_quantity == 500.0
    assert order3.current_state == OrderState.EXPIRED
    print(f"    Final: {order3}")
    order3.finalize_dot()

    # =========================================================================
    # FLOW 4: Immediate full execution (market order, high liquidity)
    # =========================================================================
    print("\n[FLOW 4] Market order: immediate full execution")

    order4 = OrderStateMachine(
        order_id="MKT-004",
        total_quantity=100.0,
        dot_file=str(dot_dir / "flow4_immediate_fill.dot")
    )

    order4.process_event(OrderEvent.SUBMIT)
    order4.process_event(OrderEvent.CHECKS_PASSED)
    order4.process_event(OrderEvent.BROKER_ACK)
    order4.process_event(OrderEvent.FULL_EXECUTION, quantity=100.0)

    assert order4.filled_quantity == 100.0
    assert order4.current_state == OrderState.FILLED
    print(f"    Final: {order4}")
    order4.finalize_dot()

    # =========================================================================
    # FLOW 5: Validation failure (bad order parameters)
    # =========================================================================
    print("\n[FLOW 5] Invalid order: validation failure")

    order5 = OrderStateMachine(
        order_id="BAD-005",
        total_quantity=100.0,
        dot_file=str(dot_dir / "flow5_validation_fail.dot")
    )

    order5.process_event(OrderEvent.SUBMIT)
    order5.process_event(OrderEvent.CHECKS_FAILED, reason="invalid symbol")

    assert order5.current_state == OrderState.VALIDATION_FAILED
    print(f"    Final: {order5}")
    order5.finalize_dot()

    # =========================================================================
    # FLOW 6: Broker rejection (credit limit, position limit, etc.)
    # =========================================================================
    print("\n[FLOW 6] Broker rejection: credit limit exceeded")

    order6 = OrderStateMachine(
        order_id="REJ-006",
        total_quantity=1000000.0,
        dot_file=str(dot_dir / "flow6_broker_reject.dot")
    )

    order6.process_event(OrderEvent.SUBMIT)
    order6.process_event(OrderEvent.CHECKS_PASSED)
    order6.process_event(OrderEvent.BROKER_REJECT, reason="credit limit")

    assert order6.current_state == OrderState.REJECTED
    print(f"    Final: {order6}")
    order6.finalize_dot()

    # =========================================================================
    # FLOW 7: TTL expires before any fills (no liquidity)
    # =========================================================================
    print("\n[FLOW 7] No liquidity: TTL expires from ACCEPTED state")

    order7 = OrderStateMachine(
        order_id="ILLIQ-007",
        total_quantity=100.0,
        dot_file=str(dot_dir / "flow7_no_fills_expired.dot")
    )

    order7.process_event(OrderEvent.SUBMIT)
    order7.process_event(OrderEvent.CHECKS_PASSED)
    order7.process_event(OrderEvent.BROKER_ACK)
    order7.process_event(OrderEvent.TTL_ELAPSED)

    assert order7.filled_quantity == 0.0
    assert order7.current_state == OrderState.EXPIRED
    print(f"    Final: {order7}")
    order7.finalize_dot()

    # =========================================================================
    # FLOW 8: Aggressive HFT-style order with many small fills
    # =========================================================================
    print("\n[FLOW 8] HFT order: many small rapid executions")

    order8 = OrderStateMachine(
        order_id="HFT-008",
        total_quantity=1000.0,
        dot_file=str(dot_dir / "flow8_hft_many_fills.dot")
    )

    order8.process_event(OrderEvent.SUBMIT)
    order8.process_event(OrderEvent.CHECKS_PASSED)
    order8.process_event(OrderEvent.BROKER_ACK)

    # Simulate many small fills (typical HFT pattern)
    for i in range(9):
        order8.process_event(OrderEvent.EXECUTION, quantity=100.0)
    order8.process_event(OrderEvent.FULL_EXECUTION, quantity=100.0)

    assert order8.filled_quantity == 1000.0
    assert order8.current_state == OrderState.FILLED
    print(f"    Final: {order8}")
    order8.finalize_dot()

    # =========================================================================
    # FLOW 9: Cancel immediately after acceptance (changed mind)
    # =========================================================================
    print("\n[FLOW 9] Cancel immediately after acceptance")

    order9 = OrderStateMachine(
        order_id="CANCEL-009",
        total_quantity=500.0,
        dot_file=str(dot_dir / "flow9_immediate_cancel.dot")
    )

    order9.process_event(OrderEvent.SUBMIT)
    order9.process_event(OrderEvent.CHECKS_PASSED)
    order9.process_event(OrderEvent.BROKER_ACK)
    order9.process_event(OrderEvent.CANCEL_ACK)

    assert order9.filled_quantity == 0.0
    assert order9.current_state == OrderState.CANCELLED
    print(f"    Final: {order9}")
    order9.finalize_dot()

    # =========================================================================
    # FLOW 10: Complex flow - multiple cancel rejects, partial fills, then expire
    # =========================================================================
    print("\n[FLOW 10] Complex: fills, multiple cancel rejects, then TTL expires")

    order10 = OrderStateMachine(
        order_id="COMPLEX-010",
        total_quantity=5000.0,
        dot_file=str(dot_dir / "flow10_complex_expire.dot")
    )

    order10.process_event(OrderEvent.SUBMIT)
    order10.process_event(OrderEvent.CHECKS_PASSED)
    order10.process_event(OrderEvent.BROKER_ACK)
    order10.process_event(OrderEvent.EXECUTION, quantity=500.0)
    order10.process_event(OrderEvent.CANCEL_REJ, reason="in flight")
    order10.process_event(OrderEvent.EXECUTION, quantity=750.0)
    order10.process_event(OrderEvent.CANCEL_REJ, reason="partial match")
    order10.process_event(OrderEvent.EXECUTION, quantity=250.0)
    order10.process_event(OrderEvent.CANCEL_REJ, reason="queue position")
    order10.process_event(OrderEvent.EXECUTION, quantity=500.0)
    order10.process_event(OrderEvent.TTL_ELAPSED)

    assert order10.filled_quantity == 2000.0
    assert order10.current_state == OrderState.EXPIRED
    print(f"    Final: {order10}")
    order10.finalize_dot()

    # =========================================================================
    print("\n" + "=" * 70)
    print("ALL DOT FILES GENERATED!")
    print("=" * 70)

    # List generated files
    print(f"\nGenerated files in '{dot_dir}/':")
    for f in sorted(dot_dir.glob("*.dot")):
        print(f"    {f.name}")

    print("\nTo visualize, run:")
    print(f"    cd {dot_dir}")
    print("    for f in *.dot; do dot -Tpng \"$f\" -o \"${f%.dot}.png\"; done")
    print("\nOr for a single file:")
    print(f"    dot -Tpng {dot_dir}/flow1_institutional_fill.dot -o flow1.png")
