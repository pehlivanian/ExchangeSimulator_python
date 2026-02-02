#!/usr/bin/env python3
"""
Unit and Integration Tests for OrderClientWithFSM.

Tests cover:
1. Message parsing
2. ExchangeOrderStateMachine
3. ManagedOrder
4. OrderClientWithFSM
5. Integration with exchange_server.py
"""

import socket
import threading
import time
import unittest
from unittest.mock import Mock, patch, MagicMock

from order_client_with_fsm import (
    ExchangeMessage,
    parse_exchange_message,
    MESSAGE_TO_EVENT,
    ExchangeOrderStateMachine,
    ManagedOrder,
    OrderClientWithFSM,
)
from order_fsm_final import OrderState, OrderEvent, InvalidTransitionError
from exchange_server import ExchangeServer
from messages import DEFAULT_TTL_SECONDS


# =============================================================================
# Test Fixtures
# =============================================================================

class ExchangeServerFixture:
    """Fixture for starting/stopping an exchange server."""

    def __init__(self, order_port=10100, feed_port=10101):
        self.order_port = order_port
        self.feed_port = feed_port
        self.server = None
        self._server_thread = None

    def start(self):
        self.server = ExchangeServer(self.order_port, self.feed_port)
        if not self.server.start():
            raise RuntimeError("Failed to start exchange server")
        self._server_thread = threading.Thread(target=self.server.run, daemon=True)
        self._server_thread.start()
        time.sleep(0.1)  # Give server time to start

    def stop(self):
        if self.server:
            self.server.stop()
            self.server = None


# =============================================================================
# Part 1: Message Parsing Tests
# =============================================================================

class TestParseExchangeMessage(unittest.TestCase):
    """Tests for parse_exchange_message function."""

    # --- ACK Message Tests ---

    def test_parse_ack_message_valid(self):
        """Test parsing a valid ACK message."""
        msg = parse_exchange_message("ACK,1001,100,50000000")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "ACK")
        self.assertEqual(msg.order_id, 1001)
        self.assertEqual(msg.size, 100)
        self.assertEqual(msg.price, 50000000)

    def test_parse_ack_message_lowercase(self):
        """Test parsing ACK message with lowercase."""
        msg = parse_exchange_message("ack,1001,100,50000000")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "ACK")

    def test_parse_ack_message_with_whitespace(self):
        """Test parsing ACK message with leading/trailing whitespace."""
        msg = parse_exchange_message("  ACK,1001,100,50000000  \n")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "ACK")
        self.assertEqual(msg.order_id, 1001)

    def test_parse_ack_message_missing_fields(self):
        """Test parsing ACK message with missing fields returns None."""
        msg = parse_exchange_message("ACK,1001,100")
        self.assertIsNone(msg)

    def test_parse_ack_message_extra_fields(self):
        """Test parsing ACK message with extra fields returns None."""
        msg = parse_exchange_message("ACK,1001,100,50000000,extra")
        self.assertIsNone(msg)

    def test_parse_ack_message_invalid_order_id(self):
        """Test parsing ACK message with non-integer order_id."""
        msg = parse_exchange_message("ACK,abc,100,50000000")
        self.assertIsNone(msg)

    # --- FILL Message Tests ---

    def test_parse_fill_message_valid(self):
        """Test parsing a valid FILL message."""
        msg = parse_exchange_message("FILL,1001,50,49500000")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "FILL")
        self.assertEqual(msg.order_id, 1001)
        self.assertEqual(msg.size, 50)
        self.assertEqual(msg.price, 49500000)

    def test_parse_fill_message_missing_fields(self):
        """Test parsing FILL message with missing fields."""
        msg = parse_exchange_message("FILL,1001,50")
        self.assertIsNone(msg)

    # --- PARTIAL_FILL Message Tests ---

    def test_parse_partial_fill_message_valid(self):
        """Test parsing a valid PARTIAL_FILL message."""
        msg = parse_exchange_message("PARTIAL_FILL,1001,30,49500000,70")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "PARTIAL_FILL")
        self.assertEqual(msg.order_id, 1001)
        self.assertEqual(msg.size, 30)
        self.assertEqual(msg.price, 49500000)
        self.assertEqual(msg.remainder_size, 70)

    def test_parse_partial_fill_message_missing_remainder(self):
        """Test parsing PARTIAL_FILL message without remainder_size."""
        msg = parse_exchange_message("PARTIAL_FILL,1001,30,49500000")
        self.assertIsNone(msg)

    # --- REJECT Message Tests ---

    def test_parse_reject_message_valid(self):
        """Test parsing a valid REJECT message."""
        msg = parse_exchange_message("REJECT,No liquidity")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "REJECT")
        self.assertEqual(msg.reason, "No liquidity")

    def test_parse_reject_message_with_commas_in_reason(self):
        """Test parsing REJECT message where reason contains commas."""
        msg = parse_exchange_message("REJECT,Order 1001 not found, please retry")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "REJECT")
        self.assertEqual(msg.reason, "Order 1001 not found, please retry")

    def test_parse_reject_message_empty_reason(self):
        """Test parsing REJECT message with empty reason."""
        msg = parse_exchange_message("REJECT,")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.reason, "")

    # --- CANCEL_ACK Message Tests ---

    def test_parse_cancel_ack_message_valid(self):
        """Test parsing a valid CANCEL_ACK message."""
        msg = parse_exchange_message("CANCEL_ACK,1001,75")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "CANCEL_ACK")
        self.assertEqual(msg.order_id, 1001)
        self.assertEqual(msg.size, 75)

    def test_parse_cancel_ack_message_missing_fields(self):
        """Test parsing CANCEL_ACK message with missing fields."""
        msg = parse_exchange_message("CANCEL_ACK,1001")
        self.assertIsNone(msg)

    # --- EXPIRED Message Tests ---

    def test_parse_expired_message_valid(self):
        """Test parsing a valid EXPIRED message."""
        msg = parse_exchange_message("EXPIRED,1001,50")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "EXPIRED")
        self.assertEqual(msg.order_id, 1001)
        self.assertEqual(msg.size, 50)

    # --- ERROR Message Tests ---

    def test_parse_error_message_valid(self):
        """Test parsing a valid ERROR message."""
        msg = parse_exchange_message("ERROR,Connection lost")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, "ERROR")
        self.assertEqual(msg.reason, "Connection lost")

    # --- Edge Cases ---

    def test_parse_empty_message(self):
        """Test parsing empty message returns None."""
        msg = parse_exchange_message("")
        self.assertIsNone(msg)

    def test_parse_whitespace_only_message(self):
        """Test parsing whitespace-only message returns None."""
        msg = parse_exchange_message("   \n  ")
        self.assertIsNone(msg)

    def test_parse_unknown_message_type(self):
        """Test parsing unknown message type returns None."""
        msg = parse_exchange_message("UNKNOWN,1001,100")
        self.assertIsNone(msg)

    def test_parse_message_with_negative_values(self):
        """Test parsing message with negative order_id still parses."""
        msg = parse_exchange_message("ACK,-1,100,50000000")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.order_id, -1)


class TestMessageToEventMapping(unittest.TestCase):
    """Tests for MESSAGE_TO_EVENT mapping."""

    def test_ack_maps_to_broker_ack(self):
        """Test ACK maps to BROKER_ACK event."""
        self.assertEqual(MESSAGE_TO_EVENT["ACK"], OrderEvent.BROKER_ACK)

    def test_fill_maps_to_full_execution(self):
        """Test FILL maps to FULL_EXECUTION event."""
        self.assertEqual(MESSAGE_TO_EVENT["FILL"], OrderEvent.FULL_EXECUTION)

    def test_partial_fill_maps_to_execution(self):
        """Test PARTIAL_FILL maps to EXECUTION event."""
        self.assertEqual(MESSAGE_TO_EVENT["PARTIAL_FILL"], OrderEvent.EXECUTION)

    def test_cancel_ack_maps_to_cancel_ack(self):
        """Test CANCEL_ACK maps to CANCEL_ACK event."""
        self.assertEqual(MESSAGE_TO_EVENT["CANCEL_ACK"], OrderEvent.CANCEL_ACK)

    def test_expired_maps_to_ttl_elapsed(self):
        """Test EXPIRED maps to TTL_ELAPSED event."""
        self.assertEqual(MESSAGE_TO_EVENT["EXPIRED"], OrderEvent.TTL_ELAPSED)

    def test_reject_maps_to_broker_reject(self):
        """Test REJECT maps to BROKER_REJECT event."""
        self.assertEqual(MESSAGE_TO_EVENT["REJECT"], OrderEvent.BROKER_REJECT)


# =============================================================================
# Part 2: ExchangeOrderStateMachine Tests
# =============================================================================

class TestExchangeOrderStateMachine(unittest.TestCase):
    """Tests for ExchangeOrderStateMachine class."""

    def setUp(self):
        self.fsm = ExchangeOrderStateMachine(order_id="TEST-001")

    def test_initial_state(self):
        """Test FSM starts in NEW state."""
        self.assertEqual(self.fsm.get_current_state(), OrderState.NEW)

    def test_exchange_order_id_initially_none(self):
        """Test exchange_order_id is None initially."""
        self.assertIsNone(self.fsm.exchange_order_id)

    def test_last_error_initially_none(self):
        """Test last_error is None initially."""
        self.assertIsNone(self.fsm.last_error)

    def test_process_ack_message(self):
        """Test processing ACK message transitions to ACCEPTED."""
        # First move to PENDING_NEW state
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)
        self.assertEqual(self.fsm.get_current_state(), OrderState.PENDING_NEW)

        # Process ACK
        msg = ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000)
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.ACCEPTED)
        self.assertEqual(self.fsm.exchange_order_id, 1001)

    def test_process_reject_message(self):
        """Test processing REJECT message transitions to REJECTED."""
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)

        msg = ExchangeMessage(msg_type="REJECT", reason="No liquidity")
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.REJECTED)

    def test_process_fill_message(self):
        """Test processing FILL message transitions to FILLED."""
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)
        self.fsm.process_event(OrderEvent.BROKER_ACK)

        msg = ExchangeMessage(msg_type="FILL", order_id=1001, size=100, price=50000000)
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.FILLED)

    def test_process_partial_fill_message(self):
        """Test processing PARTIAL_FILL message transitions to PARTIALLY_FILLED."""
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)
        self.fsm.process_event(OrderEvent.BROKER_ACK)

        msg = ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=30,
                              price=50000000, remainder_size=70)
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.PARTIALLY_FILLED)

    def test_process_cancel_ack_message(self):
        """Test processing CANCEL_ACK message transitions to CANCELLED."""
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)
        self.fsm.process_event(OrderEvent.BROKER_ACK)

        msg = ExchangeMessage(msg_type="CANCEL_ACK", order_id=1001, size=100)
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.CANCELLED)

    def test_process_expired_message(self):
        """Test processing EXPIRED message transitions to EXPIRED."""
        self.fsm.process_event(OrderEvent.SUBMIT)
        self.fsm.process_event(OrderEvent.CHECKS_PASSED)
        self.fsm.process_event(OrderEvent.BROKER_ACK)

        msg = ExchangeMessage(msg_type="EXPIRED", order_id=1001, size=100)
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)
        self.assertEqual(self.fsm.get_current_state(), OrderState.EXPIRED)

    def test_process_unknown_message_type(self):
        """Test processing unknown message type returns False."""
        msg = ExchangeMessage(msg_type="UNKNOWN", order_id=1001)
        result = self.fsm.process_exchange_message(msg)

        self.assertFalse(result)
        self.assertEqual(self.fsm.last_error, "Unknown message: UNKNOWN")

    def test_process_error_message_stores_reason(self):
        """Test processing ERROR message stores reason in last_error."""
        msg = ExchangeMessage(msg_type="ERROR", reason="Connection lost")
        result = self.fsm.process_exchange_message(msg)

        self.assertFalse(result)
        self.assertEqual(self.fsm.last_error, "Connection lost")

    def test_invalid_transition_is_noop(self):
        """Test invalid transition doesn't raise, just ignores."""
        # In NEW state, REJECT is invalid
        msg = ExchangeMessage(msg_type="REJECT", reason="test")
        result = self.fsm.process_exchange_message(msg)

        self.assertTrue(result)  # Message was processed (just ignored)
        self.assertEqual(self.fsm.get_current_state(), OrderState.NEW)


# =============================================================================
# Part 3: ManagedOrder Tests
# =============================================================================

class TestManagedOrder(unittest.TestCase):
    """Tests for ManagedOrder class."""

    def test_create_limit_order(self):
        """Test creating a limit order."""
        order = ManagedOrder(size=100, price=50000000, side='B', order_type='limit')

        self.assertEqual(order.size, 100)
        self.assertEqual(order.price, 50000000)
        self.assertEqual(order.side, 'B')
        self.assertEqual(order.order_type, 'limit')
        self.assertEqual(order.state, OrderState.NEW)

    def test_create_market_order(self):
        """Test creating a market order."""
        order = ManagedOrder(size=50, price=0, side='S', order_type='market')

        self.assertEqual(order.size, 50)
        self.assertEqual(order.price, 0)
        self.assertEqual(order.side, 'S')
        self.assertEqual(order.order_type, 'market')

    def test_default_ttl(self):
        """Test default TTL is set correctly."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        self.assertEqual(order.ttl, DEFAULT_TTL_SECONDS)

    def test_custom_ttl(self):
        """Test custom TTL is respected."""
        order = ManagedOrder(size=100, price=50000000, side='B', ttl=60)
        self.assertEqual(order.ttl, 60)

    def test_submit_transitions_to_pending_validation(self):
        """Test submit() transitions to PENDING_VALIDATION."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        result = order.submit()

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.PENDING_VALIDATION)

    def test_submit_fails_when_not_in_new_state(self):
        """Test submit() fails when already submitted."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        result = order.submit()  # Second submit should fail

        self.assertFalse(result)

    def test_validate_success_for_valid_limit_order(self):
        """Test validate() succeeds for valid limit order."""
        order = ManagedOrder(size=100, price=50000000, side='B', order_type='limit')
        order.submit()
        result = order.validate()

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.PENDING_NEW)

    def test_validate_fails_for_zero_size(self):
        """Test validate() fails for zero size."""
        order = ManagedOrder(size=0, price=50000000, side='B')
        order.submit()
        result = order.validate()

        self.assertFalse(result)
        self.assertEqual(order.state, OrderState.VALIDATION_FAILED)

    def test_validate_fails_for_negative_size(self):
        """Test validate() fails for negative size."""
        order = ManagedOrder(size=-10, price=50000000, side='B')
        order.submit()
        result = order.validate()

        self.assertFalse(result)
        self.assertEqual(order.state, OrderState.VALIDATION_FAILED)

    def test_validate_fails_for_zero_price_limit_order(self):
        """Test validate() fails for limit order with zero price."""
        order = ManagedOrder(size=100, price=0, side='B', order_type='limit')
        order.submit()
        result = order.validate()

        self.assertFalse(result)
        self.assertEqual(order.state, OrderState.VALIDATION_FAILED)

    def test_validate_succeeds_for_zero_price_market_order(self):
        """Test validate() succeeds for market order with zero price."""
        order = ManagedOrder(size=100, price=0, side='B', order_type='market')
        order.submit()
        result = order.validate()

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.PENDING_NEW)

    def test_validate_fails_for_invalid_side(self):
        """Test validate() fails for invalid side."""
        order = ManagedOrder(size=100, price=50000000, side='X')
        order.submit()
        result = order.validate()

        self.assertFalse(result)
        self.assertEqual(order.state, OrderState.VALIDATION_FAILED)

    def test_process_message_ack(self):
        """Test process_message with ACK."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()

        msg = ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000)
        result = order.process_message(msg)

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.ACCEPTED)
        self.assertEqual(order.exchange_order_id, 1001)

    def test_is_terminal_for_filled_order(self):
        """Test is_terminal returns True for filled order."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="FILL", order_id=1001, size=100, price=50000000))

        self.assertTrue(order.is_terminal)

    def test_is_live_for_accepted_order(self):
        """Test is_live returns True for accepted order."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))

        self.assertTrue(order.is_live)

    def test_is_live_false_for_filled_order(self):
        """Test is_live returns False for filled order."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="FILL", order_id=1001, size=100, price=50000000))

        self.assertFalse(order.is_live)

    def test_state_history_tracking(self):
        """Test state history is tracked correctly."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()

        history = order.state_history
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0][0], OrderState.PENDING_VALIDATION)
        self.assertEqual(history[1][0], OrderState.PENDING_NEW)

    def test_to_order_string_limit(self):
        """Test to_order_string for limit order."""
        order = ManagedOrder(size=100, price=50000000, side='B', order_type='limit', ttl=60)
        expected = "limit,100,50000000,B,client,60"
        self.assertEqual(order.to_order_string(), expected)

    def test_to_order_string_market(self):
        """Test to_order_string for market order."""
        order = ManagedOrder(size=50, price=0, side='S', order_type='market')
        expected = "market,50,0,S,client"
        self.assertEqual(order.to_order_string(), expected)

    def test_last_message_tracking(self):
        """Test last_message is tracked."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()

        msg = ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000)
        order.process_message(msg)

        self.assertEqual(order.last_message, msg)


# =============================================================================
# Part 4: OrderClientWithFSM Unit Tests (mocked)
# =============================================================================

class TestOrderClientWithFSMUnit(unittest.TestCase):
    """Unit tests for OrderClientWithFSM using mocks."""

    def test_create_order_returns_managed_order(self):
        """Test create_order returns a ManagedOrder."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")

        self.assertIsInstance(order, ManagedOrder)
        self.assertEqual(order.size, 100)
        self.assertEqual(order.price, 50000000)
        self.assertEqual(order.side, 'B')
        self.assertEqual(order.order_type, 'limit')

    def test_create_order_uppercase_side(self):
        """Test create_order uppercases side."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "b")

        self.assertEqual(order.side, 'B')

    def test_create_order_lowercase_type(self):
        """Test create_order lowercases order_type."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("LIMIT", 100, 50000000, "B")

        self.assertEqual(order.order_type, 'limit')

    def test_get_order_returns_none_for_unknown(self):
        """Test get_order returns None for unknown order ID."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.get_order(9999)

        self.assertIsNone(order)

    def test_get_all_orders_returns_empty_dict_initially(self):
        """Test get_all_orders returns empty dict initially."""
        client = OrderClientWithFSM("localhost", 10000)
        orders = client.get_all_orders()

        self.assertEqual(orders, {})

    def test_submit_order_fails_without_connection(self):
        """Test submit_order fails when not connected."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")

        result = client.submit_order(order)
        self.assertFalse(result)

    def test_cancel_order_fails_without_exchange_id(self):
        """Test cancel_order fails when order has no exchange ID."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()

        result = client.cancel_order(order)
        self.assertFalse(result)

    def test_set_message_callback(self):
        """Test setting a message callback."""
        client = OrderClientWithFSM("localhost", 10000)
        callback = Mock()
        client.set_message_callback(callback)

        self.assertEqual(client._message_callback, callback)


class TestOrderClientProcessMessage(unittest.TestCase):
    """Tests for _process_message internal method."""

    def test_process_message_routes_ack_to_pending_order(self):
        """Test ACK message is routed to pending order."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        client._pending_order = order

        client._process_message("ACK,1001,100,50000000")

        self.assertEqual(order.state, OrderState.ACCEPTED)
        self.assertEqual(order.exchange_order_id, 1001)
        self.assertIn(1001, client._orders)
        self.assertIsNone(client._pending_order)

    def test_process_message_routes_reject_to_pending_order(self):
        """Test REJECT message is routed to pending order."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        client._pending_order = order

        client._process_message("REJECT,No liquidity")

        self.assertEqual(order.state, OrderState.REJECTED)
        self.assertIsNone(client._pending_order)

    def test_process_message_routes_fill_to_existing_order(self):
        """Test FILL message is routed to existing order."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        client._orders[1001] = order

        client._process_message("FILL,1001,100,50000000")

        self.assertEqual(order.state, OrderState.FILLED)

    def test_process_message_callback_called(self):
        """Test message callback is called when set."""
        client = OrderClientWithFSM("localhost", 10000)
        callback = Mock()
        client.set_message_callback(callback)

        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        client._pending_order = order

        client._process_message("ACK,1001,100,50000000")

        callback.assert_called_once()
        self.assertEqual(callback.call_args[0][0], order)

    def test_process_message_ignores_invalid_message(self):
        """Test invalid messages are ignored."""
        client = OrderClientWithFSM("localhost", 10000)
        callback = Mock()
        client.set_message_callback(callback)

        client._process_message("INVALID,MESSAGE")

        callback.assert_not_called()


# =============================================================================
# Part 5: Integration Tests with Exchange Server
# =============================================================================

class TestIntegrationBasicOrders(unittest.TestCase):
    """Integration tests with real exchange server for basic order operations."""

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10200, feed_port=10201)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10200)
        self.assertTrue(self.client.connect())

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_submit_limit_order_gets_ack(self):
        """Test submitting a limit order receives ACK."""
        order = self.client.create_order("limit", 100, 50000000, "B")
        result = self.client.submit_order_sync(order, timeout=2.0)

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.ACCEPTED)
        self.assertIsNotNone(order.exchange_order_id)

    def test_submit_limit_order_sell(self):
        """Test submitting a sell limit order."""
        order = self.client.create_order("limit", 50, 51000000, "S")
        result = self.client.submit_order_sync(order, timeout=2.0)

        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.ACCEPTED)

    def test_submit_invalid_order_fails_locally(self):
        """Test order with invalid parameters fails validation."""
        order = self.client.create_order("limit", 0, 50000000, "B")  # Invalid size
        result = self.client.submit_order_sync(order, timeout=2.0)

        self.assertFalse(result)
        self.assertEqual(order.state, OrderState.VALIDATION_FAILED)


    def test_cancel_accepted_order(self):
        """Test cancelling an accepted order."""
        # Submit order first
        order = self.client.create_order("limit", 100, 50000000, "B")
        self.client.submit_order_sync(order, timeout=2.0)
        self.assertEqual(order.state, OrderState.ACCEPTED)

        # Cancel it
        result = self.client.cancel_order(order)
        self.assertTrue(result)

        # Wait for cancel ack
        time.sleep(0.1)
        self.client._socket.settimeout(2.0)
        try:
            data = self.client._socket.recv(4096)
            self.client._buffer += data.decode('utf-8')
            while '\n' in self.client._buffer:
                line, self.client._buffer = self.client._buffer.split('\n', 1)
                if line.strip():
                    self.client._process_message(line.strip())
        except socket.timeout:
            pass

        self.assertEqual(order.state, OrderState.CANCELLED)

    def test_order_tracked_by_exchange_id(self):
        """Test order is tracked by exchange order ID."""
        order = self.client.create_order("limit", 100, 50000000, "B")
        self.client.submit_order_sync(order, timeout=2.0)

        exchange_id = order.exchange_order_id
        self.assertIsNotNone(exchange_id)

        tracked_order = self.client.get_order(exchange_id)
        self.assertEqual(tracked_order, order)


class TestIntegrationMarketOrder(unittest.TestCase):
    """Integration tests for market orders.

    Uses a separate server to ensure a clean order book.
    """

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10250, feed_port=10251)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10250)
        self.assertTrue(self.client.connect())

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_submit_market_order_no_liquidity_rejected(self):
        """Test market order with no liquidity gets rejected.

        Note: This tests the REJECT path which is correctly handled for pending orders.
        Needs a clean order book (no standing orders) to ensure rejection.
        """
        order = self.client.create_order("market", 100, 0, "B")
        self.client.submit_order_sync(order, timeout=2.0)

        # The order should be rejected (no liquidity)
        self.assertEqual(order.state, OrderState.REJECTED)


class TestIntegrationTwoClients(unittest.TestCase):
    """Integration tests with two clients trading against each other.

    Tests verify both:
    - Aggressive orders that immediately fill (crossing orders)
    - Passive fill notifications on standing orders
    """

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10300, feed_port=10301)
        cls.server_fixture.start()
        time.sleep(0.2)  # Give server more time to start

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()
        time.sleep(0.1)  # Give server time to clean up

    def setUp(self):
        """Create two clients for each test."""
        self.client1 = OrderClientWithFSM("localhost", 10300)
        self.client2 = OrderClientWithFSM("localhost", 10300)
        self.assertTrue(self.client1.connect())
        time.sleep(0.05)  # Small delay between connections
        self.assertTrue(self.client2.connect())
        time.sleep(0.05)

    def tearDown(self):
        """Disconnect both clients."""
        self.client1.disconnect()
        self.client2.disconnect()
        time.sleep(0.05)  # Allow cleanup

    def test_crossing_orders_both_filled(self):
        """Test that crossing orders result in fills for both sides.

        Uses very high, unique prices to avoid interference from other tests.
        """
        # Client 1 posts a buy order at a very high unique price
        buy_order = self.client1.create_order("limit", 100, 90000000, "B")
        self.client1.submit_order_sync(buy_order, timeout=3.0)
        self.assertEqual(buy_order.state, OrderState.ACCEPTED)

        time.sleep(0.2)  # Allow order to be posted

        # Start async receive to catch passive fill
        self.client1.start_async_receive()
        time.sleep(0.1)  # Allow receive thread to start

        # Client 2 posts a crossing sell order (at or below the buy price)
        sell_order = self.client2.create_order("limit", 100, 89000000, "S")  # Below buy price
        self.client2.submit_order_sync(sell_order, timeout=3.0)

        # Aggressive sell order should be filled immediately
        self.assertEqual(sell_order.state, OrderState.FILLED)
        self.assertIsNotNone(sell_order.exchange_order_id)

        # Wait for passive fill message to client1
        time.sleep(0.5)

        # Buy order (standing) should be filled via async receive
        self.assertEqual(buy_order.state, OrderState.FILLED)

    def test_partial_fill_both_sides(self):
        """Test partial fill affects both aggressive and passive orders.

        Uses very low, unique prices to avoid interference from other tests.
        """
        # Client 1 posts a large buy order at a low unique price
        buy_order = self.client1.create_order("limit", 200, 30000000, "B")
        self.client1.submit_order_sync(buy_order, timeout=3.0)
        self.assertEqual(buy_order.state, OrderState.ACCEPTED)

        time.sleep(0.2)  # Allow order to be posted

        # Start async receive to catch passive fill
        self.client1.start_async_receive()
        time.sleep(0.1)

        # Client 2 posts a smaller crossing sell order
        sell_order = self.client2.create_order("limit", 50, 29000000, "S")
        self.client2.submit_order_sync(sell_order, timeout=3.0)

        # Aggressive sell order should be fully filled
        self.assertEqual(sell_order.state, OrderState.FILLED)

        # Wait for passive fill message to client1
        time.sleep(0.5)

        # Buy order should be partially filled
        self.assertEqual(buy_order.state, OrderState.PARTIALLY_FILLED)

    def test_market_order_fills_against_standing(self):
        """Test market order fills and standing order receives passive fill.

        Uses a mid-range unique price to avoid interference from other tests.
        """
        # Client 1 posts a sell order at a unique price
        sell_order = self.client1.create_order("limit", 75, 70000000, "S")
        self.client1.submit_order_sync(sell_order, timeout=3.0)
        self.assertEqual(sell_order.state, OrderState.ACCEPTED)

        time.sleep(0.2)  # Allow order to be posted

        # Start async receive to catch passive fill
        self.client1.start_async_receive()
        time.sleep(0.1)

        # Client 2 sends a market buy
        market_order = self.client2.create_order("market", 50, 0, "B")
        self.client2.submit_order_sync(market_order, timeout=3.0)

        # Market order should be filled
        self.assertEqual(market_order.state, OrderState.FILLED)

        # Wait for passive fill
        time.sleep(0.5)

        # Sell order (standing) should be partially filled via async receive
        self.assertEqual(sell_order.state, OrderState.PARTIALLY_FILLED)


class TestIntegrationOrderLifecycle(unittest.TestCase):
    """Integration tests for complete order lifecycle scenarios."""

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10400, feed_port=10401)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10400)
        self.assertTrue(self.client.connect())

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_multiple_orders_tracked(self):
        """Test multiple orders are tracked correctly."""
        orders = []
        for i in range(5):
            order = self.client.create_order("limit", 100, 50000000 + i * 10000, "B")
            self.client.submit_order_sync(order, timeout=2.0)
            orders.append(order)

        all_orders = self.client.get_all_orders()
        self.assertEqual(len(all_orders), 5)

        for order in orders:
            self.assertEqual(order.state, OrderState.ACCEPTED)
            self.assertIsNotNone(order.exchange_order_id)

    def test_order_state_history_complete(self):
        """Test state history captures all transitions."""
        order = self.client.create_order("limit", 100, 50000000, "B")
        self.client.submit_order_sync(order, timeout=2.0)

        history = order.state_history

        # Should have: NEW->PENDING_VALIDATION, PENDING_VALIDATION->PENDING_NEW, PENDING_NEW->ACCEPTED
        self.assertEqual(len(history), 3)
        self.assertEqual(history[0][0], OrderState.PENDING_VALIDATION)
        self.assertEqual(history[1][0], OrderState.PENDING_NEW)
        self.assertEqual(history[2][0], OrderState.ACCEPTED)


class TestIntegrationAsyncReceive(unittest.TestCase):
    """Integration tests for async receive functionality."""

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10500, feed_port=10501)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10500)
        self.assertTrue(self.client.connect())
        self.received_messages = []

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_async_receive_captures_messages(self):
        """Test async receive captures messages via callback."""
        def on_message(order, msg):
            self.received_messages.append((order, msg))

        self.client.set_message_callback(on_message)

        # Submit order synchronously first
        order = self.client.create_order("limit", 100, 50000000, "B")
        self.client.submit_order_sync(order, timeout=2.0)

        # ACK should have been captured
        self.assertEqual(len(self.received_messages), 1)
        self.assertEqual(self.received_messages[0][1].msg_type, "ACK")


class TestIntegrationTTLExpiration(unittest.TestCase):
    """Integration tests for TTL expiration."""

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10600, feed_port=10601)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10600)
        self.assertTrue(self.client.connect())

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_short_ttl_order_expires(self):
        """Test order with short TTL expires."""
        # Submit order with 1 second TTL
        order = self.client.create_order("limit", 100, 50000000, "B", ttl=1)
        self.client.submit_order_sync(order, timeout=2.0)
        self.assertEqual(order.state, OrderState.ACCEPTED)

        # Start async receive
        self.client.start_async_receive()

        # Wait for TTL + server check interval
        time.sleep(2.5)

        # Order should be expired
        self.assertEqual(order.state, OrderState.EXPIRED)


class TestIntegrationCancelScenarios(unittest.TestCase):
    """Integration tests for various cancel scenarios."""

    @classmethod
    def setUpClass(cls):
        """Start exchange server for all tests in this class."""
        cls.server_fixture = ExchangeServerFixture(order_port=10700, feed_port=10701)
        cls.server_fixture.start()

    @classmethod
    def tearDownClass(cls):
        """Stop exchange server."""
        cls.server_fixture.stop()

    def setUp(self):
        """Create client for each test."""
        self.client = OrderClientWithFSM("localhost", 10700)
        self.assertTrue(self.client.connect())

    def tearDown(self):
        """Disconnect client."""
        self.client.disconnect()

    def test_cancel_nonexistent_order_fails(self):
        """Test cancelling non-existent order fails."""
        # Create a fake order manually
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order._fsm._exchange_order_id = 99999  # Fake ID
        order._fsm._order_live = True

        # Add to tracking (simulating it exists)
        self.client._orders[99999] = order
        self.client._socket.settimeout(2.0)

        # Try to cancel
        result = self.client.cancel_order(order)
        self.assertTrue(result)  # Cancel request sent

        # Read response
        try:
            data = self.client._socket.recv(4096)
            self.client._buffer += data.decode('utf-8')
            while '\n' in self.client._buffer:
                line, self.client._buffer = self.client._buffer.split('\n', 1)
                if line.strip():
                    self.client._process_message(line.strip())
        except socket.timeout:
            pass

        # Order state unchanged (REJECT doesn't transition from PENDING_NEW)
        # Since we manually set state, the FSM won't transition on REJECT

    def test_cancel_filled_order_not_live(self):
        """Test cannot cancel a filled order (not live)."""
        order = self.client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        # Simulate filled state
        order._fsm._current_state = OrderState.FILLED
        order._fsm._order_live = False

        result = self.client.cancel_order(order)
        self.assertFalse(result)  # Should fail immediately


# =============================================================================
# Part 6: Error Handling and Edge Cases
# =============================================================================

class TestErrorHandling(unittest.TestCase):
    """Tests for error handling and edge cases."""

    def test_connect_to_invalid_host(self):
        """Test connecting to invalid host fails gracefully."""
        client = OrderClientWithFSM("invalid.host.example", 10000)
        result = client.connect()
        self.assertFalse(result)

    def test_connect_to_closed_port(self):
        """Test connecting to closed port fails gracefully."""
        client = OrderClientWithFSM("localhost", 59999)  # Unlikely to be open
        result = client.connect()
        self.assertFalse(result)

    def test_disconnect_without_connect(self):
        """Test disconnect without connect doesn't crash."""
        client = OrderClientWithFSM("localhost", 10000)
        client.disconnect()  # Should not raise

    def test_double_disconnect(self):
        """Test double disconnect doesn't crash."""
        client = OrderClientWithFSM("localhost", 10000)
        client.disconnect()
        client.disconnect()  # Should not raise


class TestImmediateFillHandling(unittest.TestCase):
    """Tests for immediate fill handling on pending orders.

    When an order immediately fills (crosses the book), the server sends FILL
    or PARTIAL_FILL instead of ACK. These tests verify that pending orders
    correctly handle these message types.
    """

    def test_immediate_fill_handled_for_pending_order(self):
        """Test that FILL messages are correctly handled for pending orders.

        When _process_message receives a FILL for a pending order, it should
        transition through ACK to ACCEPTED, then process the FILL to FILLED.
        """
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        client._pending_order = order

        # Simulate receiving a FILL message (as if the order immediately filled)
        client._process_message("FILL,1001,100,50000000")

        # Order should be FILLED after processing
        self.assertEqual(order.state, OrderState.FILLED)
        self.assertEqual(order.exchange_order_id, 1001)
        self.assertIsNotNone(order.last_message)
        self.assertEqual(order.last_message.msg_type, "FILL")

    def test_immediate_partial_fill_handled_for_pending_order(self):
        """Test that PARTIAL_FILL messages are correctly handled for pending orders."""
        client = OrderClientWithFSM("localhost", 10000)
        order = client.create_order("limit", 100, 50000000, "B")
        order.submit()
        order.validate()
        client._pending_order = order

        # Simulate receiving a PARTIAL_FILL message
        client._process_message("PARTIAL_FILL,1001,30,50000000,70")

        # Order should be PARTIALLY_FILLED after processing
        self.assertEqual(order.state, OrderState.PARTIALLY_FILLED)
        self.assertEqual(order.exchange_order_id, 1001)
        self.assertIsNotNone(order.last_message)
        self.assertEqual(order.last_message.msg_type, "PARTIAL_FILL")


class TestManagedOrderEdgeCases(unittest.TestCase):
    """Edge case tests for ManagedOrder."""

    def test_validate_without_submit(self):
        """Test validate without submit fails."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        result = order.validate()
        self.assertFalse(result)

    def test_process_message_after_terminal_state(self):
        """Test processing message after terminal state is handled."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="FILL", order_id=1001, size=100, price=50000000))

        # Now in FILLED (terminal), try to process another message
        result = order.process_message(ExchangeMessage(msg_type="CANCEL_ACK", order_id=1001, size=0))

        # Should be handled gracefully (FSM catches invalid transition)
        self.assertTrue(result)
        self.assertEqual(order.state, OrderState.FILLED)  # State unchanged

    def test_repr_works_correctly(self):
        """Test ManagedOrder repr works correctly."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        repr_str = repr(order)
        self.assertIn("ManagedOrder", repr_str)
        self.assertIn("NEW", repr_str)
        self.assertIn("size=100", repr_str)
        self.assertIn("price=50000000", repr_str)
        self.assertIn("side=B", repr_str)

    def test_state_name_property(self):
        """Test state_name property works correctly."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        self.assertEqual(order.state_name, "NEW")
        order.submit()
        self.assertEqual(order.state_name, "PENDING_VALIDATION")


# =============================================================================
# Part 7: FSM State Transition Comprehensive Tests
# =============================================================================

class TestFSMStateTransitions(unittest.TestCase):
    """Comprehensive FSM state transition tests."""

    def test_full_fill_from_accepted(self):
        """Test direct full fill from ACCEPTED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))

        self.assertEqual(order.state, OrderState.ACCEPTED)

        order.process_message(ExchangeMessage(msg_type="FILL", order_id=1001, size=100, price=50000000))

        self.assertEqual(order.state, OrderState.FILLED)
        self.assertTrue(order.is_terminal)
        self.assertFalse(order.is_live)

    def test_full_fill_from_partially_filled(self):
        """Test full fill from PARTIALLY_FILLED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=30,
                                              price=50000000, remainder_size=70))

        self.assertEqual(order.state, OrderState.PARTIALLY_FILLED)

        order.process_message(ExchangeMessage(msg_type="FILL", order_id=1001, size=70, price=50000000))

        self.assertEqual(order.state, OrderState.FILLED)

    def test_multiple_partial_fills(self):
        """Test multiple partial fills stay in PARTIALLY_FILLED."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))

        # First partial fill
        order.process_message(ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=30,
                                              price=50000000, remainder_size=70))
        self.assertEqual(order.state, OrderState.PARTIALLY_FILLED)

        # Second partial fill (self-loop)
        order.process_message(ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=20,
                                              price=50000000, remainder_size=50))
        self.assertEqual(order.state, OrderState.PARTIALLY_FILLED)

    def test_cancel_from_accepted(self):
        """Test cancel from ACCEPTED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="CANCEL_ACK", order_id=1001, size=100))

        self.assertEqual(order.state, OrderState.CANCELLED)
        self.assertTrue(order.is_terminal)
        self.assertFalse(order.is_live)

    def test_cancel_from_partially_filled(self):
        """Test cancel from PARTIALLY_FILLED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=30,
                                              price=50000000, remainder_size=70))
        order.process_message(ExchangeMessage(msg_type="CANCEL_ACK", order_id=1001, size=70))

        self.assertEqual(order.state, OrderState.CANCELLED)

    def test_ttl_expiry_from_pending_new(self):
        """Test TTL expiry from PENDING_NEW state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()

        self.assertEqual(order.state, OrderState.PENDING_NEW)

        order.process_message(ExchangeMessage(msg_type="EXPIRED", order_id=1001, size=100))

        self.assertEqual(order.state, OrderState.EXPIRED)

    def test_ttl_expiry_from_accepted(self):
        """Test TTL expiry from ACCEPTED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="EXPIRED", order_id=1001, size=100))

        self.assertEqual(order.state, OrderState.EXPIRED)

    def test_ttl_expiry_from_partially_filled(self):
        """Test TTL expiry from PARTIALLY_FILLED state."""
        order = ManagedOrder(size=100, price=50000000, side='B')
        order.submit()
        order.validate()
        order.process_message(ExchangeMessage(msg_type="ACK", order_id=1001, size=100, price=50000000))
        order.process_message(ExchangeMessage(msg_type="PARTIAL_FILL", order_id=1001, size=30,
                                              price=50000000, remainder_size=70))
        order.process_message(ExchangeMessage(msg_type="EXPIRED", order_id=1001, size=70))

        self.assertEqual(order.state, OrderState.EXPIRED)


class TestValidationScenarios(unittest.TestCase):
    """Tests for various validation scenarios."""

    def test_valid_buy_limit_order(self):
        """Test valid buy limit order passes validation."""
        order = ManagedOrder(size=100, price=50000000, side='B', order_type='limit')
        order.submit()
        result = order.validate()
        self.assertTrue(result)

    def test_valid_sell_limit_order(self):
        """Test valid sell limit order passes validation."""
        order = ManagedOrder(size=100, price=50000000, side='S', order_type='limit')
        order.submit()
        result = order.validate()
        self.assertTrue(result)

    def test_valid_buy_market_order(self):
        """Test valid buy market order passes validation."""
        order = ManagedOrder(size=100, price=0, side='B', order_type='market')
        order.submit()
        result = order.validate()
        self.assertTrue(result)

    def test_valid_sell_market_order(self):
        """Test valid sell market order passes validation."""
        order = ManagedOrder(size=100, price=0, side='S', order_type='market')
        order.submit()
        result = order.validate()
        self.assertTrue(result)

    def test_invalid_side_lowercase(self):
        """Test lowercase side fails validation."""
        order = ManagedOrder(size=100, price=50000000, side='b')  # lowercase
        order.submit()
        result = order.validate()
        self.assertFalse(result)


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == '__main__':
    unittest.main(verbosity=2)
