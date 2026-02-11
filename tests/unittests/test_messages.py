"""Tests for message parsing and serialization."""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from messages import (
    EventType,
    OrderHandlerMessageType,
    Order,
    Trade,
    Ack,
    EventLOBSTER,
    LiveOrder,
    CancelOrder,
    ModifyOrder,
    OrderHandlerMessage,
    STPMessage,
    parse_live_order,
    parse_cancel_order,
    parse_modify_order,
    parse_lobster_line,
)


class TestParseLiveOrder:
    """Tests for parse_live_order function."""

    def test_parse_limit_buy(self):
        result = parse_live_order("limit,100,50000000,B,trader1")
        assert result is not None
        assert result.order_type == "limit"
        assert result.size == 100
        assert result.price == 50000000
        assert result.side == "B"
        assert result.user == "trader1"

    def test_parse_limit_sell(self):
        result = parse_live_order("limit,50,49500000,S,marketmaker")
        assert result is not None
        assert result.order_type == "limit"
        assert result.size == 50
        assert result.price == 49500000
        assert result.side == "S"
        assert result.user == "marketmaker"

    def test_parse_market_buy(self):
        result = parse_live_order("market,75,0,B,aggressive")
        assert result is not None
        assert result.order_type == "market"
        assert result.size == 75
        assert result.price == 0
        assert result.side == "B"

    def test_parse_market_sell(self):
        result = parse_live_order("market,25,0,S,trader")
        assert result is not None
        assert result.order_type == "market"
        assert result.size == 25
        assert result.side == "S"

    def test_parse_with_whitespace(self):
        result = parse_live_order("  limit , 100 , 50000000 , B , trader1  \n")
        assert result is not None
        assert result.order_type == "limit"
        assert result.size == 100

    def test_parse_lowercase_side(self):
        result = parse_live_order("limit,100,50000000,b,trader1")
        assert result is not None
        assert result.side == "B"

    def test_invalid_order_type(self):
        result = parse_live_order("stop,100,50000000,B,trader1")
        assert result is None

    def test_invalid_side(self):
        result = parse_live_order("limit,100,50000000,X,trader1")
        assert result is None

    def test_zero_size(self):
        result = parse_live_order("limit,0,50000000,B,trader1")
        assert result is None

    def test_negative_size(self):
        result = parse_live_order("limit,-100,50000000,B,trader1")
        assert result is None

    def test_limit_zero_price(self):
        result = parse_live_order("limit,100,0,B,trader1")
        assert result is None

    def test_limit_negative_price(self):
        result = parse_live_order("limit,100,-50000000,B,trader1")
        assert result is None

    def test_market_zero_price_ok(self):
        result = parse_live_order("market,100,0,B,trader1")
        assert result is not None

    def test_wrong_field_count(self):
        result = parse_live_order("limit,100,50000000,B")
        assert result is None

    def test_empty_string(self):
        result = parse_live_order("")
        assert result is None

    def test_non_numeric_size(self):
        result = parse_live_order("limit,abc,50000000,B,trader1")
        assert result is None

    def test_non_numeric_price(self):
        result = parse_live_order("limit,100,abc,B,trader1")
        assert result is None


class TestParseCancelOrder:
    """Tests for parse_cancel_order function."""

    def test_parse_cancel(self):
        result = parse_cancel_order("cancel,1000,trader1")
        assert result is not None
        assert result.order_id == 1000
        assert result.user == "trader1"

    def test_parse_cancel_with_whitespace(self):
        result = parse_cancel_order("  cancel , 1000 , trader1  \n")
        assert result is not None
        assert result.order_id == 1000
        assert result.user == "trader1"

    def test_parse_cancel_uppercase(self):
        result = parse_cancel_order("CANCEL,1000,trader1")
        assert result is not None
        assert result.order_id == 1000

    def test_invalid_not_cancel(self):
        result = parse_cancel_order("limit,100,50000000,B,trader1")
        assert result is None

    def test_invalid_order_id(self):
        result = parse_cancel_order("cancel,abc,trader1")
        assert result is None

    def test_wrong_field_count(self):
        result = parse_cancel_order("cancel,1000")
        assert result is None

    def test_empty_string(self):
        result = parse_cancel_order("")
        assert result is None


class TestParseLobsterLine:
    """Tests for parse_lobster_line function."""

    def test_parse_insert_bid(self):
        result = parse_lobster_line("34200.015,1,12345,100,5000000,1")
        assert result is not None
        assert result.event_type == EventType.INSERT
        assert result.order_id == 12345
        assert result.size == 100
        assert result.price == 5000000
        assert result.direction == "B"

    def test_parse_insert_ask(self):
        result = parse_lobster_line("34200.015,1,12345,100,5000000,-1")
        assert result is not None
        assert result.direction == "S"

    def test_parse_execute(self):
        result = parse_lobster_line("34200.015,4,12345,50,5000000,1")
        assert result is not None
        assert result.event_type == EventType.EXECUTE
        assert result.size == 50

    def test_parse_cancel(self):
        result = parse_lobster_line("34200.015,2,12345,25,5000000,1")
        assert result is not None
        assert result.event_type == EventType.CANCEL

    def test_parse_delete(self):
        result = parse_lobster_line("34200.015,3,12345,100,5000000,1")
        assert result is not None
        assert result.event_type == EventType.DELETE

    def test_invalid_field_count(self):
        result = parse_lobster_line("34200.015,1,12345,100,5000000")
        assert result is None

    def test_empty_line(self):
        result = parse_lobster_line("")
        assert result is None

    def test_whitespace_line(self):
        result = parse_lobster_line("   \n")
        assert result is None


class TestOrderHandlerMessage:
    """Tests for OrderHandlerMessage serialization."""

    def test_serialize_ack(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.ACK,
            order_id=1000,
            size=100,
            price=50000000
        )
        assert msg.serialize() == "ACK,1000,100,50000000"

    def test_serialize_fill(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.FILL,
            order_id=1001,
            size=50,
            price=49500000
        )
        assert msg.serialize() == "FILL,1001,50,49500000"

    def test_serialize_partial_fill(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.PARTIAL_FILL,
            order_id=1002,
            size=25,
            price=49500000,
            remainder_size=75
        )
        assert msg.serialize() == "PARTIAL_FILL,1002,25,49500000,75"

    def test_serialize_reject(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.REJECT,
            reason="No liquidity"
        )
        assert msg.serialize() == "REJECT,No liquidity"

    def test_serialize_cancel_ack(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.CANCEL_ACK,
            order_id=1000,
            size=100
        )
        assert msg.serialize() == "CANCEL_ACK,1000,100"


class TestParseModifyOrder:
    """Tests for parse_modify_order function."""

    def test_parse_modify(self):
        result = parse_modify_order("modify,1000,200,50000000,trader1")
        assert result is not None
        assert result.order_id == 1000
        assert result.size == 200
        assert result.price == 50000000
        assert result.user == "trader1"

    def test_parse_modify_with_whitespace(self):
        result = parse_modify_order("  modify , 1000 , 200 , 50000000 , trader1  \n")
        assert result is not None
        assert result.order_id == 1000
        assert result.size == 200

    def test_parse_modify_uppercase(self):
        result = parse_modify_order("MODIFY,1000,200,50000000,trader1")
        assert result is not None
        assert result.order_id == 1000

    def test_invalid_not_modify(self):
        result = parse_modify_order("cancel,1000,trader1")
        assert result is None

    def test_invalid_order_id(self):
        result = parse_modify_order("modify,abc,200,50000000,trader1")
        assert result is None

    def test_zero_size(self):
        result = parse_modify_order("modify,1000,0,50000000,trader1")
        assert result is None

    def test_negative_size(self):
        result = parse_modify_order("modify,1000,-100,50000000,trader1")
        assert result is None

    def test_zero_price(self):
        result = parse_modify_order("modify,1000,200,0,trader1")
        assert result is None

    def test_negative_price(self):
        result = parse_modify_order("modify,1000,200,-50000000,trader1")
        assert result is None

    def test_wrong_field_count(self):
        result = parse_modify_order("modify,1000,200")
        assert result is None

    def test_empty_string(self):
        result = parse_modify_order("")
        assert result is None

    def test_empty_user(self):
        result = parse_modify_order("modify,1000,200,50000000,")
        assert result is None

    def test_non_numeric_size(self):
        result = parse_modify_order("modify,1000,abc,50000000,trader1")
        assert result is None

    def test_non_numeric_price(self):
        result = parse_modify_order("modify,1000,200,abc,trader1")
        assert result is None


class TestModifyAckSerialization:
    """Tests for MODIFY_ACK message serialization."""

    def test_serialize_modify_ack(self):
        msg = OrderHandlerMessage(
            msg_type=OrderHandlerMessageType.MODIFY_ACK,
            order_id=1000,
            size=200,
            price=50000000,
            remainder_size=1001  # new_order_id
        )
        assert msg.serialize() == "MODIFY_ACK,1000,200,50000000,1001"


class TestSTPMessage:
    """Tests for STPMessage serialization."""

    def test_serialize_buy(self):
        msg = STPMessage(size=100, price=50000000, aggressor_side='B')
        assert msg.serialize() == "TRADE,100,50000000,BUY"

    def test_serialize_sell(self):
        msg = STPMessage(size=50, price=49500000, aggressor_side='S')
        assert msg.serialize() == "TRADE,50,49500000,SELL"
