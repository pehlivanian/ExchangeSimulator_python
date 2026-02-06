"""Integration tests for the exchange server."""

import pytest
import sys
import os
import time
import threading
import socket

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchange_server import ExchangeServer
from order_client_with_fsm import OrderClient
from stp_client import STPClient


def find_free_port():
    """Find a free port to use for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


class TestExchangeServerBasic:
    """Basic exchange server tests."""

    @pytest.fixture
    def server(self):
        """Create and start a test server."""
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_server_starts_and_stops(self, server):
        server_obj, order_port, feed_port = server
        # Server should be running
        client = OrderClient('localhost', order_port)
        assert client.connect()
        client.disconnect()

    def test_limit_order_ack(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        response = client.send_order("limit,100,50000000,B,test")
        assert "ACK" in response
        assert "1000" in response  # First order ID

        client.disconnect()

    def test_limit_order_sequence(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Use different prices to prevent crossing
        r1 = client.send_order("limit,100,50000000,B,test")
        r2 = client.send_order("limit,100,51000000,S,test")

        assert "ACK,1000" in r1
        assert "ACK,1001" in r2

        client.disconnect()

    def test_invalid_order_rejected(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        response = client.send_order("invalid,order,format")
        assert "REJECT" in response

        client.disconnect()

    def test_market_order_no_liquidity(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        response = client.send_order("market,100,0,B,test")
        assert "REJECT" in response
        assert "liquidity" in response.lower()

        client.disconnect()


class TestExchangeServerTrading:
    """Trading-specific tests."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_market_order_fills(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Post liquidity
        client.send_order("limit,100,50000000,S,seller")

        # Market buy should fill
        response = client.send_order("market,50,0,B,buyer")
        assert "FILL" in response
        assert "50" in response
        assert "50000000" in response

        client.disconnect()

    def test_market_order_multiple_fills(self, server):
        server_obj, order_port, feed_port = server

        # Use separate connection for seller to avoid passive fill interleaving
        seller = OrderClient('localhost', order_port)
        assert seller.connect()
        seller.send_order("limit,50,50000000,S,seller")
        seller.send_order("limit,50,51000000,S,seller")
        seller.disconnect()

        # Buyer on separate connection
        buyer = OrderClient('localhost', order_port)
        assert buyer.connect()

        # Market buy for 75 should fill at both levels
        response = buyer.send_order("market,75,0,B,buyer")
        lines = response.strip().split('\n')

        # Should have two FILL responses (order_id=0 for market orders)
        fills = [l for l in lines if l.startswith("FILL,0,")]
        assert len(fills) == 2

        # First fill at 50000000
        assert "50000000" in fills[0]
        # Second fill at 51000000
        assert "51000000" in fills[1]

        buyer.disconnect()

    def test_limit_order_crosses(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Post sell order
        client.send_order("limit,100,50000000,S,seller")

        # Buy at same price should cross
        response = client.send_order("limit,50,50000000,B,buyer")
        assert "FILL" in response

        client.disconnect()

    def test_limit_order_partial_cross(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Post sell for 50
        client.send_order("limit,50,50000000,S,seller")

        # Buy for 100 should partially fill
        response = client.send_order("limit,100,50000000,B,buyer")

        # Should have PARTIAL_FILL and ACK
        assert "PARTIAL_FILL" in response or "FILL" in response
        if "PARTIAL_FILL" in response:
            assert "ACK" in response

        client.disconnect()


class TestSTPFeed:
    """Tests for STP trade feed."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_stp_receives_trades(self, server):
        server_obj, order_port, feed_port = server

        # Collect STP messages
        stp_messages = []

        def stp_callback(msg):
            stp_messages.append(msg)

        stp = STPClient('localhost', feed_port)
        assert stp.connect()
        stp_thread = threading.Thread(
            target=lambda: stp.run(stp_callback),
            daemon=True
        )
        stp_thread.start()
        time.sleep(0.1)

        # Execute a trade
        client = OrderClient('localhost', order_port)
        assert client.connect()
        client.send_order("limit,100,50000000,S,seller")
        client.send_order("limit,50,50000000,B,buyer")
        client.disconnect()

        time.sleep(0.2)
        stp.disconnect()

        # Should have received trade notification
        assert len(stp_messages) >= 1
        assert "TRADE" in stp_messages[0]
        assert "50" in stp_messages[0]
        assert "50000000" in stp_messages[0]
        assert "BUY" in stp_messages[0]

    def test_stp_shows_aggressor_side(self, server):
        server_obj, order_port, feed_port = server

        stp_messages = []

        def stp_callback(msg):
            stp_messages.append(msg)

        stp = STPClient('localhost', feed_port)
        assert stp.connect()
        stp_thread = threading.Thread(
            target=lambda: stp.run(stp_callback),
            daemon=True
        )
        stp_thread.start()
        time.sleep(0.1)

        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Sell aggressor
        client.send_order("limit,100,50000000,B,buyer")
        client.send_order("limit,50,50000000,S,seller")

        time.sleep(0.2)

        assert any("SELL" in msg for msg in stp_messages)

        client.disconnect()
        stp.disconnect()


class TestPassiveFillNotifications:
    """Tests for async passive fill notifications."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_passive_fill_notification(self, server):
        server_obj, order_port, feed_port = server

        # Market maker posts order and stays connected
        maker_messages = []

        def maker_callback(msg):
            maker_messages.append(msg)

        maker = OrderClient('localhost', order_port)
        assert maker.connect()
        maker.start_async_receive(maker_callback)
        time.sleep(0.1)

        # Post limit order
        maker.send_order_async("limit,100,50000000,S,maker")
        time.sleep(0.2)

        # Separate client takes liquidity
        taker = OrderClient('localhost', order_port)
        assert taker.connect()
        taker.send_order("limit,50,50000000,B,taker")
        taker.disconnect()

        time.sleep(0.3)

        # Maker should have received passive fill
        fills = [m for m in maker_messages if "FILL" in m or "PARTIAL_FILL" in m]
        assert len(fills) >= 1

        maker.disconnect()

    def test_passive_partial_fill_shows_remainder(self, server):
        server_obj, order_port, feed_port = server

        maker_messages = []

        def maker_callback(msg):
            maker_messages.append(msg)

        maker = OrderClient('localhost', order_port)
        assert maker.connect()
        maker.start_async_receive(maker_callback)
        time.sleep(0.1)

        maker.send_order_async("limit,100,50000000,S,maker")
        time.sleep(0.2)

        taker = OrderClient('localhost', order_port)
        assert taker.connect()
        taker.send_order("limit,30,50000000,B,taker")
        taker.disconnect()

        time.sleep(0.3)

        # Should have PARTIAL_FILL with remainder
        partial_fills = [m for m in maker_messages if "PARTIAL_FILL" in m]
        assert len(partial_fills) >= 1
        assert "70" in partial_fills[0]  # 100 - 30 = 70 remaining

        maker.disconnect()


class TestMultipleClients:
    """Tests for multiple concurrent clients."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_multiple_clients_concurrent(self, server):
        server_obj, order_port, feed_port = server

        clients = []
        responses = []

        def client_work(client_id):
            client = OrderClient('localhost', order_port)
            if client.connect():
                resp = client.send_order(f"limit,100,50000000,B,client{client_id}")
                responses.append(resp)
                client.disconnect()

        threads = []
        for i in range(5):
            t = threading.Thread(target=client_work, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All clients should have received ACKs
        assert len(responses) == 5
        for resp in responses:
            assert "ACK" in resp

    def test_order_ids_unique_across_clients(self, server):
        server_obj, order_port, feed_port = server

        order_ids = []

        def extract_order_id(response):
            # ACK,1000,100,50000000
            parts = response.split(',')
            if len(parts) >= 2 and parts[0] == "ACK":
                return int(parts[1])
            return None

        def client_work():
            client = OrderClient('localhost', order_port)
            if client.connect():
                resp = client.send_order("limit,100,50000000,B,test")
                oid = extract_order_id(resp)
                if oid:
                    order_ids.append(oid)
                client.disconnect()

        threads = []
        for _ in range(10):
            t = threading.Thread(target=client_work)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All order IDs should be unique
        assert len(order_ids) == len(set(order_ids))


class TestCancelOrders:
    """Tests for order cancellation."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_cancel_order_success(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Place an order
        response = client.send_order("limit,100,50000000,B,trader1")
        assert "ACK,1000" in response

        # Cancel it
        response = client.send_order("cancel,1000,trader1")
        assert "CANCEL_ACK" in response
        assert "1000" in response
        assert "100" in response  # Cancelled size

        client.disconnect()

    def test_cancel_nonexistent_order(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        response = client.send_order("cancel,9999,trader1")
        assert "REJECT" in response
        assert "not found" in response.lower()

        client.disconnect()

    def test_cancel_wrong_user(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Place order as trader1
        client.send_order("limit,100,50000000,B,trader1")

        # Try to cancel as trader2
        response = client.send_order("cancel,1000,trader2")
        assert "REJECT" in response
        assert "not owned" in response.lower()

        client.disconnect()

    def test_cancel_removes_from_book(self, server):
        server_obj, order_port, feed_port = server
        client = OrderClient('localhost', order_port)
        assert client.connect()

        # Place a bid
        client.send_order("limit,100,50000000,B,trader1")

        # Verify it's in the book
        bids, asks = server_obj._order_book.get_snapshot()
        assert len(bids) == 1

        # Cancel it
        client.send_order("cancel,1000,trader1")

        # Verify it's gone
        bids, asks = server_obj._order_book.get_snapshot()
        assert len(bids) == 0

        client.disconnect()

    def test_cancel_partially_filled_order(self, server):
        server_obj, order_port, feed_port = server

        # Seller posts large order
        seller = OrderClient('localhost', order_port)
        assert seller.connect()
        seller.send_order("limit,100,50000000,S,seller")
        seller.disconnect()

        # Buyer partially fills it
        buyer = OrderClient('localhost', order_port)
        assert buyer.connect()
        buyer.send_order("limit,30,50000000,B,buyer")
        buyer.disconnect()

        # Seller cancels remaining
        seller = OrderClient('localhost', order_port)
        assert seller.connect()
        response = seller.send_order("cancel,1000,seller")
        assert "CANCEL_ACK" in response
        assert "70" in response  # 100 - 30 = 70 remaining

        # Verify order removed from book
        bids, asks = server_obj._order_book.get_snapshot()
        assert len(asks) == 0

        seller.disconnect()

    def test_cancel_already_filled_order(self, server):
        server_obj, order_port, feed_port = server

        # Seller posts order
        seller = OrderClient('localhost', order_port)
        assert seller.connect()
        seller.send_order("limit,50,50000000,S,seller")
        seller.disconnect()

        # Buyer fully fills it
        buyer = OrderClient('localhost', order_port)
        assert buyer.connect()
        buyer.send_order("limit,50,50000000,B,buyer")
        buyer.disconnect()

        # Seller tries to cancel (should fail - already filled)
        seller = OrderClient('localhost', order_port)
        assert seller.connect()
        response = seller.send_order("cancel,1000,seller")
        assert "REJECT" in response
        assert "not found" in response.lower()

        seller.disconnect()


class TestOrderBookDisplay:
    """Tests for order book display functionality."""

    @pytest.fixture
    def server(self):
        order_port = find_free_port()
        feed_port = find_free_port()
        server = ExchangeServer(order_port, feed_port)
        assert server.start()
        time.sleep(0.1)
        yield server, order_port, feed_port
        server.stop()

    def test_print_book_empty(self, server):
        server_obj, order_port, feed_port = server
        output = server_obj.print_book()
        assert "ORDER BOOK SNAPSHOT" in output
        assert "BIDS" in output
        assert "ASKS" in output

    def test_print_book_with_orders(self, server):
        server_obj, order_port, feed_port = server

        client = OrderClient('localhost', order_port)
        assert client.connect()
        client.send_order("limit,100,50000000,B,test")
        client.send_order("limit,50,51000000,S,test")
        client.disconnect()

        time.sleep(0.1)
        output = server_obj.print_book()

        assert "5000.00" in output  # Bid price
        assert "5100.00" in output  # Ask price
        assert "100" in output      # Bid size
        assert "50" in output       # Ask size

    def test_post_order_callback(self, server):
        server_obj, order_port, feed_port = server

        callback_calls = []

        def callback(order_str, response):
            callback_calls.append((order_str, response))

        server_obj.set_post_order_callback(callback)

        client = OrderClient('localhost', order_port)
        assert client.connect()
        client.send_order("limit,100,50000000,B,test")
        client.disconnect()

        time.sleep(0.1)

        assert len(callback_calls) == 1
        assert "limit,100,50000000,B,test" in callback_calls[0][0]
        assert "ACK" in callback_calls[0][1]
