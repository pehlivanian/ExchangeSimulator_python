"""Tests for order generator state management."""

import pytest
import sys
import os
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from order_generator import (
    OrderGeneratorState,
    create_insert_event,
    create_execute_event,
    create_delete_event,
    create_cancel_event,
)
from messages import EventType


class TestOrderGeneratorState:
    """Tests for OrderGeneratorState."""

    def test_seq_num_starts_at_one(self):
        state = OrderGeneratorState()
        assert state.get_next_seq_num() == 1

    def test_seq_num_increments(self):
        state = OrderGeneratorState()
        assert state.get_next_seq_num() == 1
        assert state.get_next_seq_num() == 2
        assert state.get_next_seq_num() == 3

    def test_order_id_starts_at_1000(self):
        state = OrderGeneratorState()
        assert state.get_next_order_id() == 1000

    def test_order_id_increments(self):
        state = OrderGeneratorState()
        assert state.get_next_order_id() == 1000
        assert state.get_next_order_id() == 1001
        assert state.get_next_order_id() == 1002

    def test_record_and_get_user(self):
        state = OrderGeneratorState()
        state.record_order(1000, "trader1", 1)
        assert state.get_user(1000) == "trader1"

    def test_record_and_get_connection(self):
        state = OrderGeneratorState()
        state.record_order(1000, "trader1", 42)
        assert state.get_connection(1000) == 42

    def test_get_nonexistent_user(self):
        state = OrderGeneratorState()
        assert state.get_user(9999) is None

    def test_get_nonexistent_connection(self):
        state = OrderGeneratorState()
        assert state.get_connection(9999) is None

    def test_remove_order(self):
        state = OrderGeneratorState()
        state.record_order(1000, "trader1", 1)
        state.remove_order(1000)
        assert state.get_user(1000) is None
        assert state.get_connection(1000) is None

    def test_remove_nonexistent_order(self):
        state = OrderGeneratorState()
        # Should not raise
        state.remove_order(9999)

    def test_timestamp_simulated(self):
        state = OrderGeneratorState(use_real_time=False, time_increment=0.001)
        t1 = state.get_next_timestamp()
        t2 = state.get_next_timestamp()
        assert t2 > t1
        assert abs((t2 - t1) - 0.001) < 0.0001

    def test_timestamp_real_time(self):
        state = OrderGeneratorState(use_real_time=True)
        t1 = state.get_next_timestamp()
        time.sleep(0.01)
        t2 = state.get_next_timestamp()
        assert t2 > t1


class TestOrderGeneratorThreadSafety:
    """Thread safety tests for OrderGeneratorState."""

    def test_seq_num_thread_safe(self):
        state = OrderGeneratorState()
        results = []

        def get_seq_nums():
            for _ in range(100):
                results.append(state.get_next_seq_num())

        threads = [threading.Thread(target=get_seq_nums) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have 1000 unique values
        assert len(results) == 1000
        assert len(set(results)) == 1000

    def test_order_id_thread_safe(self):
        state = OrderGeneratorState()
        results = []

        def get_order_ids():
            for _ in range(100):
                results.append(state.get_next_order_id())

        threads = [threading.Thread(target=get_order_ids) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 1000
        assert len(set(results)) == 1000

    def test_record_order_thread_safe(self):
        state = OrderGeneratorState()

        def record_orders(start):
            for i in range(100):
                state.record_order(start + i, f"user{start + i}", start + i)

        threads = [threading.Thread(target=record_orders, args=(i * 100,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify all records are present
        for i in range(1000):
            assert state.get_user(i) == f"user{i}"
            assert state.get_connection(i) == i


class TestEventCreation:
    """Tests for event creation functions."""

    def test_create_insert_event(self):
        state = OrderGeneratorState()
        event = create_insert_event(state, 1000, 100, 50000000, 'B')

        assert event.event_type == EventType.INSERT
        assert event.order_id == 1000
        assert event.size == 100
        assert event.price == 50000000
        assert event.direction == 'B'
        assert event.seq_num == 1

    def test_create_execute_event(self):
        state = OrderGeneratorState()
        event = create_execute_event(state, 50, 50000000, 'S')

        assert event.event_type == EventType.EXECUTE
        assert event.size == 50
        assert event.price == 50000000
        assert event.direction == 'S'

    def test_create_delete_event(self):
        state = OrderGeneratorState()
        event = create_delete_event(state, 1000, 50000000, 'B')

        assert event.event_type == EventType.DELETE
        assert event.order_id == 1000
        assert event.price == 50000000
        assert event.direction == 'B'

    def test_create_cancel_event(self):
        state = OrderGeneratorState()
        event = create_cancel_event(state, 1000, 30, 50000000, 'B')

        assert event.event_type == EventType.CANCEL
        assert event.order_id == 1000
        assert event.size == 30
        assert event.price == 50000000
        assert event.direction == 'B'

    def test_events_get_unique_seq_nums(self):
        state = OrderGeneratorState()
        e1 = create_insert_event(state, 1000, 100, 50000000, 'B')
        e2 = create_insert_event(state, 1001, 100, 50000000, 'S')
        e3 = create_execute_event(state, 50, 50000000, 'B')

        assert e1.seq_num == 1
        assert e2.seq_num == 2
        assert e3.seq_num == 3
