"""
Pytest fixtures for integration tests.
"""
import pytest
import sys

# Add parent directory to path for imports
sys.path.insert(0, '../..')

from test_integration import TestResults


@pytest.fixture
def results():
    """Provide a TestResults instance for each test."""
    return TestResults()
