"""
Pytest fixtures for integration tests.
"""
import pytest
import sys

# Add parent directory to path for imports
sys.path.insert(0, '../..')

from test_integration import IntegrationResults


@pytest.fixture
def results():
    """Provide a IntegrationResults instance for each test."""
    return IntegrationResults()
