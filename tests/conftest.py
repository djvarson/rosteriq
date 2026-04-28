"""
Pytest configuration and shared fixtures for RosterIQ tests.

Provides MemoryStore instances and test utilities for auth, webhooks,
and notification services.

Usage:
    pytest tests/ -v
"""

import sys
import os
from datetime import datetime

try:
    import pytest
except ImportError:
    # pytest not available - tests will be reported as import error
    pytest = None

# Ensure rosteriq imports work from the tests directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

if pytest:
    from rosteriq.database import MemoryStore, reset_db
    from rosteriq.services.auth import AuthService

    @pytest.fixture
    def memory_store():
        """Create a fresh MemoryStore instance for each test."""
        store = MemoryStore()
        yield store
        # Cleanup is implicit (instance is garbage collected)

    @pytest.fixture
    def auth_service(memory_store):
        """Create an AuthService with MemoryStore backend."""
        service = AuthService(db=memory_store)
        yield service

    @pytest.fixture(autouse=True)
    def reset_global_db():
        """Reset the global database singleton before each test."""
        reset_db()
        yield
        reset_db()
