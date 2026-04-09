"""
API Router - Swap between api.py (v1) and api_v2.py (v2)

This module acts as a router to select which API version to use.
Update the import below to switch between versions.
"""

# Import from api_v2 (pipeline-based architecture)
from rosteriq.api_v2 import app

# To switch back to api.py (legacy architecture), uncomment:
# from rosteriq.api import app

__all__ = ["app"]
