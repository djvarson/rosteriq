"""
Load test profiles and configuration for RosterIQ.

Defines different load scenarios: smoke, load, stress, spike, endurance.
Each profile specifies number of users, spawn rate, and duration.
"""

from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class LoadProfile:
    """Configuration for a load test profile."""
    name: str
    num_users: int
    spawn_rate: float  # users per second
    duration_seconds: int
    description: str


# Load test profiles
PROFILES: Dict[str, LoadProfile] = {
    "smoke": LoadProfile(
        name="smoke",
        num_users=5,
        spawn_rate=1.0,
        duration_seconds=30,
        description="Quick sanity check with minimal load (5 users, 30 seconds)",
    ),
    "load": LoadProfile(
        name="load",
        num_users=50,
        spawn_rate=10.0,
        duration_seconds=300,  # 5 minutes
        description="Moderate production-like load (50 users, 5 minutes, 10 users/sec ramp)",
    ),
    "stress": LoadProfile(
        name="stress",
        num_users=100,
        spawn_rate=20.0,
        duration_seconds=600,  # 10 minutes
        description="High stress test (100 users, 10 minutes, 20 users/sec ramp)",
    ),
    "spike": LoadProfile(
        name="spike",
        num_users=200,
        spawn_rate=50.0,
        duration_seconds=300,  # 5 minutes (fast ramp)
        description="Spike test: rapid spike from 10→200→10 users over 5 minutes",
    ),
    "endurance": LoadProfile(
        name="endurance",
        num_users=50,
        spawn_rate=5.0,
        duration_seconds=1800,  # 30 minutes
        description="Long-running endurance test (50 users, 30 minutes)",
    ),
}


def get_profile(name: str) -> LoadProfile:
    """Get a load profile by name."""
    if name not in PROFILES:
        raise ValueError(
            f"Unknown profile '{name}'. Available: {', '.join(PROFILES.keys())}"
        )
    return PROFILES[name]


def list_profiles() -> Dict[str, str]:
    """List all available profiles with descriptions."""
    return {name: profile.description for name, profile in PROFILES.items()}
