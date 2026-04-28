"""
RosterIQ Load Testing Suite — Locust configuration.

Comprehensive load test covering all major API endpoints and user flows:
- Authentication (register, login, refresh, me)
- Venue operations (list, get, employees)
- Roster generation and retrieval
- Forecast queries and ingestion
- Webhook processing (Tanda)
- Staff portal (shifts, pay, availability)
- Health checks

Run:
    locust -f locustfile.py --host=http://localhost:8000
    locust -f locustfile.py --host=http://localhost:8000 -u 50 -r 10 -t 5m  # 50 users, ramp 10/sec, 5 min
"""

import os
import json
import time
import hmac
import hashlib
import random
import string
from datetime import datetime, date, timedelta
from decimal import Decimal

from locust import HttpUser, task, between, events
import uuid


# ============================================================================
# Configuration
# ============================================================================

# Optional: read from environment
TARGET_HOST = os.getenv("TARGET_HOST", "http://localhost:8000")
TANDA_WEBHOOK_SECRET = os.getenv("TANDA_WEBHOOK_SECRET", "test-webhook-secret-key")

# Performance metrics tracking
stats = {
    "successful_auths": 0,
    "failed_auths": 0,
    "successful_rosters": 0,
    "failed_rosters": 0,
    "webhook_signatures": 0,
}


# ============================================================================
# Payload Generators
# ============================================================================

def generate_email():
    """Generate unique email for testing."""
    return f"testuser_{uuid.uuid4().hex[:8]}@rosteriq.test"


def generate_venue_id():
    """Get or generate a venue ID."""
    return f"venue_{random.randint(1000, 9999)}"


def generate_employee_name():
    """Generate realistic employee name."""
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"


def generate_forecast_payload(venue_id: str, start_date: date = None, num_days: int = 7):
    """Generate realistic demand forecast payload."""
    if start_date is None:
        start_date = date.today()

    forecasts = []
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        # Realistic demand: higher on Friday/Saturday, lower mid-week
        weekday = current_date.weekday()
        if weekday in [4, 5]:  # Friday, Saturday
            base_covers = random.uniform(80, 150)
        else:
            base_covers = random.uniform(30, 70)

        forecasts.append({
            "date": current_date.isoformat(),
            "expected_covers": base_covers,
            "confidence": round(random.uniform(0.7, 0.99), 2),
            "source": random.choice(["pos", "manual", "historical"]),
        })

    return {
        "venue_id": venue_id,
        "forecasts": forecasts,
    }


def generate_roster_payload(venue_id: str, num_employees: int = 10, num_days: int = 7):
    """Generate realistic roster optimization payload."""
    start_date = date.today()
    end_date = start_date + timedelta(days=num_days - 1)

    employees = []
    for i in range(num_employees):
        employees.append({
            "id": f"emp_{uuid.uuid4().hex[:6]}",
            "name": generate_employee_name(),
            "email": generate_email(),
            "hourly_base_rate": round(random.uniform(22.0, 35.0), 2),
            "max_hours_per_week": random.choice([20, 25, 30, 35, 38, 40]),
            "employment_type": random.choice(["casual", "part_time", "full_time"]),
            "award_level": "level_1",
            "state": "NSW",
            "skills": random.sample(["bar", "kitchen", "floor", "management"], k=random.randint(1, 3)),
        })

    demand = []
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        weekday = current_date.weekday()
        covers = 80 if weekday in [4, 5] else 50
        demand.append({
            "date": current_date.isoformat(),
            "expected_covers": covers + random.randint(-10, 10),
        })

    return {
        "venue_id": venue_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "employees": employees,
        "demand_forecasts": demand,
        "covers_per_staff": 12,
    }


def generate_tanda_webhook_payload(event_type: str):
    """Generate realistic Tanda webhook payloads."""
    payloads = {
        "user.created": {
            "event": "user.created",
            "timestamp": datetime.utcnow().isoformat(),
            "payload": {
                "user_id": f"tanda_user_{uuid.uuid4().hex[:6]}",
                "email": generate_email(),
                "name": generate_employee_name(),
            }
        },
        "shift.updated": {
            "event": "shift.updated",
            "timestamp": datetime.utcnow().isoformat(),
            "payload": {
                "shift_id": f"shift_{uuid.uuid4().hex[:6]}",
                "employee_id": f"emp_{uuid.uuid4().hex[:6]}",
                "date": (date.today() + timedelta(days=random.randint(1, 7))).isoformat(),
                "start_time": f"{random.randint(8, 20):02d}:00",
                "end_time": f"{random.randint(14, 23):02d}:00",
                "status": "published",
            }
        },
        "roster.published": {
            "event": "roster.published",
            "timestamp": datetime.utcnow().isoformat(),
            "payload": {
                "roster_id": f"roster_{uuid.uuid4().hex[:6]}",
                "venue_id": generate_venue_id(),
                "week_start": date.today().isoformat(),
                "status": "published",
            }
        },
    }
    return payloads.get(event_type, payloads["user.created"])


def generate_hmac_signature(payload: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature for Tanda webhook."""
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return signature


def generate_availability_payload():
    """Generate availability update payload."""
    availability = {}
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
        availability[day] = {
            "morning": random.choice([True, False]),
            "afternoon": random.choice([True, False]),
            "evening": random.choice([True, False]),
        }
    return {
        "availability": availability,
        "preferred_hours_per_week": random.choice([20, 25, 30, 35, 38, 40]),
    }


# ============================================================================
# RosterIQ Load Test User
# ============================================================================

class RosterIQUser(HttpUser):
    """
    Simulated RosterIQ user performing realistic workflows.

    Task weights:
    - Auth (3): Register, login, token refresh, profile
    - Venues (5): List venues, details, employees
    - Rosters (2): Generation, retrieval, analysis
    - Forecasts (4): Query and ingestion
    - Webhooks (3): Tanda event processing
    - Staff Portal (2): Shifts, pay, availability
    - Health Checks (1): /health, /ready, /metrics
    """

    wait_time = between(1, 3)  # 1-3 seconds between requests

    def on_start(self):
        """Initialize user: register and login."""
        self.auth_token = None
        self.user_id = None
        self.email = generate_email()
        self.registered = False
        self.venue_ids = []
        self.employee_ids = []
        self.roster_ids = []

        # Register user
        self._register_user()
        # Login to get token
        self._login_user()

    def on_stop(self):
        """Cleanup on user stop."""
        pass

    def _register_user(self):
        """Helper: Register a new user."""
        response = self.client.post(
            "/api/auth/register",
            json={
                "email": self.email,
                "password": "TestPassword123!",
                "name": generate_employee_name(),
            },
            name="/api/auth/register",
        )
        if response.status_code in [201, 200]:
            self.registered = True
            stats["successful_auths"] += 1
            try:
                data = response.json()
                self.user_id = data.get("id")
                self.auth_token = data.get("access_token")
            except:
                pass
        else:
            stats["failed_auths"] += 1

    def _login_user(self):
        """Helper: Login and get auth token."""
        response = self.client.post(
            "/api/auth/login",
            json={
                "email": self.email,
                "password": "TestPassword123!",
            },
            name="/api/auth/login",
        )
        if response.status_code == 200:
            try:
                data = response.json()
                self.auth_token = data.get("access_token")
                stats["successful_auths"] += 1
            except:
                pass
        else:
            stats["failed_auths"] += 1

    def _get_auth_headers(self):
        """Get headers with bearer token."""
        if self.auth_token:
            return {"Authorization": f"Bearer {self.auth_token}"}
        return {}

    # ========================================================================
    # Auth Flow Tasks (weight 3)
    # ========================================================================

    @task(3)
    class AuthTasks(object):
        """Authentication flow tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def register_new_user(self):
            """Register a new user."""
            self.parent.client.post(
                "/api/auth/register",
                json={
                    "email": generate_email(),
                    "password": "TestPassword123!",
                    "name": generate_employee_name(),
                },
                name="/api/auth/register (load)",
            )

        @task
        def login(self):
            """Login and get token."""
            self.parent.client.post(
                "/api/auth/login",
                json={
                    "email": self.parent.email,
                    "password": "TestPassword123!",
                },
                name="/api/auth/login (load)",
            )

        @task
        def refresh_token(self):
            """Refresh auth token (if available)."""
            if self.parent.auth_token:
                self.parent.client.post(
                    "/api/auth/refresh",
                    json={"refresh_token": self.parent.auth_token},
                    name="/api/auth/refresh",
                )

        @task
        def get_profile(self):
            """Get current user profile."""
            self.parent.client.get(
                "/api/auth/me",
                headers=self.parent._get_auth_headers(),
                name="/api/auth/me",
            )

    # ========================================================================
    # Venue Operations Tasks (weight 5)
    # ========================================================================

    @task(5)
    class VenueTasks(object):
        """Venue management tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def list_venues(self):
            """List all venues."""
            response = self.client.get(
                "/venues?limit=20",
                headers=self.parent._get_auth_headers(),
                name="/venues (list)",
            )
            if response.status_code == 200:
                try:
                    data = response.json()
                    venues = data.get("venues", [])
                    self.parent.venue_ids = [v.get("id") for v in venues if v.get("id")]
                except:
                    pass

        @task
        def get_venue_details(self):
            """Get details of a specific venue."""
            if self.parent.venue_ids:
                venue_id = random.choice(self.parent.venue_ids)
                self.client.get(
                    f"/venues/{venue_id}",
                    headers=self.parent._get_auth_headers(),
                    name="/venues/{venue_id}",
                )
            else:
                self.client.get(
                    f"/venues/{generate_venue_id()}",
                    headers=self.parent._get_auth_headers(),
                    name="/venues/{venue_id}",
                )

        @task
        def list_employees(self):
            """List employees."""
            response = self.client.get(
                "/employees?limit=50",
                headers=self.parent._get_auth_headers(),
                name="/employees (list)",
            )
            if response.status_code == 200:
                try:
                    data = response.json()
                    employees = data.get("employees", [])
                    self.parent.employee_ids = [e.get("id") for e in employees if e.get("id")]
                except:
                    pass

        @task
        def get_employee_details(self):
            """Get details of a specific employee."""
            if self.parent.employee_ids:
                emp_id = random.choice(self.parent.employee_ids)
                self.client.get(
                    f"/employees/{emp_id}",
                    headers=self.parent._get_auth_headers(),
                    name="/employees/{employee_id}",
                )

    # ========================================================================
    # Roster Generation Tasks (weight 2)
    # ========================================================================

    @task(2)
    class RosterTasks(object):
        """Roster generation and retrieval tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def generate_roster(self):
            """Generate a new roster."""
            venue_id = generate_venue_id()
            payload = generate_roster_payload(venue_id, num_employees=10, num_days=7)

            response = self.parent.client.post(
                "/rosters/generate",
                json=payload,
                headers=self.parent._get_auth_headers(),
                name="/rosters/generate",
            )
            if response.status_code in [200, 201]:
                try:
                    data = response.json()
                    if isinstance(data, dict) and "id" in data:
                        self.parent.roster_ids.append(data["id"])
                    stats["successful_rosters"] += 1
                except:
                    pass
            else:
                stats["failed_rosters"] += 1

        @task
        def list_rosters(self):
            """List all rosters."""
            response = self.parent.client.get(
                "/rosters?limit=20",
                headers=self.parent._get_auth_headers(),
                name="/rosters (list)",
            )
            if response.status_code == 200:
                try:
                    data = response.json()
                    rosters = data.get("rosters", [])
                    self.parent.roster_ids = [r.get("id") for r in rosters if r.get("id")]
                except:
                    pass

        @task
        def get_roster_details(self):
            """Get details of a specific roster."""
            if self.parent.roster_ids:
                roster_id = random.choice(self.parent.roster_ids)
                self.parent.client.get(
                    f"/rosters/{roster_id}",
                    headers=self.parent._get_auth_headers(),
                    name="/rosters/{roster_id}",
                )

        @task
        def analyze_roster(self):
            """Analyze a roster."""
            if self.parent.roster_ids:
                roster_id = random.choice(self.parent.roster_ids)
                self.parent.client.get(
                    f"/rosters/{roster_id}/analyse",
                    headers=self.parent._get_auth_headers(),
                    name="/rosters/{roster_id}/analyse",
                )

    # ========================================================================
    # Forecast Tasks (weight 4)
    # ========================================================================

    @task(4)
    class ForecastTasks(object):
        """Forecast query and ingestion tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def list_forecasts(self):
            """List forecasts."""
            start_date = (date.today() - timedelta(days=7)).isoformat()
            end_date = (date.today() + timedelta(days=7)).isoformat()
            self.parent.client.get(
                f"/forecasts?start_date={start_date}&end_date={end_date}&limit=100",
                headers=self.parent._get_auth_headers(),
                name="/forecasts (list)",
            )

        @task
        def add_forecasts(self):
            """Add new demand forecasts."""
            venue_id = generate_venue_id()
            payload = generate_forecast_payload(venue_id, num_days=7)

            self.parent.client.post(
                "/forecasts",
                json=payload,
                headers=self.parent._get_auth_headers(),
                name="/forecasts (add)",
            )

        @task
        def get_required_staff(self):
            """Get required staff based on forecasts."""
            venue_id = generate_venue_id()
            start_date = date.today().isoformat()
            self.parent.client.get(
                f"/forecasts/required-staff?venue_id={venue_id}&start_date={start_date}&covers_per_staff=12",
                headers=self.parent._get_auth_headers(),
                name="/forecasts/required-staff",
            )

    # ========================================================================
    # Webhook Ingestion Tasks (weight 3)
    # ========================================================================

    @task(3)
    class WebhookTasks(object):
        """Webhook processing tasks (Tanda)."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def post_tanda_webhook(self):
            """Post a Tanda webhook event."""
            event_type = random.choice(["user.created", "shift.updated", "roster.published"])
            payload = generate_tanda_webhook_payload(event_type)
            signature = generate_hmac_signature(payload, TANDA_WEBHOOK_SECRET)

            headers = {
                "x-tanda-signature": signature,
                "Content-Type": "application/json",
            }

            self.parent.client.post(
                "/tanda/webhook",
                json=payload,
                headers=headers,
                name="/tanda/webhook",
            )
            stats["webhook_signatures"] += 1

    # ========================================================================
    # Staff Portal Tasks (weight 2)
    # ========================================================================

    @task(2)
    class StaffPortalTasks(object):
        """Staff self-service portal tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def get_my_shifts(self):
            """Get staff member's upcoming shifts."""
            week_start = date.today().isoformat()
            self.parent.client.get(
                f"/api/staff/my-shifts?week_start={week_start}",
                headers=self.parent._get_auth_headers(),
                name="/api/staff/my-shifts",
            )

        @task
        def get_pay_estimate(self):
            """Get estimated pay for the week."""
            week_start = date.today().isoformat()
            self.parent.client.get(
                f"/api/staff/pay-estimate?week_start={week_start}",
                headers=self.parent._get_auth_headers(),
                name="/api/staff/pay-estimate",
            )

        @task
        def update_availability(self):
            """Update availability preferences."""
            payload = generate_availability_payload()
            self.parent.client.put(
                "/api/staff/availability",
                json=payload,
                headers=self.parent._get_auth_headers(),
                name="/api/staff/availability (update)",
            )

    # ========================================================================
    # Health Check Tasks (weight 1)
    # ========================================================================

    @task(1)
    class HealthCheckTasks(object):
        """Health and readiness check tasks."""

        def __init__(self, parent):
            self.parent = parent

        @task
        def check_health(self):
            """Check API health."""
            self.parent.client.get("/health", name="/health")

        @task
        def check_ready(self):
            """Check API readiness."""
            self.parent.client.get("/ready", name="/ready")

        @task
        def get_metrics(self):
            """Get API metrics."""
            self.parent.client.get("/metrics", name="/metrics")


# ============================================================================
# Event Handlers
# ============================================================================

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when test starts."""
    print("\n" + "="*80)
    print("RosterIQ Load Test Starting")
    print("="*80)
    print(f"Target: {environment.host}")
    print(f"Time: {datetime.utcnow().isoformat()}")
    print("="*80 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when test stops."""
    print("\n" + "="*80)
    print("RosterIQ Load Test Summary")
    print("="*80)
    print(f"Successful Auth Operations: {stats['successful_auths']}")
    print(f"Failed Auth Operations: {stats['failed_auths']}")
    print(f"Successful Roster Generations: {stats['successful_rosters']}")
    print(f"Failed Roster Generations: {stats['failed_rosters']}")
    print(f"Webhook Signatures Generated: {stats['webhook_signatures']}")
    print("="*80 + "\n")
