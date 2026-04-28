"""
RosterIQ Core Performance Benchmarks.

Measures performance of critical core operations in isolation:
- Roster optimization for varying employee counts
- Cost calculations
- Forecast ensemble methods
- Cryptographic operations (HMAC, JWT)
- Award rules penalty calculations

Run:
    python tests/benchmarks/bench_core.py
    python -m pytest tests/benchmarks/bench_core.py -v -s
"""

import sys
import time
import hmac
import hashlib
import json
import jwt
from datetime import date, datetime, timedelta, time as dtime
from decimal import Decimal
from typing import List, Dict, Tuple
import uuid

# Add parent to path for imports
sys.path.insert(0, '/sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ')

from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State, CostBreakdown,
)
from rosteriq.award_rules import (
    get_penalty_multiplier, get_day_type, validate_shift_compliance
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown, calculate_roster_cost
)
from rosteriq.ensemble import EnsembleForecaster
from rosteriq.roster_optimiser import generate_weekly_roster


# ============================================================================
# Benchmark Utilities
# ============================================================================

class BenchmarkResult:
    """Store and format benchmark results."""

    def __init__(self, name: str, iterations: int = 1):
        self.name = name
        self.iterations = iterations
        self.total_time = 0.0
        self.min_time = float('inf')
        self.max_time = 0.0
        self.times = []

    def record(self, elapsed: float):
        """Record a single iteration time."""
        self.total_time += elapsed
        self.min_time = min(self.min_time, elapsed)
        self.max_time = max(self.max_time, elapsed)
        self.times.append(elapsed)

    @property
    def avg_time(self) -> float:
        """Average time per iteration."""
        return self.total_time / self.iterations if self.iterations > 0 else 0.0

    @property
    def throughput(self) -> float:
        """Operations per second."""
        return self.iterations / self.total_time if self.total_time > 0 else 0.0

    def __repr__(self) -> str:
        return (
            f"{self.name:50s} | "
            f"Total: {self.total_time*1000:8.2f}ms | "
            f"Avg: {self.avg_time*1000:7.3f}ms | "
            f"Min: {self.min_time*1000:7.3f}ms | "
            f"Max: {self.max_time*1000:7.3f}ms | "
            f"Ops/sec: {self.throughput:8.1f}"
        )


def benchmark(func, *args, iterations: int = 1, **kwargs) -> Tuple[float, any]:
    """
    Benchmark a function call.

    Args:
        func: Function to benchmark
        *args: Positional arguments
        iterations: Number of times to run
        **kwargs: Keyword arguments

    Returns:
        Tuple of (elapsed_time, result)
    """
    result = None
    start = time.perf_counter()

    for _ in range(iterations):
        result = func(*args, **kwargs)

    elapsed = time.perf_counter() - start
    return elapsed, result


# ============================================================================
# Test Data Generators
# ============================================================================

def create_test_employees(count: int) -> List[Employee]:
    """Create test employee records."""
    employment_types = [EmploymentType.CASUAL, EmploymentType.PART_TIME, EmploymentType.FULL_TIME]
    award_levels = [AwardLevel.LEVEL_1, AwardLevel.LEVEL_2, AwardLevel.LEVEL_3]

    employees = []
    for i in range(count):
        emp = Employee(
            id=f"emp_{i:04d}",
            name=f"Employee {i}",
            email=f"emp{i}@test.local",
            phone=f"04{i:08d}",
            hourly_base_rate=Decimal("25.50"),
            max_hours_per_week=38 if i % 3 == 0 else (25 if i % 3 == 1 else 15),
            employment_type=employment_types[i % 3],
            award_level=award_levels[i % 3],
            state=State.NSW,
            skills=["bar", "kitchen"],
            availability={"Monday": True, "Tuesday": True, "Wednesday": True},
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        employees.append(emp)

    return employees


def create_test_shifts(
    employees: List[Employee], venue_id: str, start_date: date, num_days: int = 7
) -> List[Shift]:
    """Create test shift records."""
    shifts = []
    for day_offset in range(num_days):
        shift_date = start_date + timedelta(days=day_offset)

        # Spread shifts across employees
        for emp_idx in range(len(employees)):
            shift = Shift(
                id=str(uuid.uuid4()),
                venue_id=venue_id,
                employee_id=employees[emp_idx].id,
                date=shift_date,
                start_time=dtime(10, 0),
                end_time=dtime(17, 0),
                role="bar",
                status=ShiftStatus.SCHEDULED,
                notes="",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            shifts.append(shift)

    return shifts


def create_test_venue() -> VenueConfig:
    """Create a test venue config."""
    return VenueConfig(
        id="venue_test",
        name="Test Venue",
        location="Sydney, NSW",
        timezone="Australia/Sydney",
        award_level=AwardLevel.LEVEL_1,
        state=State.NSW,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


# ============================================================================
# Benchmark Suites
# ============================================================================

def benchmark_roster_optimization():
    """Benchmark roster optimization for varying employee counts."""
    print("\n" + "="*100)
    print("ROSTER OPTIMIZATION BENCHMARKS")
    print("="*100)

    results = []
    employee_counts = [5, 10, 20, 50]
    start_date = date.today()

    for emp_count in employee_counts:
        employees = create_test_employees(emp_count)
        venue = create_test_venue()

        # Create demand forecasts
        forecasts = []
        for i in range(7):
            f_date = start_date + timedelta(days=i)
            forecasts.append(
                DemandForecast(
                    id=str(uuid.uuid4()),
                    venue_id=venue.id,
                    date=f_date,
                    expected_covers=80.0,
                    confidence=0.85,
                    source="historical",
                    created_at=datetime.utcnow(),
                )
            )

        # Benchmark roster generation
        result = BenchmarkResult(
            f"Roster generation ({emp_count} employees, 7 days)",
            iterations=1
        )

        elapsed, roster = benchmark(
            generate_weekly_roster,
            venue_id=venue.id,
            start_date=start_date,
            employees=employees,
            demand_forecasts=forecasts,
            covers_per_staff=12,
            iterations=1
        )

        result.record(elapsed)
        results.append(result)
        print(result)

    return results


def benchmark_cost_calculations():
    """Benchmark cost calculation operations."""
    print("\n" + "="*100)
    print("COST CALCULATION BENCHMARKS")
    print("="*100)

    results = []
    shift_counts = [10, 100, 500, 1000]
    start_date = date.today()

    for shift_count in shift_counts:
        employees = create_test_employees(10)
        venue = create_test_venue()

        # Create test shifts
        shifts = []
        for i in range(shift_count):
            emp = employees[i % len(employees)]
            shift = Shift(
                id=str(uuid.uuid4()),
                venue_id=venue.id,
                employee_id=emp.id,
                date=start_date + timedelta(days=(i % 30)),
                start_time=dtime(10, 0),
                end_time=dtime(17, 0),
                role="bar",
                status=ShiftStatus.SCHEDULED,
                notes="",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            shifts.append(shift)

        # Benchmark cost calculation for all shifts
        result = BenchmarkResult(
            f"Cost calculation ({shift_count} shifts)",
            iterations=1
        )

        def calc_costs():
            total_cost = Decimal("0")
            for shift in shifts:
                emp = employees[int(shift.employee_id.split('_')[1]) % len(employees)]
                breakdown = calculate_shift_cost_breakdown(emp, shift, venue.state)
                total_cost += breakdown.total_cost
            return total_cost

        elapsed, total_cost = benchmark(calc_costs, iterations=1)
        result.record(elapsed)
        results.append(result)
        print(result)

    return results


def benchmark_forecast_ensemble():
    """Benchmark ensemble forecaster performance."""
    print("\n" + "="*100)
    print("FORECAST ENSEMBLE BENCHMARKS")
    print("="*100)

    results = []
    horizons = [7, 14, 30]

    for horizon in horizons:
        forecaster = EnsembleForecaster()

        # Create dummy historical data
        historical_covers = [80 + (i % 30) for i in range(180)]
        for i, covers in enumerate(historical_covers):
            date_val = date.today() - timedelta(days=180-i)
            forecaster.add_observation(date_val, covers)

        # Benchmark forecast generation
        result = BenchmarkResult(
            f"Ensemble forecast ({horizon}-day horizon)",
            iterations=10
        )

        elapsed, forecast = benchmark(
            forecaster.forecast,
            horizon=horizon,
            iterations=10
        )

        result.record(elapsed / 10)  # Normalize to per-iteration
        results.append(result)
        print(result)

    return results


def benchmark_hmac_signature():
    """Benchmark HMAC signature generation and verification."""
    print("\n" + "="*100)
    print("HMAC SIGNATURE BENCHMARKS")
    print("="*100)

    results = []
    secret = "test-webhook-secret-key"

    # Test payload
    payload = {
        "event": "shift.updated",
        "timestamp": datetime.utcnow().isoformat(),
        "payload": {
            "shift_id": str(uuid.uuid4()),
            "employee_id": str(uuid.uuid4()),
            "date": date.today().isoformat(),
        }
    }
    body = json.dumps(payload, separators=(",", ":")).encode()

    # Benchmark signature generation
    result_gen = BenchmarkResult("HMAC-SHA256 signature generation", iterations=1000)

    def gen_signature():
        return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    elapsed, sig = benchmark(gen_signature, iterations=1000)
    result_gen.record(elapsed / 1000)
    results.append(result_gen)
    print(result_gen)

    # Benchmark signature verification
    result_verify = BenchmarkResult("HMAC-SHA256 signature verification", iterations=1000)

    def verify_signature():
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, expected)

    elapsed, _ = benchmark(verify_signature, iterations=1000)
    result_verify.record(elapsed / 1000)
    results.append(result_verify)
    print(result_verify)

    return results


def benchmark_jwt_operations():
    """Benchmark JWT token generation and verification."""
    print("\n" + "="*100)
    print("JWT TOKEN BENCHMARKS")
    print("="*100)

    results = []
    secret_key = "test-jwt-secret-key-for-benchmarking"

    # Test payload
    payload = {
        "sub": "user_12345",
        "email": "user@test.local",
        "role": "owner",
        "exp": datetime.utcnow() + timedelta(hours=1),
    }

    # Benchmark token generation
    result_gen = BenchmarkResult("JWT token generation (HS256)", iterations=1000)

    def gen_token():
        return jwt.encode(payload, secret_key, algorithm="HS256")

    elapsed, token = benchmark(gen_token, iterations=1000)
    result_gen.record(elapsed / 1000)
    results.append(result_gen)
    print(result_gen)

    # Benchmark token verification
    result_verify = BenchmarkResult("JWT token verification (HS256)", iterations=1000)

    def verify_token():
        try:
            return jwt.decode(token, secret_key, algorithms=["HS256"])
        except:
            return None

    elapsed, _ = benchmark(verify_token, iterations=1000)
    result_verify.record(elapsed / 1000)
    results.append(result_verify)
    print(result_verify)

    return results


def benchmark_award_rules():
    """Benchmark award rules penalty calculations."""
    print("\n" + "="*100)
    print("AWARD RULES BENCHMARKS")
    print("="*100)

    results = []

    # Benchmark day type determination
    result_daytype = BenchmarkResult("Determine day type (penalty lookup)", iterations=10000)

    def get_day():
        test_date = date.today() + timedelta(days=(int(time.perf_counter() * 1000) % 365))
        return get_day_type(test_date, State.NSW)

    elapsed, _ = benchmark(get_day, iterations=10000)
    result_daytype.record(elapsed / 10000)
    results.append(result_daytype)
    print(result_daytype)

    # Benchmark penalty multiplier calculation
    result_penalty = BenchmarkResult("Calculate penalty multiplier", iterations=10000)

    def get_multiplier():
        emp_type = [EmploymentType.CASUAL, EmploymentType.PART_TIME, EmploymentType.FULL_TIME][
            int(time.perf_counter() * 1000) % 3
        ]
        day_type = get_day_type(date.today(), State.NSW)
        return get_penalty_multiplier(emp_type, day_type)

    elapsed, _ = benchmark(get_multiplier, iterations=10000)
    result_penalty.record(elapsed / 10000)
    results.append(result_penalty)
    print(result_penalty)

    # Benchmark shift compliance validation
    employees = create_test_employees(5)
    shifts = create_test_shifts(employees, "venue_test", date.today(), num_days=7)

    result_compliance = BenchmarkResult("Validate shift compliance", iterations=100)

    def validate():
        return validate_shift_compliance(shifts[0], employees[0], [])

    elapsed, _ = benchmark(validate, iterations=100)
    result_compliance.record(elapsed / 100)
    results.append(result_compliance)
    print(result_compliance)

    return results


# ============================================================================
# Main
# ============================================================================

def main():
    """Run all benchmarks."""
    print("\n")
    print("#" * 100)
    print("# RosterIQ PERFORMANCE BENCHMARKS")
    print("#" * 100)

    all_results = []

    try:
        # Run benchmark suites
        all_results.extend(benchmark_roster_optimization())
        all_results.extend(benchmark_cost_calculations())
        all_results.extend(benchmark_forecast_ensemble())
        all_results.extend(benchmark_hmac_signature())
        all_results.extend(benchmark_jwt_operations())
        all_results.extend(benchmark_award_rules())

        # Summary
        print("\n" + "="*100)
        print("TARGET PERFORMANCE METRICS")
        print("="*100)
        print("Roster generation (10 employees, 7 days):  < 2000 ms")
        print("Cost calculation (100 shifts):             < 100 ms")
        print("Cost calculation (500 shifts):             < 500 ms")
        print("HMAC signature verification:               < 1 ms per check")
        print("JWT sign + verify:                         < 5 ms per pair")
        print("Day type determination:                    < 0.1 ms")
        print("Penalty multiplier calculation:            < 0.1 ms")
        print("="*100)
        print("\n")

    except Exception as e:
        print(f"\nBenchmark failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
