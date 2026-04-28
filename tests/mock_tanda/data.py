"""
Mock Tanda test data — realistic Australian hospitality venues and employees.

Contains 3 venues with 15 employees, sample rosters, and timesheets.
"""

from datetime import datetime, date, timedelta, time
from decimal import Decimal
from typing import Dict, List, Any
import uuid

# AU Hospitality award rates (2026 approximate) — Level 1 minimum wage
AWARD_RATES = {
    "barista": Decimal("27.50"),      # Level 2
    "bartender": Decimal("29.30"),    # Level 3
    "waiter": Decimal("25.10"),       # Level 1
    "chef": Decimal("31.50"),         # Level 4
    "head_chef": Decimal("35.00"),    # Level 5
    "manager": Decimal("38.50"),      # Level 6
    "kitchen_hand": Decimal("23.90"), # Level 1
}

# Realistic AU names
FIRST_NAMES = [
    "James", "Jessica", "Michael", "Emma", "David",
    "Sarah", "Chris", "Sophie", "Daniel", "Olivia",
    "Alex", "Mia", "Jordan", "Ava", "Taylor",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones",
    "Miller", "Davis", "Wilson", "Moore", "Taylor",
    "Anderson", "Thomas", "Jackson", "White", "Harris",
]

VENUES = [
    {
        "id": "venue_001",
        "name": "The Tipsy Koala",
        "type": "bar",
        "location": "Melbourne, VIC",
    },
    {
        "id": "venue_002",
        "name": "Salt & Pepper Café",
        "type": "cafe",
        "location": "Melbourne, VIC",
    },
    {
        "id": "venue_003",
        "name": "Flame & Vine",
        "type": "restaurant",
        "location": "Melbourne, VIC",
    },
]

DEPARTMENTS = [
    {"id": "dept_001", "name": "FOH", "venue_id": "venue_001"},  # Bar
    {"id": "dept_002", "name": "BOH", "venue_id": "venue_001"},  # Bar kitchen
    {"id": "dept_003", "name": "FOH", "venue_id": "venue_002"},  # Café
    {"id": "dept_004", "name": "BOH", "venue_id": "venue_002"},  # Café kitchen
    {"id": "dept_005", "name": "FOH", "venue_id": "venue_003"},  # Restaurant
    {"id": "dept_006", "name": "BOH", "venue_id": "venue_003"},  # Restaurant kitchen
    {"id": "dept_007", "name": "Management", "venue_id": "venue_001"},
    {"id": "dept_008", "name": "Management", "venue_id": "venue_002"},
    {"id": "dept_009", "name": "Management", "venue_id": "venue_003"},
]


def _generate_employees() -> List[Dict[str, Any]]:
    """Generate 15 realistic employees across 3 venues."""
    employees = []
    employee_id = 1000

    # Bar employees (5)
    bar_roles = [
        ("bartender", "dept_001"),
        ("bartender", "dept_001"),
        ("waiter", "dept_001"),
        ("kitchen_hand", "dept_002"),
        ("manager", "dept_007"),
    ]

    for role, dept_id in bar_roles:
        employees.append({
            "id": employee_id,
            "name": f"{FIRST_NAMES[employee_id % len(FIRST_NAMES)]} {LAST_NAMES[employee_id % len(LAST_NAMES)]}",
            "email": f"emp{employee_id}@tippykoala.com",
            "phone": f"04{20000000 + employee_id}",
            "employment_type": "casual" if employee_id % 3 == 0 else "part_time",
            "hourly_rate": float(AWARD_RATES[role]),
            "base_hourly_rate": float(AWARD_RATES[role]),
            "department": role,
            "qualifications": [role],
            "date_of_birth": (datetime.now() - timedelta(days=365 * (25 + (employee_id % 30)))).date().isoformat(),
            "role": role,
        })
        employee_id += 1

    # Café employees (5)
    cafe_roles = [
        ("barista", "dept_003"),
        ("barista", "dept_003"),
        ("waiter", "dept_003"),
        ("kitchen_hand", "dept_004"),
        ("manager", "dept_008"),
    ]

    for role, dept_id in cafe_roles:
        employees.append({
            "id": employee_id,
            "name": f"{FIRST_NAMES[employee_id % len(FIRST_NAMES)]} {LAST_NAMES[employee_id % len(LAST_NAMES)]}",
            "email": f"emp{employee_id}@saltpeppercafe.com",
            "phone": f"04{20000000 + employee_id}",
            "employment_type": "casual" if employee_id % 2 == 0 else "part_time",
            "hourly_rate": float(AWARD_RATES[role]),
            "base_hourly_rate": float(AWARD_RATES[role]),
            "department": role,
            "qualifications": [role],
            "date_of_birth": (datetime.now() - timedelta(days=365 * (22 + (employee_id % 25)))).date().isoformat(),
            "role": role,
        })
        employee_id += 1

    # Restaurant employees (5)
    restaurant_roles = [
        ("chef", "dept_006"),
        ("waiter", "dept_005"),
        ("waiter", "dept_005"),
        ("head_chef", "dept_006"),
        ("manager", "dept_009"),
    ]

    for role, dept_id in restaurant_roles:
        employees.append({
            "id": employee_id,
            "name": f"{FIRST_NAMES[employee_id % len(FIRST_NAMES)]} {LAST_NAMES[employee_id % len(LAST_NAMES)]}",
            "email": f"emp{employee_id}@flameandvine.com",
            "phone": f"04{20000000 + employee_id}",
            "employment_type": "full_time" if role in ["chef", "head_chef"] else "casual",
            "hourly_rate": float(AWARD_RATES[role]),
            "base_hourly_rate": float(AWARD_RATES[role]),
            "department": role,
            "qualifications": [role],
            "date_of_birth": (datetime.now() - timedelta(days=365 * (28 + (employee_id % 20)))).date().isoformat(),
            "role": role,
        })
        employee_id += 1

    return employees


def _generate_shift(
    employee_id: int,
    shift_date: date,
    start_time: time,
    end_time: time,
    break_minutes: int = 0,
    status: str = "pending",
) -> Dict[str, Any]:
    """Generate a single shift."""
    shift_id = int(f"{employee_id}{shift_date.strftime('%Y%m%d')}")

    start_dt = datetime.combine(shift_date, start_time)
    end_dt = datetime.combine(shift_date, end_time)

    # Handle overnight shifts
    if end_time < start_time:
        end_dt += timedelta(days=1)

    return {
        "id": shift_id,
        "user_id": employee_id,
        "employee_id": employee_id,
        "date": shift_date.isoformat(),
        "start": int(start_dt.timestamp()),
        "finish": int(end_dt.timestamp()),
        "start_time": start_dt.isoformat(),
        "end_time": end_dt.isoformat(),
        "end": int(end_dt.timestamp()),
        "status": status,
        "breaks": [{"start": 0, "finish": break_minutes * 60}] if break_minutes > 0 else [],
        "department": "general",
        "role": "general",
    }


def _generate_rosters_for_week(week_start: date) -> List[Dict[str, Any]]:
    """Generate realistic rosters for a week."""
    employees = _generate_employees()
    rosters = []

    # Assign employees to venues and shifts
    bar_employees = [e["id"] for e in employees[:5]]
    cafe_employees = [e["id"] for e in employees[5:10]]
    restaurant_employees = [e["id"] for e in employees[10:15]]

    # Realistic shift patterns per venue type
    # Bar: 10am-6pm, 4pm-midnight, 8pm-2am
    # Café: 6am-2pm, 10am-6pm, 2pm-close (3pm)
    # Restaurant: 11am-3pm, 5pm-11pm (2-3 staff per service)

    for day_offset in range(7):
        shift_date = week_start + timedelta(days=day_offset)

        # Bar shifts
        for emp_id in bar_employees[:2]:  # 2-3 staff per shift
            if day_offset < 3:
                rosters.append(_generate_shift(emp_id, shift_date, time(10, 0), time(18, 0), 30))
            else:
                rosters.append(_generate_shift(emp_id, shift_date, time(16, 0), time(23, 59), 30))

        # Café shifts
        for emp_id in cafe_employees[:2]:
            if day_offset < 2:
                rosters.append(_generate_shift(emp_id, shift_date, time(6, 0), time(14, 0), 30))
            elif day_offset < 4:
                rosters.append(_generate_shift(emp_id, shift_date, time(10, 0), time(18, 0), 30))
            else:
                rosters.append(_generate_shift(emp_id, shift_date, time(14, 0), time(21, 0), 30))

        # Restaurant shifts
        for emp_id in restaurant_employees[:2]:
            if day_offset in [0, 1, 2]:  # Lunch
                rosters.append(_generate_shift(emp_id, shift_date, time(11, 0), time(15, 0), 15))
            if day_offset >= 2:  # Dinner
                rosters.append(_generate_shift(emp_id, shift_date, time(17, 0), time(23, 0), 30))

    return rosters


def generate_mock_data() -> Dict[str, Any]:
    """Generate complete mock Tanda data for testing."""
    week_start = date.today() - timedelta(days=date.today().weekday())  # Monday of current week

    return {
        "venues": VENUES,
        "departments": DEPARTMENTS,
        "employees": _generate_employees(),
        "shifts": _generate_rosters_for_week(week_start),
        "rosters": [
            {
                "id": f"roster_{week_start.isoformat()}",
                "week_start": week_start.isoformat(),
                "week_end": (week_start + timedelta(days=6)).isoformat(),
                "shifts": _generate_rosters_for_week(week_start),
                "status": "draft",
            }
        ],
        "timesheets": _generate_timesheets(_generate_employees(), week_start),
        "webhooks": [],
    }


def _generate_timesheets(employees: List[Dict[str, Any]], week_start: date) -> List[Dict[str, Any]]:
    """Generate timesheets with realistic clock-in/out variance."""
    timesheets = []

    for emp in employees:
        emp_id = emp["id"]
        for day_offset in range(7):
            shift_date = week_start + timedelta(days=day_offset)

            # Simulate clock-in/out with ±5 min variance
            variance = (emp_id % 10 - 5) * 60  # -300 to +300 seconds

            timesheets.append({
                "id": int(f"{emp_id}{shift_date.strftime('%Y%m%d')}ts"),
                "user_id": emp_id,
                "employee_id": emp_id,
                "date": shift_date.isoformat(),
                "clock_in": int(datetime.combine(shift_date, time(9, 0)).timestamp()) + variance,
                "clock_out": int(datetime.combine(shift_date, time(17, 0)).timestamp()) + variance,
                "status": "approved" if day_offset < 5 else "pending",
            })

    return timesheets
