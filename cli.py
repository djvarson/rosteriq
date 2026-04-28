"""
RosterIQ CLI — main entry point for roster generation and management.

Usage:
    python -m rosteriq generate --config venue.json --forecast forecast.csv
    python -m rosteriq analyse --roster roster.json
    python -m rosteriq import-pos --file sales.csv --system auto
    python -m rosteriq demo
"""

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from rosteriq import __version__
from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
)
from rosteriq.roster_optimiser import (
    generate_weekly_roster, analyse_roster, suggest_improvements,
    DEFAULT_COVERS_PER_STAFF,
)
from rosteriq.cost_calculator import calculate_shift_cost_breakdown
from rosteriq.award_rules import get_day_type, get_penalty_multiplier


# ============================================================================
# JSON serialisation helpers
# ============================================================================

class RosterIQEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal, date, time, datetime, and Enums."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        if hasattr(obj, "value"):  # Enum
            return obj.value
        return super().default(obj)


def _json_dumps(data, indent=2):
    return json.dumps(data, cls=RosterIQEncoder, indent=indent)


# ============================================================================
# Demo data — generates a realistic venue for testing
# ============================================================================

def _build_demo_employees() -> list[Employee]:
    """Create a realistic 12-person employee pool for a pub."""
    now = datetime.now()
    employees = [
        # Full-time
        Employee(
            id="ft-sarah", name="Sarah Mitchell",
            employment_type=EmploymentType.full_time,
            award_level=AwardLevel.level_3, state=State.vic,
            hourly_base_rate=Decimal("31.50"),
            skills=["bar", "floor", "close"], max_hours_per_week=38.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="ft-james", name="James Chen",
            employment_type=EmploymentType.full_time,
            award_level=AwardLevel.level_2, state=State.vic,
            hourly_base_rate=Decimal("29.50"),
            skills=["bar", "floor"], max_hours_per_week=38.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="ft-priya", name="Priya Sharma",
            employment_type=EmploymentType.full_time,
            award_level=AwardLevel.level_3, state=State.vic,
            hourly_base_rate=Decimal("31.50"),
            skills=["bar", "floor", "open", "close"], max_hours_per_week=38.0,
            created_at=now, updated_at=now,
        ),
        # Part-time
        Employee(
            id="pt-tom", name="Tom Williams",
            employment_type=EmploymentType.part_time,
            award_level=AwardLevel.level_2, state=State.vic,
            hourly_base_rate=Decimal("29.50"),
            skills=["floor"], max_hours_per_week=25.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="pt-lily", name="Lily Nguyen",
            employment_type=EmploymentType.part_time,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["bar", "floor"], max_hours_per_week=20.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="pt-marcus", name="Marcus Brown",
            employment_type=EmploymentType.part_time,
            award_level=AwardLevel.level_2, state=State.vic,
            hourly_base_rate=Decimal("29.50"),
            skills=["bar", "close"], max_hours_per_week=25.0,
            created_at=now, updated_at=now,
        ),
        # Casual
        Employee(
            id="cas-emma", name="Emma Taylor",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["floor"], max_hours_per_week=38.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="cas-jake", name="Jake O'Brien",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["bar", "floor"], max_hours_per_week=38.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="cas-zoe", name="Zoe Park",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["floor"], max_hours_per_week=25.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="cas-ryan", name="Ryan Cooper",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["bar"], max_hours_per_week=20.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="cas-mia", name="Mia Johnson",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1, state=State.vic,
            hourly_base_rate=Decimal("28.50"),
            skills=["floor", "bar"], max_hours_per_week=30.0,
            created_at=now, updated_at=now,
        ),
        Employee(
            id="cas-liam", name="Liam Davis",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_2, state=State.vic,
            hourly_base_rate=Decimal("29.50"),
            skills=["bar", "close"], max_hours_per_week=20.0,
            created_at=now, updated_at=now,
        ),
    ]
    return employees


def _build_demo_forecasts(week_start: date) -> list[DemandForecast]:
    """Create a week of realistic demand forecasts for a pub."""
    # Covers by day of week (Mon=0)
    daily_base = {0: 80, 1: 90, 2: 100, 3: 120, 4: 180, 5: 200, 6: 140}
    # Hourly distribution — % of daily total per hour (10am-midnight)
    hourly_pct = {
        10: 0.02, 11: 0.05, 12: 0.12, 13: 0.10, 14: 0.04, 15: 0.03,
        16: 0.04, 17: 0.08, 18: 0.14, 19: 0.15, 20: 0.10, 21: 0.07,
        22: 0.04, 23: 0.02,
    }

    forecasts = []
    for day_offset in range(7):
        d = week_start + timedelta(days=day_offset)
        base = daily_base[day_offset]
        for hour, pct in hourly_pct.items():
            covers = round(base * pct, 1)
            if covers < 1:
                covers = 1.0
            forecasts.append(DemandForecast(
                id=f"fc-{d}-{hour}",
                venue_id="demo-venue",
                date=d,
                hour=hour,
                predicted_covers=covers,
                confidence=0.8,
                model_version="demo-v1",
            ))
    return forecasts


def _build_demo_venue() -> VenueConfig:
    return VenueConfig(
        id="demo-venue", name="The Royal Oak",
        tanda_org_id="demo-org", state=State.vic,
        timezone="Australia/Melbourne",
        min_staff={"bar": 1, "floor": 2},
        max_labour_pct=32.0,
        pos_system="lightspeed",
        created_at=datetime.now(),
    )


# ============================================================================
# CLI commands
# ============================================================================

def cmd_demo(args):
    """Run a full demo: generate roster, analyse, suggest improvements."""
    print(f"\n{'='*60}")
    print(f"  RosterIQ v{__version__} — Demo Mode")
    print(f"{'='*60}\n")

    # Determine week start (next Monday)
    today = date.today()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    week_start = today + timedelta(days=days_until_monday)
    week_end = week_start + timedelta(days=6)

    print(f"Venue:        The Royal Oak (VIC)")
    print(f"Roster week:  {week_start.strftime('%a %d %b')} — {week_end.strftime('%a %d %b %Y')}")
    print()

    # Build data
    employees = _build_demo_employees()
    venue = _build_demo_venue()
    forecasts = _build_demo_forecasts(week_start)

    print(f"Employees:    {len(employees)} ({sum(1 for e in employees if e.employment_type == EmploymentType.full_time)} FT, "
          f"{sum(1 for e in employees if e.employment_type == EmploymentType.part_time)} PT, "
          f"{sum(1 for e in employees if e.employment_type == EmploymentType.casual)} Casual)")
    print(f"Forecast:     {len(forecasts)} hourly predictions across 7 days")
    print()

    # Generate roster
    print("Generating optimised roster...", end=" ", flush=True)
    roster = generate_weekly_roster(week_start, forecasts, employees, venue)
    print(f"Done! ({len(roster.shifts)} shifts)")
    print()

    # Analyse
    emp_dict = {e.id: e for e in employees}
    analysis = analyse_roster(roster, emp_dict, venue.state)

    print(f"{'─'*45}")
    print(f"  ROSTER SUMMARY")
    print(f"{'─'*45}")
    print(f"  Total shifts:       {analysis['total_shifts']}")
    print(f"  Total hours:        {analysis['total_hours']:.1f}h")
    print(f"  Total cost:         ${analysis['total_cost']:,.2f}")
    print(f"  Avg cost/hour:      ${analysis['avg_cost_per_hour']:,.2f}")
    print(f"  Unique employees:   {analysis['unique_employees']}")
    print(f"  Compliance clean:   {'Yes' if analysis['compliance_clean'] else 'No — see issues below'}")
    print()

    # Cost by day
    print(f"  Cost by day:")
    for d in sorted(analysis["cost_by_day"]):
        day_name = d.strftime("%a %d %b")
        cost = analysis["cost_by_day"][d]
        shifts_count = analysis["shifts_per_day"].get(d, 0)
        print(f"    {day_name}:  ${cost:>8,.2f}  ({shifts_count} shifts)")
    print()

    # Employee hours
    print(f"  Employee hours:")
    for emp_id, hours in sorted(analysis["employee_hours"].items(),
                                 key=lambda x: x[1], reverse=True):
        emp = emp_dict.get(emp_id)
        if emp:
            emp_type = emp.employment_type.value[:3].upper()
            print(f"    {emp.name:<20s} [{emp_type}] {hours:>5.1f}h / {emp.max_hours_per_week:.0f}h")
    print()

    # Compliance issues
    if analysis["compliance_issues"]:
        print(f"  Compliance issues ({len(analysis['compliance_issues'])}):")
        for issue in analysis["compliance_issues"][:5]:
            print(f"    - {issue['employee']} on {issue['date']}: {issue['violation']}")
        if len(analysis["compliance_issues"]) > 5:
            print(f"    ... and {len(analysis['compliance_issues']) - 5} more")
        print()

    # Suggestions
    suggestions = suggest_improvements(roster, forecasts, emp_dict, venue.state)
    if suggestions:
        print(f"  Improvement suggestions ({len(suggestions)}):")
        for s in suggestions[:8]:
            print(f"    - {s}")
        if len(suggestions) > 8:
            print(f"    ... and {len(suggestions) - 8} more")
    else:
        print("  No improvement suggestions — roster looks well-optimised!")
    print()

    # Export option
    if args.output:
        output_path = Path(args.output)
        roster_data = {
            "roster": roster.model_dump(),
            "analysis": {k: v for k, v in analysis.items()
                         if k != "compliance_issues"},
            "suggestions": suggestions[:20],
        }
        output_path.write_text(_json_dumps(roster_data))
        print(f"Roster exported to: {output_path}")
    else:
        print("Tip: use --output roster.json to export the full roster data.")

    print()
    return 0


def cmd_generate(args):
    """Generate a roster from config and forecast files."""
    # Load venue config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        return 1

    with open(config_path) as f:
        config_data = json.load(f)

    venue = VenueConfig(**config_data["venue"])
    employees = [Employee(**e) for e in config_data["employees"]]

    # Load forecasts
    forecast_path = Path(args.forecast)
    if not forecast_path.exists():
        print(f"Error: forecast file not found: {forecast_path}", file=sys.stderr)
        return 1

    with open(forecast_path) as f:
        forecast_data = json.load(f)

    forecasts = [DemandForecast(**fc) for fc in forecast_data]

    # Determine week start
    if args.week_start:
        week_start = date.fromisoformat(args.week_start)
    else:
        week_start = min(fc.date for fc in forecasts)

    print(f"Generating roster for week of {week_start}...")
    roster = generate_weekly_roster(
        week_start, forecasts, employees, venue,
        covers_per_staff=args.covers_per_staff or DEFAULT_COVERS_PER_STAFF,
    )

    # Output
    output = roster.model_dump()
    if args.output:
        Path(args.output).write_text(_json_dumps(output))
        print(f"Roster saved to: {args.output}")
    else:
        print(_json_dumps(output))

    return 0


def cmd_analyse(args):
    """Analyse an existing roster JSON file."""
    roster_path = Path(args.roster)
    if not roster_path.exists():
        print(f"Error: roster file not found: {roster_path}", file=sys.stderr)
        return 1

    with open(roster_path) as f:
        data = json.load(f)

    # Reconstruct objects
    roster = Roster(**data["roster"]) if "roster" in data else Roster(**data)
    emp_dict = {}
    if "employees" in data:
        for e in data["employees"]:
            emp = Employee(**e)
            emp_dict[emp.id] = emp

    state = State(args.state) if args.state else State.vic
    analysis = analyse_roster(roster, emp_dict, state)
    print(_json_dumps(analysis))
    return 0


def cmd_version(args):
    """Print version."""
    print(f"RosterIQ v{__version__}")
    return 0


# ============================================================================
# Argument parser
# ============================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rosteriq",
        description="RosterIQ — AI-powered predictive rostering for Australian hospitality",
    )
    parser.add_argument("--version", action="store_true", help="Show version")
    subparsers = parser.add_subparsers(dest="command")

    # demo
    demo_parser = subparsers.add_parser("demo", help="Run a demo with sample data")
    demo_parser.add_argument("--output", "-o", help="Export roster to JSON file")

    # generate
    gen_parser = subparsers.add_parser("generate", help="Generate a roster")
    gen_parser.add_argument("--config", "-c", required=True, help="Venue config JSON file")
    gen_parser.add_argument("--forecast", "-f", required=True, help="Forecast JSON file")
    gen_parser.add_argument("--week-start", help="Week start date (YYYY-MM-DD)")
    gen_parser.add_argument("--covers-per-staff", type=float, help="Covers per staff ratio")
    gen_parser.add_argument("--output", "-o", help="Output JSON file")

    # analyse
    analyse_parser = subparsers.add_parser("analyse", help="Analyse an existing roster")
    analyse_parser.add_argument("--roster", "-r", required=True, help="Roster JSON file")
    analyse_parser.add_argument("--state", help="Australian state (default: vic)")

    return parser


def main(argv=None):
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        return cmd_version(args)

    commands = {
        "demo": cmd_demo,
        "generate": cmd_generate,
        "analyse": cmd_analyse,
    }

    if args.command in commands:
        return commands[args.command](args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
