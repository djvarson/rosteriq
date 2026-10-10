"""
Pay basis: hourly or salaried.

A salaried person (Employee.pay_basis == "salaried" with a weekly_salary) is
paid the same every week whatever hours they work, so:

  * Labour planning (shift costs, roster totals, snapshot labour %, the Today
    briefing) charges weekly_salary / 7 for each calendar day they are
    rostered, plus super on it — once per day however many shifts that day
    has. A day they are not rostered carries none of it, so a manager
    rostered five days shows 5/7 of the week in rostered labour; the full
    week appears in payroll prep. Per rostered day (not per week) keeps a
    roster's total equal to the sum of its days.
  * Salaried people never attract penalty rates, casual loading or overtime
    pricing.
  * Payroll prep shows them as one fixed salary line: weekly_salary / 7 for
    each day of the pay period, punches or not.

hourly_base_rate stays on the record as their planning rate for any reader
that still needs an hourly figure. Whether a salary is award-compliant is the
payroll advisor's call; RosterIQ only uses the figure the venue recorded.

Anyone without a recorded weekly_salary is costed hourly, whatever the
pay_basis field says.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

_CENT = Decimal("0.01")


def is_salaried(employee) -> bool:
    if employee is None:
        return False
    if str(getattr(employee, "pay_basis", "hourly") or "hourly").lower() != "salaried":
        return False
    salary = getattr(employee, "weekly_salary", None)
    try:
        return salary is not None and Decimal(str(salary)) > 0
    except Exception:
        return False


def weekly_salary(employee) -> Optional[Decimal]:
    if not is_salaried(employee):
        return None
    return Decimal(str(employee.weekly_salary))


def salary_per_day(employee) -> Decimal:
    """weekly_salary / 7, to the cent (zero for anyone hourly)."""
    salary = weekly_salary(employee)
    if salary is None:
        return Decimal("0.00")
    return (salary / Decimal("7")).quantize(_CENT, rounding=ROUND_HALF_UP)


def salary_for_days(employee, days: int) -> Decimal:
    """The salary for a run of calendar days (a pay period)."""
    salary = weekly_salary(employee)
    if salary is None or days <= 0:
        return Decimal("0.00")
    return (salary * Decimal(days) / Decimal("7")).quantize(_CENT, rounding=ROUND_HALF_UP)
