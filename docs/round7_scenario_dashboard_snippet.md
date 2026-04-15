# Scenario Solver Dashboard Widget — Round 7 Track A

## Overview

The Scenario Solver is a bidirectional wage-cost calculator integrated into the RosterIQ Roster Maker dashboard (L2+ access level). It answers two critical questions:

1. **"I want an 18% wage cost. What sales do I need?"** (solve_sales mode)
2. **"Forecast is $28k tonight. What's my wage budget and staffing?"** (solve_wage_budget mode)
3. **"Given my current roster and forecast, what's my wage %?"** (diagnose mode)

The underlying math lives in `rosteriq/scenario_solver.py` (pure Decimal arithmetic, zero DB, zero HTTP). The dashboard widget at `/api/v1/scenarios/wage-cost` calls this module, with a client-side fallback for network resilience.

## Architecture

### Backend

**Module:** `rosteriq/scenario_solver.py`

Three public functions:
- `solve_required_sales(wage_cost, target_pct, forecast_sales=None) -> ScenarioResult`
- `solve_wage_budget(forecast_sales, target_pct, blended_hourly_rate=None, on_cost_multiplier=1.165, planned_wage_cost=None) -> ScenarioResult`
- `diagnose(wage_cost, forecast_sales, target_pct=None) -> ScenarioResult`

All return a `ScenarioResult` dataclass with:
- `mode`: "solve_sales" | "solve_wage_budget" | "diagnose"
- `target_wage_cost_pct`: normalized fraction (0.18)
- `inputs`: dict of inputs supplied
- `outputs`: dict of computed results
- `assumptions`: list of assumed premises
- `warnings`: list of guardrail alerts (e.g., "wage% > 80%")
- `suggestions`: list of actionable recommendations

**Router:** `rosteriq/api_v2.py` line 1004+

```
POST /api/v1/scenarios/wage-cost
```

- Requires L2_ROSTER_MAKER access (or demo mode with no auth)
- Accepts ScenarioSolveRequest (mode, target_wage_cost_pct, wage_cost, forecast_sales, blended_hourly_rate, on_cost_multiplier, planned_wage_cost)
- Returns ScenarioSolveResponse (mirrors ScenarioResult.to_dict())

### Frontend

**Location:** `static/dashboard.html` lines 5399–5441 (widget HTML) + 2127–2326 (CSS) + 6428–6599 (JS)

The widget is already fully built and integrated into the Roster Maker dashboard (L2 view). It includes:

1. **Mode tabs** at the top: "Sales needed" | "Wage budget" | "Diagnose"
2. **Conditional form fields** that show/hide based on mode
3. **Input validation** (client-side: prevents empty required fields)
4. **Result panel** that renders JSON from the server with formatting

The widget gracefully handles network failures with a client-side fallback (`solveScenarioLocally`).

## HTML Widget Specification

### Location in Dashboard

The widget is placed in `static/dashboard.html` within the Roster Maker view (`role-l2` elements, visible only for L2+ users).

**Container:** `<div class="scenario-card role-l2" id="scenarioCard">`

### Modes & Required Fields

| Mode | Required Fields |
|------|-----------------|
| `solve_sales` | target_wage_cost_pct, wage_cost |
| `solve_wage_budget` | target_wage_cost_pct, forecast_sales |
| `diagnose` | wage_cost, forecast_sales |

Optional fields (all modes):
- `forecast_sales` (solve_sales) — for gap analysis
- `blended_hourly_rate`, `on_cost_multiplier`, `planned_wage_cost` (solve_wage_budget)

### HTML Structure

```html
<!-- Scenario Solver: POST /api/v1/scenarios/wage-cost (L2 + OWNER) -->
<div class="scenario-card role-l2" id="scenarioCard">
    <div class="scenario-header">
        <span class="scenario-title">Scenario Solver</span>
        <div class="scenario-mode-tabs" id="scenarioModeTabs">
            <button type="button" class="scenario-mode-tab active" data-mode="solve_sales">Sales needed</button>
            <button type="button" class="scenario-mode-tab" data-mode="solve_wage_budget">Wage budget</button>
            <button type="button" class="scenario-mode-tab" data-mode="diagnose">Diagnose</button>
        </div>
    </div>
    <form class="scenario-form" id="scenarioForm" onsubmit="event.preventDefault(); solveScenario();">
        <!-- Fields will be shown/hidden based on mode -->
        <div class="scenario-field" id="scenarioFieldTarget">
            <label class="scenario-field-label" for="scenarioTargetPct">Target wage % (e.g. 18)</label>
            <input id="scenarioTargetPct" type="number" step="0.1" placeholder="18" />
        </div>
        <div class="scenario-field" id="scenarioFieldWageCost">
            <label class="scenario-field-label" for="scenarioWageCost">Wage cost ($, loaded)</label>
            <input id="scenarioWageCost" type="number" step="1" placeholder="4500" />
        </div>
        <div class="scenario-field" id="scenarioFieldForecastSales">
            <label class="scenario-field-label" for="scenarioForecastSales">Forecast sales ($)</label>
            <input id="scenarioForecastSales" type="number" step="1" placeholder="25000" />
        </div>
        <div class="scenario-field" id="scenarioFieldBlendedRate">
            <label class="scenario-field-label" for="scenarioBlendedRate">Blended hourly rate ($)</label>
            <input id="scenarioBlendedRate" type="number" step="0.5" placeholder="35" />
        </div>
        <div class="scenario-field" id="scenarioFieldOnCost">
            <label class="scenario-field-label" for="scenarioOnCost">On-cost multiplier</label>
            <input id="scenarioOnCost" type="number" step="0.01" placeholder="1.165" />
        </div>
        <div class="scenario-field" id="scenarioFieldPlanned">
            <label class="scenario-field-label" for="scenarioPlanned">Planned wage cost ($)</label>
            <input id="scenarioPlanned" type="number" step="1" placeholder="4200" />
        </div>
    </form>
    <div class="scenario-actions">
        <button type="button" class="scenario-solve-btn" id="scenarioSolveBtn" onclick="solveScenario()">Solve</button>
        <button type="button" class="scenario-reset-btn" onclick="resetScenario()">Clear</button>
    </div>
    <div class="scenario-result empty" id="scenarioResult">
        Pick a mode, fill in the numbers above, and hit Solve. The math runs entirely on the server — same engine the wage pulse card uses.
    </div>
</div>
```

### CSS Classes (Already Defined)

All CSS is in `static/dashboard.html` `:root` variables and `.scenario-*` classes (lines 2127–2326):

**Key Color Tokens:**
- `--card-bg: #ffffff` — white background
- `--text-primary: #0f172a` — dark text
- `--text-secondary: #64748b` — gray text
- `--border-color: #e2e8f0` — light border
- `--info-blue: #0ea5e9` — accent color for active tabs, results
- `--accent-amber: #0284c7` — button primary color
- `--alert-red: #ef4444` — error color (not used in scenario widget)

**Key Classes:**
- `.scenario-card` — main container
- `.scenario-header` — title + mode tabs
- `.scenario-form` — 3-column grid of input fields (responsive)
- `.scenario-actions` — Solve + Clear buttons
- `.scenario-result` — output panel (with `.empty` variant)
- `.scenario-result-breakdown` — 3-column grid for key metrics
- `.scenario-result-suggestion` — actionable text
- `.scenario-result-warning` — alert-colored text

### JavaScript Functions

**Key Functions (in `static/dashboard.html` lines 6428–6599):**

1. **`solveScenario()`** — Reads form inputs, POSTs to `/api/v1/scenarios/wage-cost`, renders result
   - Validates required fields before sending (client-side)
   - Falls back to `solveScenarioLocally()` on network error
   - Disables button while loading

2. **`resetScenario()`** — Clears all form fields and result panel

3. **`populateScenarioResult(data, meta)`** — Renders the result JSON into formatted HTML
   - Shows headline (e.g., "Required sales: $5,000")
   - Displays key metrics in a breakdown grid
   - Lists all suggestions and warnings

4. **`solveScenarioLocally(body)`** — Minimal client-side math for network resilience
   - Mirrors solve_sales and diagnose modes
   - solve_wage_budget returns a stub warning "use server"

### Mode Visibility Logic

The form fields are shown/hidden via JavaScript (in dashboard.html, managed by mode tabs):

```javascript
// Pseudo-code; actual implementation shows/hides fields by ID
const SCENARIO_REQUIRED_FIELDS = {
    'solve_sales': [
        { key: 'target_wage_cost_pct', label: 'Target wage %' },
        { key: 'wage_cost', label: 'Wage cost' },
    ],
    'solve_wage_budget': [
        { key: 'target_wage_cost_pct', label: 'Target wage %' },
        { key: 'forecast_sales', label: 'Forecast sales' },
    ],
    'diagnose': [
        { key: 'wage_cost', label: 'Wage cost' },
        { key: 'forecast_sales', label: 'Forecast sales' },
    ],
};
```

## Testing

**Test file:** `tests/test_scenario_solver.py`

Covers:
- Percentage normalization (0.18, 18, "18%", "18")
- All three solve modes with happy paths and edge cases
- Input validation (zero, negative, None values)
- Guardrails (wild target percentages)
- Forecast gap analysis
- On-cost multiplier customization
- ScenarioResult serialization (Decimal → float for JSON)
- Decimal precision maintenance

**Run tests:**
```bash
PYTHONPATH=. python3 -m unittest tests.test_scenario_solver -v
```

**Expected:** 41 tests pass, all stdlib, no pytest required.

## Integration Notes

### Auth Gating

The endpoint requires L2_ROSTER_MAKER access. Pattern from `rosteriq/tanda_marketplace_router.py`:

```python
@app.post("/api/v1/scenarios/wage-cost", response_model=ScenarioSolveResponse)
async def solve_wage_scenario(
    request: ScenarioSolveRequest,
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)) if require_access else None,
) -> ScenarioSolveResponse:
    # require_access is None in demo mode (auth disabled) → endpoint is open
```

### Demo Mode

When auth is disabled (ROSTERIQ_AUTH_ENABLED not set):
- `require_access` is None
- Dependency is skipped
- Endpoint is open to all requests

### Feature Flag

Scenario solver is gated by feature tier in `rosteriq/tenants.py`:

```python
if feature in ("conversational_ai", "scenario_solver", "extended_forecast_horizon"):
    # L2 required for these
```

Users at lower access levels won't see the widget (it has `role-l2` CSS class).

## Future Enhancements

1. **Role-level staffing breakdown** — `recommended_staff_count`, `breakdown_by_role` (dict role → hours)
   - Would require integration with `rosteriq/award_engine.py` for role-specific rates
   - Currently solver uses a blended rate; future version could expand this

2. **Feasibility warnings** — "minimum staffing not met", "shift coverage gap"
   - Requires shift pattern logic from the forecast engine
   - Would populate `feasibility_warnings` list in ScenarioResult

3. **Save scenarios to roster draft** — "Apply this staffing to the current roster"
   - UX: post-solve button "Create Draft Roster"
   - Backend: would create a draft with the recommended staffing

4. **Historical wage % tracking** — show recent wage% for this venue to contextualize targets
   - Would query a wage_metrics table if one exists

## Files Modified / Created

**Created:**
- `rosteriq/scenario_solver.py` (15.1 KB) — pure logic module
- `tests/test_scenario_solver.py` (13.5 KB) — 41 unit tests
- `docs/round7_scenario_dashboard_snippet.md` (this file)

**Modified:**
- `rosteriq/api_v2.py` — `/api/v1/scenarios/wage-cost` endpoint already exists (lines 1004–1082)
- `static/dashboard.html` — widget, CSS, and JS already integrated (lines 2127–2326, 5399–5441, 6428–6599)

**No changes to:**
- `rosteriq/forecast_engine.py`, `rosteriq/award_engine.py`, `rosteriq/auth.py`

## Deployment Checklist

- [ ] All Python files compile: `python3 -m py_compile rosteriq/scenario_solver.py`
- [ ] Tests pass: `PYTHONPATH=. python3 -m unittest tests.test_scenario_solver`
- [ ] API endpoint responds: `curl -X POST http://localhost:8000/api/v1/scenarios/wage-cost -H "Content-Type: application/json" -d '{"mode":"diagnose","wage_cost":1000,"forecast_sales":5000}'`
- [ ] Dashboard widget loads on `/dashboard` for L2+ users
- [ ] Mode switching works (tabs update form visibility)
- [ ] Client-side validation prevents empty required fields
- [ ] Results render correctly for all three modes
