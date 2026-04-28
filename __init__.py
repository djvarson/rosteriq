"""
RosterIQ — AI-powered predictive rostering for Australian hospitality.

Modules:
- models: Pydantic data contracts (Employee, Shift, Roster, etc.)
- award_rules: Fair Work Australia compliance engine
- cost_calculator: Labour cost breakdowns with penalty rates and super
- variance_engine: Real-time demand variance detection (5-signal weighted)
- decision_engine: Cut/call-in staff ranking and recommendations
- ensemble: XGBoost/Prophet demand forecasting with cold-start fallback
- tanda_adapter: Async Tanda API integration with OAuth and rate limiting
- pos_import: POS CSV import with auto-detection (H&L, Lightspeed, Square)
- roster_optimiser: Greedy constraint-satisfaction roster generation
- cli: Command-line interface for roster generation and management

Usage:
    python -m rosteriq demo          # Run demo with sample data
    python -m rosteriq generate ...  # Generate a roster from config
    python -m rosteriq analyse ...   # Analyse an existing roster
"""

__version__ = "0.1.0"
