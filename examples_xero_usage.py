"""
Example usage of the Xero integration module.

Shows how to use XeroClient, XeroOAuth, and database helpers in real code.
"""

import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

from rosteriq.xero_integration import (
    XeroOAuth,
    XeroClient,
    XeroCredentials,
    LabourCostJournal,
    get_xero_credentials,
    save_xero_credentials,
)
from rosteriq.database import get_db


# ============================================================================
# Example 1: OAuth2 Flow
# ============================================================================

async def example_oauth_flow():
    """
    Step-by-step OAuth2 PKCE flow.

    In production:
    1. Flask/FastAPI route receives POST /api/xero/connect
    2. Generate authorize URL and redirect user
    3. Xero calls back to /api/xero/callback with code
    4. Exchange code for tokens
    """

    oauth = XeroOAuth(
        client_id="your_client_id_here",
        client_secret="your_client_secret_here",
        redirect_uri="https://rosteriq.com.au/api/xero/callback",
    )

    # Step 1: Generate authorization URL
    authorize_url, state = oauth.generate_authorize_url()
    print(f"1. Redirect user to: {authorize_url}")
    print(f"   Store this state in session: {state}")

    # Step 2: Xero redirects back with code
    # (In real flow, this happens in browser)
    # code = request.args.get('code')
    # state = request.args.get('state')

    # Step 3: Exchange code for tokens
    # code_verifier = session.pop('code_verifier')  # Retrieved from session
    # credentials = await oauth.exchange_code(code, state, code_verifier)
    # print(f"2. Got tokens! Tenant ID: {credentials.tenant_id}")


# ============================================================================
# Example 2: Revenue Sync
# ============================================================================

async def example_revenue_sync():
    """
    Pull daily revenue from Xero.

    Shows:
    - Single day pull
    - Multi-day backfill
    - Data persistence
    """
    db = get_db()
    venue_id = "v1"

    # Get stored credentials
    creds_dict = db.get_xero_credentials(venue_id)
    if not creds_dict:
        print(f"Xero not connected for {venue_id}")
        return

    credentials = XeroCredentials(**creds_dict)

    # Pull revenue for single day
    async with XeroClient(credentials, db) as xero:
        snapshot = await xero.pull_revenue(date(2026, 4, 23), venue_id)
        print(f"Revenue for 2026-04-23:")
        print(f"  Total:    ${snapshot.total_revenue:,.2f}")
        print(f"  Food:     ${snapshot.food_revenue:,.2f}")
        print(f"  Beverage: ${snapshot.beverage_revenue:,.2f}")
        print(f"  Gaming:   ${snapshot.gaming_revenue:,.2f}")
        print(f"  GST (10%):${snapshot.gst_collected:,.2f}")

    # Backfill last 90 days (on first connect)
    async with XeroClient(credentials, db) as xero:
        today = date.today()
        ninety_days_ago = today - timedelta(days=90)

        snapshots = await xero.pull_revenue_range(
            ninety_days_ago,
            today,
            venue_id,
        )
        print(f"\nBackfilled {len(snapshots)} days of revenue")
        total = sum(s.total_revenue for s in snapshots)
        print(f"Total 90-day revenue: ${total:,.2f}")


# ============================================================================
# Example 3: Labour Cost Export
# ============================================================================

async def example_labour_cost_export():
    """
    Build and push labour cost journal to Xero.

    Shows:
    - Building a LabourCostJournal
    - Pushing to Xero as draft
    - Breakdown by role and award level
    """
    db = get_db()
    venue_id = "v1"

    credentials_dict = db.get_xero_credentials(venue_id)
    credentials = XeroCredentials(**credentials_dict)

    # Example: Build labour costs for 2026-04-23
    # In production, fetch from rosters and cost_calculator

    journal = LabourCostJournal(
        venue_id=venue_id,
        journal_date=date(2026, 4, 23),
        total_wages_expense=Decimal("1250.50"),
        total_super_payable=Decimal("143.81"),  # 11.5% of wages
        breakdown={
            "venue": {
                "v1": Decimal("1250.50")
            },
            "roles": {
                "bartender": Decimal("300.00"),
                "chef": Decimal("450.25"),
                "waiter": Decimal("500.25"),
            }
        },
        award_levels={
            "level_1": Decimal("200.00"),
            "level_2": Decimal("600.50"),
            "level_3": Decimal("450.00"),
        },
        description="Daily labour costs",
    )

    async with XeroClient(credentials, db) as xero:
        response = await xero.push_labour_costs(journal)
        print("Labour journal pushed to Xero:")
        print(f"  Journal ID: {response.get('ManualJournals', [{}])[0].get('ManualJournalID')}")
        print(f"  Status: DRAFT (review in Xero before posting)")


# ============================================================================
# Example 4: P&L Reporting
# ============================================================================

async def example_pnl_reporting():
    """
    Pull P&L from Xero and calculate labour metrics.

    Used on dashboard to show labour efficiency.
    Target for AU hospitality: 28-32% of revenue.
    """
    db = get_db()
    venue_id = "v1"

    credentials_dict = db.get_xero_credentials(venue_id)
    credentials = XeroCredentials(**credentials_dict)

    # Pull last 30 days
    today = date.today()
    thirty_days_ago = today - timedelta(days=30)

    async with XeroClient(credentials, db) as xero:
        pnl = await xero.get_pnl_report(thirty_days_ago, today)

        print(f"P&L Report: {thirty_days_ago} to {today}")
        print(f"  Revenue:          ${pnl.total_revenue:>10,.2f}")
        print(f"  COGS:             ${pnl.cogs:>10,.2f}")
        print(f"  Gross Profit:     ${pnl.gross_profit:>10,.2f}")
        print(f"  Labour Costs:     ${pnl.labour_costs:>10,.2f}")
        print(f"  Other Expenses:   ${pnl.other_expenses:>10,.2f}")
        print(f"  Net Profit:       ${pnl.net_profit:>10,.2f}")
        print()
        print(f"  Labour % of Revenue: {pnl.labour_percentage:.1f}%")

        if pnl.labour_percentage > 32:
            print("  ⚠️  ALERT: Labour costs above target (28-32%)")
        elif pnl.labour_percentage < 28:
            print("  ✓ Labour costs within target")


# ============================================================================
# Example 5: Integration with FastAPI
# ============================================================================

async def example_fastapi_integration():
    """
    How to integrate Xero routes into FastAPI app.
    """
    from fastapi import FastAPI
    from rosteriq.xero_routes import setup_xero_routes
    from rosteriq.database import get_db

    app = FastAPI(
        title="RosterIQ",
        description="AI rostering for Australian hospitality",
    )

    db = get_db()

    # Register Xero routes
    setup_xero_routes(app, db)

    # Now available:
    # POST   /api/xero/connect
    # GET    /api/xero/callback
    # GET    /api/xero/status/{venue_id}
    # POST   /api/xero/disconnect/{venue_id}
    # POST   /api/xero/sync/revenue
    # POST   /api/xero/sync/labour-costs
    # GET    /api/xero/pnl/{venue_id}

    return app


# ============================================================================
# Example 6: Error Handling
# ============================================================================

async def example_error_handling():
    """
    Proper error handling for Xero API calls.
    """
    db = get_db()
    venue_id = "v1"

    try:
        credentials_dict = db.get_xero_credentials(venue_id)
        if not credentials_dict:
            print(f"ERROR: Xero not connected for {venue_id}")
            return

        credentials = XeroCredentials(**credentials_dict)

        async with XeroClient(credentials, db) as xero:
            revenue = await xero.pull_revenue(date.today(), venue_id)
            print(f"Revenue: ${revenue.total_revenue:,.2f}")

    except ValueError as e:
        # Xero API error (400, token exchange failed, etc.)
        print(f"API Error: {e}")
        # Log to monitoring system
        # Send alert to admin

    except RuntimeError as e:
        # Max retries exceeded
        print(f"Retry Error: {e}")
        # Check network connectivity
        # Check Xero service status

    except Exception as e:
        # Unexpected error
        print(f"Unexpected Error: {e}")
        import traceback
        traceback.print_exc()


# ============================================================================
# Example 7: Scheduled Sync (Daily)
# ============================================================================

async def example_scheduled_daily_sync():
    """
    Run daily at close-of-day (e.g., 11 PM).

    Typically called from a scheduler like APScheduler or Celery.
    """
    import logging

    logger = logging.getLogger(__name__)
    db = get_db()

    # Get all connected venues
    venues = db.list_venues()

    for venue in venues:
        try:
            credentials_dict = db.get_xero_credentials(venue.id)
            if not credentials_dict:
                logger.info(f"Skipping {venue.name}: Xero not connected")
                continue

            credentials = XeroCredentials(**credentials_dict)

            # Pull revenue for today
            async with XeroClient(credentials, db) as xero:
                snapshot = await xero.pull_revenue(date.today(), venue.id)
                logger.info(
                    f"{venue.name}: ${snapshot.total_revenue:,.2f} revenue"
                )
                # Save to database
                # db.save_revenue_snapshot(snapshot)

            # Push labour costs if roster complete
            # async with XeroClient(credentials, db) as xero:
            #     journal = build_labour_journal(venue.id, date.today())
            #     await xero.push_labour_costs(journal)

        except Exception as e:
            logger.error(f"Sync failed for {venue.name}: {e}")
            # Send alert notification


# ============================================================================
# Run Examples
# ============================================================================

if __name__ == "__main__":
    print("Xero Integration Examples")
    print("=" * 60)

    # Uncomment to run examples:
    # asyncio.run(example_oauth_flow())
    # asyncio.run(example_revenue_sync())
    # asyncio.run(example_labour_cost_export())
    # asyncio.run(example_pnl_reporting())
    # asyncio.run(example_error_handling())

    print("\nNote: These examples require:")
    print("  - Xero OAuth credentials in environment")
    print("  - PostgreSQL database running")
    print("  - Xero account with permissions")
