"""
RosterIQ Application Entrypoint

Starts the FastAPI application server via Uvicorn.
Uses environment variable PORT for deployment flexibility (default 8000).
"""

import os
import uvicorn

from rosteriq.api_v2 import app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )
