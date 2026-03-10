"""
chriscal entry point

Starts the APScheduler background scheduler and serves the FastAPI app
via uvicorn. Single process — scheduler runs in the asyncio event loop
alongside the API (both share the same DB pool).
"""
import asyncio
import logging
import os
import uvicorn
from api import app, DATABASE_URL
from scheduler import Scheduler

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("chriscal.main")


@app.on_event("startup")
async def start_scheduler():
    """Start the scheduler using the API's shared DB pool."""
    pool = app.state.pool
    scheduler = Scheduler(pool)
    app.state.scheduler = scheduler
    await scheduler.start()
    log.info("Scheduler started.")


if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))

    log.info(f"Starting chriscal on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
