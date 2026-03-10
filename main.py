"""
chriscal entry point

Starts the APScheduler background scheduler and serves the FastAPI app
via uvicorn. Single process — scheduler runs in background thread,
API serves on main thread.
"""
import asyncio
import logging
import os
import uvicorn
from api import app
from scheduler import start_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("chriscal.main")

if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))

    log.info("Starting chriscal scheduler...")
    start_scheduler()

    log.info(f"Starting chriscal API on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
