"""
chriscal scheduler

Owns: job registration, timing, stale detection, startup catch-up,
      try/except safety net, duration measurement.
Delegates everything else to the ingest layer.

Run via systemd with Restart=on-failure, RestartSec=30,
StartLimitIntervalSec to prevent crash-loop DB hammering.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import time
from datetime import datetime, timezone

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from fetcher_contract import FetchOutcome, FetchResult
from ingest import ingest

logger = logging.getLogger("chriscal.scheduler")

# How often the stale detection loop runs
STALE_CHECK_INTERVAL_MINUTES = 30

# On startup, if a source is overdue by more than 1x its fetch_interval, run immediately
# Otherwise wait for next scheduled slot
CATCHUP_THRESHOLD_MULTIPLIER = 1.0


class Scheduler:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.scheduler = AsyncIOScheduler()
        self._fetcher_modules: dict[str, object] = {}

    async def start(self) -> None:
        """Initialize scheduler: register fetchers, run catch-up, start loop."""
        sources = await self._get_enabled_sources()

        if not sources:
            logger.warning("No enabled sources found. Scheduler started with no jobs.")

        for source in sources:
            self._register_source(source)

        # Stale detection loop
        self.scheduler.add_job(
            self._check_stale_sources,
            trigger=IntervalTrigger(minutes=STALE_CHECK_INTERVAL_MINUTES),
            id="stale_check",
            max_instances=1,
        )

        # Daily reports at 6:00am and 6:05am LA time
        from apscheduler.triggers.cron import CronTrigger
        self.scheduler.add_job(
            self._generate_surf_report,
            trigger=CronTrigger(hour=6, minute=0, timezone="America/Los_Angeles"),
            id="surf_report",
            max_instances=1,
        )
        self.scheduler.add_job(
            self._generate_events_report,
            trigger=CronTrigger(hour=6, minute=5, timezone="America/Los_Angeles"),
            id="events_report",
            max_instances=1,
        )

        # Wavecast forecast fetch at 5:30am LA time (before report)
        self.scheduler.add_job(
            self._fetch_wavecast,
            trigger=CronTrigger(hour=5, minute=30, timezone="America/Los_Angeles"),
            id="wavecast_fetch",
            max_instances=1,
        )

        # Tide data refresh quarterly (1st of Jan/Apr/Jul/Oct at midnight)
        self.scheduler.add_job(
            self._refresh_tides,
            trigger=CronTrigger(month="1,4,7,10", day=1, hour=0, timezone="America/Los_Angeles"),
            id="tide_refresh",
            max_instances=1,
        )

        self.scheduler.start()
        logger.info(f"Scheduler started with {len(sources)} source(s).")

        # Catch-up: immediately run overdue sources
        await self._run_catchup(sources)

    def _register_source(self, source: asyncpg.Record) -> None:
        """Register a single source as a scheduled job."""
        source_name = source["name"]
        interval_seconds = source["fetch_interval"].total_seconds()

        self.scheduler.add_job(
            self._run_fetcher,
            trigger=IntervalTrigger(seconds=interval_seconds),
            args=[source_name],
            id=f"fetch_{source_name}",
            max_instances=1,  # prevent stacking if a fetch overruns its interval
            replace_existing=True,
        )
        logger.info(
            f"Registered source '{source_name}' "
            f"(interval: {interval_seconds}s)"
        )

    async def _run_fetcher(self, source_name: str) -> None:
        """Execute a single fetcher with timing and safety net.

        This is the core scheduling unit. It:
          1. Loads the fetcher module
          2. Calls fetch() with wall-clock timing
          3. Passes the result to the ingest layer
          4. Catches any unhandled exceptions (belt and suspenders)
        """
        fetcher_module = self._load_fetcher(source_name)
        if fetcher_module is None:
            return

        start_ms = _now_ms()
        try:
            result: FetchResult = await fetcher_module.fetch()
        except Exception as e:
            # Fetcher violated the contract by raising — catch and log as ERROR
            duration_ms = _now_ms() - start_ms
            logger.error(
                f"Fetcher '{source_name}' raised an exception "
                f"(contract violation): {e}",
                exc_info=True,
            )
            result = FetchResult(
                source_name=source_name,
                outcome=FetchOutcome.ERROR,
                error_message=f"Unhandled exception: {type(e).__name__}: {e}",
                duration_ms=duration_ms,
            )

        # Set duration if the fetcher didn't (it shouldn't, but be safe)
        duration_ms = _now_ms() - start_ms
        if result.duration_ms is None:
            # FetchResult is frozen, so we need to reconstruct
            result = FetchResult(
                source_name=result.source_name,
                outcome=result.outcome,
                events=result.events,
                events_found=result.events_found,
                parse_warnings=result.parse_warnings,
                error_message=result.error_message,
                duration_ms=duration_ms,
            )

        # Hand off to ingest layer
        try:
            await ingest(self.pool, result)
        except Exception as e:
            logger.error(
                f"Ingest layer failed for source '{source_name}': {e}",
                exc_info=True,
            )

    def _load_fetcher(self, source_name: str) -> object | None:
        """Lazy-load a fetcher module from chriscal/fetchers/<source_name>.py.

        Modules are cached after first load. A failed import is logged
        and the source is skipped (not crashed).
        """
        if source_name in self._fetcher_modules:
            return self._fetcher_modules[source_name]

        module_path = f"fetchers.{source_name}"
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            logger.error(
                f"Could not load fetcher module '{module_path}': {e}"
            )
            return None

        if not hasattr(module, "fetch") or not asyncio.iscoroutinefunction(module.fetch):
            logger.error(
                f"Fetcher module '{module_path}' missing async fetch() function."
            )
            return None

        self._fetcher_modules[source_name] = module
        return module

    # ================================================================
    # Stale detection
    # ================================================================

    async def _check_stale_sources(self) -> None:
        """Periodic check for sources that haven't fetched within their expected window.

        Writes a synthetic 'stale' entry to fetch_log and updates source state.
        This is the ONLY place 'stale' status originates — fetchers never set it.
        """
        rows = await self.pool.fetch(
            """
            SELECT id, name
            FROM sources
            WHERE enabled = TRUE
              AND last_fetch_status != 'stale'
              AND (
                  last_successful_fetch IS NULL
                  OR last_successful_fetch < NOW() - (fetch_interval * 2)
              )
            """
        )

        for row in rows:
            logger.warning(
                f"Source '{row['name']}' detected as stale. "
                f"No successful fetch within 2x fetch_interval."
            )
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO fetch_log (source_id, status, error_message)
                        VALUES ($1, 'stale', 'Scheduler: no successful fetch within expected window')
                        """,
                        row["id"],
                    )
                    await conn.execute(
                        """
                        UPDATE sources SET
                            last_fetch_at = NOW(),
                            last_fetch_status = 'stale',
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        row["id"],
                    )
                    # Recalculate health score with the new stale entry
                    from ingest import _update_health_score
                    await _update_health_score(conn, row["id"])

    # ================================================================
    # Startup catch-up
    # ================================================================

    async def _run_catchup(self, sources: list[asyncpg.Record]) -> None:
        """On startup, immediately run fetchers that are overdue.

        Rule: if a source's last_successful_fetch is more than 1x fetch_interval
        ago, run it now. Otherwise let it wait for the next scheduled slot.

        Runs overdue sources sequentially to avoid a thundering herd after
        a long outage.
        """
        now = datetime.now(timezone.utc)
        overdue = []

        for source in sources:
            last = source["last_successful_fetch"]
            interval = source["fetch_interval"]

            if last is None:
                # Never fetched — definitely overdue
                overdue.append(source)
            elif (now - last).total_seconds() > interval.total_seconds() * CATCHUP_THRESHOLD_MULTIPLIER:
                overdue.append(source)

        if not overdue:
            logger.info("Startup catch-up: all sources are current.")
            return

        logger.info(
            f"Startup catch-up: {len(overdue)} source(s) overdue. "
            f"Running sequentially."
        )
        for source in overdue:
            logger.info(f"Catch-up fetch: {source['name']}")
            await self._run_fetcher(source["name"])

    # ================================================================
    # Helpers
    # ================================================================

    async def _get_enabled_sources(self) -> list[asyncpg.Record]:
        """Fetch all enabled sources with their scheduling config."""
        return await self.pool.fetch(
            """
            SELECT name, fetch_interval, last_successful_fetch
            FROM sources
            WHERE enabled = TRUE
            """
        )

    # ================================================================
    # Daily report + forecast + tide jobs
    # ================================================================

    async def _generate_surf_report(self) -> None:
        """Generate the daily surf forecast report via Claude API."""
        try:
            from report_generator import generate_surf_report
            report = await generate_surf_report(self.pool)
            if report:
                logger.info(f"Surf report generated ({len(report)} chars)")
            else:
                logger.warning("Surf report generation returned None")
        except Exception as e:
            logger.error(f"Surf report generation failed: {e}", exc_info=True)

    async def _generate_events_report(self) -> None:
        """Generate the daily events outlook report via Claude API."""
        try:
            from report_generator import generate_events_report
            report = await generate_events_report(self.pool)
            if report:
                logger.info(f"Events report generated ({len(report)} chars)")
            else:
                logger.warning("Events report generation returned None")
        except Exception as e:
            logger.error(f"Events report generation failed: {e}", exc_info=True)

    async def _fetch_wavecast(self) -> None:
        """Fetch and store the Wavecast surf forecast."""
        try:
            from fetchers.wavecast import fetch as fetch_wavecast
            result = await fetch_wavecast()
            if result.outcome == "success" and result.forecast_text:
                await self.pool.execute(
                    """
                    INSERT INTO forecasts (forecast_date, source, forecast_text)
                    VALUES ($1, 'wavecast', $2)
                    ON CONFLICT (forecast_date, source) DO UPDATE SET
                        forecast_text = EXCLUDED.forecast_text, fetched_at = now()
                    """,
                    result.forecast_date,
                    result.forecast_text,
                )
                logger.info(f"Wavecast forecast stored ({len(result.forecast_text)} chars)")
            else:
                logger.warning(f"Wavecast fetch failed: {result.error_message}")
        except Exception as e:
            logger.error(f"Wavecast fetch failed: {e}", exc_info=True)

    async def _refresh_tides(self) -> None:
        """Refresh tide data for the current quarter."""
        try:
            from fetchers.noaa_tides import fetch_quarter
            from tide_utils import ingest_tides
            result = await fetch_quarter()
            if result.outcome == "success":
                count = await ingest_tides(self.pool, result.predictions)
                logger.info(f"Tide refresh: {count} predictions ingested")
            else:
                logger.warning(f"Tide refresh failed: {result.error_message}")
        except Exception as e:
            logger.error(f"Tide refresh failed: {e}", exc_info=True)


def _now_ms() -> int:
    """Current time in milliseconds (monotonic clock for duration measurement)."""
    return int(time.monotonic() * 1000)
