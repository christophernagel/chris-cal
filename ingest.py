"""
chriscal ingest layer

Consumes FetchResult from fetchers, handles all stateful operations:
  1. Venue resolution (with alias table)
  2. Event dedup + upsert
  3. Baseline deviation detection (may reclassify success → partial)
  4. Fetch log write
  5. Source health update (health_score, baseline avg)

Two transaction boundaries:
  TX1: Steps 1-2 (venue resolution + event writes) — atomic
  TX2: Steps 3-5 (fetch log + source health) — atomic, independent of TX1

If TX1 succeeds but TX2 fails, events are preserved.
The next fetch cycle will correct the metadata state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone

import asyncpg

from fetcher_contract import (
    EventData,
    FetchOutcome,
    FetchResult,
    ParseWarning,
)

logger = logging.getLogger("chriscal.ingest")

# Health score weights per fetch_status
HEALTH_WEIGHTS: dict[str, float] = {
    "success": 1.0,
    "partial": 0.5,
    "error": 0.0,
    "timeout": 0.0,
    "stale": 0.0,
}

HEALTH_WINDOW = 20  # number of recent fetches used for health score
BASELINE_DEVIATION_THRESHOLD = 0.3  # flag if events_found < 30% of avg


async def ingest(pool: asyncpg.Pool, result: FetchResult) -> None:
    """Main entry point. Consumes a FetchResult and performs all DB operations."""

    # Resolve source_id from source_name
    source = await _get_source(pool, result.source_name)
    if source is None:
        logger.error(f"Unknown source: {result.source_name}")
        return

    source_id = source["id"]

    # ================================================================
    # TX1: Venue resolution + event upsert
    # ================================================================
    events_inserted = 0
    events_skipped = 0

    if result.events:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for event_data in result.events:
                    venue_id, zone = await _resolve_venue(conn, event_data)
                    was_inserted = await _upsert_event(
                        conn, source_id, event_data, venue_id, zone
                    )
                    if was_inserted:
                        events_inserted += 1
                    else:
                        events_skipped += 1

    # ================================================================
    # Baseline deviation check (between TX1 and TX2)
    # Reclassify fetcher-reported success → partial if deviation detected
    # ================================================================
    final_status = result.outcome.value

    if result.outcome == FetchOutcome.SUCCESS and source["avg_events_per_fetch"] is not None:
        avg = source["avg_events_per_fetch"]
        if avg > 0 and result.events_found < (avg * BASELINE_DEVIATION_THRESHOLD):
            final_status = "partial"
            logger.warning(
                f"Source {result.source_name}: baseline deviation detected. "
                f"events_found={result.events_found}, avg={avg:.1f}. "
                f"Reclassified success → partial."
            )

    # ================================================================
    # TX2: Fetch log + source health update
    # ================================================================
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 4: Write fetch_log entry
            await _write_fetch_log(
                conn,
                source_id=source_id,
                status=final_status,
                events_found=result.events_found,
                events_inserted=events_inserted,
                events_skipped=events_skipped,
                parse_warnings=result.parse_warnings,
                error_message=result.error_message,
                duration_ms=result.duration_ms,
            )

            # Step 5a: Update source health state
            await _update_source_state(
                conn,
                source_id=source_id,
                status=final_status,
                error_message=result.error_message,
            )

            # Step 5b: Recalculate health_score from last N fetch_log entries
            await _update_health_score(conn, source_id)

            # Step 5c: Update baseline (only when final status is 'success')
            if final_status == "success":
                await _update_baseline(
                    conn,
                    source_id=source_id,
                    events_found=result.events_found,
                    current_avg=source["avg_events_per_fetch"],
                    current_sample_size=source["baseline_sample_size"],
                )


# ============================================================
# Internal helpers
# ============================================================


async def _get_source(pool: asyncpg.Pool, source_name: str) -> asyncpg.Record | None:
    """Fetch source record by name."""
    return await pool.fetchrow(
        """
        SELECT id, avg_events_per_fetch, baseline_sample_size
        FROM sources
        WHERE name = $1 AND enabled = TRUE
        """,
        source_name,
    )


async def _resolve_venue(
    conn: asyncpg.Connection, event: EventData
) -> tuple[int | None, str | None]:
    """Resolve venue_id and zone from event's venue_name.

    Resolution order:
      1. Exact match on venues.name
      2. Alias match on venue_aliases.alias
      3. No match → return (None, None), log for manual alias creation

    Does NOT auto-create venues — unknown venues surface as NULL venue_id
    on the event, which is a signal to add the venue + alias manually.
    This prevents garbage venue records from accumulating silently.
    """
    if not event.venue_name:
        return None, None

    # Try exact match
    row = await conn.fetchrow(
        "SELECT id, zone FROM venues WHERE name = $1",
        event.venue_name,
    )
    if row:
        return row["id"], row["zone"]

    # Try alias match
    row = await conn.fetchrow(
        """
        SELECT v.id, v.zone
        FROM venue_aliases va
        JOIN venues v ON v.id = va.venue_id
        WHERE va.alias = $1
        """,
        event.venue_name,
    )
    if row:
        return row["id"], row["zone"]

    # No match — log it so we know to add the alias
    logger.info(
        f"Unresolved venue: '{event.venue_name}' "
        f"(from event: '{event.title}'). Add to venues or venue_aliases."
    )
    return None, None


async def _upsert_event(
    conn: asyncpg.Connection,
    source_id: int,
    event: EventData,
    venue_id: int | None,
    zone: str | None,
) -> bool:
    """Insert or update an event. Returns True if inserted, False if skipped/updated.

    Dedup key: (source_id, source_url).
    On conflict: update mutable fields (title, description, start_at, end_at, etc.)
    and bump last_verified. This handles sources that update event details after
    initial publication.
    """
    result = await conn.execute(
        """
        INSERT INTO events (
            title, description, source_id, source_url, series_id,
            venue_id, zone, start_at, end_at,
            category, tags, is_free, ticket_url, price_range,
            is_one_off, last_verified, raw_source
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, NOW(), $16
        )
        ON CONFLICT (source_id, source_url) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            series_id = EXCLUDED.series_id,
            venue_id = EXCLUDED.venue_id,
            zone = COALESCE(
                NULLIF(events.zone, (SELECT v.zone FROM venues v WHERE v.id = EXCLUDED.venue_id)),
                EXCLUDED.zone
            ),
            start_at = EXCLUDED.start_at,
            end_at = EXCLUDED.end_at,
            category = EXCLUDED.category,
            tags = EXCLUDED.tags,
            is_free = EXCLUDED.is_free,
            ticket_url = EXCLUDED.ticket_url,
            price_range = EXCLUDED.price_range,
            is_one_off = EXCLUDED.is_one_off,
            last_verified = NOW(),
            raw_source = EXCLUDED.raw_source,
            updated_at = NOW()
        """,
        event.title,
        event.description,
        source_id,
        event.source_url,
        event.series_id,
        venue_id,
        zone,
        event.start_at,
        event.end_at,
        event.category,
        event.tags,
        event.is_free,
        event.ticket_url,
        event.price_range,
        event.is_one_off,
        json.dumps(event.raw_source) if event.raw_source else None,
    )

    # asyncpg returns 'INSERT 0 1' or 'UPDATE 0 1'
    return result == "INSERT 0 1"


async def _write_fetch_log(
    conn: asyncpg.Connection,
    *,
    source_id: int,
    status: str,
    events_found: int,
    events_inserted: int,
    events_skipped: int,
    parse_warnings: list[ParseWarning],
    error_message: str | None,
    duration_ms: int | None,
) -> None:
    """Append a row to fetch_log."""
    warnings_json = None
    if parse_warnings:
        warnings_json = json.dumps([asdict(w) for w in parse_warnings])

    await conn.execute(
        """
        INSERT INTO fetch_log (
            source_id, status, events_found, events_inserted,
            events_skipped, parse_warnings, error_message, duration_ms
        ) VALUES ($1, $2::fetch_status, $3, $4, $5, $6::jsonb, $7, $8)
        """,
        source_id,
        status,
        events_found,
        events_inserted,
        events_skipped,
        warnings_json,
        error_message,
        duration_ms,
    )


async def _update_source_state(
    conn: asyncpg.Connection,
    *,
    source_id: int,
    status: str,
    error_message: str | None,
) -> None:
    """Update source's last fetch state fields."""
    now = datetime.now(timezone.utc)

    await conn.execute(
        """
        UPDATE sources SET
            last_fetch_at = $1,
            last_fetch_status = $2::fetch_status,
            last_successful_fetch = CASE
                WHEN $2 = 'success' THEN $1
                ELSE last_successful_fetch
            END,
            fetch_error_log = CASE
                WHEN $2 IN ('error', 'timeout') THEN $3
                ELSE fetch_error_log
            END,
            updated_at = $1
        WHERE id = $4
        """,
        now,
        status,
        error_message,
        source_id,
    )


async def _update_health_score(conn: asyncpg.Connection, source_id: int) -> None:
    """Recalculate health_score from last N fetch_log entries.

    Weights: success=1.0, partial=0.5, error/timeout/stale=0.0
    Score = (sum of weights / HEALTH_WINDOW) * 100, capped at [0, 100].
    If fewer than HEALTH_WINDOW entries exist, denominator is still HEALTH_WINDOW
    (new sources start with a conservative score that climbs as successes accumulate).
    """
    rows = await conn.fetch(
        """
        SELECT status::text
        FROM fetch_log
        WHERE source_id = $1
        ORDER BY fetched_at DESC
        LIMIT $2
        """,
        source_id,
        HEALTH_WINDOW,
    )

    if not rows:
        return

    total = sum(HEALTH_WEIGHTS.get(row["status"], 0.0) for row in rows)
    score = int((total / HEALTH_WINDOW) * 100)
    score = max(0, min(100, score))

    await conn.execute(
        "UPDATE sources SET health_score = $1 WHERE id = $2",
        score,
        source_id,
    )


async def _update_baseline(
    conn: asyncpg.Connection,
    *,
    source_id: int,
    events_found: int,
    current_avg: float | None,
    current_sample_size: int | None,
) -> None:
    """Update the incremental moving average for events_per_fetch.

    Only called when final written status is 'success'.
    Formula: new_avg = (old_avg * n + events_found) / (n + 1)
    """
    n = current_sample_size or 0
    old_avg = current_avg or 0.0

    new_avg = (old_avg * n + events_found) / (n + 1)
    new_n = n + 1

    await conn.execute(
        """
        UPDATE sources SET
            avg_events_per_fetch = $1,
            baseline_sample_size = $2
        WHERE id = $3
        """,
        new_avg,
        new_n,
        source_id,
    )
