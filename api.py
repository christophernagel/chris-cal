"""
chriscal API

FastAPI application serving three endpoints:
  GET /feed.ics   — iCal feed (public, filterable)
  GET /api/events — JSON event list (public, filterable, with derived fields)
  GET /health     — source health dashboard (HTTP Basic Auth)

All event queries hit events_view, which computes derived fields
(time_of_day, auto_priority, confidence_flag) in SQL.
"""

from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

# ============================================================
# Config
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/chriscal")
HEALTH_USER = os.environ.get("CHRISCAL_HEALTH_USER", "admin")
HEALTH_PASS = os.environ.get("CHRISCAL_HEALTH_PASS", "changeme")

# ============================================================
# App + lifecycle
# ============================================================

app = FastAPI(
    title="chriscal",
    description="Personal event calendar — UCLA/Westside/canyon ecosystem",
    version="0.1.0",
)


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


def get_pool() -> asyncpg.Pool:
    return app.state.pool


# ============================================================
# Auth
# ============================================================

security = HTTPBasic()


def verify_health_auth(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> str:
    """HTTP Basic Auth for /health endpoint."""
    correct_user = secrets.compare_digest(credentials.username.encode(), HEALTH_USER.encode())
    correct_pass = secrets.compare_digest(credentials.password.encode(), HEALTH_PASS.encode())
    if not (correct_user and correct_pass):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return credentials.username


# ============================================================
# Response models
# ============================================================

class EventResponse(BaseModel):
    id: int
    title: str
    description: str | None
    source_url: str | None
    series_id: str | None

    # time
    start_at: datetime
    end_at: datetime | None
    time_of_day: str

    # place
    venue_name: str | None
    venue_address: str | None
    venue_lat: float | None
    venue_lng: float | None
    zone: str | None

    # category + tags
    category: str
    tags: list[str]

    # access
    is_free: bool
    ticket_url: str | None
    price_range: str | None

    # priority + trust
    auto_priority: int
    is_pinned: bool
    confidence_flag: str
    is_new_source: bool

    # source
    source_name: str
    source_display_name: str


class SourceHealthResponse(BaseModel):
    id: int
    name: str
    display_name: str
    tier: str
    enabled: bool
    health_score: int
    last_fetch_at: datetime | None
    last_fetch_status: str | None
    last_successful_fetch: datetime | None
    fetch_error_log: str | None
    avg_events_per_fetch: float | None
    baseline_sample_size: int
    fetch_interval_seconds: float
    recent_fetches: list[FetchLogEntry]


class FetchLogEntry(BaseModel):
    fetched_at: datetime
    status: str
    events_found: int
    events_inserted: int
    events_skipped: int
    error_message: str | None
    duration_ms: int | None


# Rebuild SourceHealthResponse now that FetchLogEntry is defined
SourceHealthResponse.model_rebuild()


class HealthResponse(BaseModel):
    sources: list[SourceHealthResponse]
    alerts: list[str]


# ============================================================
# GET /api/events
# ============================================================

@app.get("/api/events", response_model=list[EventResponse])
async def get_events(
    pool: asyncpg.Pool = Depends(get_pool),
    category: str | None = Query(None, description="Filter by event_category enum value"),
    zone: str | None = Query(None, description="Filter by location_zone enum value"),
    tag: str | None = Query(None, description="Filter by tag (events containing this tag)"),
    min_priority: int | None = Query(None, ge=1, le=3, description="Minimum auto_priority"),
    start_after: datetime | None = Query(None, description="Events starting after this datetime"),
    start_before: datetime | None = Query(None, description="Events starting before this datetime"),
    confidence: str | None = Query(None, description="Filter by confidence_flag: fresh, aging, stale"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Public JSON event list with derived fields. Queries events_view."""

    # Build query dynamically with parameterized filters
    conditions = ["start_at >= NOW()"]  # default: future events only
    params: list = []
    param_idx = 0

    if category:
        param_idx += 1
        conditions.append(f"category = ${param_idx}::event_category")
        params.append(category)

    if zone:
        param_idx += 1
        conditions.append(f"zone = ${param_idx}::location_zone")
        params.append(zone)

    if tag:
        param_idx += 1
        conditions.append(f"tags @> ARRAY[${param_idx}]::text[]")
        params.append(tag)

    if min_priority:
        param_idx += 1
        conditions.append(f"auto_priority >= ${param_idx}")
        params.append(min_priority)

    if start_after:
        param_idx += 1
        conditions.append(f"start_at >= ${param_idx}")
        params.append(start_after)
        # Remove the default future-only filter since explicit start_after overrides it
        conditions.remove("start_at >= NOW()")

    if start_before:
        param_idx += 1
        conditions.append(f"start_at <= ${param_idx}")
        params.append(start_before)

    if confidence:
        param_idx += 1
        conditions.append(f"confidence_flag = ${param_idx}")
        params.append(confidence)

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    param_idx += 1
    limit_param = param_idx
    param_idx += 1
    offset_param = param_idx
    params.extend([limit, offset])

    query = f"""
        SELECT
            id, title, description, source_url, series_id,
            start_at, end_at, time_of_day,
            venue_name, venue_address, venue_lat, venue_lng,
            zone::text, category::text, tags,
            is_free, ticket_url, price_range,
            auto_priority, is_pinned, confidence_flag, is_new_source,
            source_name, source_display_name
        FROM events_view
        WHERE {where_clause}
        ORDER BY auto_priority DESC, start_at ASC
        LIMIT ${limit_param} OFFSET ${offset_param}
    """

    rows = await pool.fetch(query, *params)

    return [
        EventResponse(
            id=r["id"],
            title=r["title"],
            description=r["description"],
            source_url=r["source_url"],
            series_id=r["series_id"],
            start_at=r["start_at"],
            end_at=r["end_at"],
            time_of_day=r["time_of_day"],
            venue_name=r["venue_name"],
            venue_address=r["venue_address"],
            venue_lat=r["venue_lat"],
            venue_lng=r["venue_lng"],
            zone=r["zone"],
            category=r["category"],
            tags=r["tags"] or [],
            is_free=r["is_free"],
            ticket_url=r["ticket_url"],
            price_range=r["price_range"],
            auto_priority=r["auto_priority"],
            is_pinned=r["is_pinned"],
            confidence_flag=r["confidence_flag"],
            is_new_source=r["is_new_source"],
            source_name=r["source_name"],
            source_display_name=r["source_display_name"],
        )
        for r in rows
    ]


# ============================================================
# GET /feed.ics
# ============================================================

@app.get("/feed.ics")
async def get_ical_feed(
    pool: asyncpg.Pool = Depends(get_pool),
    category: str | None = Query(None),
    zone: str | None = Query(None),
):
    """Public iCal feed. Subscribe from any calendar app.

    Filterable by category and zone so subscribers can get focused feeds:
      /feed.ics?category=music_performance
      /feed.ics?zone=westwood_wilshire
      /feed.ics?category=film_screening&zone=ucla_campus
    """
    conditions = ["start_at >= NOW() - INTERVAL '7 days'"]  # include recent past for context
    params: list = []
    param_idx = 0

    if category:
        param_idx += 1
        conditions.append(f"category = ${param_idx}::event_category")
        params.append(category)

    if zone:
        param_idx += 1
        conditions.append(f"zone = ${param_idx}::location_zone")
        params.append(zone)

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT
            id, title, description, source_url,
            start_at, end_at,
            venue_name, venue_address,
            category::text, is_free, auto_priority, confidence_flag
        FROM events_view
        WHERE {where_clause}
        ORDER BY start_at ASC
    """

    rows = await pool.fetch(query, *params)

    # Build iCal output
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//chriscal//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:chriscal",
        "X-WR-TIMEZONE:America/Los_Angeles",
    ]

    for r in rows:
        lines.extend(_event_to_vevent(r))

    lines.append("END:VCALENDAR")

    ical_text = "\r\n".join(lines) + "\r\n"

    return Response(
        content=ical_text,
        media_type="text/calendar",
        headers={
            "Content-Disposition": "inline; filename=chriscal.ics",
            "Cache-Control": "public, max-age=3600",  # 1 hour cache
        },
    )


def _event_to_vevent(row: asyncpg.Record) -> list[str]:
    """Convert a DB row to iCal VEVENT lines."""
    lines = ["BEGIN:VEVENT"]

    uid = f"chriscal-{row['id']}@chriscal"
    lines.append(f"UID:{uid}")

    # Timestamps in UTC format
    start = row["start_at"].astimezone(timezone.utc)
    lines.append(f"DTSTART:{start.strftime('%Y%m%dT%H%M%SZ')}")

    if row["end_at"]:
        end = row["end_at"].astimezone(timezone.utc)
        lines.append(f"DTEND:{end.strftime('%Y%m%dT%H%M%SZ')}")

    lines.append(f"SUMMARY:{_ical_escape(row['title'])}")

    # Description: include source link and metadata
    desc_parts = []
    if row["description"]:
        desc_parts.append(row["description"][:500])
    if row["source_url"]:
        desc_parts.append(f"Details: {row['source_url']}")

    priority_label = {3: "Marquee", 2: "Strong Pick", 1: "Worth Knowing"}.get(row["auto_priority"], "")
    if priority_label:
        desc_parts.append(f"Priority: {priority_label}")

    free_label = "Free" if row["is_free"] else "Ticketed"
    desc_parts.append(free_label)

    if row["confidence_flag"] != "fresh":
        desc_parts.append(f"Note: event data is {row['confidence_flag']}")

    if desc_parts:
        lines.append(f"DESCRIPTION:{_ical_escape(chr(10).join(desc_parts))}")

    # Location
    loc_parts = []
    if row["venue_name"]:
        loc_parts.append(row["venue_name"])
    if row["venue_address"]:
        loc_parts.append(row["venue_address"])
    if loc_parts:
        lines.append(f"LOCATION:{_ical_escape(', '.join(loc_parts))}")

    # URL
    if row["source_url"]:
        lines.append(f"URL:{row['source_url']}")

    # Categories
    lines.append(f"CATEGORIES:{row['category']}")

    lines.append("END:VEVENT")
    return lines


def _ical_escape(text: str) -> str:
    """Escape text for iCal field values per RFC 5545."""
    text = text.replace("\\", "\\\\")
    text = text.replace(";", "\\;")
    text = text.replace(",", "\\,")
    text = text.replace("\n", "\\n")
    return text


# ============================================================
# GET /health
# ============================================================

@app.get("/health", response_model=HealthResponse)
async def get_health(
    pool: asyncpg.Pool = Depends(get_pool),
    _user: str = Depends(verify_health_auth),
):
    """Password-protected source health dashboard."""

    sources = await pool.fetch(
        """
        SELECT
            id, name, display_name, tier::text, enabled,
            health_score, last_fetch_at, last_fetch_status::text,
            last_successful_fetch, fetch_error_log,
            avg_events_per_fetch, baseline_sample_size,
            EXTRACT(EPOCH FROM fetch_interval) AS fetch_interval_seconds
        FROM sources
        ORDER BY health_score ASC, name
        """
    )

    source_responses = []
    alerts = []

    for s in sources:
        # Get last 10 fetch_log entries for this source
        logs = await pool.fetch(
            """
            SELECT
                fetched_at, status::text, events_found,
                events_inserted, events_skipped,
                error_message, duration_ms
            FROM fetch_log
            WHERE source_id = $1
            ORDER BY fetched_at DESC
            LIMIT 10
            """,
            s["id"],
        )

        log_entries = [
            FetchLogEntry(
                fetched_at=l["fetched_at"],
                status=l["status"],
                events_found=l["events_found"] or 0,
                events_inserted=l["events_inserted"] or 0,
                events_skipped=l["events_skipped"] or 0,
                error_message=l["error_message"],
                duration_ms=l["duration_ms"],
            )
            for l in logs
        ]

        source_responses.append(
            SourceHealthResponse(
                id=s["id"],
                name=s["name"],
                display_name=s["display_name"],
                tier=s["tier"],
                enabled=s["enabled"],
                health_score=s["health_score"] or 0,
                last_fetch_at=s["last_fetch_at"],
                last_fetch_status=s["last_fetch_status"],
                last_successful_fetch=s["last_successful_fetch"],
                fetch_error_log=s["fetch_error_log"],
                avg_events_per_fetch=s["avg_events_per_fetch"],
                baseline_sample_size=s["baseline_sample_size"] or 0,
                fetch_interval_seconds=s["fetch_interval_seconds"] or 0,
                recent_fetches=log_entries,
            )
        )

        # Generate alerts
        if s["last_fetch_status"] in ("error", "timeout", "stale"):
            alerts.append(
                f"{s['display_name']}: status is {s['last_fetch_status']}"
                + (f" — {s['fetch_error_log'][:200]}" if s["fetch_error_log"] else "")
            )

        if s["health_score"] is not None and s["health_score"] < 50 and s["enabled"]:
            alerts.append(
                f"{s['display_name']}: health score {s['health_score']}/100"
            )

        if (
            s["avg_events_per_fetch"] is not None
            and s["baseline_sample_size"] and s["baseline_sample_size"] >= 5
            and logs
            and logs[0]["status"] == "partial"
        ):
            alerts.append(
                f"{s['display_name']}: possible baseline deviation "
                f"(last fetch: {logs[0]['events_found']} events, "
                f"baseline avg: {s['avg_events_per_fetch']:.0f})"
            )

    return HealthResponse(sources=source_responses, alerts=alerts)
