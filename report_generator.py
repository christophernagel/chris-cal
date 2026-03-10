"""
chriscal daily report generator

Generates two separate daily reports using Claude via the Anthropic API:
  1. Surf report — swell outlook + tide windows
  2. Events report — 14-day cultural calendar highlights

Each is stored as a separate row in daily_reports keyed on (report_date, report_type).
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import asyncpg
from anthropic import AsyncAnthropic

logger = logging.getLogger("chriscal.report_generator")

LA_TZ = ZoneInfo("America/Los_Angeles")

if not os.environ.get("ANTHROPIC_API_KEY"):
    raise EnvironmentError(
        "ANTHROPIC_API_KEY environment variable is not set. "
        "The report generator requires a valid Anthropic API key."
    )

MODEL = "claude-sonnet-4-6"

# ============================================================
# System prompts
# ============================================================

SURF_SYSTEM_PROMPT = """You are writing a surf conditions briefing for a busy professional \
who surfs. Tone: direct, efficient, zero fluff. Like a Bloomberg terminal for surf.

Plain prose only. Exactly 3 sections separated by a blank line. No bullets, no markdown \
except **bold** for key stats and section subheaders formatted as ALL CAPS followed by \
a line break.

Bold the following when they appear: swell heights (e.g. **chest to head high**), \
swell periods (e.g. **19-20 seconds**), swell directions (e.g. **185°**), tide heights \
(e.g. **4.3 feet**), tide times (e.g. **10:31 AM**), sunset time, and the name of the \
single best surf day (e.g. **Thursday the 12th**).

Format each section exactly like this — subheader on its own line, then the paragraph:

SWELL OUTLOOK
[paragraph text]

TIDE WINDOWS
[paragraph text]

CONDITIONS
[one sentence]

SWELL OUTLOOK — one sentence per major swell event in chronological order. Name the day, \
size, direction, period, and which breaks benefit. Final sentence names the best day in \
the window and why. Maximum 6 sentences.

TIDE WINDOWS — \
Sentence 1: current tide state — height, time, direction of movement.
Sentence 2: dawn patrol window and conditions.
Sentence 3: after-work window and sunset time.
Exactly 3 sentences.

CONDITIONS — one sentence. Specific observation on water temp, seasonal trend, or what \
to watch beyond the window. No sign-offs, no generic closers.

Never say "lineup", "digest", "report", "don't miss", "be sure to"."""

EVENTS_SYSTEM_PROMPT = """You are writing a cultural calendar briefing for a busy executive \
in Los Angeles. Tone: direct, curatorial, zero fluff. Assume the reader is intelligent, \
time-constrained, and has high standards.

Plain prose only. Exactly 2 sections separated by a blank line. No bullets, no markdown \
except **bold** for venue names, *italics* for event titles, and section subheaders \
formatted as ALL CAPS followed by a line break.

Formatting rules — apply consistently:
- Event titles: *italics* (e.g. *Winter Jazzfest, Concert 3*)
- Venue names: **bold** (e.g. **Schoenberg Hall**)
- Dates and times: plain text

Format each section exactly like this — subheader on its own line, then the paragraph:

THIS WEEK
[paragraph text]

LOOKING AHEAD
[paragraph text]

THIS WEEK — one sentence per event covering days 1-7. Lead with the strongest. Each \
sentence must include the full event title in italics, venue in bold, date, and time. \
Maximum 4 sentences. No editorializing.

LOOKING AHEAD — one sentence per event covering days 8-14. Same format. If anything \
notable is visible beyond the 14-day window, fold it into the final sentence of this \
paragraph. Maximum 4 sentences.

Never say "lineup", "don't miss", "be sure to check out", "digest", "roundup", \
"worth noting", or "it's worth"."""


# ============================================================
# Context formatting
# ============================================================


def _format_tide_summary(tide_rows: list[asyncpg.Record], sunset_str: str | None) -> str:
    """Format tide hi/lo as: High: 4.3 ft at 8:25 AM | Low: 0.8 ft at 3:12 PM | Sunset: 6:57 PM"""
    if not tide_rows:
        return "No tide data available for today."

    parts = []
    for row in tide_rows:
        label = "High" if row["tide_type"] == "H" else "Low"
        ts = row["timestamp"]
        if hasattr(ts, "astimezone"):
            ts = ts.astimezone(LA_TZ)
        time_str = ts.strftime("%I:%M %p").lstrip("0")
        parts.append(f"{label}: {row['height_ft']:.1f} ft at {time_str}")

    if sunset_str:
        parts.append(f"Sunset: {sunset_str}")

    return " | ".join(parts)


def _format_events_summary(events: list[asyncpg.Record], max_lines: int = 25) -> str:
    """Format events as flat text, one per line, starred for priority 3.

    [★] Title | Venue | March 10, 7:30 PM | music_performance
    """
    if not events:
        return "No upcoming events in the next 14 days."

    p3 = [e for e in events if e.get("auto_priority", 1) >= 3]
    p2 = [e for e in events if e.get("auto_priority", 1) == 2]

    selected = list(p3)
    remaining_slots = max(0, max_lines - len(selected))
    selected.extend(p2[:remaining_slots])
    selected.sort(key=lambda e: e["start_at"])

    lines = []
    for ev in selected:
        start = ev["start_at"]
        if hasattr(start, "astimezone"):
            start = start.astimezone(LA_TZ)

        date_str = start.strftime("%B %-d")
        time_str = start.strftime("%I:%M %p").lstrip("0")
        venue = ev.get("venue_name") or "TBD"
        category = ev.get("category") or "other"
        star = "[★] " if ev.get("auto_priority", 1) >= 3 else ""

        lines.append(f"{star}{ev['title']} | {venue} | {date_str}, {time_str} | {category}")

    return "\n".join(lines)


def _get_sunset_str(today: date) -> str | None:
    """Get sunset time string for today."""
    try:
        from astral import LocationInfo
        from astral.sun import sun as astral_sun

        location = LocationInfo("Santa Monica", "US", "America/Los_Angeles", 34.0195, -118.4912)
        s = astral_sun(location.observer, date=today)
        sunset = s["sunset"].astimezone(LA_TZ)
        return sunset.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return None


# ============================================================
# Database queries
# ============================================================


async def _fetch_events_next_14_days(
    pool: asyncpg.Pool, today: date
) -> list[asyncpg.Record]:
    start = datetime(today.year, today.month, today.day, tzinfo=LA_TZ)
    end = start + timedelta(days=14)

    return await pool.fetch(
        """
        SELECT
            id, title, description, source_url,
            start_at, end_at, time_of_day,
            venue_name, venue_address, zone::text,
            category::text, tags, is_free, ticket_url,
            auto_priority, is_pinned, confidence_flag
        FROM events_view
        WHERE start_at >= $1 AND start_at < $2
        ORDER BY auto_priority DESC, start_at ASC
        """,
        start,
        end,
    )


async def _fetch_todays_tides(
    pool: asyncpg.Pool, today: date
) -> list[asyncpg.Record]:
    start = datetime(today.year, today.month, today.day, tzinfo=LA_TZ)
    end = start + timedelta(days=1)

    return await pool.fetch(
        """
        SELECT timestamp, height_ft, tide_type
        FROM tides
        WHERE timestamp >= $1 AND timestamp < $2
        ORDER BY timestamp ASC
        """,
        start,
        end,
    )


async def _fetch_wavecast_forecast(
    pool: asyncpg.Pool,
) -> tuple[str | None, str | None]:
    row = await pool.fetchrow(
        """
        SELECT forecast_text, forecast_date
        FROM forecasts
        WHERE source = 'wavecast'
        ORDER BY fetched_at DESC
        LIMIT 1
        """
    )
    if row and row["forecast_text"]:
        return row["forecast_text"][:3000], str(row["forecast_date"])
    return None, None


async def _write_report(
    pool: asyncpg.Pool,
    report_date: date,
    report_type: str,
    report_text: str,
    model_used: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> None:
    await pool.execute(
        """
        INSERT INTO daily_reports (report_date, report_type, report_text, model_used, prompt_tokens, completion_tokens)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (report_date, report_type) DO UPDATE SET
            report_text = EXCLUDED.report_text,
            generated_at = now(),
            model_used = EXCLUDED.model_used,
            prompt_tokens = EXCLUDED.prompt_tokens,
            completion_tokens = EXCLUDED.completion_tokens
        """,
        report_date,
        report_type,
        report_text,
        model_used,
        prompt_tokens,
        completion_tokens,
    )


# ============================================================
# Surf report
# ============================================================


async def generate_surf_report(pool: asyncpg.Pool) -> str | None:
    """Generate the surf forecast report."""
    today = date.today()

    try:
        tide_rows = await _fetch_todays_tides(pool, today)
        forecast_text, forecast_date = await _fetch_wavecast_forecast(pool)
    except Exception as e:
        logger.error(f"Failed to query data for surf report: {e}", exc_info=True)
        return None

    sunset_str = _get_sunset_str(today)
    tide_summary = _format_tide_summary(tide_rows, sunset_str)

    fc_date_label = forecast_date or "unknown"
    fc_text = forecast_text or "No Wavecast forecast available."

    user_prompt = f"""Today is {today.strftime('%A, %B %-d, %Y')}. Here is the data:

TIDE DATA ({today}):
{tide_summary}

WAVECAST FORECAST (fetched {fc_date_label}):
{fc_text}

Write the surf forecast summary."""

    try:
        client = AsyncAnthropic()
        response = await client.messages.create(
            model=MODEL,
            max_tokens=600,
            system=SURF_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        report_text = response.content[0].text
        prompt_tokens = response.usage.input_tokens
        completion_tokens = response.usage.output_tokens
    except Exception as e:
        logger.error(f"Claude API call failed for surf report: {e}", exc_info=True)
        return None

    try:
        await _write_report(pool, today, "surf", report_text, MODEL, prompt_tokens, completion_tokens)
    except Exception as e:
        logger.error(f"Failed to write surf report to DB: {e}", exc_info=True)
        return report_text

    logger.info(f"Surf report generated for {today}. Tokens: {prompt_tokens} in / {completion_tokens} out.")
    return report_text


# ============================================================
# Events report
# ============================================================


async def generate_events_report(pool: asyncpg.Pool) -> str | None:
    """Generate the events outlook report."""
    today = date.today()

    try:
        events = await _fetch_events_next_14_days(pool, today)
    except Exception as e:
        logger.error(f"Failed to query data for events report: {e}", exc_info=True)
        return None

    events_summary = _format_events_summary(events, max_lines=25)

    user_prompt = f"""Today is {today.strftime('%A, %B %-d, %Y')}. Here are the upcoming events:

UPCOMING EVENTS (next 14 days, priority events first):
{events_summary}

Write the events outlook."""

    try:
        client = AsyncAnthropic()
        response = await client.messages.create(
            model=MODEL,
            max_tokens=600,
            system=EVENTS_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        report_text = response.content[0].text
        prompt_tokens = response.usage.input_tokens
        completion_tokens = response.usage.output_tokens
    except Exception as e:
        logger.error(f"Claude API call failed for events report: {e}", exc_info=True)
        return None

    try:
        await _write_report(pool, today, "events", report_text, MODEL, prompt_tokens, completion_tokens)
    except Exception as e:
        logger.error(f"Failed to write events report to DB: {e}", exc_info=True)
        return report_text

    logger.info(f"Events report generated for {today}. Tokens: {prompt_tokens} in / {completion_tokens} out.")
    return report_text


# ============================================================
# Legacy combined report (kept for backward compat during transition)
# ============================================================


async def generate_daily_report(pool: asyncpg.Pool) -> str | None:
    """Generate both reports. Returns the surf report text (or events if surf fails)."""
    surf = await generate_surf_report(pool)
    events = await generate_events_report(pool)
    return surf or events


# ============================================================
# Read helpers
# ============================================================


async def _get_report_by_type(
    pool: asyncpg.Pool, target_date: date, report_type: str
) -> dict | None:
    """Get a specific report type for a date, falling back to most recent."""
    row = await pool.fetchrow(
        "SELECT report_date, report_text, generated_at FROM daily_reports WHERE report_date = $1 AND report_type = $2",
        target_date,
        report_type,
    )
    if row:
        return {
            "report_date": str(row["report_date"]),
            "report_text": row["report_text"],
            "generated_at": row["generated_at"],
            "is_stale": False,
        }

    # Fallback: most recent of this type
    row = await pool.fetchrow(
        "SELECT report_date, report_text, generated_at FROM daily_reports WHERE report_type = $1 ORDER BY report_date DESC LIMIT 1",
        report_type,
    )
    if row:
        return {
            "report_date": str(row["report_date"]),
            "report_text": row["report_text"],
            "generated_at": row["generated_at"],
            "is_stale": True,
        }

    return None
