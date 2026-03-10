"""
chriscal fetcher interface contract

Fetchers are pure functions: HTTP in, structured result out.
They do NOT touch the database, handle dedup, update health, or write logs.
All stateful operations belong to the ingest layer.

A fetcher module must expose:
    async def fetch() -> FetchResult

The scheduler calls fetch(), the ingest layer consumes the result.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ============================================================
# Fetcher result types
# ============================================================

class FetchOutcome(str, Enum):
    """Status of a single fetch attempt, as reported by the fetcher itself.

    Note: the ingest layer may reclassify 'success' as 'partial' if baseline
    deviation is detected (events_found < 30% of avg_events_per_fetch).
    The fetcher doesn't know the baseline — it only knows what it got.
    """
    SUCCESS = "success"      # clean fetch, all events parsed
    PARTIAL = "partial"      # fetch ran but some events failed to parse
    ERROR = "error"          # hard failure (HTTP error, unreachable, auth wall)
    TIMEOUT = "timeout"      # exceeded time limit


@dataclass(frozen=True)
class ParseWarning:
    """A single parse issue within an otherwise successful fetch."""
    event_index: int | None    # position in source listing, None if not applicable
    field: str                 # which field failed to parse
    message: str               # human-readable description
    raw_value: str | None      # the unparseable value, for debugging


@dataclass(frozen=True)
class EventData:
    """A single event as returned by a fetcher.

    All fields populated by the fetcher — this is the fetcher's interpretation
    of the source data. The ingest layer trusts these values for writes.

    Fetchers own classification: category, tags, is_one_off, is_free.
    These are facts about the source, not heuristics.
    """
    # identity
    title: str
    source_url: str                          # direct link to event; used as dedup key with source_id
    series_id: str | None = None             # for recurring events; None if one-off or unknown

    # time
    start_at: datetime                       # must be timezone-aware (UTC or local with tzinfo)
    end_at: datetime | None = None

    # place
    venue_name: str | None = None            # matched to venues table by ingest layer
    venue_address: str | None = None         # used for venue lookup/creation if venue_name is new

    # category + tags (fetcher owns classification)
    category: str = "music_performance"      # must match event_category enum values
    tags: list[str] = field(default_factory=list)

    # access
    is_free: bool = True
    ticket_url: str | None = None
    price_range: str | None = None           # e.g. '$15-25', 'Free', 'Donation'

    # classification
    is_one_off: bool = False                 # fetcher sets deliberately; see schema note on series_id contradiction

    # description
    description: str | None = None

    # raw source data for debugging (stored as JSONB)
    raw_source: dict[str, Any] | None = None


@dataclass(frozen=True)
class FetchResult:
    """Complete result of a single fetcher execution.

    This is the only thing a fetcher returns. The ingest layer uses it
    to write events, update fetch_log, and evaluate source health.

    A fetcher should ALWAYS return this — never raise exceptions as control flow.
    If something goes wrong, return a FetchResult with outcome=ERROR and
    the error detail in error_message.
    """
    # which source produced this result
    source_name: str                         # matches sources.name in DB

    # outcome
    outcome: FetchOutcome
    events: list[EventData] = field(default_factory=list)

    # diagnostics
    events_found: int = 0                    # total events seen in source (before parse failures)
    parse_warnings: list[ParseWarning] = field(default_factory=list)
    error_message: str | None = None         # populated on ERROR/TIMEOUT, optional on PARTIAL
    duration_ms: int | None = None           # wall-clock time for the fetch; set by scheduler wrapper

    @property
    def events_parsed(self) -> int:
        """Events successfully parsed (len of events list)."""
        return len(self.events)


# ============================================================
# Fetcher module contract
# ============================================================

# Every fetcher module lives in chriscal/fetchers/<source_name>.py
# and exposes a single async function:
#
#     async def fetch() -> FetchResult
#
# The function:
#   - Makes HTTP requests to the source
#   - Parses the response into EventData instances
#   - Returns a FetchResult with outcome, events, and diagnostics
#   - Does NOT write to the database
#   - Does NOT handle dedup (that's the ingest layer's job)
#   - Does NOT update source health or fetch_log
#   - Should catch its own exceptions and return ERROR/PARTIAL results
#   - Must set events_found to the total count seen in the source HTML/feed,
#     even if some failed to parse (so ingest layer can detect partial failure)
#
# The scheduler wraps each fetch() call in a try/except as a safety net.
# If a fetcher raises despite the contract, the scheduler logs it as ERROR.


# ============================================================
# Ingest layer responsibilities (not implemented here)
# ============================================================

# After receiving a FetchResult, the ingest layer:
#
# 1. DEDUP — for each EventData, check (source_id, source_url) uniqueness.
#    Insert new events, update existing events if content changed,
#    skip exact duplicates. Track counts: events_inserted, events_skipped.
#
# 2. VENUE RESOLUTION — match EventData.venue_name to venues table.
#    Create new venue if not found (with zone from a default lookup table).
#    Copy venue.zone to event.zone (unless manually overridden).
#
# 3. BASELINE DEVIATION — compare FetchResult.events_found against
#    source.avg_events_per_fetch. If events_found < (avg * 0.3) AND
#    the fetcher reported SUCCESS, reclassify to PARTIAL in the log.
#    This catches the "200 OK but content is empty/different" failure mode.
#
# 4. FETCH LOG — write a row to fetch_log with:
#    source_id, status (possibly reclassified), events_found,
#    events_inserted, events_skipped, parse_warnings (as JSONB),
#    error_message, duration_ms.
#
# 5. SOURCE HEALTH UPDATE —
#    a. Update sources: last_fetch_at, last_fetch_status, last_successful_fetch
#       (only on success), fetch_error_log (on error).
#    b. Recalculate health_score from last 20 fetch_log entries:
#       success=100, partial=50, error/timeout/stale=0, avg of scores.
#    c. Update baseline (only after SUCCESS, not partial/error):
#       new_avg = (old_avg * sample_size + events_found) / (sample_size + 1)
#       sample_size += 1
#       This is an incremental moving average — no window query needed.
