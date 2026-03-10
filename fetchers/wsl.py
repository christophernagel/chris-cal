"""
WSL (World Surf League) seed fetcher.

Source: seed data (no live scraping)
Tier: seed

Returns hardcoded WSL and surf events on the California coast for the
current season. Updated annually with new dates. The scheduler ingests
these like any other source, and the ingest layer handles dedup and
venue resolution.

Dedup key: source_url using seed:// scheme (e.g., seed://wsl/surf-ranch-pro-2026).
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fetcher_contract import EventData, FetchOutcome, FetchResult

SOURCE_NAME = "wsl"
LA_TZ = ZoneInfo("America/Los_Angeles")


# ============================================================
# Seed data — update annually
# ============================================================

_SEED_EVENTS: list[dict] = [
    {
        "title": "Surf Ranch Pro",
        "venue_name": "WSL Surf Ranch",
        "venue_address": "Lemoore, CA",
        "start_at": datetime(2026, 6, 15, 7, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 6, 21, 18, 0, tzinfo=LA_TZ),
        "source_url": "https://www.worldsurfleague.com/events",
        "tags": ["surfing", "wsl"],
        "slug": "surf-ranch-pro-2026",
    },
    {
        "title": "US Open of Surfing",
        "venue_name": "Huntington Beach",
        "venue_address": "Huntington Beach, CA",
        "start_at": datetime(2026, 7, 25, 7, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 8, 2, 18, 0, tzinfo=LA_TZ),
        "source_url": "https://www.usopenofsurfing.com",
        "tags": ["surfing", "us_open"],
        "slug": "us-open-of-surfing-2026",
    },
    {
        "title": "WSL Finals",
        "venue_name": "Lower Trestles",
        "venue_address": "San Clemente, CA",
        "start_at": datetime(2026, 9, 7, 7, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 9, 14, 18, 0, tzinfo=LA_TZ),
        "source_url": "https://www.worldsurfleague.com/events",
        "tags": ["surfing", "wsl", "trestles"],
        "slug": "wsl-finals-2026",
    },
]


# ============================================================
# Fetcher entry point
# ============================================================

async def fetch() -> FetchResult:
    """Build EventData objects from seed data, filtering out past events."""
    now = datetime.now(LA_TZ)
    events: list[EventData] = []

    for seed in _SEED_EVENTS:
        if seed["start_at"] < now:
            continue

        events.append(EventData(
            title=seed["title"],
            source_url=f"seed://{SOURCE_NAME}/{seed['slug']}",
            start_at=seed["start_at"],
            end_at=seed["end_at"],
            venue_name=seed["venue_name"],
            venue_address=seed["venue_address"],
            category="festival_outdoor",
            tags=seed["tags"],
            is_free=True,
            is_one_off=False,
        ))

    return FetchResult(
        source_name=SOURCE_NAME,
        outcome=FetchOutcome.SUCCESS,
        events=events,
        events_found=len(events),
    )
