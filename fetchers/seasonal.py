"""
Seasonal anchor events seed fetcher.

Source: seed data (no live scraping)
Tier: seed

Returns hardcoded seasonal anchor events — festivals, car shows, museum
openings, community events — that don't have a dedicated scraper but are
important enough to track. Updated annually with new dates.

Unlike other seed fetchers, events here span multiple categories and have
individually set is_one_off and is_free flags.

Dedup key: source_url using seed:// scheme (e.g., seed://seasonal/topanga-days-2026).
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fetcher_contract import EventData, FetchOutcome, FetchResult

SOURCE_NAME = "seasonal"
LA_TZ = ZoneInfo("America/Los_Angeles")


# ============================================================
# Seed data — update annually
# ============================================================

_SEED_EVENTS: list[dict] = [
    {
        "title": "Reggae on the Mountain",
        "venue_name": "Topanga Community House",
        "venue_address": "Topanga, CA",
        "start_at": datetime(2026, 7, 25, 11, 0, tzinfo=LA_TZ),
        "end_at": None,
        "category": "music_performance",
        "tags": ["reggae", "festival", "topanga"],
        "is_free": False,
        "is_one_off": False,
        "slug": "reggae-on-the-mountain-2026",
    },
    {
        "title": "Topanga Days",
        "venue_name": "Topanga Community Center",
        "venue_address": "Topanga, CA",
        "start_at": datetime(2026, 5, 23, 10, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 5, 25, 18, 0, tzinfo=LA_TZ),
        "category": "festival_outdoor",
        "tags": ["festival", "topanga", "community"],
        "is_free": False,
        "is_one_off": False,
        "slug": "topanga-days-2026",
    },
    {
        "title": "LACMA David Geffen Galleries Opening",
        "venue_name": "LACMA",
        "venue_address": "Los Angeles, CA",
        "start_at": datetime(2026, 4, 19, 10, 0, tzinfo=LA_TZ),
        "end_at": None,
        "category": "visual_art",
        "tags": ["lacma", "museum", "opening", "priority"],
        "is_free": True,
        "is_one_off": True,
        "slug": "lacma-geffen-galleries-opening-2026",
    },
    {
        "title": "Pebble Beach Concours d'Elegance",
        "venue_name": "Pebble Beach Golf Links",
        "venue_address": "Pebble Beach, CA",
        "start_at": datetime(2026, 8, 16, 9, 0, tzinfo=LA_TZ),
        "end_at": None,
        "category": "festival_outdoor",
        "tags": ["cars", "concours", "monterey"],
        "is_free": False,
        "is_one_off": False,
        "slug": "pebble-beach-concours-2026",
    },
    {
        "title": "Werks Reunion Monterey",
        "venue_name": None,
        "venue_address": "Monterey, CA",
        "start_at": datetime(2026, 8, 14, 9, 0, tzinfo=LA_TZ),
        "end_at": None,
        "category": "festival_outdoor",
        "tags": ["cars", "porsche", "monterey"],
        "is_free": False,
        "is_one_off": False,
        "slug": "werks-reunion-monterey-2026",
    },
    {
        "title": "LA Roadster Show",
        "venue_name": "Fairplex",
        "venue_address": "Pomona, CA",
        "start_at": datetime(2026, 6, 13, 9, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 6, 14, 17, 0, tzinfo=LA_TZ),
        "category": "festival_outdoor",
        "tags": ["cars", "hot_rod", "show"],
        "is_free": False,
        "is_one_off": False,
        "slug": "la-roadster-show-2026",
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
            category=seed["category"],
            tags=seed["tags"],
            is_free=seed["is_free"],
            is_one_off=seed["is_one_off"],
        ))

    return FetchResult(
        source_name=SOURCE_NAME,
        outcome=FetchOutcome.SUCCESS,
        events=events,
        events_found=len(events),
    )
