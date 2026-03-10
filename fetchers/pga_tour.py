"""
PGA Tour West Coast seed fetcher.

Source: seed data (no live scraping)
Tier: seed

Returns hardcoded PGA Tour events on the West Coast for the current season.
Updated annually with new dates. The scheduler ingests these like any other
source, and the ingest layer handles dedup and venue resolution.

Dedup key: source_url using seed:// scheme (e.g., seed://pga_tour/farmers-insurance-open-2026).
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fetcher_contract import EventData, FetchOutcome, FetchResult

SOURCE_NAME = "pga_tour"
LA_TZ = ZoneInfo("America/Los_Angeles")


# ============================================================
# Seed data — update annually
# ============================================================

_SEED_EVENTS: list[dict] = [
    {
        "title": "Farmers Insurance Open",
        "venue_name": "Torrey Pines Golf Course",
        "venue_address": "La Jolla, CA",
        "start_at": datetime(2026, 1, 21, 8, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 1, 24, 18, 0, tzinfo=LA_TZ),
        "ticket_url": "https://www.pgatour.com/tournaments/farmers-insurance-open",
        "tags": ["golf", "pga"],
        "slug": "farmers-insurance-open-2026",
    },
    {
        "title": "AT&T Pebble Beach Pro-Am",
        "venue_name": "Pebble Beach Golf Links",
        "venue_address": "Pebble Beach, CA",
        "start_at": datetime(2026, 2, 4, 8, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 2, 7, 18, 0, tzinfo=LA_TZ),
        "ticket_url": "https://www.pgatour.com/tournaments/att-pebble-beach-pro-am",
        "tags": ["golf", "pga"],
        "slug": "att-pebble-beach-pro-am-2026",
    },
    {
        "title": "Genesis Invitational",
        "venue_name": "Riviera Country Club",
        "venue_address": "Pacific Palisades, CA",
        "start_at": datetime(2026, 2, 18, 8, 0, tzinfo=LA_TZ),
        "end_at": datetime(2026, 2, 21, 18, 0, tzinfo=LA_TZ),
        "ticket_url": "https://www.pgatour.com/tournaments/the-genesis-invitational",
        "tags": ["golf", "pga", "priority"],
        "slug": "genesis-invitational-2026",
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
            is_free=False,
            ticket_url=seed["ticket_url"],
            is_one_off=False,
        ))

    return FetchResult(
        source_name=SOURCE_NAME,
        outcome=FetchOutcome.SUCCESS,
        events=events,
        events_found=len(events),
    )
