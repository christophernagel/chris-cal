"""
Resident Advisor Los Angeles fetcher.

Source: RA GraphQL API (https://ra.co/graphql)
Tier: supplemental (24h fetch)

Uses RA's public GraphQL API to fetch electronic music events in the
Los Angeles area (area ID 23). No HTML scraping — pure JSON API.

Dedup key: source_url (https://ra.co/events/{id}).

Classification: all RA events are music_performance. This is a fact
about the source — RA is exclusively a music/nightlife platform.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "ra_la"
GRAPHQL_URL = "https://ra.co/graphql"
RA_AREA_ID = 23  # Los Angeles
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://ra.co/events/us/losangeles",
    "Content-Type": "application/json",
}


def _build_query(start_date: str) -> dict[str, Any]:
    """Build the GraphQL request payload.

    Args:
        start_date: YYYY-MM-DD formatted date for the listingDate filter.
    """
    query = """
    {
      eventListings(
        filters: { areas: { eq: 23 }, listingDate: { gte: "%s" } },
        pageSize: 50
      ) {
        data {
          listingDate
          event {
            id
            title
            contentUrl
            startTime
            endTime
            cost
            minimumAge
            venue {
              name
              address
              contentUrl
            }
            artists {
              name
            }
          }
        }
      }
    }
    """ % start_date
    return {"query": query}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse RA Los Angeles events. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        today = date.today().isoformat()
        payload = _build_query(today)

        resp = await client.post(GRAPHQL_URL, json=payload, follow_redirects=True)
        resp.raise_for_status()

        data = resp.json()

        # Navigate the GraphQL response
        listings = (
            data.get("data", {})
            .get("eventListings", {})
            .get("data", [])
        )
        events_found = len(listings)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No listings returned from RA GraphQL API. Query or structure may have changed.",
            )

        for i, listing in enumerate(listings):
            try:
                event = _parse_listing(listing)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="listing",
                    message=f"Failed to parse listing {i}: {e}",
                    raw_value=str(listing),
                ))

        outcome = FetchOutcome.SUCCESS if not warnings else FetchOutcome.PARTIAL
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=outcome,
            events=events,
            events_found=events_found,
            parse_warnings=warnings,
        )

    except httpx.TimeoutException as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.TIMEOUT,
            events_found=events_found,
            error_message=f"Timeout fetching {GRAPHQL_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"HTTP {e.response.status_code} from {GRAPHQL_URL}",
        )
    except Exception as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"Unexpected error: {type(e).__name__}: {e}",
        )
    finally:
        if owns_client:
            await client.aclose()


# ============================================================
# Listing parsing
# ============================================================

def _parse_listing(listing: dict[str, Any]) -> EventData | None:
    """Parse a single RA GraphQL listing into an EventData."""
    event = listing.get("event")
    if not event:
        return None

    title = event.get("title")
    if not title:
        return None

    content_url = event.get("contentUrl")
    if not content_url:
        return None
    source_url = f"https://ra.co{content_url}"

    # Parse start time (local LA time, no TZ in the string)
    start_time_str = event.get("startTime")
    if not start_time_str:
        return None
    start_at = _parse_ra_datetime(start_time_str)
    if start_at is None:
        return None

    # Parse end time (optional)
    end_at = None
    end_time_str = event.get("endTime")
    if end_time_str:
        end_at = _parse_ra_datetime(end_time_str)

    # Venue
    venue = event.get("venue") or {}
    venue_name = venue.get("name")
    venue_address = venue.get("address")

    # Cost / pricing
    cost = event.get("cost")
    is_free = cost is None or cost == "" or cost == "0"
    price_range = None if is_free else cost

    # Tags
    tags = _build_tags(event)

    # Description from artists
    description = _build_description(event)

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        venue_address=venue_address,
        category="music_performance",
        tags=tags,
        is_free=is_free,
        price_range=price_range,
        is_one_off=True,
        description=description,
        raw_source=listing,
    )


def _parse_ra_datetime(dt_str: str) -> datetime | None:
    """Parse RA datetime string into a timezone-aware datetime.

    RA returns local LA times without timezone info, e.g.:
      "2026-03-10T23:00:00.000"
    """
    try:
        # Strip trailing milliseconds variations
        dt_str = dt_str.rstrip("0").rstrip(".")
        dt = datetime.fromisoformat(dt_str)
        return dt.replace(tzinfo=LA_TZ)
    except (ValueError, AttributeError):
        return None


def _build_tags(event: dict[str, Any]) -> list[str]:
    """Build tags list from event data."""
    tags = ["electronic", "nightlife"]

    minimum_age = event.get("minimumAge")
    if minimum_age is not None and minimum_age >= 21:
        tags.append("21_plus")

    return tags


def _build_description(event: dict[str, Any]) -> str | None:
    """Build description from artists list if present."""
    artists = event.get("artists") or []
    if not artists:
        return None

    names = [a.get("name") for a in artists if a.get("name")]
    if not names:
        return None

    return f"Artists: {', '.join(names)}"
