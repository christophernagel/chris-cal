"""
Billy Wilder Theater / UCLA Film & TV Archive fetcher.

Source: https://cinema.ucla.edu
Tier: anchor

Fetches events from the UCLA FTVA Elasticsearch API. No HTML scraping —
all data comes from structured JSON via the ES _search endpoint.

Dedup key: source_url (built from BASE_URL + uri field).

Classification: all Billy Wilder events are film_screening. This is a fact
about the source — the UCLA Film & Television Archive programs repertory
cinema, restorations, and archival screenings.

Structure:
  - Single POST to Elasticsearch endpoint returns up to 30 events
  - Each hit contains title, dates, location, ticket info, screening details
  - Dates are LA-local strings without timezone info
  - Events are pre-sorted by startDate ascending
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import os

import httpx

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "billy_wilder"
BASE_URL = "https://cinema.ucla.edu"
ES_URL = "https://elastical.library.ucla.edu/apps-prod-ftva-website/_search"
ES_API_KEY = os.environ.get("UCLA_ES_API_KEY", "")
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)",
    "Authorization": f"ApiKey {ES_API_KEY}",
    "Content-Type": "application/json",
}

# ES query: match ftvaEvent section, only future events, sorted by startDate asc
ES_QUERY = {
    "size": 30,
    "query": {
        "bool": {
            "must": [
                {"match": {"sectionHandle": "ftvaEvent"}},
            ],
            "filter": [
                {"range": {"startDate": {"gte": "now"}}},
            ],
        }
    },
    "sort": [{"startDate": {"order": "asc"}}],
    "_source": [
        "title",
        "startDateWithTime",
        "startDate",
        "endDateWithTime",
        "location",
        "uri",
        "slug",
        "ftvaTicketInformation",
        "ftvaEventTypeFilters",
        "ftvaEventScreeningDetails",
        "eventDescription",
        "introduction",
        "guestSpeaker",
        "tagLabels",
    ],
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Billy Wilder Theater events from ES API. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        resp = await client.post(ES_URL, json=ES_QUERY, headers=HEADERS)
        resp.raise_for_status()

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        events_found = len(hits)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No events returned from ES query. Index or query may have changed.",
            )

        for i, hit in enumerate(hits):
            try:
                source = hit.get("_source", {})
                event = _parse_event(source)
                if event is not None:
                    events.append(event)
                else:
                    warnings.append(ParseWarning(
                        event_index=i,
                        field="title_or_date",
                        message="Missing required title or start date",
                        raw_value=str(source.get("title")),
                    ))
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="event",
                    message=f"Failed to parse event: {e}",
                    raw_value=str(hit.get("_source", {}).get("title")),
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
            error_message=f"Timeout fetching {ES_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"HTTP {e.response.status_code} from {ES_URL}",
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
# Event parsing
# ============================================================

def _parse_event(source: dict) -> EventData | None:
    """Parse a single ES hit _source into an EventData."""
    title = source.get("title")
    if not title or not isinstance(title, str):
        return None

    # Parse start time
    start_at = _parse_datetime(source.get("startDateWithTime") or source.get("startDate"))
    if start_at is None:
        return None

    # Parse end time (optional)
    end_at = _parse_datetime(source.get("endDateWithTime"))

    # Build source URL from uri
    uri = source.get("uri", "")
    source_url = f"{BASE_URL}/{uri}" if uri else BASE_URL

    # Venue from location array
    venue_name = _extract_venue(source.get("location"))

    # Free/paid from ticket info
    is_free, ticket_url = _extract_ticket_info(source.get("ftvaTicketInformation"))

    # Tags from event type filters
    tags = _extract_tags(source.get("ftvaEventTypeFilters"))

    # Description from eventDescription (strip HTML)
    description = _strip_html(source.get("eventDescription"))
    if description and len(description) > 1000:
        description = description[:1000]

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        category="film_screening",
        tags=tags,
        is_free=is_free,
        ticket_url=ticket_url,
        is_one_off=True,
        description=description,
        raw_source=source,
    )


# ============================================================
# Field extraction helpers
# ============================================================

def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ES datetime string into timezone-aware datetime.

    Input formats: "2026-02-15T19:00" or "2026-02-15T19:00:00" (LA local, no tz).
    """
    if not value or not isinstance(value, str):
        return None

    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=LA_TZ)
        except ValueError:
            continue
    return None


def _extract_venue(location: list | None) -> str | None:
    """Extract venue name from location array."""
    if not location or not isinstance(location, list):
        return None
    if len(location) > 0 and isinstance(location[0], dict):
        return location[0].get("title")
    return None


def _extract_ticket_info(ticket_info: list | None) -> tuple[bool, str | None]:
    """Extract free/paid status and ticket URL from ftvaTicketInformation.

    Returns (is_free, ticket_url).
    """
    if not ticket_info or not isinstance(ticket_info, list):
        return True, None  # default to free if no ticket info

    is_free = False
    ticket_url = None

    for item in ticket_info:
        if not isinstance(item, dict):
            continue
        item_title = item.get("title", "")
        if "free" in item_title.lower():
            is_free = True
        item_uri = item.get("uri")
        if item_uri and not ticket_url:
            ticket_url = item_uri if item_uri.startswith("http") else f"{BASE_URL}/{item_uri}"

    return is_free, ticket_url


def _extract_tags(type_filters: list | None) -> list[str]:
    """Extract tags from ftvaEventTypeFilters titles.

    Converts titles to lowercase with underscores (e.g., "Guest Speaker" -> "guest_speaker").
    """
    if not type_filters or not isinstance(type_filters, list):
        return []

    tags = []
    for item in type_filters:
        if isinstance(item, dict) and item.get("title"):
            tag = item["title"].strip().lower().replace(" ", "_")
            if tag and tag not in tags:
                tags.append(tag)
    return tags


def _strip_html(html: str | None) -> str | None:
    """Strip HTML tags from a string, returning plain text."""
    if not html or not isinstance(html, str):
        return None
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None
