"""
CAP UCLA calendar fetcher.

Source: https://cap.ucla.edu/calendar
Tier: anchor (weight 3.0)

Scrapes the calendar listing page for event cards, then fetches each
event detail page for full metadata (description, ticket links, price).

Dedup key: source_url (stable per-event URLs like /event/charles-gaines).

Classification: all CAP UCLA events are music_performance. This is a fact
about the source, not a heuristic — CAP programs contemporary music, jazz,
experimental performance, dance, and theater.

Structure (as of March 2026):
  - Listing page: <a class="views-row plain" href="/event/slug"> cards
    with Drupal Views field divs:
      .views-field-field-event-date    → <time datetime="ISO"> + text
      .views-field-field-event-artist  → artist name
      .views-field-field-event-title   → event title
      .views-field-field-event-venue   → venue name
      .views-field-field-event-type    → e.g. "Live performance"
      .views-field-field-event-genre   → e.g. "Contemporary Classical, Theater"
  - Detail page: <h1> title, <a href="/venue/..."> venue, description,
    ticket links to ucla.evenue.net
  - No pagination (single page, ~10-15 events)
  - Drupal 10 CMS
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "cap_ucla"
BASE_URL = "https://cap.ucla.edu"
CALENDAR_URL = f"{BASE_URL}/calendar"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse CAP UCLA calendar. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        # Fetch listing page
        resp = await client.get(CALENDAR_URL, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        event_links = _extract_event_links(soup)
        events_found = len(event_links)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event links found on calendar page. Structure may have changed.",
            )

        # Fetch each detail page
        for i, (href, listing_data) in enumerate(event_links):
            try:
                event = await _fetch_event_detail(client, href, listing_data)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="detail_page",
                    message=f"Failed to parse detail page {href}: {e}",
                    raw_value=href,
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
            error_message=f"Timeout fetching {CALENDAR_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"HTTP {e.response.status_code} from {CALENDAR_URL}",
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
# Listing page parsing
# ============================================================

def _extract_event_links(soup: BeautifulSoup) -> list[tuple[str, dict[str, str]]]:
    """Extract event hrefs and listing-level metadata from the calendar page.

    Returns list of (href, {title, artist, date_text, venue_text, genre}) tuples.
    Listing data is used as fallback if the detail page is incomplete.
    """
    results = []

    # Find all event cards: <a class="views-row" href="/event/...">
    for link in soup.find_all("a", class_="views-row", href=True):
        href = link["href"]
        if not href.startswith("/event/"):
            continue

        listing_data: dict[str, str] = {}

        # Helper: get text from a Drupal Views field div
        def _field_text(field_name: str) -> str | None:
            div = link.find("div", class_=f"views-field-field-event-{field_name}")
            if div:
                content = div.find("div", class_="field-content")
                if content:
                    return content.get_text(strip=True)
            return None

        # Title: combine artist + event title (e.g. "Charles Gaines — Manifestos 6")
        artist = _field_text("artist")
        event_title = _field_text("title")
        if artist and event_title:
            listing_data["title"] = f"{artist} — {event_title}"
            listing_data["artist"] = artist
        elif artist:
            listing_data["title"] = artist
        elif event_title:
            listing_data["title"] = event_title

        # Date: build text from <time> tag + surrounding text
        date_div = link.find("div", class_="views-field-field-event-date")
        if date_div:
            content = date_div.find("div", class_="field-content")
            if content:
                listing_data["date_text"] = content.get_text(strip=True)
                # Also grab ISO datetime from <time> tag as a bonus
                time_tag = content.find("time")
                if time_tag and time_tag.get("datetime"):
                    listing_data["datetime_iso"] = time_tag["datetime"]

        # Venue
        venue = _field_text("venue")
        if venue:
            listing_data["venue_text"] = venue

        # Genre (for tag extraction)
        genre = _field_text("genre")
        if genre:
            listing_data["genre"] = genre

        if listing_data.get("title"):
            results.append((href, listing_data))

    return results


def _looks_like_date(text: str) -> bool:
    """Heuristic: does this string look like a CAP UCLA date line?
    Format: 'Sat, Mar 14 | 8 pm' or 'Fri, Mar 20 - Sat, Mar 21 | 8 pm'
    """
    return bool(re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)", text) and "|" in text)


# ============================================================
# Detail page parsing
# ============================================================

async def _fetch_event_detail(
    client: httpx.AsyncClient,
    href: str,
    listing_data: dict[str, str],
) -> EventData | None:
    """Fetch and parse a single event detail page."""
    url = f"{BASE_URL}{href}"
    resp = await client.get(url, follow_redirects=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title: prefer detail page <h1>, fall back to listing
    title = None
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    title = title or listing_data.get("title")
    if not title:
        return None  # can't create an event without a title

    # Date/time: parse from detail page text or listing fallback
    date_text = listing_data.get("date_text", "")
    start_at, end_at = _parse_date_range(date_text)
    if start_at is None:
        return None  # can't create an event without a start time

    # Venue: from detail page link or listing fallback
    venue_name = _extract_venue_from_detail(soup) or listing_data.get("venue_text")

    # Description: first substantial paragraph from the body
    description = _extract_description(soup)

    # Ticket info
    ticket_url, is_free = _extract_ticket_info(soup)

    # Detect series: "Wild Up at The Nimoy" prefix pattern, school matinees
    series_id = _detect_series(title)
    is_one_off = series_id is None

    # Tags from content signals
    tags = _extract_tags(title, description or "")

    return EventData(
        title=title,
        source_url=url,
        series_id=series_id,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        category="music_performance",
        tags=tags,
        is_free=is_free,
        ticket_url=ticket_url,
        is_one_off=is_one_off,
        description=description,
        raw_source={
            "href": href,
            "listing_data": listing_data,
            "detail_url": url,
        },
    )


def _extract_venue_from_detail(soup: BeautifulSoup) -> str | None:
    """Extract venue name from detail page. CAP links venues as <a href='/venue/...'>."""
    link = soup.find("a", href=re.compile(r"^/venue/"))
    if link:
        return link.get_text(strip=True)
    return None


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract first substantial paragraph as description.

    Skips short lines (navigation, dates, venue names) and returns
    the first paragraph with real descriptive content.
    """
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        # Skip short strings, date-like strings, navigation
        if len(text) > 80 and not _looks_like_date(text):
            # Truncate to 1000 chars for storage
            return text[:1000] if len(text) > 1000 else text
    return None


def _extract_ticket_info(soup: BeautifulSoup) -> tuple[str | None, bool]:
    """Extract ticket URL and free/paid status.

    CAP UCLA uses ucla.evenue.net for ticketing. If no ticket link
    is found, assume free (conservative default for this source).
    """
    ticket_link = soup.find("a", href=re.compile(r"evenue\.net"))
    if ticket_link:
        href = ticket_link["href"]
        # Check if the link text suggests free admission
        text = ticket_link.get_text(strip=True).upper()
        is_free = "FREE" in text
        return href, is_free

    # No ticket link — check for explicit "free" text on page
    page_text = soup.get_text().upper()
    if "FREE ADMISSION" in page_text or "FREE EVENT" in page_text:
        return None, True

    # Default: assume ticketed for CAP UCLA (most events are)
    return None, False


def _detect_series(title: str) -> str | None:
    """Detect recurring series from title patterns.

    Known CAP UCLA series:
      - "Wild Up at The Nimoy: ..." → series_id "cap_ucla_wild_up"
      - "School Matinee" events → series_id "cap_ucla_school_matinee"
    """
    title_lower = title.lower()
    if "wild up" in title_lower:
        return "cap_ucla_wild_up"
    if "school matinee" in title_lower:
        return "cap_ucla_school_matinee"
    return None


def _extract_tags(title: str, description: str) -> list[str]:
    """Extract tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {description}".lower()

    tag_signals = {
        "jazz": ["jazz", "trio", "quartet"],
        "contemporary": ["contemporary", "new music", "experimental"],
        "dance": ["dance", "choreograph"],
        "theater": ["theater", "theatre", "playhouse", "play"],
        "world_premiere": ["world premiere", "west coast premiere"],
        "composer": ["composer", "composition"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    return tags


# ============================================================
# Date parsing
# ============================================================

def _parse_date_range(text: str) -> tuple[datetime | None, datetime | None]:
    """Parse CAP UCLA date strings into timezone-aware datetimes.

    Formats:
      'Sat, Mar 14 | 8 pm'                          → single date
      'Fri, Mar 20 - Sat, Mar 21 | 8 pm'            → multi-day (same time)
      'Fri, Apr 10 | 10:45 am'                       → morning event

    Returns (start_at, end_at). end_at is None for single-date events.
    All times are America/Los_Angeles.
    """
    if not text:
        return None, None

    # Split on pipe to separate date(s) from time
    parts = text.split("|")
    if len(parts) != 2:
        return None, None

    date_part = parts[0].strip()
    time_part = parts[1].strip()

    # Parse time (e.g., "8 pm", "10:45 am")
    hour, minute = _parse_time(time_part)
    if hour is None:
        return None, None

    # Check for date range (contains " - ")
    if " - " in date_part:
        date_strs = date_part.split(" - ")
        start_date = _parse_single_date(date_strs[0].strip())
        end_date = _parse_single_date(date_strs[1].strip())

        if start_date and end_date:
            start_at = start_date.replace(hour=hour, minute=minute, tzinfo=LA_TZ)
            end_at = end_date.replace(hour=hour, minute=minute, tzinfo=LA_TZ)
            return start_at, end_at
        elif start_date:
            return start_date.replace(hour=hour, minute=minute, tzinfo=LA_TZ), None
        return None, None
    else:
        date = _parse_single_date(date_part)
        if date:
            return date.replace(hour=hour, minute=minute, tzinfo=LA_TZ), None
        return None, None


def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time string like '8 pm' or '10:45 am' into (hour_24, minute)."""
    text = text.strip().lower()
    match = re.match(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)", text)
    if not match:
        return None, 0

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    period = match.group(3)

    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0

    return hour, minute


def _parse_single_date(text: str) -> datetime | None:
    """Parse a single date like 'Sat, Mar 14' or 'Mar 14' into a datetime.

    Assumes current year. Handles the case where year wraps (Dec event
    parsed in January) by checking if the date is unreasonably far in the past.
    """
    # Strip day-of-week prefix (e.g., "Sat, ")
    text = re.sub(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s*", "", text.strip())

    # Try "Mon DD" format
    match = re.match(r"([A-Za-z]+)\s+(\d{1,2})", text)
    if not match:
        return None

    month_str = match.group(1)
    day = int(match.group(2))

    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    month = month_map.get(month_str.lower()[:3])
    if month is None:
        return None

    now = datetime.now(LA_TZ)
    year = now.year

    try:
        dt = datetime(year, month, day)
    except ValueError:
        return None

    # If the date is more than 2 months in the past, it's probably next year
    if (now.replace(tzinfo=None) - dt).days > 60:
        dt = dt.replace(year=year + 1)

    return dt
