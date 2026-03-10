"""
Visit Topanga Canyon events fetcher.

Source: https://www.visittopangacanyon.com/events
Tier: supplemental (24h fetch)

Scrapes the Squarespace events listing page for event cards containing
date, title, venue, time, category tags, and event links.

Dedup key: source_url (stable per-event URLs like /events/silent-disco-august-2025).

Classification: category mapped from tags — "Live Music" -> music_performance,
everything else -> festival_outdoor (Topanga events are mostly outdoor/community).

Structure (Squarespace events page):
  - Date sections with month abbreviation + day number
  - Category tags like [Events], [Live Music], [Community]
  - Event title as heading/link
  - Venue name with map link
  - Time range: "5:30 PM 8:30 PM"
  - Brief description text
  - "View Event →" link with href like /events/slug
  - Google Calendar and ICS export links
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "topanga"
BASE_URL = "https://www.visittopangacanyon.com"
EVENTS_URL = f"{BASE_URL}/events"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Squarespace blocks generic bots; use a browser-like User-Agent
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Category tag -> event category mapping
CATEGORY_MAP = {
    "live music": "music_performance",
}
DEFAULT_CATEGORY = "festival_outdoor"

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Visit Topanga Canyon events. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        resp = await client.get(EVENTS_URL, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = _extract_event_cards(soup)
        events_found = len(cards)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event cards found on events page. Structure may have changed.",
            )

        for i, card_data in enumerate(cards):
            try:
                event = _parse_event_card(card_data)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="card",
                    message=f"Failed to parse event card: {e}",
                    raw_value=card_data.get("title"),
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
            error_message=f"Timeout fetching {EVENTS_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"HTTP {e.response.status_code} from {EVENTS_URL}",
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
# Event card extraction
# ============================================================

def _extract_event_cards(soup: BeautifulSoup) -> list[dict[str, str | list[str]]]:
    """Extract event data from Squarespace event listing cards.

    Squarespace event pages use an eventlist structure with individual
    event items. Each item contains date, title, venue, time, tags,
    description, and a link to the full event page.

    Returns a list of dicts with keys: month, day, title, venue, time_text,
    tags, description, href.
    """
    results: list[dict[str, str | list[str]]] = []

    # Squarespace event list items — try multiple selectors for resilience
    event_items = soup.select(".eventlist-event")
    if not event_items:
        # Fallback: look for article tags with event-like structure
        event_items = soup.select("article.eventlist-event")
    if not event_items:
        # Broader fallback: any article with date + title structure
        event_items = soup.select("[data-item-id]")

    for item in event_items:
        card: dict[str, str | list[str]] = {}

        # Date: month abbreviation and day number
        # Squarespace uses .eventlist-datetag with .eventlist-datetag-startdate
        date_tag = item.select_one(".eventlist-datetag-startdate")
        if date_tag:
            month_el = date_tag.select_one(".eventlist-datetag-startdate--month")
            day_el = date_tag.select_one(".eventlist-datetag-startdate--day")
            if month_el:
                card["month"] = month_el.get_text(strip=True)
            if day_el:
                card["day"] = day_el.get_text(strip=True)

        # Fallback: look for date in broader structure
        if "month" not in card:
            date_tag = item.select_one(".eventlist-datetag")
            if date_tag:
                text = date_tag.get_text(" ", strip=True)
                m = re.match(r"([A-Za-z]+)\s+(\d{1,2})", text)
                if m:
                    card["month"] = m.group(1)
                    card["day"] = m.group(2)

        # Title: heading inside the event item, usually an <a> with the event link
        title_el = item.select_one(".eventlist-title a")
        if title_el:
            card["title"] = title_el.get_text(strip=True)
            href = title_el.get("href", "")
            if href:
                card["href"] = href
        else:
            # Fallback: any heading
            for tag_name in ("h1", "h2", "h3", "h4"):
                heading = item.find(tag_name)
                if heading:
                    card["title"] = heading.get_text(strip=True)
                    link = heading.find("a", href=True)
                    if link:
                        card["href"] = link["href"]
                    break

        # Category tags: Squarespace uses .eventlist-cats with <a> links
        tags: list[str] = []
        cats_el = item.select_one(".eventlist-cats")
        if cats_el:
            for tag_link in cats_el.find_all("a"):
                tag_text = tag_link.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)
        card["tags"] = tags

        # Venue: look for address or location element
        # Squarespace uses .eventlist-meta-address or similar
        venue_el = item.select_one(".eventlist-meta-address")
        if venue_el:
            # The venue name is typically in a <span> or link
            venue_link = venue_el.find("a")
            if venue_link:
                card["venue"] = venue_link.get_text(strip=True)
            else:
                card["venue"] = venue_el.get_text(strip=True)

        # Fallback: look for any link with "map" in href (Google Maps links)
        if "venue" not in card:
            map_link = item.find("a", href=re.compile(r"maps\.google|google\.com/maps"))
            if map_link:
                card["venue"] = map_link.get_text(strip=True)

        # Time range: Squarespace uses .event-time-12hr or similar
        time_el = item.select_one(".event-time-12hr")
        if time_el:
            card["time_text"] = time_el.get_text(" ", strip=True)
        else:
            time_el = item.select_one(".event-time-12hr-start")
            if time_el:
                start_time = time_el.get_text(strip=True)
                end_time_el = item.select_one(".event-time-12hr-end")
                end_time = end_time_el.get_text(strip=True) if end_time_el else ""
                card["time_text"] = f"{start_time} {end_time}".strip()

        # Fallback: look for time in the meta section
        if "time_text" not in card:
            meta_el = item.select_one(".eventlist-meta-time")
            if meta_el:
                card["time_text"] = meta_el.get_text(" ", strip=True)

        # Broader time fallback: search for AM/PM pattern in meta area
        if "time_text" not in card:
            meta = item.select_one(".eventlist-meta")
            if meta:
                meta_text = meta.get_text(" ", strip=True)
                time_match = re.search(
                    r"(\d{1,2}(?::\d{2})?\s*[AaPp][Mm])\s+(\d{1,2}(?::\d{2})?\s*[AaPp][Mm])",
                    meta_text,
                )
                if time_match:
                    card["time_text"] = f"{time_match.group(1)} {time_match.group(2)}"
                else:
                    single_time = re.search(r"(\d{1,2}(?::\d{2})?\s*[AaPp][Mm])", meta_text)
                    if single_time:
                        card["time_text"] = single_time.group(1)

        # Description: brief excerpt
        desc_el = item.select_one(".eventlist-description")
        if desc_el:
            desc_text = desc_el.get_text(strip=True)
            if desc_text:
                card["description"] = desc_text[:1000]

        # Full card text for free detection
        card["full_text"] = item.get_text(" ", strip=True).lower()

        # Only include cards with at least a title
        if card.get("title"):
            results.append(card)

    return results


# ============================================================
# Event card parsing
# ============================================================

def _parse_event_card(card: dict[str, str | list[str]]) -> EventData | None:
    """Convert extracted card data into an EventData instance."""
    title = card.get("title", "")
    if not title:
        return None

    # Parse date
    start_at, end_at = _parse_card_datetime(card)
    if start_at is None:
        return None  # can't create an event without a start time

    # Build source URL
    href = card.get("href", "")
    if href and not href.startswith("http"):
        source_url = f"{BASE_URL}{href}"
    elif href:
        source_url = href
    else:
        # Fallback: use events page with title slug
        source_url = EVENTS_URL

    # Category from tags
    tags_list = card.get("tags", [])
    category = _classify_category(tags_list)

    # Venue
    venue_name = card.get("venue")

    # Free detection
    full_text = card.get("full_text", "")
    is_free = "free" in str(full_text)

    # Build tags
    event_tags = _build_tags(tags_list)

    # Description
    description = card.get("description")

    return EventData(
        title=str(title),
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=str(venue_name) if venue_name else None,
        category=category,
        tags=event_tags,
        is_free=is_free,
        is_one_off=True,
        description=str(description) if description else None,
        raw_source={
            "href": str(href),
            "card_tags": [str(t) for t in tags_list] if tags_list else [],
            "time_text": str(card.get("time_text", "")),
            "month": str(card.get("month", "")),
            "day": str(card.get("day", "")),
        },
    )


def _classify_category(tags: list[str] | str) -> str:
    """Map source category tags to event category."""
    if isinstance(tags, str):
        tags = [tags]
    for tag in tags:
        mapped = CATEGORY_MAP.get(tag.lower().strip())
        if mapped:
            return mapped
    return DEFAULT_CATEGORY


def _build_tags(source_tags: list[str] | str) -> list[str]:
    """Build event tags from source category tags plus standard Topanga tags."""
    base_tags = ["topanga", "canyon", "outdoor"]
    if isinstance(source_tags, str):
        source_tags = [source_tags]
    for tag in source_tags:
        normalized = tag.lower().strip().replace(" ", "_")
        if normalized and normalized not in base_tags:
            base_tags.append(normalized)
    return base_tags


# ============================================================
# Date/time parsing
# ============================================================

def _parse_card_datetime(
    card: dict[str, str | list[str]],
) -> tuple[datetime | None, datetime | None]:
    """Parse date and time from card data into timezone-aware datetimes.

    Date comes from month abbreviation + day number (e.g., "Aug" + "29").
    Time comes from time_text (e.g., "5:30 PM 8:30 PM").

    Returns (start_at, end_at). end_at is None if no end time found.
    All times are America/Los_Angeles.
    """
    month_str = str(card.get("month", ""))
    day_str = str(card.get("day", ""))

    if not month_str or not day_str:
        return None, None

    month = MONTH_MAP.get(month_str.lower()[:3])
    if month is None:
        return None, None

    try:
        day = int(day_str)
    except ValueError:
        return None, None

    # Determine year: use current year, bump to next year if date seems past
    now = datetime.now(LA_TZ)
    year = now.year

    try:
        base_date = datetime(year, month, day)
    except ValueError:
        return None, None

    # If the date is more than 2 months in the past, assume next year
    if (now.replace(tzinfo=None) - base_date).days > 60:
        base_date = base_date.replace(year=year + 1)

    # Parse time range
    time_text = str(card.get("time_text", ""))
    start_hour, start_min, end_hour, end_min = _parse_time_range(time_text)

    if start_hour is not None:
        start_at = base_date.replace(
            hour=start_hour, minute=start_min, tzinfo=LA_TZ
        )
    else:
        # Default to noon if no time found
        start_at = base_date.replace(hour=12, minute=0, tzinfo=LA_TZ)

    end_at = None
    if end_hour is not None:
        end_at = base_date.replace(
            hour=end_hour, minute=end_min, tzinfo=LA_TZ
        )

    return start_at, end_at


def _parse_time_range(
    text: str,
) -> tuple[int | None, int, int | None, int]:
    """Parse a time range like '5:30 PM 8:30 PM' or '5:30 PM'.

    Returns (start_hour, start_min, end_hour, end_min).
    Hours are in 24h format. end_hour is None if no end time.
    """
    if not text:
        return None, 0, None, 0

    # Find all time tokens: "5:30 PM", "8:30 PM", "12 AM", etc.
    time_pattern = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([AaPp][Mm])")
    matches = time_pattern.findall(text)

    if not matches:
        return None, 0, None, 0

    def _to_24h(hour_s: str, min_s: str, period: str) -> tuple[int, int]:
        hour = int(hour_s)
        minute = int(min_s) if min_s else 0
        period = period.upper()
        if period == "PM" and hour != 12:
            hour += 12
        elif period == "AM" and hour == 12:
            hour = 0
        return hour, minute

    start_h, start_m = _to_24h(*matches[0])

    end_h: int | None = None
    end_m = 0
    if len(matches) >= 2:
        end_h, end_m = _to_24h(*matches[1])

    return start_h, start_m, end_h, end_m
