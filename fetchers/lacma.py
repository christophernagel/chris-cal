"""
LACMA events fetcher.

Source: https://www.lacma.org/events
Tier: regular

Scrapes the events listing page for event cards grouped by date headings.
Each card is an anchor tag linking to a detail page with category, title,
time, and location info.

Dedup key: source_url (stable per-event URLs like /event/focus-tour-modern-art-853).

Classification: mapped from LACMA category labels — Music -> music_performance,
Films -> film_screening, Tours/Talks/Classes/Workshops -> talks_lectures,
everything else -> visual_art.

Structure (as of March 2026):
  - Date group headings like "Today, March 10, 2026" or "Thursday, March 12, 2026"
  - Event cards are <a href="/event/..."> tags within each date group
  - Each card contains: category label, title, date/time string, location
  - No pagination observed; events listed on single scrolling page
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "lacma"
BASE_URL = "https://www.lacma.org"
EVENTS_URL = f"{BASE_URL}/events"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Category label -> chriscal category mapping
CATEGORY_MAP: dict[str, str] = {
    "music": "music_performance",
    "films": "film_screening",
    "tours": "talks_lectures",
    "talks": "talks_lectures",
    "classes": "talks_lectures",
    "workshops": "talks_lectures",
}

VENUE_NAME = "LACMA"
VENUE_ADDRESS = "5905 Wilshire Blvd, Los Angeles, CA 90036"

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse LACMA events. Injectable client for testing."""
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
        raw_events = _extract_events(soup)
        events_found = len(raw_events)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No events found on listing page. Structure may have changed.",
            )

        for i, raw in enumerate(raw_events):
            try:
                event = _build_event(raw)
                if event is not None:
                    events.append(event)
                else:
                    warnings.append(ParseWarning(
                        event_index=i,
                        field="event",
                        message="Could not parse event — missing title or start time.",
                        raw_value=str(raw),
                    ))
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="event",
                    message=f"Failed to parse event: {e}",
                    raw_value=str(raw),
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
# Listing page parsing
# ============================================================

def _extract_events(soup: BeautifulSoup) -> list[dict[str, str | None]]:
    """Extract events from the listing page, grouped by date headings.

    Walks through the page looking for date group headings, then collects
    event card anchors within each group. The date from the heading is
    attached to each card's data.

    Returns a list of dicts with keys:
        date_heading, category_label, title, time_text, location, href, card_text
    """
    results: list[dict[str, str | None]] = []

    # Strategy: find all date headings and their sibling/child event cards.
    # Date headings contain text like "Today, March 10, 2026" or
    # "Thursday, March 12, 2026". We look for elements whose text matches
    # a date-like pattern, then find event card anchors that follow.
    date_heading_pattern = re.compile(
        r"(?:Today|Tomorrow|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
        r",?\s+\w+\s+\d{1,2},?\s+\d{4}",
        re.IGNORECASE,
    )

    # Find all text nodes/elements that look like date headings
    date_sections: list[tuple[str, Tag]] = []
    for tag in soup.find_all(True):
        if tag.name in ("script", "style", "meta", "link"):
            continue
        text = tag.get_text(strip=True)
        match = date_heading_pattern.search(text)
        if match and len(text) < 80:
            # Only accept if this element's own direct text matches
            # (avoid matching parent containers that contain event text too)
            own_text = "".join(
                child.strip() for child in tag.children if isinstance(child, str)
            )
            if date_heading_pattern.search(own_text or text):
                date_sections.append((match.group(0), tag))

    if date_sections:
        # For each date heading, find event cards between this heading and the next
        for idx, (date_text, heading_tag) in enumerate(date_sections):
            # Find event card anchors that are siblings/descendants after this heading
            # Walk the parent to find event links associated with this date group
            parent = heading_tag.parent
            if parent is None:
                continue

            event_links = parent.find_all("a", href=re.compile(r"^/event/"))
            for link in event_links:
                card_data = _parse_event_card(link, date_text)
                if card_data:
                    results.append(card_data)
    else:
        # Fallback: no date headings found, just grab all event links
        for link in soup.find_all("a", href=re.compile(r"^/event/")):
            card_data = _parse_event_card(link, None)
            if card_data:
                results.append(card_data)

    # Deduplicate by href (same event may appear in multiple parent scans)
    seen_hrefs: set[str] = set()
    unique: list[dict[str, str | None]] = []
    for item in results:
        href = item.get("href")
        if href and href not in seen_hrefs:
            seen_hrefs.add(href)
            unique.append(item)

    return unique


def _parse_event_card(link: Tag, date_heading: str | None) -> dict[str, str | None] | None:
    """Parse a single event card anchor tag into a raw data dict."""
    href = link.get("href", "")
    if not href or not href.startswith("/event/"):
        return None

    card_text = link.get_text(separator="\n", strip=True)
    lines = [line.strip() for line in card_text.split("\n") if line.strip()]

    if not lines:
        return None

    # Extract structured fields from card lines.
    # Expected structure: category label, title, date/time, location
    # But order and presence can vary, so we use heuristics.
    category_label: str | None = None
    title: str | None = None
    time_text: str | None = None
    location: str | None = None

    # Known category labels
    category_keywords = {"music", "films", "tours", "talks", "classes", "workshops",
                         "art", "exhibition", "performance", "family", "members"}

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        if line_lower in category_keywords and category_label is None:
            category_label = line
        elif _looks_like_time(line) and time_text is None:
            time_text = line
        elif _looks_like_location(line) and location is None:
            location = line
        elif title is None and line_lower not in category_keywords:
            # First non-category, non-time, non-location line is the title
            title = line

    # If we still don't have a title, use the first line
    if not title and lines:
        title = lines[0]

    return {
        "date_heading": date_heading,
        "category_label": category_label,
        "title": title,
        "time_text": time_text,
        "location": location,
        "href": href,
        "card_text": card_text,
    }


def _looks_like_time(text: str) -> bool:
    """Check if text looks like a time string (e.g., 'Tue Mar 10 | 1 pm')."""
    return bool(re.search(r"\d{1,2}\s*(am|pm)", text, re.IGNORECASE))


def _looks_like_location(text: str) -> bool:
    """Check if text looks like a LACMA location (e.g., 'BCAM, Level 3 | LACMA')."""
    location_signals = ["lacma", "bcam", "level", "theater", "gallery", "plaza",
                        "pavilion", "bing", "resnick", "broad"]
    text_lower = text.lower()
    # Must have a location signal and not look like a time
    return (
        any(signal in text_lower for signal in location_signals)
        and not _looks_like_time(text)
    )


# ============================================================
# Event building
# ============================================================

def _build_event(raw: dict[str, str | None]) -> EventData | None:
    """Build an EventData from raw parsed card data."""
    title = raw.get("title")
    href = raw.get("href")
    if not title or not href:
        return None

    # Parse start time
    start_at = _parse_start_time(raw.get("date_heading"), raw.get("time_text"))
    if start_at is None:
        return None

    source_url = f"{BASE_URL}{href}"

    # Category classification
    category_label = raw.get("category_label") or ""
    category = CATEGORY_MAP.get(category_label.lower().strip(), "visual_art")

    # Description: include location detail if present
    location = raw.get("location")
    description = None
    if location:
        description = f"Location: {location}"

    # Free detection: check card text for "free" signals
    card_text = raw.get("card_text") or ""
    is_free = bool(re.search(r"\bfree\b", card_text, re.IGNORECASE))

    # Tags
    tags = _extract_tags(category_label, title, card_text)

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        venue_name=VENUE_NAME,
        venue_address=VENUE_ADDRESS,
        category=category,
        tags=tags,
        is_free=is_free,
        is_one_off=True,
        description=description,
        raw_source={
            "href": href,
            "card_text": card_text,
            "date_heading": raw.get("date_heading"),
            "category_label": category_label,
            "location": location,
        },
    )


# ============================================================
# Date/time parsing
# ============================================================

def _parse_start_time(
    date_heading: str | None,
    time_text: str | None,
) -> datetime | None:
    """Combine date from heading and time from card into a timezone-aware datetime.

    date_heading: e.g., "Today, March 10, 2026" or "Thursday, March 12, 2026"
    time_text: e.g., "Tue Mar 10 | 1 pm" or "1 pm"
    """
    # Try to get date from the heading first
    date_from_heading = _parse_date_heading(date_heading) if date_heading else None

    # Extract time from the time_text
    hour, minute = None, 0
    if time_text:
        hour, minute = _parse_time(time_text)

    # If we have no date from heading, try to parse the time_text for a full date
    if date_from_heading is None and time_text:
        date_from_heading = _parse_date_from_time_text(time_text)

    if date_from_heading is None:
        return None

    if hour is None:
        # Default to noon if no time found
        hour, minute = 12, 0

    try:
        return date_from_heading.replace(hour=hour, minute=minute, tzinfo=LA_TZ)
    except (ValueError, OverflowError):
        return None


def _parse_date_heading(text: str) -> datetime | None:
    """Parse a date heading like 'Today, March 10, 2026' or 'Thursday, March 12, 2026'.

    Returns a naive datetime (date only, time zeroed) or None.
    """
    if not text:
        return None

    # Handle "Today" / "Tomorrow"
    now = datetime.now(LA_TZ)
    text_lower = text.lower()
    if text_lower.startswith("today"):
        return datetime(now.year, now.month, now.day)
    if text_lower.startswith("tomorrow"):
        from datetime import timedelta
        tomorrow = now + timedelta(days=1)
        return datetime(tomorrow.year, tomorrow.month, tomorrow.day)

    # Parse "DayOfWeek, Month DD, YYYY" or "Month DD, YYYY"
    match = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if match:
        month_str = match.group(1)
        day = int(match.group(2))
        year = int(match.group(3))

        month = _month_number(month_str)
        if month is None:
            return None

        try:
            return datetime(year, month, day)
        except ValueError:
            return None

    return None


def _parse_date_from_time_text(text: str) -> datetime | None:
    """Try to extract a date from time_text like 'Tue Mar 10 | 1 pm'.

    Parses the date portion before the pipe.
    """
    # Split on pipe, take the date part
    parts = text.split("|")
    date_part = parts[0].strip()

    # Strip day-of-week prefix
    date_part = re.sub(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*", "", date_part, flags=re.IGNORECASE)

    # Try "Mon DD" format
    match = re.match(r"([A-Za-z]+)\s+(\d{1,2})", date_part)
    if not match:
        return None

    month_str = match.group(1)
    day = int(match.group(2))
    month = _month_number(month_str)
    if month is None:
        return None

    now = datetime.now(LA_TZ)
    year = now.year

    try:
        dt = datetime(year, month, day)
    except ValueError:
        return None

    # If date is more than 2 months in the past, assume next year
    if (now.replace(tzinfo=None) - dt).days > 60:
        dt = dt.replace(year=year + 1)

    return dt


def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time from a string that may contain 'HH:MM am/pm' or 'H am/pm'.

    Handles both standalone times ('1 pm') and combined strings ('Tue Mar 10 | 1 pm').
    """
    match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)", text, re.IGNORECASE)
    if not match:
        return None, 0

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    period = match.group(3).lower()

    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0

    return hour, minute


def _month_number(month_str: str) -> int | None:
    """Convert month name/abbreviation to number."""
    month_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    return month_map.get(month_str.lower()[:9])


# ============================================================
# Tag extraction
# ============================================================

def _extract_tags(category_label: str, title: str, card_text: str) -> list[str]:
    """Extract tags from category labels and content signals."""
    tags: list[str] = []
    combined = f"{category_label} {title} {card_text}".lower()

    # Add category label as a tag if present
    if category_label:
        label_lower = category_label.lower().strip()
        if label_lower and label_lower not in tags:
            tags.append(label_lower)

    # Content-based tag signals
    tag_signals = {
        "jazz": ["jazz"],
        "contemporary": ["contemporary", "modern art"],
        "photography": ["photograph", "photo"],
        "sculpture": ["sculpture", "sculpt"],
        "painting": ["painting", "painter"],
        "family": ["family", "kids", "children"],
        "free": ["free"],
        "outdoor": ["outdoor", "garden", "plaza"],
        "latin_american": ["latin american", "latino", "chicano"],
        "asian_art": ["asian art", "japanese", "chinese", "korean"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords) and tag not in tags:
            tags.append(tag)

    return tags
