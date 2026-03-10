"""
Getty events fetcher.

Source: https://www.getty.edu/visit/cal/
Tier: regular

Scrapes the Getty calendar page for events at both the Getty Center
and Getty Villa. Events are listed as .calendar-event divs with
title, date/time, location, category, and description.

Dedup key: source_url (stable per-event URLs like /visit/cal/events/ev_4285.html).

Classification: mapped from Getty category page links — film -> film_screening,
tours/talks -> talks_lectures, concerts/performances -> music_performance,
everything else -> visual_art.

Structure (as of March 2026):
  - Events listed as div.calendar-event blocks
  - Each contains: linked h4 title, date/time text, location (p.alt-location),
    optional category links, thumbnail image, description snippet
  - Getty Center and Getty Villa events on the same page
  - No pagination; static HTML listing
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "getty"
BASE_URL = "https://www.getty.edu"
EVENTS_URL = f"{BASE_URL}/visit/cal/"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Category path -> chriscal category mapping
CATEGORY_MAP: dict[str, str] = {
    "film": "film_screening",
    "concerts": "music_performance",
    "performances": "music_performance",
    "music": "music_performance",
    "tours": "talks_lectures",
    "talks": "talks_lectures",
    "lectures": "talks_lectures",
    "workshops": "talks_lectures",
    "courses": "talks_lectures",
    "family": "festival_outdoor",
}

VENUES: dict[str, tuple[str, str, float, float]] = {
    "center": (
        "Getty Center",
        "1200 Getty Center Dr, Los Angeles, CA 90049",
        34.0780, -118.4741,
    ),
    "villa": (
        "Getty Villa",
        "17985 Pacific Coast Hwy, Pacific Palisades, CA 90272",
        34.0459, -118.5650,
    ),
}

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Getty events. Injectable client for testing."""
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
    """Extract events from the Getty calendar listing page.

    Finds all .calendar-event divs and parses their contents into
    raw data dicts.

    Returns a list of dicts with keys:
        title, href, date_text, time_text, location, category_slug, description
    """
    results: list[dict[str, str | None]] = []
    seen_hrefs: set[str] = set()

    event_divs = soup.find_all("div", class_="calendar-event")

    for div in event_divs:
        data = _parse_event_div(div)
        if data:
            href = data.get("href")
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                results.append(data)

    return results


def _parse_event_div(div: Tag) -> dict[str, str | None] | None:
    """Parse a single .calendar-event div into a raw data dict.

    Actual structure (as of March 2026):
      div.calendar-event.getty-center (or .getty-villa)
        div.date-time
          p.day  — "Friday" or "Weekdays" or "Daily"
          p.date — "OCT 25" or "through Nov 18"
          p.time — "6:30 pm" or "11 am, 12 pm, 1 pm"
          p.center / p.villa — "GETTY CENTER" / "GETTY VILLA"
        div.info
          ul.category-tags li a — "FILM", "TOURS", etc.
          h4.heading a — title (with link to detail page)
          p.description — venue in <strong>, then description text
    """
    # Title and link from h4.heading > a
    heading = div.find("h4", class_="heading")
    if not heading:
        return None
    title_link = heading.find("a", href=re.compile(r"/visit/cal/events/ev_\d+"))
    if not title_link:
        return None

    href = title_link.get("href", "")
    title = title_link.get_text(strip=True)
    if not title:
        return None

    # Date/time from .date-time div
    date_time_div = div.find(class_="date-time")
    day_text = None
    date_text = None
    time_text = None
    if date_time_div:
        day_p = date_time_div.find("p", class_="day")
        if day_p:
            day_text = day_p.get_text(strip=True)

        date_p = date_time_div.find("p", class_="date")
        if date_p:
            date_text = date_p.get_text(strip=True)

        time_p = date_time_div.find("p", class_="time")
        if time_p:
            time_text = time_p.get_text(strip=True)

    # Location: detect from div classes or p.center / p.villa
    location = "center"  # default
    div_classes = div.get("class", [])
    if "getty-villa" in div_classes:
        location = "villa"
    elif date_time_div:
        villa_p = date_time_div.find("p", class_="villa")
        if villa_p:
            location = "villa"

    # Category from category-tags links
    category_slug = None
    cat_list = div.find("ul", class_="category-tags")
    if cat_list:
        for li in cat_list.find_all("a", href=re.compile(r"/visit/cal/\w+\.html")):
            match = re.search(r"/visit/cal/(\w+)\.html", li.get("href", ""))
            if match:
                slug = match.group(1)
                if slug != "events":
                    category_slug = slug
                    break

    # Description from p.description
    description = None
    venue_detail = None
    desc_p = div.find("p", class_="description")
    if desc_p:
        strong = desc_p.find("strong")
        if strong:
            venue_detail = strong.get_text(strip=True)
        # Get text without the "Details" link
        desc_parts = []
        for child in desc_p.children:
            if hasattr(child, 'name') and child.name == 'span':
                continue  # skip "Details >" link
            if hasattr(child, 'name') and child.name == 'strong':
                continue  # skip venue in strong
            if hasattr(child, 'name') and child.name == 'br':
                continue
            text = child.get_text(strip=True) if hasattr(child, 'get_text') else str(child).strip()
            if text:
                desc_parts.append(text)
        if desc_parts:
            description = " ".join(desc_parts)

    return {
        "title": title,
        "href": href,
        "day_text": day_text,
        "date_text": date_text,
        "time_text": time_text,
        "location": location,
        "venue_detail": venue_detail,
        "category_slug": category_slug,
        "description": description,
    }


# ============================================================
# Event building
# ============================================================

def _build_event(raw: dict[str, str | None]) -> EventData | None:
    """Build an EventData from raw parsed event data."""
    title = raw.get("title")
    href = raw.get("href")
    if not title or not href:
        return None

    # Skip recurring/ongoing events without a specific date (e.g. "Weekdays", "Daily")
    day_text = (raw.get("day_text") or "").lower()
    date_text = raw.get("date_text") or ""

    # Parse start time
    start_at = _parse_start_time(day_text, date_text, raw.get("time_text"))
    if start_at is None:
        return None

    # Build full URL
    if href.startswith("/"):
        source_url = f"{BASE_URL}{href}"
    else:
        source_url = href

    # Category
    category_slug = raw.get("category_slug") or ""
    category = CATEGORY_MAP.get(category_slug.lower(), "visual_art")

    # Venue from location field (already resolved to "center" or "villa")
    venue_key = raw.get("location") or "center"
    venue_name, venue_address, venue_lat, venue_lng = VENUES.get(
        venue_key, VENUES["center"]
    )

    # Free detection — Getty is free admission, but some events require tickets
    combined = f"{title} {raw.get('description') or ''}".lower()
    is_free = "ticket" not in combined and "rsvp" not in combined

    # Tags
    tags = _extract_tags(category_slug, title, raw.get("description") or "")

    # Description: prepend venue detail if available
    description = raw.get("description")
    venue_detail = raw.get("venue_detail")
    if venue_detail and description:
        description = f"{venue_detail} — {description}"
    elif venue_detail:
        description = venue_detail

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        venue_name=venue_name,
        venue_address=venue_address,
        category=category,
        tags=tags,
        is_free=is_free,
        is_one_off=True,
        description=description,
        raw_source={
            "href": href,
            "day_text": raw.get("day_text"),
            "date_text": date_text,
            "location": venue_key,
            "category_slug": category_slug,
        },
    )


# ============================================================
# Date/time parsing
# ============================================================

def _parse_start_time(
    day_text: str,
    date_text: str,
    time_text: str | None,
) -> datetime | None:
    """Parse date and time from Getty event fields.

    day_text: e.g., "friday", "weekdays", "daily" (already lowercased)
    date_text: e.g., "OCT 25", "through Nov 18", ""
    time_text: e.g., "6:30 pm", "11 am, 12 pm, 1 pm"
    """
    # For recurring events (Weekdays, Daily, etc.) without a specific date,
    # use today as the start date so they appear in the calendar
    recurring_patterns = ("weekdays", "daily", "weekends", "select days",
                          "weekdays and sunday", "weekdays and saturday")
    is_recurring = any(day_text.startswith(p) for p in recurring_patterns)

    parsed_date = _parse_date(date_text) if date_text else None

    if parsed_date is None:
        if is_recurring:
            # Use today as the event date for recurring programs
            now = datetime.now(LA_TZ)
            parsed_date = datetime(now.year, now.month, now.day)
        else:
            # Try parsing the day_text itself (e.g. "Friday" → next occurrence)
            parsed_date = _parse_day_of_week(day_text)
            if parsed_date is None:
                return None

    # Extract first time from time_text (take earliest if multiple listed)
    hour, minute = 12, 0  # default to noon
    if time_text:
        h, m = _parse_time(time_text)
        if h is not None:
            hour, minute = h, m

    try:
        return parsed_date.replace(hour=hour, minute=minute, tzinfo=LA_TZ)
    except (ValueError, OverflowError):
        return None


def _parse_day_of_week(day_text: str) -> datetime | None:
    """Parse a day name like 'friday' into the next occurrence of that day."""
    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    target = day_map.get(day_text.lower())
    if target is None:
        return None

    now = datetime.now(LA_TZ)
    current_day = now.weekday()
    days_ahead = (target - current_day) % 7
    if days_ahead == 0:
        days_ahead = 0  # today counts
    from datetime import timedelta
    target_date = now + timedelta(days=days_ahead)
    return datetime(target_date.year, target_date.month, target_date.day)


def _parse_date(text: str) -> datetime | None:
    """Parse a date from Getty date_text field.

    Handles formats like:
      - "OCT 25" or "MAR 15"
      - "March 15, 2026"
      - "through Nov 18" (extract the date)
    """
    if not text:
        return None

    now = datetime.now(LA_TZ)

    # Try "Month DD, YYYY" format
    match = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if match:
        month = _month_number(match.group(1))
        if month:
            try:
                return datetime(int(match.group(3)), month, int(match.group(2)))
            except ValueError:
                pass

    # Try "MON DD" or "Month DD" format (e.g. "OCT 25", "Mar 15")
    match = re.search(r"(\w{3,9})\s+(\d{1,2})", text)
    if match:
        month = _month_number(match.group(1))
        if month:
            day = int(match.group(2))
            year = now.year
            try:
                dt = datetime(year, month, day)
            except ValueError:
                return None
            # If more than 2 months in the past, assume next year
            if (now.replace(tzinfo=None) - dt).days > 60:
                dt = dt.replace(year=year + 1)
            return dt

    return None


def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time from text containing 'H:MM am/pm' or 'H am/pm'."""
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

def _extract_tags(category_slug: str, title: str, description: str) -> list[str]:
    """Extract tags from category and content signals."""
    tags: list[str] = []
    combined = f"{category_slug} {title} {description}".lower()

    if category_slug:
        slug = category_slug.lower().strip()
        if slug and slug not in tags:
            tags.append(slug)

    tag_signals = {
        "jazz": ["jazz"],
        "contemporary": ["contemporary", "modern art"],
        "photography": ["photograph", "photo"],
        "sculpture": ["sculpture"],
        "painting": ["painting", "painter"],
        "family": ["family", "kids", "children"],
        "free": ["free"],
        "outdoor": ["outdoor", "garden"],
        "ancient": ["ancient", "roman", "greek", "antiquity", "antiquities"],
        "medieval": ["medieval", "illuminat", "manuscript"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords) and tag not in tags:
            tags.append(tag)

    return tags
