"""
19hz.info Los Angeles event listing fetcher.

Source: https://19hz.info/eventlisting_LosAngeles.php
Tier: supplemental (24h fetch)

Scrapes the HTML table of electronic music / DJ events in Los Angeles.
Each row contains: Date/Time, Event@Venue, Tags, Price|Age, Organizers, Links.

Dedup key: source_url (the EVENTS_URL is the same for all events, so we
construct a synthetic source_url from the event title + date to enable dedup
via the ingest layer).

Classification: all 19hz events are music_performance — the site exclusively
lists electronic music / DJ nights, raves, and club events.

Structure (as of March 2026):
  - Main HTML table with <tr> rows, each having 6 <td> cells
  - Date column: "Wed: Mar 11" with time in parens "(7pm-11pm)"
  - Event@Venue column: "Event Name @ Venue Name"
  - Tags column: comma/space-separated genre tags
  - Price|Age column: "$15" or "free" | "21+"
  - Links column: hyperlinks to ticket vendors
  - Page also contains Venues List, Promoter List, Recurring Events
    sections — only the main event table is parsed
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "nineteen_hz"
EVENTS_URL = "https://19hz.info/eventlisting_LosAngeles.php"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(20.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse 19hz.info LA event listings. Injectable client for testing."""
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
        rows = _find_event_rows(soup)
        events_found = len(rows)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event rows found in table. Structure may have changed.",
            )

        for i, row in enumerate(rows):
            try:
                event = _parse_row(row, i)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="row",
                    message=f"Failed to parse row {i}: {e}",
                    raw_value=_row_text(row),
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
# Table discovery
# ============================================================

def _find_event_rows(soup: BeautifulSoup) -> list[Tag]:
    """Find the main event table rows, skipping header rows and non-event tables.

    The main event table is the first large <table> on the page. Other tables
    (Venues List, Promoter List, Recurring Events) appear later and have
    different column counts or are preceded by identifying headers.
    """
    # Find all tables on the page
    tables = soup.find_all("table")
    if not tables:
        return []

    # The main event table is the first table with rows containing 6 cells
    for table in tables:
        rows = table.find_all("tr")
        event_rows = []
        for row in rows:
            cells = row.find_all("td")
            # Main event table has 6 columns:
            # Date/Time, Event@Venue, Tags, Price|Age, Organizers, Links
            if len(cells) >= 6:
                # Skip header-like rows (all cells are bold or contain header text)
                first_text = cells[0].get_text(strip=True).lower()
                if first_text in ("date", "date/time", ""):
                    continue
                event_rows.append(row)

        if event_rows:
            return event_rows

    return []


def _cell_direct_text(cell: Tag) -> str:
    """Extract text only from direct children of a cell, ignoring nested <td> tags.

    19hz HTML is often malformed with unclosed <td> tags, causing BS4 to nest
    subsequent cells inside the first one. This function walks only direct
    children (NavigableString and direct child tags that aren't <td>).
    """
    parts: list[str] = []
    from bs4 import NavigableString
    for child in cell.children:
        if isinstance(child, NavigableString):
            parts.append(child.strip())
        elif isinstance(child, Tag) and child.name != "td":
            parts.append(child.get_text(strip=True))
        elif isinstance(child, Tag) and child.name == "td":
            break  # stop at the first nested <td>
    return " ".join(p for p in parts if p)


def _row_text(row: Tag) -> str:
    """Get truncated text from a row for error reporting."""
    text = row.get_text(" | ", strip=True)
    return text[:200] if len(text) > 200 else text


# ============================================================
# Row parsing
# ============================================================

def _parse_row(row: Tag, index: int) -> EventData | None:
    """Parse a single event table row into an EventData."""
    cells = row.find_all("td")
    if len(cells) < 6:
        return None

    # Column 0: Date/Time — e.g. "Wed: Mar 11 (7pm-11pm)"
    date_cell = cells[0]
    date_text = date_cell.get_text(strip=True)

    # Column 1: Event@Venue — e.g. "Event Name @ Venue Name"
    # NOTE: 19hz HTML is malformed — cell[1] often lacks a closing </td>,
    # so get_text() absorbs subsequent cells' content. Extract only direct
    # text and anchor text from this cell, ignoring nested <td> elements.
    event_venue_cell = cells[1]
    event_venue_text = _cell_direct_text(event_venue_cell)

    # Column 2: Tags — e.g. "house, techno, dubstep"
    tags_cell = cells[2]
    tags_text = tags_cell.get_text(strip=True)

    # Column 3: Price|Age — e.g. "$15 | 21+"
    price_cell = cells[3]
    price_text = price_cell.get_text(strip=True)

    # Column 4: Organizers (captured for raw_source)
    organizers_cell = cells[4]
    organizers_text = organizers_cell.get_text(strip=True)

    # Column 5: Links — hyperlinks to ticket vendors
    links_cell = cells[5]

    # Parse date/time
    start_at, end_at = _parse_date_time(date_text)
    if start_at is None:
        return None  # can't create event without a start time

    # Split event @ venue
    title, venue_name = _split_event_venue(event_venue_text)
    if not title:
        return None  # can't create event without a title

    # Parse tags
    tags = _parse_tags(tags_text)

    # Parse price
    is_free, price_range = _parse_price(price_text)

    # Extract ticket URL (first link in the Links column)
    ticket_url = _extract_ticket_url(links_cell)

    # If no ticket URL in links column, check event@venue column for a link
    if ticket_url is None:
        link_tag = event_venue_cell.find("a", href=True)
        if link_tag:
            href = link_tag["href"]
            if href.startswith("http"):
                ticket_url = href

    # Build a synthetic source_url for dedup (19hz has no per-event pages).
    # Use ticket_url if available, otherwise construct from title + date.
    if ticket_url:
        source_url = ticket_url
    else:
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        date_str = start_at.strftime("%Y-%m-%d")
        source_url = f"https://19hz.info/eventlisting_LosAngeles.php#{slug}-{date_str}"

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        category="music_performance",
        tags=tags,
        is_free=is_free,
        ticket_url=ticket_url,
        price_range=price_range,
        is_one_off=True,
        raw_source={
            "date_text": date_text,
            "event_venue_text": event_venue_text,
            "tags_text": tags_text,
            "price_text": price_text,
            "organizers_text": organizers_text,
            "row_index": index,
        },
    )


# ============================================================
# Field parsers
# ============================================================

def _split_event_venue(text: str) -> tuple[str | None, str | None]:
    """Split 'Event Name @ Venue Name' into (title, venue_name).

    Handles both ' @ ' and '@' separators. If no separator is found,
    the whole text is the title.
    """
    # Try ' @ ' first (with spaces), then '@' without spaces
    for sep in (" @ ", "@ ", " @", "@"):
        if sep in text:
            parts = text.split(sep, 1)
            title = parts[0].strip() or None
            venue = parts[1].strip() or None
            # Clean up venue — remove trailing location in parens if it's just city
            return title, venue
            break
    return text.strip() or None, None


def _parse_tags(text: str) -> list[str]:
    """Parse tags from the tags column. Tags may be comma or space separated."""
    if not text or text.strip() == "":
        return []

    # Split on commas first, then clean up each tag
    raw_tags = re.split(r"[,/]", text)
    tags = []
    for tag in raw_tags:
        cleaned = tag.strip().lower()
        if cleaned and cleaned not in tags:
            tags.append(cleaned)
    return tags


def _parse_price(text: str) -> tuple[bool, str | None]:
    """Parse price/age text like '$15 | 21+' or 'free'.

    Returns (is_free, price_range). The age restriction is stripped from price_range.
    """
    if not text:
        return True, None

    # Check for free
    if "free" in text.lower():
        return True, "Free"

    # Try to extract price portion (before the age restriction)
    # Format is typically "$15" or "$20-30" optionally followed by "| 21+"
    price_match = re.search(r"\$[\d]+(?:\s*[-–]\s*\$?[\d]+)?", text)
    if price_match:
        return False, price_match.group(0)

    # If there's text but no recognizable price and not free, assume not free
    # but we don't have a price range
    return False, text.strip() if text.strip() else None


def _extract_ticket_url(cell: Tag) -> str | None:
    """Extract the first HTTP link from the links cell."""
    link = cell.find("a", href=True)
    if link:
        href = link["href"]
        if href.startswith("http"):
            return href
    return None


# ============================================================
# Date/time parsing
# ============================================================

def _parse_date_time(text: str) -> tuple[datetime | None, datetime | None]:
    """Parse 19hz date/time strings into timezone-aware datetimes.

    Formats:
      'Wed: Mar 11'                    → date only, default 9pm start
      'Wed: Mar 11 (7pm-11pm)'         → date with time range
      'Wed: Mar 11 (7pm)'              → date with start time only
      'Wed: Mar 11 - Thu: Mar 12'      → multi-day (rare)

    Returns (start_at, end_at). All times are America/Los_Angeles.
    """
    if not text:
        return None, None

    # Extract time range from parentheses if present
    time_match = re.search(r"\(([^)]+)\)", text)
    start_hour, start_minute = 21, 0  # default 9pm for nightlife events
    end_hour, end_minute = None, None

    if time_match:
        time_text = time_match.group(1)
        # Parse start time
        start_h, start_m = _parse_time(time_text.split("-")[0].strip())
        if start_h is not None:
            start_hour, start_minute = start_h, start_m

        # Parse end time if present
        if "-" in time_text:
            end_part = time_text.split("-", 1)[1].strip()
            end_h, end_m = _parse_time(end_part)
            if end_h is not None:
                end_hour, end_minute = end_h, end_m

    # Remove parenthetical time from text for date parsing
    date_text = re.sub(r"\([^)]*\)", "", text).strip()

    # Check for multi-day range
    if " - " in date_text:
        date_parts = date_text.split(" - ", 1)
        start_date = _parse_single_date(date_parts[0].strip())
        end_date = _parse_single_date(date_parts[1].strip())

        if start_date:
            start_at = start_date.replace(
                hour=start_hour, minute=start_minute, tzinfo=LA_TZ
            )
            end_at = None
            if end_date and end_hour is not None:
                end_at = end_date.replace(
                    hour=end_hour, minute=end_minute, tzinfo=LA_TZ
                )
            elif end_date:
                end_at = end_date.replace(
                    hour=start_hour, minute=start_minute, tzinfo=LA_TZ
                )
            return start_at, end_at
        return None, None
    else:
        date = _parse_single_date(date_text)
        if date is None:
            return None, None

        start_at = date.replace(hour=start_hour, minute=start_minute, tzinfo=LA_TZ)

        end_at = None
        if end_hour is not None:
            # End time: if it's earlier than start, it's the next day (e.g., 10pm-2am)
            end_at = date.replace(hour=end_hour, minute=end_minute, tzinfo=LA_TZ)
            if end_at <= start_at:
                end_at = end_at.replace(day=date.day + 1)

        return start_at, end_at


def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time string like '7pm', '10:30pm', '7 pm' into (hour_24, minute)."""
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
    """Parse a single date like 'Wed: Mar 11' or 'Mar 11' into a datetime.

    Assumes current year. Handles year-wrap (Dec event parsed in January).
    """
    # Strip day-of-week prefix: "Wed: ", "Wed, ", "Wed " etc.
    text = re.sub(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*[,:.]?\s*", "", text.strip())

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
