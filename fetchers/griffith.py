"""
Griffith Observatory programs fetcher.

Source: https://griffithobservatory.lacity.gov/programs
Tier: regular

Scrapes the programs listing page for event cards with h4 headings,
date text, time ranges, and description paragraphs.

Dedup key: source_url (detail page links from h4 anchor tags).

Classification: all Griffith Observatory events are talks_lectures.
Programs include public star parties, telescope viewing, lectures,
planetarium shows, and science talks — all typically free.

Structure (as of March 2026):
  - Card-based grid layout
  - h4 headings with anchor links for event titles
  - Date format: "March 19, 2026" as plain text above the title
  - Time format: "7:00 PM – 8:30 PM" listed separately
  - p tags for descriptions
  - Venue text on its own line
  - "Learn More" links at bottom of cards
  - Some events are "Online" (virtual)
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "griffith"
BASE_URL = "https://griffithobservatory.lacity.gov"
PROGRAMS_URL = f"{BASE_URL}/programs"
LA_TZ = ZoneInfo("America/Los_Angeles")

VENUE_NAME = "Griffith Observatory"
VENUE_ADDRESS = "2800 E Observatory Rd, Los Angeles, CA 90027"

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Griffith Observatory programs. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        # Fetch programs page (follows redirect from .org to .lacity.gov)
        resp = await client.get(PROGRAMS_URL, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = _extract_event_cards(soup)
        events_found = len(cards)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event cards found on programs page. Structure may have changed.",
            )

        for i, card_data in enumerate(cards):
            try:
                event = _parse_event(card_data)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="card",
                    message=f"Failed to parse event card: {e}",
                    raw_value=card_data.get("title", "unknown"),
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
            error_message=f"Timeout fetching {PROGRAMS_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=FetchOutcome.ERROR,
            events_found=events_found,
            error_message=f"HTTP {e.response.status_code} from {PROGRAMS_URL}",
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

def _extract_event_cards(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Extract event data from card-based grid layout.

    Looks for h4 headings with anchor links. Walks surrounding elements
    to find date text, time text, description, and venue info.

    Returns list of dicts with keys: title, href, date_text, time_text,
    description, venue_text.
    """
    results: list[dict[str, str]] = []

    for h4 in soup.find_all("h4"):
        link = h4.find("a", href=True)
        if not link:
            continue

        card_data: dict[str, str] = {}

        # Title from h4 > a text
        title = link.get_text(strip=True)
        if not title:
            continue
        card_data["title"] = title

        # href for source_url
        href = link["href"]
        if href.startswith("/"):
            href = f"{BASE_URL}{href}"
        elif not href.startswith("http"):
            href = f"{BASE_URL}/{href}"
        card_data["href"] = href

        # Walk the parent card container to find date, time, description, venue
        # The card is typically the parent or grandparent of the h4
        card = h4.parent
        if card is not None and card.parent is not None:
            # Try grandparent if parent is too narrow
            grandparent = card.parent
            card_container = grandparent if grandparent.name in ("div", "article", "li", "section") else card

            card_text = card_container.get_text("\n", strip=True)
            _extract_card_fields(card_data, card_text, card_container)

        results.append(card_data)

    return results


def _extract_card_fields(card_data: dict[str, str], card_text: str, container: Tag) -> None:
    """Extract date, time, description, and venue from card text and elements."""
    lines = [line.strip() for line in card_text.split("\n") if line.strip()]

    for line in lines:
        # Date pattern: "March 19, 2026" or "January 5, 2026"
        if not card_data.get("date_text") and re.match(
            r"^[A-Z][a-z]+\s+\d{1,2},\s+\d{4}$", line
        ):
            card_data["date_text"] = line
            continue

        # Time pattern: "7:00 PM – 8:30 PM" or "7:00 PM - 8:30 PM" or "7:00 PM"
        if not card_data.get("time_text") and re.match(
            r"^\d{1,2}:\d{2}\s*[APap][Mm]", line
        ):
            card_data["time_text"] = line
            continue

        # Venue/location line: contains "Online" or known venue indicators
        if not card_data.get("venue_text") and (
            "online" in line.lower()
            or "observatory" in line.lower()
            or "planetarium" in line.lower()
        ):
            # Don't capture the title or date as venue
            if line != card_data.get("title") and not re.match(r"^[A-Z][a-z]+\s+\d", line):
                card_data["venue_text"] = line

    # Description: look for p tags within the container
    for p in container.find_all("p"):
        text = p.get_text(strip=True)
        # Skip short strings, dates, times, "Learn More" links
        if (
            len(text) > 40
            and not re.match(r"^[A-Z][a-z]+\s+\d{1,2},\s+\d{4}$", text)
            and not re.match(r"^\d{1,2}:\d{2}\s*[APap][Mm]", text)
            and "learn more" not in text.lower()
        ):
            card_data["description"] = text[:1000] if len(text) > 1000 else text
            break


# ============================================================
# Event construction
# ============================================================

def _parse_event(card_data: dict[str, str]) -> EventData | None:
    """Convert extracted card data into an EventData."""
    title = card_data.get("title")
    href = card_data.get("href")
    if not title or not href:
        return None

    # Parse date and time
    date_text = card_data.get("date_text", "")
    time_text = card_data.get("time_text", "")
    start_at, end_at = _parse_datetime(date_text, time_text)
    if start_at is None:
        return None  # can't create an event without a start time

    # Determine if online event
    venue_text = card_data.get("venue_text", "")
    is_online = "online" in venue_text.lower()

    venue_name = None if is_online else VENUE_NAME
    venue_address = None if is_online else VENUE_ADDRESS

    # Tags from content signals
    tags = _extract_tags(title, card_data.get("description", ""), is_online)

    # Description
    description = card_data.get("description")

    return EventData(
        title=title,
        source_url=href,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        venue_address=venue_address,
        category="talks_lectures",
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=description,
        raw_source={
            "card_data": card_data,
            "source": "griffith_observatory_programs",
        },
    )


# ============================================================
# Tag extraction
# ============================================================

def _extract_tags(title: str, description: str, is_online: bool) -> list[str]:
    """Extract tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {description}".lower()

    tag_signals = {
        "astronomy": ["astronomy", "astronomical", "celestial", "cosmos"],
        "stargazing": ["stargazing", "star party", "star parties", "night sky"],
        "telescope": ["telescope", "telescop"],
        "science": ["science", "scientific", "physicist", "physics"],
        "planetarium": ["planetarium", "samuel oschin"],
        "lecture": ["lecture", "talk", "speaker", "presentation"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    if is_online:
        tags.append("online")

    return tags


# ============================================================
# Date/time parsing
# ============================================================

def _parse_datetime(
    date_text: str, time_text: str
) -> tuple[datetime | None, datetime | None]:
    """Parse Griffith Observatory date and time strings.

    Date format: "March 19, 2026"
    Time format: "7:00 PM – 8:30 PM" or "7:00 PM - 8:30 PM" or "7:00 PM"

    Returns (start_at, end_at). end_at is None if no end time given.
    All times are America/Los_Angeles.
    """
    if not date_text:
        return None, None

    # Parse date: "March 19, 2026"
    date_match = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", date_text)
    if not date_match:
        return None, None

    month_str = date_match.group(1)
    day = int(date_match.group(2))
    year = int(date_match.group(3))

    month = _month_to_int(month_str)
    if month is None:
        return None, None

    try:
        base_date = datetime(year, month, day)
    except ValueError:
        return None, None

    if not time_text:
        # No time provided — default to noon
        start_at = base_date.replace(hour=12, tzinfo=LA_TZ)
        return start_at, None

    # Parse time(s): "7:00 PM – 8:30 PM" or just "7:00 PM"
    # Split on dash/em-dash/en-dash
    time_parts = re.split(r"\s*[–—-]\s*", time_text)

    start_hour, start_minute = _parse_time(time_parts[0].strip())
    if start_hour is None:
        # Time text present but unparseable — default to noon
        start_at = base_date.replace(hour=12, tzinfo=LA_TZ)
        return start_at, None

    start_at = base_date.replace(hour=start_hour, minute=start_minute, tzinfo=LA_TZ)

    end_at = None
    if len(time_parts) > 1:
        end_hour, end_minute = _parse_time(time_parts[1].strip())
        if end_hour is not None:
            end_at = base_date.replace(hour=end_hour, minute=end_minute, tzinfo=LA_TZ)

    return start_at, end_at


def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time string like '7:00 PM' or '10:45 AM' into (hour_24, minute)."""
    text = text.strip().lower()
    match = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)", text)
    if not match:
        return None, 0

    hour = int(match.group(1))
    minute = int(match.group(2))
    period = match.group(3)

    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0

    return hour, minute


def _month_to_int(month_str: str) -> int | None:
    """Convert month name to integer (1-12)."""
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    return month_map.get(month_str.lower())
