"""
Royce Hall calendar fetcher.

Source: https://roycehall.org/calendar
Tier: anchor

Scrapes the calendar listing page for event cards. Each card is an anchor tag
wrapping image + text content. The listing page contains enough info to build
EventData without fetching detail pages.

Dedup key: source_url (detail page URLs like /calendar/details/{event-slug}).

Classification: all Royce Hall events are music_performance. Royce Hall programs
contemporary music, world music, dance, spoken word, and comedy.

Structure (as of March 2026):
  - Listing page: <a> cards wrapping image + text content
  - Detail URLs: /calendar/details/{event-slug}
  - Date format: "Sun, March 15, 2026 at 2:00 PM" or "Mon, March 30 at 7:00 PM"
  - Ticket links to: https://ucla.evenue.net/events/RH
  - Free events show "FREE EVENT" badge, paid show "Buy Tickets" / "RESERVE TICKETS"
  - No pagination (single page listing)
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "royce_hall"
BASE_URL = "https://roycehall.org"
CALENDAR_URL = f"{BASE_URL}/calendar"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Royce Hall calendar. Injectable client for testing."""
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
        cards = _extract_event_cards(soup)
        events_found = len(cards)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event cards found on calendar page. Structure may have changed.",
            )

        # Parse each event card into EventData
        for i, card_data in enumerate(cards):
            try:
                event = _parse_event_card(card_data)
                if event is not None:
                    events.append(event)
                else:
                    warnings.append(ParseWarning(
                        event_index=i,
                        field="card",
                        message="Could not parse event card (missing title or date)",
                        raw_value=card_data.get("title"),
                    ))
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

def _extract_event_cards(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Extract event data from anchor-tag cards on the calendar page.

    Each event card is an <a> tag with an href pointing to a detail page
    (/calendar/details/{slug}). Cards contain image + text with presenter,
    title, date, venue, and free/paid status.

    Returns list of dicts with keys: href, title, presenter, date_text,
    venue_text, card_text (full text for free detection).
    """
    results = []

    for link in soup.find_all("a", href=True):
        href = link["href"]

        # Only grab links to event detail pages
        if "/calendar/details/" not in href:
            continue

        card_text = link.get_text(" ", strip=True)
        if not card_text:
            continue

        card_data: dict[str, str] = {
            "href": href,
            "card_text": card_text,
        }

        # Extract structured fields from the card's text content.
        # The card contains multiple text nodes; we look for recognizable
        # patterns rather than relying on specific CSS classes.
        _extract_card_fields(link, card_data)

        if card_data.get("title"):
            results.append(card_data)

    return results


def _extract_card_fields(link: Tag, card_data: dict[str, str]) -> None:
    """Extract title, presenter, date, venue from an event card's inner elements.

    The card structure has text elements for presenter name, event title,
    date/time, and venue. We walk the text nodes and identify them by pattern.
    """
    # Collect non-empty text segments from immediate and nested elements
    text_segments: list[str] = []
    for el in link.descendants:
        if isinstance(el, str):
            text = el.strip()
            if text:
                text_segments.append(text)
        elif hasattr(el, "get_text") and el.name in ("span", "div", "p", "h2", "h3", "h4", "strong", "em"):
            text = el.get_text(strip=True)
            if text and text not in text_segments:
                text_segments.append(text)

    # Identify segments by their content patterns
    date_text = None
    venue_text = None
    presenter = None
    title = None
    free_badge = False

    for seg in text_segments:
        seg_clean = seg.strip()
        if not seg_clean:
            continue

        # Date pattern: day name + month + day number + "at" + time
        if _looks_like_date(seg_clean):
            date_text = seg_clean
        elif seg_clean.upper() in ("FREE EVENT", "FREE"):
            free_badge = True
        elif seg_clean.upper() in ("RESERVE TICKETS", "BUY TICKETS"):
            continue  # skip button text
        elif seg_clean == "Royce Hall":
            venue_text = seg_clean
        elif date_text is None and title is None:
            # First non-date, non-badge text is likely presenter or title
            if presenter is None:
                presenter = seg_clean
            elif title is None:
                title = seg_clean
        elif title is None:
            title = seg_clean

    # Build the event title: combine presenter + title if both exist
    if presenter and title:
        card_data["title"] = f"{presenter} — {title}"
        card_data["presenter"] = presenter
    elif presenter:
        card_data["title"] = presenter
    elif title:
        card_data["title"] = title

    if date_text:
        card_data["date_text"] = date_text
    if venue_text:
        card_data["venue_text"] = venue_text
    if free_badge:
        card_data["is_free"] = "true"


def _looks_like_date(text: str) -> bool:
    """Heuristic: does this string look like a Royce Hall date line?

    Formats:
      "Sun, March 15, 2026 at 2:00 PM"
      "Mon, March 30 at 7:00 PM"
    """
    return bool(
        re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)", text)
        and re.search(r"at\s+\d{1,2}(:\d{2})?\s*(AM|PM|am|pm)", text)
    )


# ============================================================
# Event construction
# ============================================================

def _parse_event_card(card_data: dict[str, str]) -> EventData | None:
    """Convert extracted card data into an EventData instance."""
    title = card_data.get("title")
    if not title:
        return None

    # Parse date
    date_text = card_data.get("date_text", "")
    start_at = _parse_date(date_text)
    if start_at is None:
        return None

    # Build source URL
    href = card_data["href"]
    if href.startswith("/"):
        source_url = f"{BASE_URL}{href}"
    elif href.startswith("http"):
        source_url = href
    else:
        source_url = f"{BASE_URL}/{href}"

    # Free/paid detection
    card_text = card_data.get("card_text", "").upper()
    is_free = (
        card_data.get("is_free") == "true"
        or "FREE EVENT" in card_text
        or "FREE ADMISSION" in card_text
    )

    # Ticket URL: Royce Hall uses ucla.evenue.net
    ticket_url = "https://ucla.evenue.net/events/RH" if not is_free else None

    # Tags from content signals
    tags = _extract_tags(title, card_text)

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        venue_name="Royce Hall",
        category="music_performance",
        tags=tags,
        is_free=is_free,
        ticket_url=ticket_url,
        is_one_off=True,
        description=None,
        raw_source={
            "href": href,
            "card_data": card_data,
        },
    )


def _extract_tags(title: str, card_text: str) -> list[str]:
    """Extract tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {card_text}".lower()

    tag_signals = {
        "jazz": ["jazz", "trio", "quartet", "quintet"],
        "world_music": ["world music", "afrobeat", "fado", "flamenco", "gamelan",
                        "raga", "sitar", "tabla", "oud", "kora"],
        "dance": ["dance", "choreograph", "ballet"],
        "comedy": ["comedy", "comedian", "stand-up", "standup", "humor"],
        "spoken_word": ["spoken word", "poetry", "poet", "reading", "lecture"],
        "classical": ["classical", "symphony", "orchestra", "chamber",
                       "philharmonic", "concerto", "sonata"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    return tags


# ============================================================
# Date parsing
# ============================================================

def _parse_date(text: str) -> datetime | None:
    """Parse Royce Hall date strings into timezone-aware datetimes.

    Formats:
      "Sun, March 15, 2026 at 2:00 PM"   → with explicit year
      "Mon, March 30 at 7:00 PM"          → no year, infer from current date

    Returns timezone-aware datetime in America/Los_Angeles, or None on failure.
    """
    if not text:
        return None

    # Split on "at" to separate date from time
    match = re.match(
        r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+"   # day of week
        r"([A-Za-z]+)\s+"                              # month name
        r"(\d{1,2})"                                   # day
        r"(?:,?\s+(\d{4}))?"                           # optional year
        r"\s+at\s+"                                    # "at" separator
        r"(\d{1,2}(?::\d{2})?)\s*(AM|PM|am|pm)",       # time
        text.strip(),
    )
    if not match:
        return None

    month_str = match.group(1)
    day = int(match.group(2))
    year_str = match.group(3)
    time_str = match.group(4)
    period = match.group(5).upper()

    # Parse month
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    month = month_map.get(month_str.lower())
    if month is None:
        # Try abbreviated month names
        abbrev_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        month = abbrev_map.get(month_str.lower()[:3])
    if month is None:
        return None

    # Parse year (default to current year, with forward-looking heuristic)
    if year_str:
        year = int(year_str)
    else:
        now = datetime.now(LA_TZ)
        year = now.year
        # If the date is more than 2 months in the past, assume next year
        try:
            candidate = datetime(year, month, day)
        except ValueError:
            return None
        if (now.replace(tzinfo=None) - candidate).days > 60:
            year += 1

    # Parse time
    if ":" in time_str:
        parts = time_str.split(":")
        hour = int(parts[0])
        minute = int(parts[1])
    else:
        hour = int(time_str)
        minute = 0

    if period == "PM" and hour != 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0

    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=LA_TZ)
    except ValueError:
        return None

    return dt
