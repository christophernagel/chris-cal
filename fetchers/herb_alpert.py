"""
UCLA Herb Alpert School of Music calendar fetcher.

Source: https://schoolofmusic.ucla.edu/calendar
Tier: regular

Scrapes the calendar listing page for .event-card elements. Events are
rendered server-side (WordPress). Each card contains date, title, and
a link to a detail page.

Dedup key: source_url (detail page URL).

Classification: all events are music_performance. The School of Music
programs student recitals, faculty concerts, ensemble performances,
and guest artist appearances across genres (classical, jazz, chamber,
choral, opera, contemporary).

Structure (as of March 2026):
  - Listing page: .event-card containers
    .event-date with .event-month-day (flex row-reverse) and span.date-time
    .content with .caption (title heading)
  - Cards are anchor-wrapped linking to detail pages
  - No known pagination; events appear in a single grid
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "herb_alpert"
BASE_URL = "https://schoolofmusic.ucla.edu"
CALENDAR_URL = f"{BASE_URL}/calendar"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Herb Alpert School of Music calendar. Injectable client for testing."""
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
        cards = soup.find_all(class_="event-card")
        events_found = len(cards)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No .event-card elements found on calendar page. Structure may have changed.",
            )

        for i, card in enumerate(cards):
            try:
                event = _parse_event_card(card, i, warnings)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="card",
                    message=f"Failed to parse event card {i}: {e}",
                    raw_value=_safe_text(card),
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
# Card parsing
# ============================================================

def _parse_event_card(
    card: Tag,
    index: int,
    warnings: list[ParseWarning],
) -> EventData | None:
    """Parse a single .event-card element into an EventData."""

    # Find the wrapping anchor for the detail page link
    link_tag = card.find("a", href=True) if card.name != "a" else card
    if card.name == "a" and card.get("href"):
        link_tag = card

    href = None
    if link_tag and link_tag.get("href"):
        href = link_tag["href"]

    # Build source_url from the detail page link
    source_url = None
    if href:
        if href.startswith("http"):
            source_url = href
        elif href.startswith("/"):
            source_url = f"{BASE_URL}{href}"
        else:
            source_url = f"{BASE_URL}/{href}"

    if not source_url:
        warnings.append(ParseWarning(
            event_index=index,
            field="source_url",
            message="No link found in event card",
            raw_value=_safe_text(card),
        ))
        return None

    # Title: look in .content / .caption for a heading or strong text
    title = _extract_title(card)
    if not title:
        warnings.append(ParseWarning(
            event_index=index,
            field="title",
            message="No title found in event card",
            raw_value=_safe_text(card),
        ))
        return None

    # Date and time
    start_at = _extract_datetime(card)
    if start_at is None:
        warnings.append(ParseWarning(
            event_index=index,
            field="start_at",
            message="Could not parse date/time from event card",
            raw_value=_safe_text(card),
        ))
        return None

    # Venue: extract from card text if present
    venue_name = _extract_venue(card)

    # Free status: check card text for "free"
    card_text = card.get_text(" ", strip=True).lower()
    is_free = "free" in card_text

    # Extract category tag from Elementor 'category' class element
    cat_el = card.find(class_="category")
    cat_text = cat_el.get_text(strip=True).lower() if cat_el else ""
    if cat_text:
        tags = [cat_text.replace(" ", "_")]
    else:
        tags = []

    # Add tags from title and card text
    tags.extend(t for t in _extract_tags(title, card_text) if t not in tags)

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        venue_name=venue_name,
        category="music_performance",
        tags=tags,
        is_free=is_free,
        is_one_off=True,
        description=None,
        raw_source={
            "href": href,
            "card_text": card_text[:500],
        },
    )


def _extract_title(card: Tag) -> str | None:
    """Extract title from the event card.

    The UCLA Herb Alpert School of Music site uses Elementor + JetEngine.
    Title is in a widget with class matching 'elementor-widget-jet-listing-*'.
    Falls back to heading tags, .caption/.content, or the link slug.
    """
    # Strategy 1: JetEngine listing widget (Elementor site pattern)
    jet_widget = card.find(
        class_=lambda c: c and "elementor-widget-jet-listing" in " ".join(c)
        if isinstance(c, list) else c and "elementor-widget-jet-listing" in c
    )
    if jet_widget:
        text = jet_widget.get_text(strip=True)
        if text:
            return text

    # Strategy 2: heading tags anywhere in the card
    for tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        heading = card.find(tag_name)
        if heading:
            text = heading.get_text(strip=True)
            if text:
                return text

    # Strategy 3: .caption or .content containers
    for cls in ("caption", "content"):
        container = card.find(class_=cls)
        if container:
            text = container.get_text(strip=True)
            if text and len(text) < 200:
                return text

    # Strategy 4: derive from the event URL slug
    link = card.find("a", href=True)
    if link and "/event/" in link["href"]:
        slug = link["href"].rstrip("/").rsplit("/", 1)[-1]
        return slug.replace("-", " ").title()

    return None


def _extract_datetime(card: Tag) -> datetime | None:
    """Extract start datetime from .event-date section.

    The .event-date div contains .event-month-day (with month and day)
    and span.date-time (with the time). The month-day div uses a
    flex row-reverse layout, so the DOM order may be day then month
    or month then day — we parse both patterns.
    """
    date_div = card.find(class_="event-date")
    if date_div is None:
        return None

    # Site uses Elementor structure with specific span classes:
    #   <div class="event-date">
    #     <div class="event-month-day">
    #       <span class="date-month">Mar</span>
    #       <span class="date-text">Sun</span>  (day-of-week, not needed)
    #     </div>
    #     <span class="date-number">8</span>
    #     <span class="date-time">4:00 PM</span>
    #   </div>

    month = None
    day = None

    # Extract month from span.date-month
    month_span = date_div.find(class_="date-month")
    if month_span:
        month = _month_to_num(month_span.get_text(strip=True))

    # Extract day number from span.date-number
    day_span = date_div.find(class_="date-number")
    if day_span:
        day_text = day_span.get_text(strip=True)
        if day_text.isdigit():
            day = int(day_text)

    # Fallback: parse from combined text if structured extraction fails
    if month is None or day is None:
        combined = date_div.get_text(" ", strip=True)
        match = re.search(r"([A-Za-z]{3,9})\s+\w+\s+(\d{1,2})", combined)
        if match:
            month = month or _month_to_num(match.group(1))
            day = day or int(match.group(2))

    if month is None or day is None:
        return None

    # Extract time from span.date-time
    hour, minute = 19, 0  # default 7pm for music events
    time_span = date_div.find(class_="date-time")
    if time_span:
        time_text = time_span.get_text(strip=True)
        parsed_hour, parsed_minute = _parse_time(time_text)
        if parsed_hour is not None:
            hour, minute = parsed_hour, parsed_minute

    # Build datetime
    now = datetime.now(LA_TZ)
    year = now.year

    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=LA_TZ)
    except ValueError:
        return None

    # If the date is more than 2 months in the past, it's probably next year
    if (now.replace(tzinfo=None) - dt.replace(tzinfo=None)).days > 60:
        try:
            dt = dt.replace(year=year + 1)
        except ValueError:
            return None

    return dt


def _parse_date_text(text: str) -> datetime | None:
    """Fallback: parse a date string like 'Mar 14 7:30 pm' or 'March 14, 2026 7:30 PM'."""
    # Try to find month + day
    match = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2})", text)
    if not match:
        return None

    month = _month_to_num(match.group(1))
    day = int(match.group(2))
    if month is None:
        return None

    hour, minute = _parse_time(text)
    if hour is None:
        hour, minute = 0, 0

    now = datetime.now(LA_TZ)
    year = now.year

    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=LA_TZ)
    except ValueError:
        return None

    if (now.replace(tzinfo=None) - dt.replace(tzinfo=None)).days > 60:
        try:
            dt = dt.replace(year=year + 1)
        except ValueError:
            return None

    return dt


# ============================================================
# Venue extraction
# ============================================================

# Known UCLA School of Music venues for matching
_KNOWN_VENUES = [
    "Schoenberg Hall",
    "Ostin Music Center",
    "Jan Popper Theater",
    "Lani Hall",
    "Kerckhoff",
    "Royce Hall",
    "Nimoy",
    "Glorya Kaufman",
    "Broad Art Center",
    "Sunset Canyon",
]


def _extract_venue(card: Tag) -> str | None:
    """Extract venue name from event card text.

    Looks for known venue names in the card text, or a dedicated
    venue element if present.
    """
    # Check for a dedicated venue element (common patterns)
    for class_name in ("venue", "event-venue", "location", "event-location"):
        venue_el = card.find(class_=class_name)
        if venue_el:
            text = venue_el.get_text(strip=True)
            if text:
                return text

    # Scan card text for known venue names
    card_text = card.get_text(" ", strip=True)
    for venue in _KNOWN_VENUES:
        if venue.lower() in card_text.lower():
            return venue

    return None


# ============================================================
# Tag extraction
# ============================================================

def _extract_tags(title: str, card_text: str) -> list[str]:
    """Extract genre/type tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {card_text}".lower()

    tag_signals = {
        "jazz": ["jazz"],
        "classical": ["classical", "symphony", "symphonic", "orchestral", "orchestra"],
        "chamber": ["chamber", "string quartet", "piano trio"],
        "choral": ["choral", "choir", "chorus", "vocal ensemble"],
        "opera": ["opera", "operatic"],
        "contemporary": ["contemporary", "new music", "experimental", "avant-garde"],
        "ensemble": ["ensemble", "wind ensemble", "brass ensemble"],
        "recital": ["recital"],
        "composer": ["composer", "composition"],
        "world_music": ["gamelan", "ethnomusicology", "world music"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    return tags


# ============================================================
# Time parsing
# ============================================================

def _parse_time(text: str) -> tuple[int | None, int]:
    """Parse time component from text like '7:30 pm', '8 PM', '10:45 am'.

    Returns (hour_24, minute). hour is None if no time found.
    """
    match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm|AM|PM|a\.m\.|p\.m\.)", text)
    if not match:
        return None, 0

    hour = int(match.group(1))
    minute = int(match.group(2)) if match.group(2) else 0
    period = match.group(3).lower().replace(".", "")

    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0

    return hour, minute


def _month_to_num(name: str) -> int | None:
    """Convert month name or abbreviation to number."""
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4,
        "june": 6, "july": 7, "august": 8, "september": 9,
        "october": 10, "november": 11, "december": 12,
    }
    return month_map.get(name.lower()[:3])


def _safe_text(tag: Tag) -> str:
    """Get truncated text from a tag for debug purposes."""
    text = tag.get_text(" ", strip=True)
    return text[:300] if len(text) > 300 else text
