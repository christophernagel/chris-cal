"""
Hammer Museum programs & events fetcher.

Source: https://hammer.ucla.edu/programs-events
Tier: regular

Scrapes the programs-events listing page for event cards. Each card is
wrapped in an anchor tag linking to a detail page. Extracts title, category,
date/time, description, and detail URL from the listing page cards.

Pagination exists via query params (?start_date=MM/DD/YYYY&page=N) but is
intentionally limited to page 1 only. The health system's baseline deviation
check will catch significant event count drops, signaling if page 1 coverage
becomes insufficient.

Dedup key: source_url (detail page URL, e.g. /programs-events/2026/lunchtime-art-talk).

Classification:
  - Category mapped from site tags: Tours & Talks -> talks_lectures,
    Screenings -> film_screening, Kids -> visual_art, Music -> music_performance,
    default -> visual_art.
  - Hammer Museum has free general admission; most events are free.
    is_free defaults True unless ticket/price text is detected.

Structure (as of March 2026):
  - Cards: <a href="/programs-events/2026/..."> wrapping image, category,
    title heading, description, and date/time text
  - Category tags as separate elements (e.g., "Tours & Talks", "Screenings")
  - Date format: "Wed Mar 11 12:30 PM"
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "hammer"
BASE_URL = "https://hammer.ucla.edu"
EVENTS_URL = f"{BASE_URL}/programs-events"
LA_TZ = ZoneInfo("America/Los_Angeles")

VENUE_NAME = "Hammer Museum"
VENUE_ADDRESS = "10899 Wilshire Blvd, Los Angeles, CA 90024"

# Category mapping from Hammer site tags to chriscal categories
CATEGORY_MAP: dict[str, str] = {
    "tours & talks": "talks_lectures",
    "screenings": "film_screening",
    "kids": "visual_art",
    "music": "music_performance",
}

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Hammer Museum programs & events (page 1 only). Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        # Fetch listing page (page 1 only; pagination via ?page=N exists but intentionally skipped)
        today = datetime.now(LA_TZ).strftime("%m/%d/%Y")
        params = {"start_date": today, "page": "1"}
        resp = await client.get(EVENTS_URL, params=params, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = _extract_event_cards(soup)
        events_found = len(cards)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event cards found on programs-events page. Structure may have changed.",
            )

        for i, card_data in enumerate(cards):
            try:
                event = _parse_card(card_data)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="card",
                    message=f"Failed to parse card: {e}",
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
# Listing page parsing
# ============================================================

def _extract_event_cards(soup: BeautifulSoup) -> list[dict[str, str | None]]:
    """Extract event data from anchor-wrapped cards on the listing page.

    Each card is an <a> tag with href like /programs-events/2026/...
    containing category tag(s), a title heading, description text,
    and date/time text.

    Returns list of dicts with keys: href, title, category, date_text,
    description.
    """
    results: list[dict[str, str | None]] = []

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if not re.match(r"/programs-events/\d{4}/", href):
            continue

        # Skip if this anchor doesn't look like a card (needs some substance)
        if not isinstance(link, Tag):
            continue

        card_text = link.get_text(separator="\n", strip=True)
        if not card_text or len(card_text) < 10:
            continue

        card_data: dict[str, str | None] = {"href": href}

        # Extract structured fields from card contents
        _parse_card_contents(link, card_data)

        # Only include if we found a title
        if card_data.get("title"):
            results.append(card_data)

    return results


def _parse_card_contents(card: Tag, data: dict[str, str | None]) -> None:
    """Parse the internal structure of an event card anchor tag.

    Extracts title (from heading), category (from tag elements),
    description (from text elements), and date/time text.
    """
    # Title: look for heading tags (h2, h3, h4) within the card
    heading = card.find(re.compile(r"^h[2-4]$"))
    if heading:
        data["title"] = heading.get_text(strip=True)
    else:
        # Fallback: use the first substantial text block
        texts = [t.strip() for t in card.stripped_strings]
        for t in texts:
            if len(t) > 5 and not _looks_like_date(t):
                data["title"] = t
                break

    # Category: look for category tag elements
    # These are typically small text elements before the title
    # Common patterns: span, div, or p with category-like text
    category_text = None
    known_categories = {"tours & talks", "screenings", "kids", "music",
                        "lectures", "performances", "exhibitions", "workshops"}

    for el in card.find_all(["span", "div", "p", "li"]):
        el_text = el.get_text(strip=True).lower()
        if el_text in known_categories or any(cat in el_text for cat in known_categories):
            category_text = el.get_text(strip=True)
            break

    # If no structured category element found, scan all text fragments
    if not category_text:
        for text in card.stripped_strings:
            if text.strip().lower() in known_categories:
                category_text = text.strip()
                break

    data["category"] = category_text

    # Date/time: look for text matching "Wed Mar 11 12:30 PM" pattern
    date_pattern = re.compile(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
        r"\d{1,2}\s+\d{1,2}(?::\d{2})?\s*(?:AM|PM)",
        re.IGNORECASE,
    )
    card_full_text = card.get_text(separator=" ", strip=True)
    date_match = date_pattern.search(card_full_text)
    if date_match:
        data["date_text"] = date_match.group(0)

    # Description: text content that isn't the title, category, or date
    desc_parts: list[str] = []
    title = data.get("title", "")
    cat = data.get("category", "")
    date_txt = data.get("date_text", "")

    for text in card.stripped_strings:
        t = text.strip()
        if not t or len(t) < 10:
            continue
        if t == title or t == cat or t == date_txt:
            continue
        if _looks_like_date(t):
            continue
        desc_parts.append(t)

    if desc_parts:
        description = " ".join(desc_parts)
        # Don't use description if it's basically just the title
        if description != title:
            data["description"] = description[:1000] if len(description) > 1000 else description


def _looks_like_date(text: str) -> bool:
    """Heuristic: does this string look like a Hammer date line?
    Format: 'Wed Mar 11 12:30 PM'
    """
    return bool(re.search(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
        text,
        re.IGNORECASE,
    ))


# ============================================================
# Card → EventData
# ============================================================

def _parse_card(card_data: dict[str, str | None]) -> EventData | None:
    """Convert a parsed card dict into an EventData instance."""
    title = card_data.get("title")
    href = card_data.get("href")
    if not title or not href:
        return None

    # Parse date
    date_text = card_data.get("date_text")
    start_at = _parse_date(date_text) if date_text else None
    if start_at is None:
        return None  # can't create an event without a start time

    # Build source URL
    source_url = f"{BASE_URL}{href}"

    # Map category
    raw_category = (card_data.get("category") or "").strip().lower()
    category = CATEGORY_MAP.get(raw_category, "visual_art")

    # Tags from category label
    tags: list[str] = []
    if card_data.get("category"):
        tags.append(card_data["category"])

    # Description
    description = card_data.get("description")

    # Free by default; check for ticket/price signals
    is_free = True
    combined_text = f"{title} {description or ''}".lower()
    if any(kw in combined_text for kw in ["ticket", "$", "admission fee", "paid"]):
        is_free = False

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
            "card_data": {k: v for k, v in card_data.items() if v is not None},
        },
    )


# ============================================================
# Date parsing
# ============================================================

def _parse_date(text: str) -> datetime | None:
    """Parse Hammer date strings into timezone-aware datetimes.

    Format: 'Wed Mar 11 12:30 PM' or 'Sat Apr 5 2 PM'

    Uses current year since the source doesn't include it.
    All times are America/Los_Angeles.
    """
    if not text:
        return None

    # Strip day-of-week prefix (e.g., "Wed ")
    text = re.sub(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+", "", text.strip(), flags=re.IGNORECASE)

    # Parse "Mar 11 12:30 PM" or "Apr 5 2 PM"
    match = re.match(
        r"([A-Za-z]+)\s+(\d{1,2})\s+(\d{1,2})(?::(\d{2}))?\s*(AM|PM)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None

    month_str = match.group(1)
    day = int(match.group(2))
    hour = int(match.group(3))
    minute = int(match.group(4)) if match.group(4) else 0
    period = match.group(5).upper()

    # Convert to 24-hour
    if period == "PM" and hour != 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0

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
        dt = datetime(year, month, day, hour, minute, tzinfo=LA_TZ)
    except ValueError:
        return None

    # If the date is more than 2 months in the past, assume next year
    if (now - dt).days > 60:
        try:
            dt = dt.replace(year=year + 1)
        except ValueError:
            return None

    return dt
