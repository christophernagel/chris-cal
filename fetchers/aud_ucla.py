"""
UCLA AUD (Architecture & Urban Design) events fetcher.

Source: https://aud.ucla.edu/news-events/events
Tier: regular

Scrapes the events listing page for linked card elements containing
thumbnail, title, and date text. Extracts event detail URLs from
the card anchor tags.

Pagination: The site has ~18 pages total, but upcoming events are
front-loaded. We fetch only pages 1 and 2 to avoid excessive requests
while still capturing all near-future events.

Dedup key: source_url (detail page URL extracted from card link).

Classification: all AUD events are talks_lectures. This is a fact
about the source — AUD primarily hosts lectures, symposia, exhibitions,
and academic events in the architecture/design domain.

HTML structure (as of March 2026):
  - Linked cards: <a> tags wrapping thumbnail image (296x168px),
    event title text, and date text
  - Date formats vary:
      "May 11, 2026" (single day)
      "Monday, June 8 and Tuesday, June 9 June 8 – June 9, 2026" (multi-day)
  - Filter categories: Lecture, Alumni, Exhibition, Symposium, etc.
  - Pagination via ?page=N query parameter
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "aud_ucla"
BASE_URL = "https://aud.ucla.edu"
EVENTS_URL = f"{BASE_URL}/news-events/events"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Pages to fetch (upcoming events are front-loaded on pages 1-2)
PAGES_TO_FETCH = [1, 2]

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}

# Tag keywords extracted from AUD filter categories
TAG_KEYWORDS: dict[str, list[str]] = {
    "lecture": ["lecture"],
    "exhibition": ["exhibition"],
    "symposium": ["symposium", "symposia"],
    "alumni": ["alumni"],
    "workshop": ["workshop"],
    "review": ["review"],
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse UCLA AUD events. Injectable client for testing.

    Fetches pages 1 and 2 only (of ~18 total). Upcoming events are
    front-loaded so this captures all near-future events without
    excessive requests.
    """
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        for page_num in PAGES_TO_FETCH:
            page_url = EVENTS_URL if page_num == 1 else f"{EVENTS_URL}?page={page_num}"

            try:
                resp = await client.get(page_url, follow_redirects=True)
                resp.raise_for_status()
            except httpx.TimeoutException as e:
                return FetchResult(
                    source_name=SOURCE_NAME,
                    outcome=FetchOutcome.TIMEOUT,
                    events=events,
                    events_found=events_found,
                    error_message=f"Timeout fetching {page_url}: {e}",
                )
            except httpx.HTTPStatusError as e:
                # If page 2 fails with 404, it might use a different URL scheme
                if page_num > 1 and e.response.status_code == 404:
                    # Try WordPress-style /page/N/ as fallback
                    try:
                        alt_url = f"{EVENTS_URL}/page/{page_num}/"
                        resp = await client.get(alt_url, follow_redirects=True)
                        resp.raise_for_status()
                    except Exception:
                        # Page 2 is optional; continue with what we have
                        warnings.append(ParseWarning(
                            event_index=None,
                            field="pagination",
                            message=f"Page {page_num} not accessible via ?page= or /page/ patterns",
                            raw_value=page_url,
                        ))
                        continue
                else:
                    return FetchResult(
                        source_name=SOURCE_NAME,
                        outcome=FetchOutcome.ERROR,
                        events_found=events_found,
                        error_message=f"HTTP {e.response.status_code} from {page_url}",
                    )

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = _extract_event_cards(soup)

            events_found += len(cards)

            for i, card_data in enumerate(cards):
                try:
                    event = _parse_card(card_data)
                    if event is not None:
                        events.append(event)
                except Exception as e:
                    warnings.append(ParseWarning(
                        event_index=i,
                        field="card",
                        message=f"Failed to parse card on page {page_num}: {e}",
                        raw_value=card_data.get("title", ""),
                    ))

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No event cards found on listing pages. Structure may have changed.",
            )

        outcome = FetchOutcome.SUCCESS if not warnings else FetchOutcome.PARTIAL
        return FetchResult(
            source_name=SOURCE_NAME,
            outcome=outcome,
            events=events,
            events_found=events_found,
            parse_warnings=warnings,
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
    """Extract event data from linked card elements on the listing page.

    Cards are anchor tags containing a thumbnail image, title text,
    and date text. We look for <a> tags with hrefs pointing to event
    detail pages.

    Returns list of dicts with keys: title, date_text, detail_url, tags.
    """
    results: list[dict[str, str]] = []

    # Find all anchor tags that link to event detail pages
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not isinstance(link, Tag):
            continue

        # Build full URL for the detail page
        if href.startswith("/"):
            detail_url = f"{BASE_URL}{href}"
        elif href.startswith(BASE_URL):
            detail_url = href
        else:
            continue

        # Skip non-event links (navigation, social, etc.)
        # Event links typically contain path segments like /news-events/ or /event/
        if not re.search(r"/news-events/|/event/|/events/", href):
            continue

        # Skip links that are just the listing page itself
        if href.rstrip("/") == "/news-events/events" or detail_url.rstrip("/") == EVENTS_URL:
            continue

        # Extract text content from the card
        card_text = link.get_text(separator="\n", strip=True)
        if not card_text:
            continue

        # Parse the card text into title and date
        card_data = _parse_card_text(card_text, detail_url)
        if card_data and card_data.get("title"):
            # Extract tag categories from the card markup
            card_tags = _extract_card_tags(link)
            if card_tags:
                card_data["tags"] = ",".join(card_tags)
            results.append(card_data)

    return results


def _parse_card_text(text: str, detail_url: str) -> dict[str, str] | None:
    """Parse the text content of an event card into structured data.

    The card text typically contains the title and date on separate lines.
    """
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if not lines:
        return None

    card_data: dict[str, str] = {"detail_url": detail_url}

    # Try to identify which lines are dates and which are titles
    title_parts: list[str] = []
    date_text: str | None = None

    for line in lines:
        if _looks_like_date(line):
            # Take the first date-like line
            if date_text is None:
                date_text = line
        else:
            title_parts.append(line)

    # Title is the non-date text
    if title_parts:
        card_data["title"] = " ".join(title_parts)
    elif lines:
        # Fallback: use first line as title
        card_data["title"] = lines[0]

    if date_text:
        card_data["date_text"] = date_text

    return card_data


def _extract_card_tags(card: Tag) -> list[str]:
    """Extract tag/category labels from card markup.

    Looks for category labels in the card's class names or child elements.
    """
    tags: list[str] = []
    card_text = card.get_text().lower()

    for tag, keywords in TAG_KEYWORDS.items():
        if any(kw in card_text for kw in keywords):
            tags.append(tag)

    # Also check class names on the card and its children
    for el in [card] + list(card.find_all(True)):
        classes = el.get("class", [])
        if isinstance(classes, list):
            class_str = " ".join(classes).lower()
            for tag, keywords in TAG_KEYWORDS.items():
                if tag not in tags and any(kw in class_str for kw in keywords):
                    tags.append(tag)

    return tags


def _looks_like_date(text: str) -> bool:
    """Heuristic: does this string look like a date?

    AUD date formats include:
      "May 11, 2026"
      "Monday, June 8 and Tuesday, June 9 June 8 – June 9, 2026"
      "June 8 – June 9, 2026"
    """
    # Contains a month name followed by a day number
    if re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2}",
        text,
        re.IGNORECASE,
    ):
        return True
    # Contains a day-of-week name with a comma (e.g., "Monday, June 8")
    if re.search(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),", text, re.IGNORECASE):
        return True
    return False


# ============================================================
# Card to EventData conversion
# ============================================================

def _parse_card(card_data: dict[str, str]) -> EventData | None:
    """Convert a parsed card dict into an EventData instance."""
    title = card_data.get("title")
    if not title:
        return None

    detail_url = card_data.get("detail_url", "")
    date_text = card_data.get("date_text", "")

    # Parse dates
    start_at, end_at = _parse_date_text(date_text)
    if start_at is None:
        return None  # can't create an event without a start time

    # Tags from card categories
    tags: list[str] = []
    if card_data.get("tags"):
        tags = card_data["tags"].split(",")

    # Venue: default to "UCLA AUD" since events are at the architecture school
    venue_name = "UCLA AUD"

    return EventData(
        title=title,
        source_url=detail_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        category="talks_lectures",
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=None,
        raw_source={
            "date_text": date_text,
            "detail_url": detail_url,
            "card_data": card_data,
        },
    )


# ============================================================
# Date parsing
# ============================================================

# Month name to number mapping
MONTH_MAP: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date_text(text: str) -> tuple[datetime | None, datetime | None]:
    """Parse AUD UCLA date strings into timezone-aware datetimes.

    Handles:
      "May 11, 2026"                              → single date
      "June 8 – June 9, 2026"                     → multi-day range
      "Monday, June 8 and Tuesday, June 9 June 8 – June 9, 2026"  → multi-day

    Times are not typically shown on listing cards, so we default to
    noon (12:00) LA time for the start time.

    Returns (start_at, end_at). end_at is None for single-date events.
    All times are America/Los_Angeles.
    """
    if not text:
        return None, None

    # Try multi-day range with en-dash or hyphen: "June 8 – June 9, 2026"
    range_match = re.search(
        r"([A-Za-z]+)\s+(\d{1,2})\s*[–\-]\s*([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        text,
    )
    if range_match:
        start_month_str = range_match.group(1).lower()
        start_day = int(range_match.group(2))
        end_month_str = range_match.group(3).lower()
        end_day = int(range_match.group(4))
        year = int(range_match.group(5))

        start_month = MONTH_MAP.get(start_month_str)
        end_month = MONTH_MAP.get(end_month_str)

        if start_month and end_month:
            try:
                start_at = datetime(year, start_month, start_day, 12, 0, tzinfo=LA_TZ)
                end_at = datetime(year, end_month, end_day, 12, 0, tzinfo=LA_TZ)
                return start_at, end_at
            except ValueError:
                pass

    # Try single date: "May 11, 2026" or "Monday, May 11, 2026"
    single_match = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        text,
    )
    if single_match:
        month_str = single_match.group(1).lower()
        day = int(single_match.group(2))
        year = int(single_match.group(3))

        month = MONTH_MAP.get(month_str)
        if month:
            try:
                start_at = datetime(year, month, day, 12, 0, tzinfo=LA_TZ)
                return start_at, None
            except ValueError:
                pass

    # Try date without year: "May 11" — assume current or next occurrence
    no_year_match = re.search(
        r"([A-Za-z]+)\s+(\d{1,2})",
        text,
    )
    if no_year_match:
        month_str = no_year_match.group(1).lower()
        day = int(no_year_match.group(2))

        month = MONTH_MAP.get(month_str)
        if month:
            now = datetime.now(LA_TZ)
            year = now.year
            try:
                dt = datetime(year, month, day, 12, 0, tzinfo=LA_TZ)
                # If date is more than 2 months in the past, assume next year
                if (now - dt).days > 60:
                    dt = dt.replace(year=year + 1)
                return dt, None
            except ValueError:
                pass

    return None, None
