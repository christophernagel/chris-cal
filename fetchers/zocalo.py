"""
Zócalo Public Square fetcher.

Source: https://www.zocalopublicsquare.org/upcoming-experiences/
Tier: regular

Scrapes the upcoming experiences page for event cards. Each card is an
anchor tag within a WordPress wp-block-newspack-blocks-homepage-articles
block, containing an image, title heading, date/location metadata line,
and a description snippet.

Dedup key: source_url (stable per-event URLs).

Classification: all Zócalo events are talks_lectures. This is a fact
about the source — Zócalo produces free public discussion events on
politics, culture, history, science, and the arts.

Structure (as of March 2026):
  - Listing page: wp-block-newspack-blocks-homepage-articles container
    with anchor tags linking to individual event detail pages
  - Each card: image (resize=1200x900), title heading, date/location
    metadata line "March 11, 2026 │ STANFORD, CA", description snippet
  - No pagination, no dedicated calendar page
  - WordPress CMS
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "zocalo"
BASE_URL = "https://www.zocalopublicsquare.org"
EVENTS_URL = "https://www.zocalopublicsquare.org/upcoming-experiences/"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Zócalo Public Square upcoming experiences. Injectable client for testing."""
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
                error_message="No event cards found on upcoming experiences page. Structure may have changed.",
            )

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

def _extract_event_cards(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Extract event data from the upcoming experiences page.

    Finds anchor tags within wp-block-newspack-blocks-homepage-articles
    containers. Each card has a title heading, date/location metadata,
    image, and description snippet.

    Returns list of dicts with keys: title, url, date_text, venue_text,
    description, image_url.
    """
    results = []

    # Find the Newspack homepage articles block
    articles_block = soup.find("div", class_=re.compile(r"wp-block-newspack-blocks-homepage-articles"))

    # If no Newspack block found, fall back to searching full page
    search_root = articles_block if articles_block else soup

    # Look for article entries — Newspack blocks use article tags or
    # div containers with post entries
    article_elements = search_root.find_all("article")
    if not article_elements:
        # Fallback: look for anchor-based cards
        article_elements = search_root.find_all("div", class_=re.compile(r"post"))

    for article in article_elements:
        card_data: dict[str, str] = {}

        # Find the link to the event detail page
        link = article.find("a", href=True)
        if not link:
            continue
        href = link.get("href", "")
        if not href:
            continue

        # Normalize URL
        if href.startswith("/"):
            card_data["url"] = f"{BASE_URL}{href}"
        elif href.startswith("http"):
            card_data["url"] = href
        else:
            continue

        # Title: find heading tag within the card
        title_el = article.find(re.compile(r"h[1-6]"))
        if title_el:
            card_data["title"] = title_el.get_text(strip=True)

        # Image
        img = article.find("img")
        if img and img.get("src"):
            card_data["image_url"] = img["src"]

        # Metadata line: date and location separated by │
        # Look for text containing the │ separator or a date-like pattern
        meta_text = _find_metadata_line(article)
        if meta_text:
            card_data["meta_text"] = meta_text
            date_part, venue_part = _split_metadata(meta_text)
            if date_part:
                card_data["date_text"] = date_part
            if venue_part:
                card_data["venue_text"] = venue_part

        # Description snippet
        desc_el = article.find("div", class_=re.compile(r"entry-content|excerpt|summary"))
        if desc_el:
            card_data["description"] = desc_el.get_text(strip=True)[:1000]
        else:
            # Try finding a <p> that looks like a description
            for p in article.find_all("p"):
                text = p.get_text(strip=True)
                if len(text) > 40 and not _looks_like_date_line(text):
                    card_data["description"] = text[:1000]
                    break

        if card_data.get("title"):
            results.append(card_data)

    return results


def _find_metadata_line(element: Tag) -> str | None:
    """Find the date/location metadata line within an event card.

    Looks for text containing the │ (box drawing vertical) separator
    or a date-like pattern like "March 11, 2026".
    """
    # Look for spans/divs that contain the │ separator
    for child in element.find_all(["span", "div", "p"]):
        text = child.get_text(strip=True)
        if "│" in text or "\\u2502" in text:
            return text

    # Fallback: look for date-like text in any text node
    all_text = element.get_text(separator="\n").split("\n")
    for line in all_text:
        line = line.strip()
        if _looks_like_date_line(line):
            return line

    return None


def _looks_like_date_line(text: str) -> bool:
    """Check if text looks like a Zócalo date/location line.
    Format: 'March 11, 2026 │ STANFORD, CA' or just 'March 11, 2026'
    """
    return bool(re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}",
        text,
    ))


def _split_metadata(text: str) -> tuple[str | None, str | None]:
    """Split a metadata line on │ into (date_part, venue_part)."""
    # Try box drawing vertical line
    if "│" in text:
        parts = text.split("│", 1)
        date_part = parts[0].strip() or None
        venue_part = parts[1].strip() if len(parts) > 1 else None
        return date_part, venue_part

    # If no separator, the whole thing might be a date
    if _looks_like_date_line(text):
        return text.strip(), None

    return None, None


# ============================================================
# Event parsing
# ============================================================

def _parse_event_card(card_data: dict[str, str]) -> EventData | None:
    """Convert extracted card data into an EventData instance."""
    title = card_data.get("title")
    url = card_data.get("url")
    if not title or not url:
        return None

    # Parse date
    date_text = card_data.get("date_text", "")
    start_at = _parse_date(date_text)
    if start_at is None:
        return None

    # Venue
    venue_name = card_data.get("venue_text")

    # Description
    description = card_data.get("description")

    # Tags from content signals
    tags = _extract_tags(title, description or "")

    return EventData(
        title=title,
        source_url=url,
        start_at=start_at,
        venue_name=venue_name,
        category="talks_lectures",
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=description,
        raw_source={
            "card_data": card_data,
        },
    )


# ============================================================
# Date parsing
# ============================================================

def _parse_date(text: str) -> datetime | None:
    """Parse Zócalo date strings like 'March 11, 2026' into timezone-aware datetimes.

    Events default to 19:30 (7:30 PM) when no time is specified,
    as Zócalo events are typically evening programs.
    """
    if not text:
        return None

    # Match "Month DD, YYYY" or "Month DD YYYY"
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2}),?\s+(\d{4})",
        text,
    )
    if not match:
        return None

    month_str = match.group(1)
    day = int(match.group(2))
    year = int(match.group(3))

    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    month = month_map.get(month_str.lower())
    if month is None:
        return None

    try:
        # Default to 7:30 PM for evening discussion events
        dt = datetime(year, month, day, 19, 30, tzinfo=LA_TZ)
    except ValueError:
        return None

    return dt


# ============================================================
# Tag extraction
# ============================================================

def _extract_tags(title: str, description: str) -> list[str]:
    """Extract tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {description}".lower()

    tag_signals = {
        "politics": ["politics", "political", "democracy", "government", "policy", "election", "voting"],
        "culture": ["culture", "cultural", "community", "society"],
        "history": ["history", "historical", "heritage"],
        "science": ["science", "scientific", "research", "technology", "climate"],
        "arts": ["arts", "art", "artist", "creative", "literature", "literary", "poetry", "music"],
        "immigration": ["immigration", "immigrant", "migration", "border", "refugee"],
        "education": ["education", "school", "university", "student", "learning"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    return tags
