"""
Fowler Museum events fetcher.

Source: https://fowler.ucla.edu/programs
Tier: regular

Scrapes the programs listing page for event cards. WordPress site with
article/div card containers containing event title, date, time, location,
category badges, and description snippets.

Dedup key: source_url (per-event links with occurrence parameters).

Classification: category varies by event type — exhibitions map to
visual_art, talks/lectures to talks_lectures, concerts to music_performance.
Fowler Museum is free admission; is_free defaults to True.

Structure (as of March 2026):
  - Listing page: article/div card containers
  - Cards contain: img, linked title (h2/h3 in anchor), event type badges
    (IN PERSON, ONLINE, RSVP), description snippet, date/time, location
  - Date format: "Thu Mar 12, 2026" with time "12:30 pm - 1:00 pm"
  - Categories like category-concert, category-talk on list items
  - Links to individual event pages with occurrence parameters
  - WordPress CMS
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "fowler"
BASE_URL = "https://fowler.ucla.edu"
PROGRAMS_URL = f"{BASE_URL}/programs"
LA_TZ = ZoneInfo("America/Los_Angeles")

VENUE_NAME = "Fowler Museum"
VENUE_ADDRESS = "308 Charles E. Young Dr N, Los Angeles, CA 90024"

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}

# Map CSS category classes to event_category enum values
CATEGORY_MAP: dict[str, str] = {
    "category-concert": "music_performance",
    "category-performance": "music_performance",
    "category-music": "music_performance",
    "category-talk": "talks_lectures",
    "category-lecture": "talks_lectures",
    "category-panel": "talks_lectures",
    "category-symposium": "talks_lectures",
    "category-exhibition": "visual_art",
    "category-gallery": "visual_art",
    "category-art": "visual_art",
    "category-film": "film_screening",
    "category-screening": "film_screening",
    "category-workshop": "workshop",
    "category-family": "community",
    "category-community": "community",
    "category-festival": "community",
}

DEFAULT_CATEGORY = "visual_art"


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Fowler Museum programs page. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
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

        for i, card in enumerate(cards):
            try:
                event = _parse_event_card(card, i)
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

def _extract_event_cards(soup: BeautifulSoup) -> list[Tag]:
    """Extract event card containers from the programs page.

    WordPress sites typically use article tags or div containers for cards.
    We look for common card patterns and return the outermost container
    for each event.
    """
    # Try article tags first (common WordPress pattern)
    cards = soup.find_all("article")
    if cards:
        return cards

    # Try common WordPress card class patterns
    for class_pattern in ["event-card", "program-card", "tribe-events",
                          "type-tribe_events", "post", "entry"]:
        cards = soup.find_all("div", class_=re.compile(class_pattern, re.I))
        if cards:
            return cards

    # Try list items with category classes (from the HTML structure description)
    cards = soup.find_all("li", class_=re.compile(r"category-", re.I))
    if cards:
        return cards

    # Fallback: any container with both a heading link and a date-like string
    candidates = []
    for container in soup.find_all(["div", "li", "section"]):
        has_link = container.find(["h2", "h3"], recursive=True)
        text = container.get_text()
        if has_link and re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\w+\s+\d{1,2}", text):
            candidates.append(container)
    if candidates:
        return candidates

    return []


def _parse_event_card(card: Tag, index: int) -> EventData | None:
    """Parse a single event card into an EventData."""
    # Extract title and link from heading anchor
    title, source_url = _extract_title_and_link(card)
    if not title:
        return None

    # Extract date and time
    start_at, end_at = _extract_datetime(card)
    if start_at is None:
        return None

    # Extract category from CSS classes
    category = _extract_category(card)

    # Extract description snippet
    description = _extract_description(card)

    # Extract tags from category classes and content
    tags = _extract_tags(card, title, description or "")

    # Extract badges (IN PERSON, ONLINE, RSVP)
    badges = _extract_badges(card)
    if badges:
        tags.extend(badges)
    # Deduplicate tags
    tags = list(dict.fromkeys(tags))

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=VENUE_NAME,
        venue_address=VENUE_ADDRESS,
        category=category,
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=description,
        raw_source={
            "card_index": index,
            "card_html": str(card)[:2000],
        },
    )


def _extract_title_and_link(card: Tag) -> tuple[str | None, str]:
    """Extract event title and URL from a card's heading link.

    Looks for h2/h3 tags inside anchors, or anchors inside h2/h3 tags.
    """
    # Pattern 1: heading inside an anchor
    for tag_name in ["h2", "h3", "h4"]:
        heading = card.find(tag_name)
        if heading:
            # Check if heading is inside an anchor
            parent_a = heading.find_parent("a", href=True)
            if parent_a:
                title = heading.get_text(strip=True)
                href = parent_a["href"]
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                return title, url

            # Check if heading contains an anchor
            a_tag = heading.find("a", href=True)
            if a_tag:
                title = a_tag.get_text(strip=True) or heading.get_text(strip=True)
                href = a_tag["href"]
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                return title, url

            # Heading with no link — use heading text and card-level link
            title = heading.get_text(strip=True)
            if title:
                card_link = card.find("a", href=True)
                href = card_link["href"] if card_link else ""
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                return title, url

    # Fallback: first anchor with substantial text
    for a_tag in card.find_all("a", href=True):
        text = a_tag.get_text(strip=True)
        if len(text) > 5:
            href = a_tag["href"]
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            return text, url

    return None, ""


def _extract_datetime(card: Tag) -> tuple[datetime | None, datetime | None]:
    """Extract start and end datetimes from a card.

    Looks for date patterns like "Thu Mar 12, 2026" and time patterns
    like "12:30 pm - 1:00 pm" in the card text.
    """
    card_text = card.get_text(" ", strip=True)

    # Try to find date: "Thu Mar 12, 2026" or "Mar 12, 2026"
    date_match = re.search(
        r"(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+)?"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
        r"(\d{1,2}),?\s*(\d{4})",
        card_text,
    )

    if not date_match:
        # Try without year: "Thu Mar 12"
        date_match = re.search(
            r"(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+)?"
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
            r"(\d{1,2})",
            card_text,
        )

    if not date_match:
        return None, None

    month_str = date_match.group(1)
    day = int(date_match.group(2))
    year = int(date_match.group(3)) if date_match.lastindex >= 3 else _infer_year(month_str, day)

    month = _month_to_int(month_str)
    if month is None:
        return None, None

    # Parse time: "12:30 pm - 1:00 pm" or "8 pm"
    start_hour, start_min, end_hour, end_min = _extract_time_range(card_text)

    try:
        start_at = datetime(year, month, day, start_hour, start_min, tzinfo=LA_TZ)
    except ValueError:
        return None, None

    end_at = None
    if end_hour is not None:
        try:
            end_at = datetime(year, month, day, end_hour, end_min, tzinfo=LA_TZ)
            # Handle case where end time is before start (shouldn't happen normally)
            if end_at <= start_at:
                end_at = None
        except ValueError:
            end_at = None

    return start_at, end_at


def _extract_time_range(text: str) -> tuple[int, int, int | None, int]:
    """Extract start and end times from text.

    Handles:
      "12:30 pm - 1:00 pm"
      "8 pm"
      "10:45 am - 12:00 pm"

    Returns (start_hour_24, start_min, end_hour_24|None, end_min).
    If no time found, defaults to noon (12:00).
    """
    # Try time range: "12:30 pm - 1:00 pm"
    range_match = re.search(
        r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*[-–]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
        text,
        re.I,
    )
    if range_match:
        sh, sm = _to_24h(
            int(range_match.group(1)),
            int(range_match.group(2)) if range_match.group(2) else 0,
            range_match.group(3),
        )
        eh, em = _to_24h(
            int(range_match.group(4)),
            int(range_match.group(5)) if range_match.group(5) else 0,
            range_match.group(6),
        )
        return sh, sm, eh, em

    # Try single time: "8 pm" or "10:45 am"
    single_match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)", text, re.I)
    if single_match:
        sh, sm = _to_24h(
            int(single_match.group(1)),
            int(single_match.group(2)) if single_match.group(2) else 0,
            single_match.group(3),
        )
        return sh, sm, None, 0

    # No time found — default to noon
    return 12, 0, None, 0


def _to_24h(hour: int, minute: int, period: str) -> tuple[int, int]:
    """Convert 12-hour time to 24-hour."""
    period = period.lower()
    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0
    return hour, minute


def _month_to_int(month_str: str) -> int | None:
    """Convert month abbreviation to integer."""
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    return month_map.get(month_str.lower()[:3])


def _infer_year(month_str: str, day: int) -> int:
    """Infer year when not provided. Assumes upcoming date."""
    now = datetime.now(LA_TZ)
    month = _month_to_int(month_str)
    if month is None:
        return now.year

    try:
        candidate = datetime(now.year, month, day)
    except ValueError:
        return now.year

    # If more than 2 months in the past, assume next year
    if (now.replace(tzinfo=None) - candidate).days > 60:
        return now.year + 1
    return now.year


# ============================================================
# Content extraction helpers
# ============================================================

def _extract_category(card: Tag) -> str:
    """Map CSS category classes on the card to an event_category value.

    Checks the card element and its ancestors for category-* classes.
    """
    # Collect all classes from the card and its parent list item
    classes: list[str] = []
    if card.get("class"):
        classes.extend(card["class"])
    parent_li = card.find_parent("li")
    if parent_li and parent_li.get("class"):
        classes.extend(parent_li["class"])

    for cls in classes:
        cls_lower = cls.lower()
        if cls_lower in CATEGORY_MAP:
            return CATEGORY_MAP[cls_lower]

    # Fallback: check card text for category hints
    text = card.get_text().lower()
    if any(w in text for w in ["exhibition", "gallery", "art"]):
        return "visual_art"
    if any(w in text for w in ["talk", "lecture", "panel", "symposium"]):
        return "talks_lectures"
    if any(w in text for w in ["concert", "music", "performance"]):
        return "music_performance"
    if any(w in text for w in ["film", "screening"]):
        return "film_screening"

    return DEFAULT_CATEGORY


def _extract_description(card: Tag) -> str | None:
    """Extract description snippet from the card.

    Looks for paragraph text or description divs, skipping short
    navigational text and date strings.
    """
    # Try explicit description/excerpt containers
    for cls in ["excerpt", "description", "summary", "entry-content",
                "event-description", "card-text"]:
        desc_el = card.find(class_=re.compile(cls, re.I))
        if desc_el:
            text = desc_el.get_text(strip=True)
            if len(text) > 20:
                return text[:1000] if len(text) > 1000 else text

    # Try paragraphs
    for p in card.find_all("p"):
        text = p.get_text(strip=True)
        if len(text) > 30 and not re.match(
            r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s", text
        ):
            return text[:1000] if len(text) > 1000 else text

    return None


def _extract_badges(card: Tag) -> list[str]:
    """Extract event type badges like IN PERSON, ONLINE, RSVP."""
    badges: list[str] = []
    badge_keywords = {"in person", "online", "rsvp", "virtual", "hybrid"}

    # Look for badge-like spans/divs with short text
    for el in card.find_all(["span", "div", "a"], class_=re.compile(
        r"badge|tag|label|type|format", re.I
    )):
        text = el.get_text(strip=True).lower()
        if text in badge_keywords:
            badges.append(text.replace(" ", "_"))

    # Also check for text nodes matching badge patterns
    card_text = card.get_text(" ", strip=True).upper()
    for keyword in ["IN PERSON", "ONLINE", "RSVP"]:
        if keyword in card_text and keyword.lower().replace(" ", "_") not in badges:
            badges.append(keyword.lower().replace(" ", "_"))

    return badges


def _extract_tags(card: Tag, title: str, description: str) -> list[str]:
    """Extract tags from category classes and content signals."""
    tags: list[str] = []

    # Tags from CSS classes
    classes: list[str] = []
    if card.get("class"):
        classes.extend(card["class"])
    parent_li = card.find_parent("li")
    if parent_li and parent_li.get("class"):
        classes.extend(parent_li["class"])

    for cls in classes:
        if cls.lower().startswith("category-"):
            tag = cls.lower().replace("category-", "")
            if tag and tag not in tags:
                tags.append(tag)

    # Tags from content signals
    combined = f"{title} {description}".lower()
    content_signals = {
        "exhibition": ["exhibition", "exhibit"],
        "african": ["africa", "african"],
        "asian": ["asia", "asian"],
        "latin_american": ["latin america", "latino", "latina", "chicano"],
        "indigenous": ["indigenous", "native"],
        "textile": ["textile", "fabric", "weaving"],
        "ceramic": ["ceramic", "pottery"],
        "photography": ["photograph", "photo"],
        "sculpture": ["sculpture"],
        "contemporary": ["contemporary"],
        "traditional": ["traditional"],
    }

    for tag, keywords in content_signals.items():
        if any(kw in combined for kw in keywords) and tag not in tags:
            tags.append(tag)

    return tags


def _safe_text(tag: Tag) -> str:
    """Get truncated text from a tag for error reporting."""
    text = tag.get_text(strip=True)
    return text[:200] if len(text) > 200 else text
