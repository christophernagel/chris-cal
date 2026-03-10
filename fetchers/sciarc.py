"""
SCI-Arc events fetcher.

Source: https://www.sciarc.edu/events
Tier: regular

Scrapes the events listing page for linked cards with event info.
Events are grouped by category sections (Lectures, Exhibitions, General Events).

Dedup key: source_url (stable per-event URLs on sciarc.edu).

Classification: talks_lectures for lectures, visual_art for exhibitions,
talks_lectures as default. SCI-Arc is an architecture school — events are
typically free public lectures, exhibitions, and workshops.

Structure (as of March 2026):
  - Linked cards (anchor tags) with thumbnails from Google Cloud Storage
    (storage.googleapis.com/sci-arc/images/_thumbnail/)
  - Date format: "March 11, 2026" as plain text
  - Titles like "Mette Ramsgaard Thomsen: Resilient Landscapes"
  - Events grouped by category: Lectures, General Events, Exhibitions
  - "View All" links for category-specific pages
  - Individual event pages on sciarc.edu domain
  - No pagination on main events page
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "sciarc"
BASE_URL = "https://www.sciarc.edu"
EVENTS_URL = f"{BASE_URL}/events"
LA_TZ = ZoneInfo("America/Los_Angeles")

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}

# Category mapping from section headings to chriscal categories
CATEGORY_MAP = {
    "lecture": "talks_lectures",
    "lectures": "talks_lectures",
    "exhibition": "visual_art",
    "exhibitions": "visual_art",
    "general events": "talks_lectures",
    "general": "talks_lectures",
    "workshop": "talks_lectures",
    "workshops": "talks_lectures",
}

# Tag extraction from section headings
TAG_MAP = {
    "lecture": "lecture",
    "lectures": "lecture",
    "exhibition": "exhibition",
    "exhibitions": "exhibition",
    "workshop": "workshop",
    "workshops": "workshop",
    "general events": "architecture",
    "general": "architecture",
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse SCI-Arc events. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    warnings: list[ParseWarning] = []
    events: list[EventData] = []
    events_found = 0

    try:
        # Fetch listing page
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
                error_message="No events found on events page. Structure may have changed.",
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
                        message="Could not build event: missing title or date",
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

def _extract_events(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Extract event data from the events page.

    Walks through sections identified by headings (Lectures, Exhibitions,
    General Events) and finds linked cards within each section. Each card
    is an anchor tag containing a title, date, and thumbnail.

    Returns a list of dicts with keys: title, date_text, href, section, category, tags.
    """
    results: list[dict[str, str]] = []

    # Strategy 1: Find section headings and extract events grouped by category
    # Look for headings that indicate event categories
    headings = soup.find_all(re.compile(r"^h[1-4]$"))

    sections_found: list[tuple[Tag, str]] = []
    for heading in headings:
        heading_text = heading.get_text(strip=True).lower()
        if any(kw in heading_text for kw in ("lecture", "exhibition", "general event", "workshop")):
            sections_found.append((heading, heading_text))

    if sections_found:
        for heading_tag, heading_text in sections_found:
            section_name = heading_text.strip()
            category = _category_from_section(section_name)
            tags = _tags_from_section(section_name)

            # Find the container: walk up to the parent section/div
            container = heading_tag.parent
            if container is None:
                container = heading_tag

            # Find all anchor tags in this container that look like event links
            links = container.find_all("a", href=True)
            for link in links:
                event = _parse_event_link(link, section_name, category, tags)
                if event is not None:
                    results.append(event)

    # Strategy 2: If no sections found, fall back to finding all event-like links
    if not results:
        all_links = soup.find_all("a", href=True)
        for link in all_links:
            event = _parse_event_link(link, "general", "talks_lectures", ["architecture"])
            if event is not None:
                results.append(event)

    # Deduplicate by href (same event may appear in multiple containers)
    seen_hrefs: set[str] = set()
    deduped: list[dict[str, str]] = []
    for event in results:
        href = event["href"]
        if href not in seen_hrefs:
            seen_hrefs.add(href)
            deduped.append(event)

    return deduped


def _parse_event_link(
    link: Tag,
    section_name: str,
    category: str,
    tags: list[str],
) -> dict[str, str] | None:
    """Parse a single anchor tag into event data, or return None if not an event link.

    Filters out navigation links, "View All" links, and non-event anchors.
    """
    href = link.get("href", "")
    if not href or not isinstance(href, str):
        return None

    # Skip "View All" links, navigation, and anchors
    link_text = link.get_text(strip=True)
    if not link_text:
        return None
    if link_text.lower() in ("view all", "see all", "more", "back"):
        return None
    if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
        return None

    # Must be an internal link or a sciarc.edu link that looks like an event
    if href.startswith("/"):
        full_url = f"{BASE_URL}{href}"
    elif href.startswith(BASE_URL):
        full_url = href
    else:
        return None

    # Skip links that are clearly not event pages (e.g., /events itself, /about, etc.)
    if href.rstrip("/") in ("/events", "/", "/about", "/academics", "/admissions"):
        return None

    # Extract title and date from the link content
    title, date_text = _extract_title_and_date(link)
    if not title:
        return None

    return {
        "title": title,
        "date_text": date_text or "",
        "href": full_url,
        "section": section_name,
        "category": category,
        "tags": ",".join(tags),
    }


def _extract_title_and_date(link: Tag) -> tuple[str | None, str | None]:
    """Extract the event title and date text from a linked card element.

    The link may contain nested elements with the title and date, or may
    have the text directly. Date is identified by the pattern "Month DD, YYYY".
    """
    full_text = link.get_text(separator="\n", strip=True)
    if not full_text:
        return None, None

    lines = [line.strip() for line in full_text.split("\n") if line.strip()]
    if not lines:
        return None, None

    title = None
    date_text = None

    for line in lines:
        if _is_date_string(line):
            date_text = line
        elif title is None and len(line) > 3:
            # First non-date line with substance is the title
            title = line

    return title, date_text


def _is_date_string(text: str) -> bool:
    """Check if text looks like a date in 'Month DD, YYYY' format."""
    return bool(re.match(
        r"^(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},?\s*\d{4}$",
        text.strip(),
    ))


def _category_from_section(section_name: str) -> str:
    """Map section heading to chriscal category."""
    section_lower = section_name.lower().strip()
    for key, cat in CATEGORY_MAP.items():
        if key in section_lower:
            return cat
    return "talks_lectures"


def _tags_from_section(section_name: str) -> list[str]:
    """Extract tags from section heading."""
    section_lower = section_name.lower().strip()
    tags = ["architecture"]  # all SCI-Arc events get this tag
    for key, tag in TAG_MAP.items():
        if key in section_lower:
            if tag not in tags:
                tags.append(tag)
            break
    return tags


# ============================================================
# Event building
# ============================================================

def _build_event(raw: dict[str, str]) -> EventData | None:
    """Build an EventData from raw extracted data."""
    title = raw.get("title")
    href = raw.get("href", "")
    date_text = raw.get("date_text", "")
    category = raw.get("category", "talks_lectures")
    tags = [t for t in raw.get("tags", "").split(",") if t]

    if not title or not href:
        return None

    # Parse date
    start_at = _parse_date(date_text)
    if start_at is None:
        return None

    # Add content-based tags
    tags = _enrich_tags(tags, title)

    return EventData(
        title=title,
        source_url=href,
        start_at=start_at,
        venue_name="SCI-Arc",
        venue_address="960 E 3rd St, Los Angeles, CA 90013",
        category=category,
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=None,
        raw_source={
            "source_url": href,
            "date_text": date_text,
            "section": raw.get("section", ""),
        },
    )


def _enrich_tags(tags: list[str], title: str) -> list[str]:
    """Add content-based tags from event title."""
    title_lower = title.lower()

    tag_signals = {
        "lecture": ["lecture", "talk", "keynote", "symposium"],
        "exhibition": ["exhibition", "exhibit", "gallery", "show"],
        "workshop": ["workshop", "studio", "seminar"],
        "architecture": ["architecture", "design", "building", "landscape"],
    }

    for tag, keywords in tag_signals.items():
        if tag not in tags and any(kw in title_lower for kw in keywords):
            tags.append(tag)

    return tags


# ============================================================
# Date parsing
# ============================================================

def _parse_date(text: str) -> datetime | None:
    """Parse SCI-Arc date strings into timezone-aware datetimes.

    Format: "March 11, 2026"

    Since SCI-Arc event listings don't include a time, we default to
    19:00 (7 PM) as most architecture school events are evening lectures.
    All times are America/Los_Angeles.
    """
    if not text:
        return None

    text = text.strip()

    # Try "Month DD, YYYY" format
    match = re.match(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2}),?\s*(\d{4})",
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
        # Default to 19:00 (7 PM) for evening events
        dt = datetime(year, month, day, 19, 0, tzinfo=LA_TZ)
    except ValueError:
        return None

    return dt
