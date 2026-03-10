"""
Petersen Automotive Museum fetcher.

Source: https://petersen.org/events
Tier: regular

Scrapes the events listing page for event cards. The site is built on
Squarespace with dates displayed as split blocks (month abbreviation
and day number in separate divs), category tags as query params, and
Shopify-embedded ticket buttons.

Dedup key: source_url (stable per-event URLs).

Classification: category mapped from Petersen's own category tags:
  - "Education" -> talks_lectures
  - "Cruise-In" -> festival_outdoor
  - "Gala" -> festival_outdoor
  - default -> visual_art

Structure (as of March 2026):
  - Listing page: event cards with split date blocks (month + day divs),
    title headings within anchor links, category tags as query params
  - Squarespace CMS
  - Shopify ticket embeds
  - Google Calendar and ICS export links
  - Physical address with Google Maps link
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "petersen"
BASE_URL = "https://petersen.org"
EVENTS_URL = "https://petersen.org/events"
LA_TZ = ZoneInfo("America/Los_Angeles")

VENUE_NAME = "Petersen Automotive Museum"
VENUE_ADDRESS = "6060 Wilshire Blvd, Los Angeles, CA 90036"

# Category mapping from Petersen's category tags
CATEGORY_MAP = {
    "education": "talks_lectures",
    "cruise-in": "festival_outdoor",
    "gala": "festival_outdoor",
}

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": "chriscal/1.0 (personal calendar aggregator; contact: chris@localhost)"
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Petersen Automotive Museum events. Injectable client for testing."""
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
                error_message="No event cards found on events page. Structure may have changed.",
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
    """Extract event data from the Petersen events page.

    Squarespace event cards have:
    - Split date blocks: month abbreviation + day number in separate divs
    - Title in heading tags within anchor links
    - Category tags as query params (?category=Education)
    - Ticket buttons (Shopify embeds)

    Returns list of dicts with keys: title, url, month, day, categories,
    description, time_text.
    """
    results = []

    # Squarespace events are typically in summary blocks or event list items
    # Look for event items by finding containers with date + title patterns
    event_items = soup.find_all("div", class_=re.compile(r"(eventlist-event|summary-item|event-item)"))

    if not event_items:
        # Fallback: look for article tags (common Squarespace pattern)
        event_items = soup.find_all("article", class_=re.compile(r"(eventlist|event)"))

    if not event_items:
        # Broader fallback: look for any container with split date blocks
        # Squarespace often uses <div class="eventlist"> as wrapper
        event_list = soup.find("div", class_=re.compile(r"eventlist"))
        if event_list:
            event_items = event_list.find_all(["article", "div"], recursive=False)

    if not event_items:
        # Last resort: find all links that look like event detail pages
        event_items = _find_event_containers_by_links(soup)

    for item in event_items:
        card_data = _parse_single_card(item)
        if card_data and card_data.get("title"):
            results.append(card_data)

    return results


def _find_event_containers_by_links(soup: BeautifulSoup) -> list[Tag]:
    """Fallback: find event containers by looking for links to event detail pages."""
    containers = []
    seen_urls = set()

    for link in soup.find_all("a", href=re.compile(r"/events/")):
        href = link.get("href", "")
        # Skip category filter links
        if "?category=" in href and href.count("/") < 3:
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)

        # Walk up to find the enclosing container
        parent = link.parent
        while parent and parent.name not in ("article", "section", "body"):
            if parent.name == "div" and parent.get("class"):
                break
            parent = parent.parent

        if parent and parent.name != "body":
            containers.append(parent)
        else:
            containers.append(link)

    return containers


def _parse_single_card(item: Tag) -> dict[str, str] | None:
    """Parse a single event card element into a data dict."""
    card_data: dict[str, str] = {}

    # Find event link
    link = item.find("a", href=True)
    if link:
        href = link.get("href", "")
        if href.startswith("/"):
            card_data["url"] = f"{BASE_URL}{href}"
        elif href.startswith("http"):
            card_data["url"] = href

    # If the item itself is an anchor
    if not card_data.get("url") and isinstance(item, Tag) and item.name == "a":
        href = item.get("href", "")
        if href:
            if href.startswith("/"):
                card_data["url"] = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                card_data["url"] = href

    if not card_data.get("url"):
        return None

    # Title: find heading tag
    title_el = item.find(re.compile(r"h[1-6]"))
    if title_el:
        card_data["title"] = title_el.get_text(strip=True)
    elif link:
        # Fallback: use link text if it looks like a title
        link_text = link.get_text(strip=True)
        if len(link_text) > 5:
            card_data["title"] = link_text

    # Date: look for split month/day blocks
    month, day = _extract_split_date(item)
    if month:
        card_data["month"] = month
    if day:
        card_data["day"] = day

    # Fallback date: look for date in Squarespace time tags
    if not month or not day:
        time_tag = item.find("time")
        if time_tag:
            dt_str = time_tag.get("datetime", "")
            if dt_str:
                card_data["datetime_iso"] = dt_str
            else:
                time_text = time_tag.get_text(strip=True)
                if time_text:
                    card_data["date_text"] = time_text

    # Time info
    time_text = _extract_time_text(item)
    if time_text:
        card_data["time_text"] = time_text

    # Categories: extract from links with ?category= params
    categories = _extract_categories(item)
    if categories:
        card_data["categories"] = ",".join(categories)

    # Description
    desc_el = item.find("div", class_=re.compile(r"(excerpt|summary|description|eventlist-description)"))
    if desc_el:
        card_data["description"] = desc_el.get_text(strip=True)[:1000]
    else:
        for p in item.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 30:
                card_data["description"] = text[:1000]
                break

    # Ticket URL: look for Shopify or external ticket links
    ticket_link = item.find("a", href=re.compile(r"(shopify|ticket|buy|register)", re.I))
    if ticket_link:
        card_data["ticket_url"] = ticket_link.get("href", "")

    return card_data


def _extract_split_date(item: Tag) -> tuple[str | None, str | None]:
    """Extract month and day from Squarespace split date blocks.

    Squarespace displays dates as separate divs:
    <div class="eventlist-datetag-startdate--month">Mar</div>
    <div class="eventlist-datetag-startdate--day">11</div>
    """
    month = None
    day = None

    # Look for Squarespace date tag elements
    month_el = item.find(class_=re.compile(r"(month|startdate.*month)"))
    day_el = item.find(class_=re.compile(r"(day|startdate.*day)"))

    if month_el:
        month = month_el.get_text(strip=True)
    if day_el:
        day = day_el.get_text(strip=True)

    if month and day:
        return month, day

    # Fallback: look for date-tag container with two child divs
    date_tag = item.find(class_=re.compile(r"(datetag|date-tag)"))
    if date_tag:
        children = date_tag.find_all(["div", "span"], recursive=False)
        if len(children) >= 2:
            month = children[0].get_text(strip=True)
            day = children[1].get_text(strip=True)
            return month, day

    return month, day


def _extract_time_text(item: Tag) -> str | None:
    """Extract time information from event card."""
    # Look for time-related elements
    time_el = item.find(class_=re.compile(r"(time|eventlist-meta-time)"))
    if time_el:
        return time_el.get_text(strip=True)

    # Look for text patterns like "7:00 PM" or "10:00 AM - 2:00 PM"
    all_text = item.get_text()
    match = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)(?:\s*[-–]\s*\d{1,2}:\d{2}\s*(?:AM|PM|am|pm))?)", all_text)
    if match:
        return match.group(1)

    return None


def _extract_categories(item: Tag) -> list[str]:
    """Extract category names from links with ?category= query params."""
    categories = []
    for link in item.find_all("a", href=re.compile(r"\?category=")):
        href = link.get("href", "")
        match = re.search(r"\?category=([^&]+)", href)
        if match:
            cat = match.group(1).replace("-", " ").replace("+", " ").strip()
            categories.append(cat)
    return categories


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
    start_at = _parse_date(card_data)
    if start_at is None:
        return None

    # Parse time if available
    time_text = card_data.get("time_text")
    if time_text:
        start_at = _apply_time(start_at, time_text)

    # Category mapping
    categories_str = card_data.get("categories", "")
    category = _map_category(categories_str)

    # Tags from category params
    tags = _extract_tags(categories_str, title, card_data.get("description", ""))

    # Free/paid detection
    is_free = _detect_free(title, card_data.get("description", ""))

    # Description
    description = card_data.get("description")

    # Ticket URL
    ticket_url = card_data.get("ticket_url")

    return EventData(
        title=title,
        source_url=url,
        start_at=start_at,
        venue_name=VENUE_NAME,
        venue_address=VENUE_ADDRESS,
        category=category,
        tags=tags,
        is_free=is_free,
        ticket_url=ticket_url,
        is_one_off=True,
        description=description,
        raw_source={
            "card_data": card_data,
        },
    )


# ============================================================
# Date parsing
# ============================================================

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _parse_date(card_data: dict[str, str]) -> datetime | None:
    """Parse date from card data. Tries split month/day, then ISO, then text.

    Uses current year for split month/day dates. Defaults to 10:00 AM
    as Petersen events are typically daytime museum events.
    """
    # Try ISO datetime first
    iso_str = card_data.get("datetime_iso")
    if iso_str:
        try:
            dt = datetime.fromisoformat(iso_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LA_TZ)
            return dt
        except ValueError:
            pass

    # Try split month/day
    month_str = card_data.get("month", "").strip().lower()
    day_str = card_data.get("day", "").strip()

    if month_str and day_str:
        month = MONTH_MAP.get(month_str[:3])
        if month is None:
            return None
        try:
            day = int(day_str)
        except ValueError:
            return None

        now = datetime.now(LA_TZ)
        year = now.year

        try:
            # Default to 10:00 AM for museum events
            dt = datetime(year, month, day, 10, 0, tzinfo=LA_TZ)
        except ValueError:
            return None

        # If the date is more than 2 months in the past, assume next year
        if (now.replace(tzinfo=None) - dt.replace(tzinfo=None)).days > 60:
            dt = dt.replace(year=year + 1)

        return dt

    # Try date text fallback
    date_text = card_data.get("date_text", "")
    if date_text:
        match = re.search(
            r"([A-Za-z]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?",
            date_text,
        )
        if match:
            month = MONTH_MAP.get(match.group(1).lower()[:3])
            if month:
                day = int(match.group(2))
                year = int(match.group(3)) if match.group(3) else datetime.now(LA_TZ).year
                try:
                    return datetime(year, month, day, 10, 0, tzinfo=LA_TZ)
                except ValueError:
                    pass

    return None


def _apply_time(dt: datetime, time_text: str) -> datetime:
    """Apply parsed time to an existing datetime.

    Handles formats like '7:00 PM', '10:00 AM - 2:00 PM'.
    Uses the start time from a range.
    """
    # Extract first time from the string
    match = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)", time_text)
    if not match:
        return dt

    hour = int(match.group(1))
    minute = int(match.group(2))
    period = match.group(3).upper()

    if period == "PM" and hour != 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0

    return dt.replace(hour=hour, minute=minute)


# ============================================================
# Classification
# ============================================================

def _map_category(categories_str: str) -> str:
    """Map Petersen category tags to chriscal event categories."""
    if not categories_str:
        return "visual_art"

    categories_lower = categories_str.lower()
    for petersen_cat, chriscal_cat in CATEGORY_MAP.items():
        if petersen_cat in categories_lower:
            return chriscal_cat

    return "visual_art"


def _detect_free(title: str, description: str) -> bool:
    """Detect if event is free. Most Petersen events require tickets."""
    combined = f"{title} {description}".lower()
    if "free" in combined:
        return True
    return False


def _extract_tags(categories_str: str, title: str, description: str) -> list[str]:
    """Extract tags from category params and content."""
    tags: list[str] = []

    # Add tags from Petersen category params
    if categories_str:
        for cat in categories_str.split(","):
            cat = cat.strip().lower().replace(" ", "_")
            if cat and cat not in tags:
                tags.append(cat)

    # Content-based tags
    combined = f"{title} {description}".lower()
    tag_signals = {
        "automotive": ["car", "cars", "automotive", "vehicle", "racing", "motorsport"],
        "classic_cars": ["classic", "vintage", "antique", "restoration"],
        "exhibition": ["exhibit", "exhibition", "display", "collection"],
        "family": ["family", "kids", "children", "youth"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords) and tag not in tags:
            tags.append(tag)

    return tags
