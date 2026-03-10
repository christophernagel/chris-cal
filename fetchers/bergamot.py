"""
Bergamot Station events fetcher.

Source: https://www.bergamotstation.com/exhibitions
Tier: regular (12h fetch)

Squarespace site hosting gallery events at Bergamot Station arts center.
The exhibitions page uses Squarespace's eventlist-event articles with
<time> tags for start/end dates.

Dedup key: source_url (per-event detail page URL).

Classification: visual_art for exhibitions/galleries, music_performance
for live shows. Default is visual_art — Bergamot Station is primarily
a gallery campus.

Requires browser-like headers to avoid Squarespace bot blocking.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning

SOURCE_NAME = "bergamot"
BASE_URL = "https://www.bergamotstation.com"
EVENTS_URL = f"{BASE_URL}/exhibitions"
LA_TZ = ZoneInfo("America/Los_Angeles")

VENUE_NAME = "Bergamot Station"
VENUE_ADDRESS = "2525 Michigan Ave, Santa Monica, CA 90404"

# Browser-like headers required to avoid Squarespace blocking
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.google.com/",
}


async def fetch(client: httpx.AsyncClient | None = None) -> FetchResult:
    """Fetch and parse Bergamot Station events. Injectable client for testing."""
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
        raw_events = _extract_events_html(soup)
        events_found = len(raw_events)

        if events_found == 0:
            return FetchResult(
                source_name=SOURCE_NAME,
                outcome=FetchOutcome.PARTIAL,
                events_found=0,
                error_message="No eventlist-event articles found. Structure may have changed.",
            )

        for i, raw in enumerate(raw_events):
            try:
                event = _parse_html_event(raw)
                if event is not None:
                    events.append(event)
            except Exception as e:
                warnings.append(ParseWarning(
                    event_index=i,
                    field="html_event",
                    message=f"Failed to parse HTML event: {e}",
                    raw_value=str(raw.get("title", ""))[:200],
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



def _parse_sqs_datetime(value: Any) -> datetime | None:
    """Parse a Squarespace datetime value (epoch millis or ISO string)."""
    if value is None:
        return None

    # Epoch milliseconds (integer or numeric string)
    if isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(value / 1000, tz=LA_TZ)
            return dt
        except (ValueError, OSError):
            return None

    if isinstance(value, str):
        # Try epoch millis as string
        if value.isdigit():
            try:
                return datetime.fromtimestamp(int(value) / 1000, tz=LA_TZ)
            except (ValueError, OSError):
                pass

        # Try ISO format variants
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=LA_TZ)
                return dt
            except ValueError:
                continue

    return None


# ============================================================
# HTML scraping fallback
# ============================================================

def _extract_events_html(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Extract event data from Squarespace HTML event listing page.

    Looks for common Squarespace event structures:
    - .eventlist-event containers
    - .sqs-block-content with event data
    - article elements with event content
    """
    results: list[dict[str, Any]] = []

    # Strategy 1: Squarespace eventlist items
    event_items = soup.find_all("article", class_=re.compile(r"eventlist-event"))
    if not event_items:
        event_items = soup.find_all("div", class_=re.compile(r"eventlist-event"))
    if not event_items:
        # Strategy 2: generic Squarespace content blocks
        event_items = soup.find_all("article", class_=re.compile(r"(event|hentry)"))
    if not event_items:
        # Strategy 3: look for any structured event-like containers
        event_items = soup.find_all(
            "div", class_=re.compile(r"(summary-item|eventlist)")
        )

    for item in event_items:
        data: dict[str, Any] = {"element": item}

        # URL: from thumbnail link or title link
        thumb_link = item.find("a", class_=re.compile(r"eventlist-column-thumbnail"))
        any_link = thumb_link or item.find("a", href=True)
        if any_link and any_link.get("href"):
            href = any_link["href"]
            if href.startswith("/"):
                data["url"] = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                data["url"] = href

        # Title: look in heading tags or title-specific classes
        title_el = (
            item.find(class_=re.compile(r"eventlist-title"))
            or item.find(class_=re.compile(r"summary-title"))
            or item.find(re.compile(r"^h[1-3]$"))
        )
        if title_el:
            data["title"] = title_el.get_text(strip=True)

        # Date: Squarespace eventlist uses 6 <time> tags per event:
        #   [0] start date text, [1] start time AM/PM, [2] start time 24h,
        #   [3] end date text, [4] end time AM/PM, [5] end time 24h
        time_tags = item.find_all("time")
        if time_tags:
            data["datetime_attr"] = time_tags[0].get("datetime")
            data["date_text"] = time_tags[0].get_text(strip=True)
        if len(time_tags) >= 2:
            data["start_time_text"] = time_tags[1].get_text(strip=True)
        if len(time_tags) >= 4:
            data["end_datetime_attr"] = time_tags[3].get("datetime")
        if not time_tags:
            date_el = item.find(class_=re.compile(r"(eventlist-datetag|event-date|date)"))
            if date_el:
                data["date_text"] = date_el.get_text(strip=True)

        # Location — extract first address line (gallery/venue name) only,
        # not the full multi-line street address
        loc_el = item.find(class_=re.compile(r"eventlist-meta-address"))
        if loc_el:
            first_line = loc_el.find(class_="eventlist-meta-address-line")
            if first_line:
                loc_text = first_line.get_text(strip=True)
                # If it looks like a street address, skip it (use default venue)
                if not re.match(r"^\d+\s", loc_text):
                    data["location"] = loc_text
            else:
                data["location"] = loc_el.get_text(strip=True)

        # Description / excerpt
        desc_el = (
            item.find(class_=re.compile(r"(eventlist-description|summary-excerpt)"))
            or item.find("p")
        )
        if desc_el:
            data["description"] = desc_el.get_text(strip=True)[:1000]

        if data.get("title"):
            results.append(data)

    return results


def _parse_html_event(raw: dict[str, Any]) -> EventData | None:
    """Convert extracted HTML data dict into an EventData."""
    title = raw.get("title", "").strip()
    if not title:
        return None

    source_url = raw.get("url", EVENTS_URL)

    # Parse start date from datetime attr or text
    start_at = None
    end_at = None

    dt_attr = raw.get("datetime_attr")
    if dt_attr:
        start_at = _parse_sqs_datetime(dt_attr)

    if start_at is None:
        date_text = raw.get("date_text", "")
        start_at = _parse_date_text(date_text)

    if start_at is None:
        return None

    # Combine date with start time if available (e.g., "11:00 AM")
    time_text = raw.get("start_time_text", "")
    if time_text:
        time_match = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)", time_text, re.IGNORECASE)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            period = time_match.group(3).upper()
            if period == "PM" and hour != 12:
                hour += 12
            elif period == "AM" and hour == 12:
                hour = 0
            start_at = start_at.replace(hour=hour, minute=minute)

    end_dt_attr = raw.get("end_datetime_attr")
    if end_dt_attr:
        end_at = _parse_sqs_datetime(end_dt_attr)

    description = raw.get("description")
    location = raw.get("location", "")
    category = _classify_category(title, description or "")
    tags = _extract_tags(title, description or "")

    # Use gallery name from location if available, append "@ Bergamot Station"
    venue_name = VENUE_NAME
    if location:
        # Strip "(map)" suffix and suite numbers
        clean_loc = re.sub(r"\(map\)", "", location).strip()
        if clean_loc and clean_loc != VENUE_NAME:
            venue_name = clean_loc

    return EventData(
        title=title,
        source_url=source_url,
        start_at=start_at,
        end_at=end_at,
        venue_name=venue_name,
        venue_address=VENUE_ADDRESS,
        category=category,
        tags=tags,
        is_free=True,
        is_one_off=True,
        description=description,
        raw_source={"html_title": title, "url": source_url},
    )


def _parse_date_text(text: str) -> datetime | None:
    """Parse human-readable date text into a timezone-aware datetime.

    Common Squarespace formats:
      'March 15, 2026'
      'Mar 15, 2026 at 6:00 PM'
      'Saturday, March 15, 2026'
    """
    if not text:
        return None

    # Strip day-of-week prefix
    text = re.sub(
        r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
        "", text.strip(), flags=re.IGNORECASE,
    )

    for fmt in (
        "%B %d, %Y at %I:%M %p",
        "%B %d, %Y, %I:%M %p",
        "%b %d, %Y at %I:%M %p",
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y",
    ):
        try:
            dt = datetime.strptime(text.strip(), fmt)
            return dt.replace(tzinfo=LA_TZ)
        except ValueError:
            continue

    return None


# ============================================================
# Classification helpers
# ============================================================

def _classify_category(title: str, description: str) -> str:
    """Classify event category based on content signals."""
    combined = f"{title} {description}".lower()

    music_signals = [
        "concert", "live music", "live show", "performance", "dj",
        "band", "singer", "musician",
    ]
    if any(signal in combined for signal in music_signals):
        return "music_performance"

    # Default for Bergamot Station: visual art
    return "visual_art"


def _extract_tags(title: str, description: str) -> list[str]:
    """Extract tags from content signals."""
    tags: list[str] = []
    combined = f"{title} {description}".lower()

    tag_signals = {
        "art": ["art ", "arts ", "artist"],
        "gallery": ["gallery", "galleries"],
        "exhibition": ["exhibition", "exhibit "],
        "opening": ["opening", "reception"],
        "sculpture": ["sculpture", "sculpt"],
        "photography": ["photography", "photograph", "photo exhibit"],
        "painting": ["painting", "painter"],
        "contemporary": ["contemporary"],
        "installation": ["installation"],
    }

    for tag, keywords in tag_signals.items():
        if any(kw in combined for kw in keywords):
            tags.append(tag)

    return tags
