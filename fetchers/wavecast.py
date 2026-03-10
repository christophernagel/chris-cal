"""
Wavecast SoCal surf forecast fetcher.

Source: https://wavecast.com/socal/
Type: text forecast (NOT an event fetcher — does not return EventData)

Fetches the narrative surf forecast for Southern California from Wavecast.
The page contains written forecast sections (at a glance, current conditions,
surf forecast, weather outlook, wind outlook) embedded in HTML paragraphs.

Structure (as of March 2026):
  - Forecast sections headed by <strong> tags (e.g. "At a glance:")
  - Forecast paragraphs in <p> tags with inline font-size/line-height styling
  - No structured JSON API — pure HTML scraping
  - No pagination, single page with all forecast content
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag

SOURCE_NAME = "wavecast"
FORECAST_URL = "https://wavecast.com/socal/"

# Shared HTTP config
TIMEOUT = httpx.Timeout(15.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

# Section headings we care about (lowercased for matching)
FORECAST_SECTIONS = [
    "at a glance",
    "current conditions",
    "surf forecast",
    "weather outlook",
    "wind outlook",
]


@dataclass(frozen=True)
class ForecastResult:
    source: str = "wavecast"
    forecast_date: date = None  # type: ignore[assignment]
    forecast_text: str = ""
    outcome: str = "success"  # "success", "error", "timeout"
    error_message: str | None = None


async def fetch(client: httpx.AsyncClient | None = None) -> ForecastResult:
    """Fetch and parse Wavecast SoCal surf forecast. Injectable client for testing."""
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS)

    try:
        resp = await client.get(FORECAST_URL, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        forecast_text = _extract_forecast(soup)

        if not forecast_text or len(forecast_text.strip()) < 50:
            return ForecastResult(
                forecast_date=date.today(),
                forecast_text=forecast_text or "",
                outcome="error",
                error_message="Forecast text too short or empty. Page structure may have changed.",
            )

        return ForecastResult(
            forecast_date=date.today(),
            forecast_text=forecast_text,
            outcome="success",
        )

    except httpx.TimeoutException as e:
        return ForecastResult(
            forecast_date=date.today(),
            outcome="timeout",
            error_message=f"Timeout fetching {FORECAST_URL}: {e}",
        )
    except httpx.HTTPStatusError as e:
        return ForecastResult(
            forecast_date=date.today(),
            outcome="error",
            error_message=f"HTTP {e.response.status_code} from {FORECAST_URL}",
        )
    except Exception as e:
        return ForecastResult(
            forecast_date=date.today(),
            outcome="error",
            error_message=f"Unexpected error: {type(e).__name__}: {e}",
        )
    finally:
        if owns_client:
            await client.aclose()


def _extract_forecast(soup: BeautifulSoup) -> str:
    """Extract forecast narrative text from the page.

    Strategy:
    1. Find all <p> tags that contain forecast content (styled paragraphs
       with substantial text, or paragraphs containing section headings).
    2. Look for <strong> tags that mark section boundaries.
    3. Clean and join the text with double newlines between paragraphs.
    """
    paragraphs: list[str] = []

    # Find all <p> tags on the page
    for p in soup.find_all("p"):
        text = p.get_text(separator=" ", strip=True)

        # Skip empty or very short paragraphs (nav, footers, etc.)
        if len(text) < 15:
            continue

        # Skip paragraphs that look like navigation, copyright, or boilerplate
        text_lower = text.lower()
        if any(skip in text_lower for skip in [
            "copyright", "all rights reserved", "privacy policy",
            "terms of", "cookie", "subscribe", "sign up",
            "follow us", "facebook", "instagram", "twitter",
        ]):
            continue

        # Include paragraphs that contain forecast section headings
        is_forecast_section = any(
            section in text_lower for section in FORECAST_SECTIONS
        )

        # Include paragraphs with forecast-like content (swell, surf, wave,
        # wind, temperature, buoy, tide references)
        has_forecast_content = any(
            kw in text_lower for kw in [
                "swell", "surf", "wave", "wind", "tide", "buoy",
                "forecast", "outlook", "conditions", "temperature",
                "marine layer", "high pressure", "low pressure",
                "chest high", "head high", "waist high", "knee high",
                "overhead", "shoulder high", "ankle",
                "ft @", "ft.", "period", "direction",
                "nw ", " sw ", " se ", " ne ",
                "onshore", "offshore", "glassy",
            ]
        )

        # Include paragraphs with inline styling typical of forecast content
        style = p.get("style", "")
        has_forecast_style = "1.2rem" in style or "line-height" in style

        if is_forecast_section or has_forecast_content or has_forecast_style:
            cleaned = _clean_text(text)
            if cleaned and len(cleaned) >= 15:
                paragraphs.append(cleaned)

    return "\n\n".join(paragraphs)


def _clean_text(text: str) -> str:
    """Clean a forecast text block: normalize whitespace, strip artifacts."""
    # Normalize whitespace (collapse multiple spaces/newlines)
    text = re.sub(r"\s+", " ", text).strip()
    # Remove any stray HTML entities that survived parsing
    text = text.replace("\xa0", " ")
    # Re-collapse after entity replacement
    text = re.sub(r"  +", " ", text)
    return text
