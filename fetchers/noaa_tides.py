"""
NOAA Tides & Currents data fetcher.

Source: NOAA CO-OPS Tides & Currents API
Station: 9410660 (Santa Monica, CA)
Tier: data_integration

This is a data integration fetcher, NOT an event fetcher. It returns
TideFetchResult (not FetchResult) containing high/low tide predictions
for the Santa Monica station.

The NOAA API limits requests to 31 days per call, so longer ranges are
split into monthly chunks and fetched sequentially with a polite delay.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, date
from zoneinfo import ZoneInfo

import httpx

# ============================================================
# Constants
# ============================================================

SOURCE_NAME = "noaa_tides"
STATION_ID = "9410660"
BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
LA_TZ = ZoneInfo("America/Los_Angeles")

# NOAA API limits requests to 31 days; we chunk by calendar month
_CHUNK_DELAY_S = 0.5


# ============================================================
# Data types
# ============================================================

@dataclass(frozen=True)
class TidePrediction:
    timestamp: datetime  # timezone-aware, America/Los_Angeles
    height_ft: float     # height in feet above MLLW
    tide_type: str       # "H" (high) or "L" (low)


@dataclass(frozen=True)
class TideFetchResult:
    source_name: str = SOURCE_NAME
    station_id: str = STATION_ID
    station_name: str = "Santa Monica"
    predictions: list[TidePrediction] = field(default_factory=list)
    outcome: str = "success"       # "success", "error", "timeout"
    error_message: str | None = None
    begin_date: str | None = None  # YYYYMMDD
    end_date: str | None = None    # YYYYMMDD


# ============================================================
# Internal helpers
# ============================================================

def _month_chunks(begin: str, end: str) -> list[tuple[str, str]]:
    """Split a YYYYMMDD range into calendar-month chunks.

    Each chunk starts on the 1st (or the original begin_date for the first
    chunk) and ends on the last day of the month (or the original end_date
    for the final chunk).
    """
    from calendar import monthrange

    b = datetime.strptime(begin, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()

    chunks: list[tuple[str, str]] = []
    cursor = b
    while cursor <= e:
        _, last_day = monthrange(cursor.year, cursor.month)
        chunk_end = date(cursor.year, cursor.month, last_day)
        if chunk_end > e:
            chunk_end = e
        chunks.append((cursor.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        # advance to 1st of next month
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    return chunks


def _parse_prediction(raw: dict) -> TidePrediction:
    """Parse a single NOAA prediction dict into a TidePrediction."""
    naive = datetime.strptime(raw["t"], "%Y-%m-%d %H:%M")
    aware = naive.replace(tzinfo=LA_TZ)
    return TidePrediction(
        timestamp=aware,
        height_ft=float(raw["v"]),
        tide_type=raw["type"],
    )


async def _fetch_chunk(
    begin: str,
    end: str,
    client: httpx.AsyncClient,
) -> list[TidePrediction]:
    """Fetch a single <= 31-day chunk from the NOAA API."""
    params = {
        "product": "predictions",
        "station": STATION_ID,
        "begin_date": begin,
        "end_date": end,
        "datum": "MLLW",
        "time_zone": "lst_ldt",
        "interval": "hilo",
        "units": "english",
        "application": "chriscal",
        "format": "json",
    }
    resp = await client.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    if "predictions" not in data:
        # NOAA returns an error object instead of predictions on bad requests
        error_msg = data.get("error", {}).get("message", str(data))
        raise ValueError(f"NOAA API error: {error_msg}")

    return [_parse_prediction(p) for p in data["predictions"]]


# ============================================================
# Public API
# ============================================================

async def fetch(
    begin_date: str | None = None,
    end_date: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> TideFetchResult:
    """Fetch NOAA tide predictions for Santa Monica station 9410660.

    Args:
        begin_date: Start date as YYYYMMDD. Defaults to Jan 1 of current year.
        end_date: End date as YYYYMMDD. Defaults to Dec 31 of current year.
        client: Optional httpx.AsyncClient to reuse.

    Returns:
        TideFetchResult with predictions or error information.
    """
    today = date.today()
    if begin_date is None:
        begin_date = f"{today.year}0101"
    if end_date is None:
        end_date = f"{today.year}1231"

    owns_client = client is None

    try:
        if owns_client:
            client = httpx.AsyncClient(timeout=30.0)

        chunks = _month_chunks(begin_date, end_date)
        all_predictions: list[TidePrediction] = []

        for i, (chunk_begin, chunk_end) in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(_CHUNK_DELAY_S)
            preds = await _fetch_chunk(chunk_begin, chunk_end, client)
            all_predictions.extend(preds)

        return TideFetchResult(
            predictions=all_predictions,
            outcome="success",
            begin_date=begin_date,
            end_date=end_date,
        )

    except httpx.TimeoutException as exc:
        return TideFetchResult(
            outcome="timeout",
            error_message=str(exc),
            begin_date=begin_date,
            end_date=end_date,
        )
    except Exception as exc:
        return TideFetchResult(
            outcome="error",
            error_message=str(exc),
            begin_date=begin_date,
            end_date=end_date,
        )
    finally:
        if owns_client and client is not None:
            await client.aclose()


async def fetch_quarter(
    quarter: int | None = None,
    client: httpx.AsyncClient | None = None,
) -> TideFetchResult:
    """Fetch tide predictions for a full quarter.

    Args:
        quarter: 1-4 (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec).
                 Defaults to the current quarter.
        client: Optional httpx.AsyncClient to reuse.

    Returns:
        TideFetchResult for the requested quarter.
    """
    today = date.today()
    if quarter is None:
        quarter = (today.month - 1) // 3 + 1

    if quarter not in (1, 2, 3, 4):
        return TideFetchResult(
            outcome="error",
            error_message=f"Invalid quarter: {quarter}. Must be 1-4.",
        )

    year = today.year
    quarter_ranges = {
        1: (f"{year}0101", f"{year}0331"),
        2: (f"{year}0401", f"{year}0630"),
        3: (f"{year}0701", f"{year}0930"),
        4: (f"{year}1001", f"{year}1231"),
    }
    begin, end = quarter_ranges[quarter]
    return await fetch(begin_date=begin, end_date=end, client=client)
