#!/usr/bin/env python3
"""
chriscal smoke test

End-to-end pipeline validation: fetcher → ingest → DB → priority derivation.
Run manually: python smoke_test.py

Requires:
  - Postgres running with chriscal DB (schema.sql + seed.sql applied)
  - Network access to cap.ucla.edu
  - Environment variable DATABASE_URL or defaults to localhost

What it checks:
  1. Fetcher output: well-formed FetchResult, required fields, structured warnings
  2. Ingest behavior: venue resolution, dedup on second run, fetch_log written, health_score
  3. Priority derivation: calculate_auto_priority returns sensible values
  4. Setup validation: missing venues surface as NULL venue_id, DB connection errors caught
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetcher_contract import EventData, FetchOutcome, FetchResult, ParseWarning
from ingest import ingest

# ============================================================
# Config
# ============================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/chriscal"
)

# Terminal colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def ok(msg: str) -> None:
    print(f"  {GREEN}OK{RESET}  {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}!!{RESET}  {msg}")


def fail(msg: str) -> None:
    print(f"  {RED}FAIL{RESET}  {msg}")


def info(msg: str) -> None:
    print(f"  {DIM}--{RESET}  {msg}")


def header(msg: str) -> None:
    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{BOLD}{msg}{RESET}")
    print(f"{BOLD}{'=' * 60}{RESET}")


async def main() -> int:
    """Run all smoke test phases. Returns 0 on success, 1 on failure."""
    errors = 0

    # ============================================================
    # Phase 0: DB connection
    # ============================================================
    header("Phase 0: Database connection")

    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        ok(f"Connected to {DATABASE_URL}")
    except Exception as e:
        fail(f"Cannot connect to database: {e}")
        print(f"\n  Make sure Postgres is running and the chriscal DB exists.")
        print(f"  Run: createdb chriscal && psql chriscal < schema.sql && psql chriscal < seed.sql")
        return 1

    try:
        # Verify schema exists
        tables = await pool.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        table_names = {r["tablename"] for r in tables}
        required = {"sources", "venues", "venue_aliases", "events", "fetch_log"}
        missing = required - table_names
        if missing:
            fail(f"Missing tables: {missing}")
            print(f"  Run: psql chriscal < schema.sql")
            return 1
        ok(f"Schema verified: {sorted(required)}")

        # Verify source exists
        source = await pool.fetchrow("SELECT * FROM sources WHERE name = 'cap_ucla'")
        if not source:
            fail("Source 'cap_ucla' not found in sources table.")
            print(f"  Run: psql chriscal < seed.sql")
            return 1
        ok(f"Source found: {source['display_name']} (tier: {source['tier']})")

        # Check venue seeding
        venue_count = await pool.fetchval("SELECT COUNT(*) FROM venues")
        alias_count = await pool.fetchval("SELECT COUNT(*) FROM venue_aliases")
        if venue_count == 0:
            warn("No venues seeded. Venue resolution will return NULL for all events.")
            warn("Run: psql chriscal < seed.sql")
        else:
            ok(f"Venues seeded: {venue_count} venues, {alias_count} aliases")

        # ============================================================
        # Phase 1: Fetcher output
        # ============================================================
        header("Phase 1: Fetcher — CAP UCLA live fetch")

        from fetchers.cap_ucla import fetch
        result: FetchResult = await fetch()

        info(f"Outcome: {result.outcome.value}")
        info(f"Events found: {result.events_found}")
        info(f"Events parsed: {result.events_parsed}")
        info(f"Parse warnings: {len(result.parse_warnings)}")

        if result.outcome in (FetchOutcome.ERROR, FetchOutcome.TIMEOUT):
            fail(f"Fetch failed: {result.error_message}")
            errors += 1
        elif result.events_found == 0:
            fail("No events found. Calendar page structure may have changed.")
            errors += 1
        else:
            ok(f"Fetched {result.events_found} events, parsed {result.events_parsed}")

        # Validate FetchResult structure
        if result.source_name != "cap_ucla":
            fail(f"source_name is '{result.source_name}', expected 'cap_ucla'")
            errors += 1

        # Validate each event
        for i, event in enumerate(result.events):
            issues = _validate_event(event, i)
            if issues:
                for issue in issues:
                    fail(issue)
                    errors += 1

        if not errors:
            ok("All events pass structural validation")

        # Print event summary
        print(f"\n  {BOLD}Events:{RESET}")
        for event in result.events:
            series_tag = f" [series: {event.series_id}]" if event.series_id else " [one-off]"
            free_tag = " [free]" if event.is_free else " [ticketed]"
            venue_tag = f" @ {event.venue_name}" if event.venue_name else " @ unknown venue"
            tags_str = f" tags={event.tags}" if event.tags else ""
            print(
                f"    {event.start_at.strftime('%b %d %I:%M%p')} "
                f"{event.title[:50]}{venue_tag}{series_tag}{free_tag}{tags_str}"
            )

        # Print warnings
        if result.parse_warnings:
            print(f"\n  {BOLD}Parse warnings:{RESET}")
            for w in result.parse_warnings:
                warn(f"[event {w.event_index}] {w.field}: {w.message}")

        # ============================================================
        # Phase 2: Ingest — first run
        # ============================================================
        header("Phase 2: Ingest — first run")

        if result.outcome in (FetchOutcome.ERROR, FetchOutcome.TIMEOUT):
            warn("Skipping ingest — fetcher failed")
        else:
            # Clear previous test data
            await pool.execute("DELETE FROM fetch_log WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla')")
            await pool.execute("DELETE FROM events WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla')")
            await pool.execute("UPDATE sources SET health_score = 100, avg_events_per_fetch = NULL, baseline_sample_size = 0 WHERE name = 'cap_ucla'")
            info("Cleared previous test data")

            # Inject duration_ms since we're not going through the scheduler
            result = FetchResult(
                source_name=result.source_name,
                outcome=result.outcome,
                events=result.events,
                events_found=result.events_found,
                parse_warnings=result.parse_warnings,
                error_message=result.error_message,
                duration_ms=1500,  # synthetic
            )

            await ingest(pool, result)

            # Check events written
            event_count = await pool.fetchval(
                "SELECT COUNT(*) FROM events WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla')"
            )
            if event_count == result.events_parsed:
                ok(f"Events written: {event_count}")
            elif event_count > 0:
                warn(f"Events written: {event_count} (expected {result.events_parsed})")
            else:
                fail(f"No events written to DB")
                errors += 1

            # Check venue resolution
            null_venue_count = await pool.fetchval(
                "SELECT COUNT(*) FROM events WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla') AND venue_id IS NULL"
            )
            resolved_count = event_count - null_venue_count
            if null_venue_count > 0:
                warn(f"Venue resolution: {resolved_count} resolved, {null_venue_count} NULL (add to venues/aliases)")
                # Show which venue names didn't resolve
                unresolved = await pool.fetch(
                    """
                    SELECT DISTINCT e.zone, e.title
                    FROM events e
                    WHERE e.source_id = (SELECT id FROM sources WHERE name = 'cap_ucla')
                      AND e.venue_id IS NULL
                    LIMIT 5
                    """
                )
                for row in unresolved:
                    info(f"  Unresolved: '{row['title'][:40]}...'")
            else:
                ok(f"All {resolved_count} events resolved to venues")

            # Check zone propagation
            zoned_count = await pool.fetchval(
                "SELECT COUNT(*) FROM events WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla') AND zone IS NOT NULL"
            )
            ok(f"Zone propagation: {zoned_count}/{event_count} events have zone set")

            # Check fetch_log
            log = await pool.fetchrow(
                "SELECT * FROM fetch_log WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla') ORDER BY fetched_at DESC LIMIT 1"
            )
            if log:
                ok(f"Fetch log written: status={log['status']}, found={log['events_found']}, inserted={log['events_inserted']}, skipped={log['events_skipped']}")
                if log["duration_ms"]:
                    info(f"Duration: {log['duration_ms']}ms")
            else:
                fail("No fetch_log entry written")
                errors += 1

            # Check source health
            source_after = await pool.fetchrow("SELECT * FROM sources WHERE name = 'cap_ucla'")
            ok(f"Source health: score={source_after['health_score']}, last_status={source_after['last_fetch_status']}")
            if source_after["avg_events_per_fetch"] is not None:
                ok(f"Baseline: avg={source_after['avg_events_per_fetch']:.1f}, samples={source_after['baseline_sample_size']}")
            else:
                warn("Baseline not set (expected after successful fetch)")

            # ============================================================
            # Phase 2b: Ingest — second run (dedup test)
            # ============================================================
            header("Phase 2b: Ingest — dedup test (second run)")

            await ingest(pool, result)

            log2 = await pool.fetchrow(
                "SELECT * FROM fetch_log WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla') ORDER BY fetched_at DESC LIMIT 1"
            )
            if log2:
                if log2["events_inserted"] == 0 and log2["events_skipped"] == result.events_parsed:
                    ok(f"Dedup working: 0 inserted, {log2['events_skipped']} skipped")
                elif log2["events_inserted"] == 0:
                    # Upsert updates rather than inserting, so skipped might not match exactly
                    ok(f"Dedup working: 0 new inserts (skipped={log2['events_skipped']})")
                else:
                    warn(f"Dedup may not be working: inserted={log2['events_inserted']}, skipped={log2['events_skipped']}")

            # Check total event count hasn't doubled
            final_count = await pool.fetchval(
                "SELECT COUNT(*) FROM events WHERE source_id = (SELECT id FROM sources WHERE name = 'cap_ucla')"
            )
            if final_count == event_count:
                ok(f"Event count stable after second run: {final_count}")
            else:
                fail(f"Event count changed: {event_count} → {final_count}")
                errors += 1

            # Check health score after 2 fetches
            source_after2 = await pool.fetchrow("SELECT * FROM sources WHERE name = 'cap_ucla'")
            info(f"Health after 2 runs: score={source_after2['health_score']}, samples={source_after2['baseline_sample_size']}")

            # ============================================================
            # Phase 3: Priority derivation
            # ============================================================
            header("Phase 3: Priority derivation")

            priorities = await pool.fetch(
                """
                SELECT
                    e.title,
                    e.is_one_off,
                    e.is_free,
                    e.category::text,
                    e.is_pinned,
                    s.tier::text,
                    s.weight_override,
                    s.last_fetch_status::text,
                    calculate_auto_priority(
                        s.tier, s.weight_override, e.is_pinned,
                        e.is_one_off, e.is_free, e.category, s.last_fetch_status
                    ) AS auto_priority
                FROM events e
                JOIN sources s ON s.id = e.source_id
                WHERE s.name = 'cap_ucla'
                ORDER BY auto_priority DESC, e.start_at
                """
            )

            if not priorities:
                warn("No events to check priority for")
            else:
                print(f"\n  {BOLD}Priority results:{RESET}")
                p3_count = 0
                p2_count = 0
                p1_count = 0
                for row in priorities:
                    p = row["auto_priority"]
                    star = {3: "***", 2: "** ", 1: "*  "}[p]
                    one_off = "one-off" if row["is_one_off"] else "series"
                    free = "free" if row["is_free"] else "paid"
                    print(f"    [{star}] {row['title'][:45]:45s} ({one_off}, {free})")
                    if p == 3:
                        p3_count += 1
                    elif p == 2:
                        p2_count += 1
                    else:
                        p1_count += 1

                ok(f"Priority distribution: {p3_count} marquee, {p2_count} strong, {p1_count} worth-knowing")

                # Sanity: an anchor source one-off music event should score high
                one_off_music = [r for r in priorities if r["is_one_off"] and r["category"] == "music_performance"]
                if one_off_music:
                    max_p = max(r["auto_priority"] for r in one_off_music)
                    if max_p >= 2:
                        ok(f"One-off anchor music events scoring {max_p} (expected >= 2)")
                    else:
                        warn(f"One-off anchor music events only scoring {max_p}")

                # Sanity: series events should generally score lower than one-offs
                series_events = [r for r in priorities if not r["is_one_off"]]
                one_off_events = [r for r in priorities if r["is_one_off"]]
                if series_events and one_off_events:
                    avg_series = sum(r["auto_priority"] for r in series_events) / len(series_events)
                    avg_one_off = sum(r["auto_priority"] for r in one_off_events) / len(one_off_events)
                    if avg_one_off >= avg_series:
                        ok(f"One-offs avg priority ({avg_one_off:.1f}) >= series ({avg_series:.1f})")
                    else:
                        warn(f"Series avg priority ({avg_series:.1f}) > one-offs ({avg_one_off:.1f}) — unexpected")

    finally:
        await pool.close()

    # ============================================================
    # Summary
    # ============================================================
    header("Summary")
    if errors == 0:
        print(f"\n  {GREEN}{BOLD}All checks passed.{RESET}\n")
    else:
        print(f"\n  {RED}{BOLD}{errors} check(s) failed.{RESET}\n")

    return 0 if errors == 0 else 1


def _validate_event(event: EventData, index: int) -> list[str]:
    """Validate structural requirements on a single EventData."""
    issues = []
    prefix = f"Event[{index}] '{event.title[:30] if event.title else 'NO TITLE'}'"

    if not event.title:
        issues.append(f"{prefix}: missing title")

    if not event.source_url:
        issues.append(f"{prefix}: missing source_url (dedup key)")

    if event.start_at is None:
        issues.append(f"{prefix}: missing start_at")
    elif event.start_at.tzinfo is None:
        issues.append(f"{prefix}: start_at is not timezone-aware")

    if event.end_at is not None and event.end_at.tzinfo is None:
        issues.append(f"{prefix}: end_at is not timezone-aware")

    valid_categories = {"music_performance", "film_screening", "visual_art", "talks_lectures", "festival_outdoor"}
    if event.category not in valid_categories:
        issues.append(f"{prefix}: invalid category '{event.category}'")

    if not isinstance(event.tags, list):
        issues.append(f"{prefix}: tags is not a list")

    if not isinstance(event.is_free, bool):
        issues.append(f"{prefix}: is_free is not a bool")

    if not isinstance(event.is_one_off, bool):
        issues.append(f"{prefix}: is_one_off is not a bool")

    # is_one_off + series_id contradiction check
    if event.is_one_off and event.series_id is not None:
        issues.append(f"{prefix}: is_one_off=True but series_id='{event.series_id}' (contradictory)")

    return issues


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
