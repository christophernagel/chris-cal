-- chriscal schema
-- Event aggregation calendar with source health tracking
-- Personal calendar pipeline — UCLA/Westside/canyon event ecosystem

-- ============================================================
-- ENUMS
-- ============================================================

CREATE TYPE event_category AS ENUM (
    'music_performance',   -- live music, theater, dance, DJ/nightlife
    'film_screening',      -- repertory, archive screenings, film Q&As
    'visual_art',          -- gallery openings, museum exhibitions, DMA shows
    'talks_lectures',      -- architecture, humanities, panels, design talks
    'festival_outdoor'     -- canyon festivals, outdoor events, seasonal anchors
);

CREATE TYPE location_zone AS ENUM (
    'ucla_campus',
    'westwood_wilshire',
    'santa_monica',
    'west_hollywood_mid_city',
    'silver_lake_echo_park',
    'topanga_malibu_canyon',
    'hollywood'
);

CREATE TYPE fetch_status AS ENUM (
    'success',    -- clean fetch, all events parsed
    'partial',    -- fetch ran but content structurally wrong or incomplete
    'error',      -- fetch failed (HTTP error, parse exception)
    'timeout',    -- fetch exceeded time limit
    'stale'       -- fetch hasn't run within expected window (scheduler may be down)
);

CREATE TYPE source_tier AS ENUM (
    'anchor',         -- weight 3.0: CAP UCLA, Billy Wilder, etc.
    'regular',        -- weight 2.0: department calendars, smaller venues
    'supplemental'    -- weight 1.0: scraped aggregators, low-signal sources
);

-- ============================================================
-- FUNCTIONS — pure, IMMUTABLE, no DB access
-- ============================================================

-- Resolve effective source weight from tier + optional override
CREATE OR REPLACE FUNCTION effective_weight(
    tier source_tier,
    weight_override float
) RETURNS float
LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(weight_override, CASE tier
        WHEN 'anchor'       THEN 3.0
        WHEN 'regular'      THEN 2.0
        WHEN 'supplemental' THEN 1.0
    END)
$$;

-- Derive auto-priority (1-3) from event + source signals
-- Returns 1 (worth knowing), 2 (strong pick), or 3 (marquee)
CREATE OR REPLACE FUNCTION calculate_auto_priority(
    p_source_tier     source_tier,
    p_weight_override float,
    p_is_pinned       boolean,
    p_is_one_off      boolean,
    p_is_free         boolean,
    p_category        event_category,
    p_source_status   fetch_status
) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        -- Manual pin always returns marquee
        WHEN p_is_pinned THEN 3

        -- Degraded source caps at 1 regardless of score
        WHEN p_source_status IN ('error', 'timeout', 'stale') THEN 1

        -- Derive from weighted score
        ELSE CASE
            WHEN (
                effective_weight(p_source_tier, p_weight_override)
                + CASE WHEN p_is_one_off THEN 1.0 ELSE 0.0 END
                + CASE WHEN p_category IN ('music_performance', 'film_screening') THEN 0.5 ELSE 0.0 END
                + CASE WHEN p_is_free THEN 0.25 ELSE 0.0 END
            ) >= 3.5 THEN 3
            WHEN (
                effective_weight(p_source_tier, p_weight_override)
                + CASE WHEN p_is_one_off THEN 1.0 ELSE 0.0 END
                + CASE WHEN p_category IN ('music_performance', 'film_screening') THEN 0.5 ELSE 0.0 END
                + CASE WHEN p_is_free THEN 0.25 ELSE 0.0 END
            ) >= 2.0 THEN 2
            ELSE 1
        END
    END
$$;

-- ============================================================
-- SOURCES — fetcher registry and health state
-- ============================================================

CREATE TABLE sources (
    id                    SERIAL PRIMARY KEY,
    name                  TEXT NOT NULL UNIQUE,           -- e.g. 'cap_ucla', 'billy_wilder'
    display_name          TEXT NOT NULL,                   -- e.g. 'CAP UCLA'
    url                   TEXT NOT NULL,                   -- calendar page URL

    -- classification
    tier                  source_tier NOT NULL DEFAULT 'regular',
    weight_override       FLOAT,                          -- overrides tier-derived weight if set

    -- scheduling
    fetch_interval        INTERVAL NOT NULL DEFAULT '6 hours',

    -- health state (updated by ingest layer after each fetch)
    last_fetch_at         TIMESTAMPTZ,
    last_fetch_status     fetch_status,
    last_successful_fetch TIMESTAMPTZ,
    fetch_error_log       TEXT,                            -- last error message/traceback
    health_score          SMALLINT DEFAULT 100             -- 0-100, derived from recent fetch_log
        CHECK (health_score >= 0 AND health_score <= 100),

    -- baseline tracking (updated after successful fetches only)
    avg_events_per_fetch  FLOAT,                          -- incremental moving average
    baseline_sample_size  INT DEFAULT 0,                  -- how many successful fetches inform the average

    enabled               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- VENUES — normalized place data
-- ============================================================

CREATE TABLE venues (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    address         TEXT,
    zone            location_zone NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    source_id       INT REFERENCES sources(id),     -- which source typically produces events here
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (name, address)
);

CREATE INDEX idx_venues_zone ON venues (zone);

-- ============================================================
-- VENUE ALIASES — predictable name resolution for ingest
-- ============================================================
-- Fetchers produce venue name strings that may vary across sources.
-- Instead of fuzzy matching (silent wrong matches), map known variants explicitly.
-- Populated manually as new variants appear in fetch results.

CREATE TABLE venue_aliases (
    id              SERIAL PRIMARY KEY,
    alias           TEXT NOT NULL UNIQUE,             -- the variant string from a fetcher
    venue_id        INT NOT NULL REFERENCES venues(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_venue_aliases_alias ON venue_aliases (alias);

-- ============================================================
-- EVENTS — core event data
-- ============================================================

CREATE TABLE events (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    description     TEXT,
    source_id       INT NOT NULL REFERENCES sources(id),
    source_url      TEXT,                            -- direct link to event on source site
    series_id       TEXT,                            -- groups recurring instances; NULL until classified
    venue_id        INT REFERENCES venues(id),
    zone            location_zone,                   -- auto-populated from venue, manually overridable

    -- time
    start_at        TIMESTAMPTZ NOT NULL,
    end_at          TIMESTAMPTZ,

    -- category + tags
    category        event_category NOT NULL,
    tags            TEXT[] DEFAULT '{}',

    -- access
    is_free         BOOLEAN NOT NULL DEFAULT TRUE,
    ticket_url      TEXT,
    price_range     TEXT,                            -- e.g. '$15-25', 'Free', 'Donation'

    -- classification
    is_one_off      BOOLEAN NOT NULL DEFAULT FALSE,  -- explicit signal; fetcher must set deliberately
    -- Note: is_one_off = true AND series_id IS NOT NULL is contradictory.
    -- Enforced by fetcher contract, not a DB constraint. Add CHECK if it becomes a data quality issue.

    -- priority
    is_pinned       BOOLEAN NOT NULL DEFAULT FALSE,  -- manual override, always surfaces as priority 3

    -- trust
    last_verified   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- debug
    raw_source      JSONB,                           -- original scraped data for debugging

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- dedup: same event from same source shouldn't appear twice
    UNIQUE (source_id, source_url)
);

-- Query patterns: upcoming events by time, filter by category, filter by zone, filter by tags
CREATE INDEX idx_events_start_at ON events (start_at);
CREATE INDEX idx_events_category ON events (category);
CREATE INDEX idx_events_zone ON events (zone);
CREATE INDEX idx_events_tags ON events USING GIN (tags);
CREATE INDEX idx_events_source_id ON events (source_id);
CREATE INDEX idx_events_series_id ON events (series_id) WHERE series_id IS NOT NULL;
CREATE INDEX idx_events_pinned ON events (is_pinned) WHERE is_pinned = TRUE;

-- ============================================================
-- FETCH LOG — append-only history for health score derivation
-- ============================================================
-- Retention: DELETE FROM fetch_log WHERE fetched_at < NOW() - INTERVAL '90 days'
-- Run via cron daily. 90 days preserves enough history for seasonal reliability patterns.

CREATE TABLE fetch_log (
    id              SERIAL PRIMARY KEY,
    source_id       INT NOT NULL REFERENCES sources(id),
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          fetch_status NOT NULL,
    events_found    INT DEFAULT 0,                   -- total events parsed from source
    events_inserted INT DEFAULT 0,                   -- new events written to DB
    events_skipped  INT DEFAULT 0,                   -- duplicates that already existed
    parse_warnings  JSONB,                           -- structured error detail for partial fetches
    error_message   TEXT,
    duration_ms     INT                              -- how long the fetch took
);

-- Composite index for health score query: last N fetches per source
CREATE INDEX idx_fetch_log_source_time ON fetch_log (source_id, fetched_at DESC);

-- ============================================================
-- EVENTS VIEW — all derived fields computed at query time
-- ============================================================
-- This is the primary read interface for the API layer.
-- Queries should hit this view, not the base events table.

CREATE OR REPLACE VIEW events_view AS
SELECT
    e.id,
    e.title,
    e.description,
    e.source_id,
    e.source_url,
    e.series_id,
    e.venue_id,
    e.zone,
    e.start_at,
    e.end_at,
    e.category,
    e.tags,
    e.is_free,
    e.ticket_url,
    e.price_range,
    e.is_one_off,
    e.is_pinned,
    e.last_verified,
    e.raw_source,
    e.created_at,
    e.updated_at,

    -- Venue join
    v.name AS venue_name,
    v.address AS venue_address,
    v.latitude AS venue_lat,
    v.longitude AS venue_lng,

    -- Source join
    s.name AS source_name,
    s.display_name AS source_display_name,
    s.tier AS source_tier,
    s.health_score AS source_health_score,
    s.last_fetch_status AS source_last_fetch_status,
    s.baseline_sample_size AS source_baseline_sample_size,

    -- Derived: time_of_day from start_at in LA timezone
    CASE
        WHEN EXTRACT(HOUR FROM e.start_at AT TIME ZONE 'America/Los_Angeles') < 17
            THEN 'daytime'
        WHEN EXTRACT(HOUR FROM e.start_at AT TIME ZONE 'America/Los_Angeles') < 21
            THEN 'evening'
        ELSE 'late_night'
    END AS time_of_day,

    -- Derived: auto_priority via immutable function
    calculate_auto_priority(
        s.tier, s.weight_override, e.is_pinned,
        e.is_one_off, e.is_free, e.category, s.last_fetch_status
    ) AS auto_priority,

    -- Derived: confidence_flag from verification age + source health
    CASE
        WHEN s.last_fetch_status IN ('error', 'timeout', 'stale')
            THEN 'stale'
        WHEN e.last_verified >= NOW() - s.fetch_interval
            THEN 'fresh'
        WHEN e.last_verified >= NOW() - (s.fetch_interval * 2)
            THEN 'aging'
        ELSE 'stale'
    END AS confidence_flag,

    -- Derived: is_new_source (for UI to show "new" badge instead of low-health warning)
    (s.baseline_sample_size < 5) AS is_new_source

FROM events e
JOIN sources s ON s.id = e.source_id
LEFT JOIN venues v ON v.id = e.venue_id;

-- ============================================================
-- DERIVED FIELD REFERENCE
-- ============================================================

-- time_of_day: derived from start_at (in America/Los_Angeles)
--   before 17:00  → 'daytime'
--   17:00–21:00   → 'evening'
--   after 21:00   → 'late_night'

-- auto_priority: via calculate_auto_priority() IMMUTABLE function
--   Returns: 1 (worth knowing), 2 (strong pick), 3 (marquee)
--   Degraded sources capped at 1. Manual pin overrides to 3.

-- confidence_flag: from event.last_verified age + source health
--   'fresh'  → verified within source.fetch_interval
--   'aging'  → verified within 2x source.fetch_interval
--   'stale'  → older than 2x fetch_interval OR source status is error/timeout/stale

-- is_new_source: true when baseline_sample_size < 5
--   UI should render "new source" indicator, not low-health warning

-- health_score on sources: derived from last 20 entries in fetch_log
--   success=1.0, partial=0.5, error/timeout/stale=0.0
--   health_score = (sum of last 20 / 20) * 100
--   Updated by ingest layer after each fetch

-- baseline deviation alert: if events_found < (avg_events_per_fetch * 0.3)
--   on a 'success' fetch, flag as potential structural change in source
--   Baseline only updated after 'success' fetches to avoid poisoning
