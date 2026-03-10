-- chriscal seed data
-- Run after schema.sql to populate initial sources, venues, and aliases.

-- ============================================================
-- SOURCES
-- ============================================================

INSERT INTO sources (name, display_name, url, tier, fetch_interval) VALUES
    ('cap_ucla', 'CAP UCLA', 'https://cap.ucla.edu/calendar', 'anchor', '6 hours')
ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- VENUES — CAP UCLA venues
-- ============================================================

INSERT INTO venues (name, address, zone, latitude, longitude) VALUES
    ('The Nimoy', '1262 Westwood Blvd, Los Angeles, CA 90024', 'westwood_wilshire', 34.0585, -118.4440),
    ('Freud Playhouse', '245 Charles E Young Dr E, Los Angeles, CA 90095', 'ucla_campus', 34.0708, -118.4412),
    ('Royce Hall', '340 Royce Dr, Los Angeles, CA 90095', 'ucla_campus', 34.0729, -118.4423)
ON CONFLICT (name, address) DO NOTHING;

-- ============================================================
-- VENUE ALIASES — known string variants from fetchers
-- ============================================================

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'The Nimoy', id FROM venues WHERE name = 'The Nimoy'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'Nimoy', id FROM venues WHERE name = 'The Nimoy'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'the Nimoy', id FROM venues WHERE name = 'The Nimoy'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'Freud Playhouse', id FROM venues WHERE name = 'Freud Playhouse'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'Freud', id FROM venues WHERE name = 'Freud Playhouse'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'Royce Hall', id FROM venues WHERE name = 'Royce Hall'
    ON CONFLICT (alias) DO NOTHING;

INSERT INTO venue_aliases (alias, venue_id)
    SELECT 'Royce', id FROM venues WHERE name = 'Royce Hall'
    ON CONFLICT (alias) DO NOTHING;
