-- 03_dirty_fixture.sql
-- *** TEST DATA - NOT REAL HACKER NEWS DATA ***
-- The real files are clean (no duplicates, no invalid values), so this adds a small,
-- clearly labelled dirty batch to demonstrate cleaning, rejects and deduplication.
-- It stages 13 rows as a new 'incremental' run: a trailing re-pull of the last real window
-- (2026-10-03 18:00 - 2026-10-04 00:00 UTC), so the checkpoint does not move past real data.
-- Fake story ids start at 99000001. Needs 02, 03 and 04 run first (04 closes run 1);
-- two real stories are cloned from staging.
-- Run:  psql -v ON_ERROR_STOP=1 -f /sql/03_dirty_fixture.sql
-- Then run 03_clean.sql and 04_transform.sql again. Expected:
--   03: staged 13, rejected 7, valid 6, duplicates collapsed 1, inserted 3, updated 1, unchanged 1
--   04: fact inserted 3, updated 1 (records_loaded 4)
-- Undo: recreate the database.

DO $$
DECLARE
    v_run   BIGINT;
    v_batch BIGINT;
BEGIN
    INSERT INTO etl.pipeline_runs (run_type, range_start, range_end, records_extracted)
    VALUES ('incremental', '2026-10-03 18:00+00', '2026-10-04 00:00+00', 13)
    RETURNING run_id INTO v_run;

    INSERT INTO etl.batch_log (run_id, window_start, window_end, records_extracted)
    VALUES (v_run, '2026-10-03 18:00+00', '2026-10-04 00:00+00', 13)
    RETURNING batch_id INTO v_batch;

    INSERT INTO stg.hn_stories_raw
        (run_id, batch_id, object_id, title, url, author, points, num_comments,
        created_at, created_at_i, updated_at, tags, payload)
    SELECT v_run, v_batch,
        p ->> 'objectID', p ->> 'title', p ->> 'url', p ->> 'author',
        p ->> 'points', p ->> 'num_comments',
        p ->> 'created_at', p ->> 'created_at_i', p ->> 'updated_at',
        p ->> '_tags', p
    FROM (VALUES
        -- UPDATE path: real story 49941699 re-pulled with more points and a later updated_at
        ((SELECT payload || '{"points": 99, "updated_at": "2026-10-04T08:00:00Z"}'::jsonb
            FROM stg.hn_stories_raw WHERE object_id = '49941699' ORDER BY stg_id LIMIT 1)),
        -- UNCHANGED path: real story 49941692 re-pulled identically
        ((SELECT payload FROM stg.hn_stories_raw WHERE object_id = '49941692' ORDER BY stg_id LIMIT 1)),
        -- cleaning: padded title with entities, empty-string url, padded author
        ('{"objectID":"99000001","title":"  Show HN: Tom &amp; Jerry&#x27;s &quot;demo&quot;  ","url":"","author":" tester ","points":5,"num_comments":0,"created_at":"2026-10-03T19:00:00Z","created_at_i":1791054000,"updated_at":"2026-10-03T19:05:00Z","_tags":["story","show_hn"]}'::jsonb),
        -- within-run duplicate: newer copy listed FIRST; points 7 must win
        ('{"objectID":"99000002","title":"Duplicate demo","url":"https://example.com/dup","author":"dupuser","points":7,"num_comments":2,"created_at":"2026-10-03T20:00:00Z","created_at_i":1791057600,"updated_at":"2026-10-04T11:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000002","title":"Duplicate demo","url":"https://example.com/dup","author":"dupuser","points":1,"num_comments":0,"created_at":"2026-10-03T20:00:00Z","created_at_i":1791057600,"updated_at":"2026-10-04T10:00:00Z","_tags":["story"]}'::jsonb),
        -- valid text post: no url key at all
        ('{"objectID":"99000013","title":"Ask HN: Is this a fixture?","author":"asker","points":3,"num_comments":1,"created_at":"2026-10-03T21:00:00Z","created_at_i":1791061200,"updated_at":"2026-10-03T21:10:00Z","_tags":["story","ask_hn"]}'::jsonb),
        -- rejects
        ('{"title":"No objectID","author":"x","points":1,"num_comments":0,"created_at":"2026-10-03T22:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"abc","title":"Non-integer objectID","author":"x","points":1,"num_comments":0,"created_at":"2026-10-03T22:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000007","title":"Unparseable created_at","author":"x","points":1,"num_comments":0,"created_at":"not-a-date","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000008","title":"Impossible date","author":"x","points":1,"num_comments":0,"created_at":"2026-13-45T00:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000010","title":"Negative points","author":"x","points":-3,"num_comments":0,"created_at":"2026-10-03T22:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000011","title":"Non-integer num_comments","author":"x","points":1,"num_comments":2.5,"created_at":"2026-10-03T22:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":["story"]}'::jsonb),
        ('{"objectID":"99000012","title":"_tags not an array","author":"x","points":1,"num_comments":0,"created_at":"2026-10-03T22:00:00Z","updated_at":"2026-10-03T22:00:00Z","_tags":"story"}'::jsonb)
    ) AS t(p);

    RAISE NOTICE 'fixture staged as run % batch % (13 rows)', v_run, v_batch;
END
$$;