-- 04_transform.sql
-- clean.hn_stories  ->  dw.dim_author, dw.dim_domain, dw.fact_story; then closes the run.
-- Run with:  psql -v ON_ERROR_STOP=1 -f /sql/04_transform.sql      (single transaction)
-- Run it after each 03_clean.sql, in run order.
--
-- * Dimensions are insert-only (ON CONFLICT DO NOTHING). NULL author/domain => NULL key in the fact.
-- * The fact table is reconciled against ALL of clean.hn_stories, and a row is written only
--   if a value actually changed. Re-running changes nothing.
-- * Metadata, written in the same transaction: records_loaded (fact rows inserted + updated),
--   per-batch records_loaded, batch/run status 'success', etl.checkpoint.
--   If the run is already 'success', metadata and checkpoint are left alone.
-- * Refuses to run while an EARLIER run is still 'running' (it would never get closed).
-- Latest run = highest run_id that has rows in stg.hn_stories_raw (same rule as 03_clean).

BEGIN;

-- Lowercase hostname of an http(s)-style URL: no userinfo, no port, no leading 'www.'.
-- NULL when there is no scheme://host part.
CREATE OR REPLACE FUNCTION etl.extract_domain(url TEXT) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    host TEXT;
BEGIN
    host := substring(btrim(url) FROM '^[A-Za-z][A-Za-z0-9+.-]*://([^/?#]*)');
    IF host IS NULL THEN
        RETURN NULL;
    END IF;
    host := regexp_replace(host, '^.*@', '');
    host := regexp_replace(host, ':[0-9]*$', '');
    host := regexp_replace(lower(host), '^www\.', '');
    RETURN nullif(host, '');
END
$$;

-- clean rows with their domain, computed once
CREATE TEMP TABLE _src ON COMMIT DROP AS
SELECT c.object_id, c.author, etl.extract_domain(c.url) AS domain,
    c.created_at, c.story_type, c.points, c.num_comments
FROM clean.hn_stories c;

CREATE TEMP TABLE _written (story_id BIGINT, inserted BOOLEAN, batch_id BIGINT) ON COMMIT DROP;
CREATE TEMP TABLE _counts (ord INT, metric TEXT, n TEXT) ON COMMIT DROP;
CREATE TEMP TABLE _run ON COMMIT DROP AS
SELECT max(run_id) AS run_id FROM stg.hn_stories_raw;

DO $$
DECLARE
    v_run      BIGINT;
    v_status   TEXT;
    v_early    BIGINT;
    n_auth     INT;
    n_dom      INT;
    n_ins      INT;
    n_upd      INT;
    n_clean    INT;
    v_win_end  TIMESTAMPTZ;
    v_max_upd  TIMESTAMPTZ;
    v_closed   BOOLEAN := FALSE;
BEGIN
    SELECT run_id INTO v_run FROM _run;
    IF v_run IS NULL THEN
        RAISE EXCEPTION 'no staging rows; run 02 and 03 first';
    END IF;

    SELECT min(run_id) INTO v_early
    FROM etl.pipeline_runs WHERE run_id < v_run AND status = 'running';
    IF v_early IS NOT NULL THEN
        RAISE EXCEPTION 'run % is still ''running''. Run 04 after each 03, in run order (or reset the database).', v_early;
    END IF;

    SELECT status INTO v_status FROM etl.pipeline_runs WHERE run_id = v_run;

    -- dimensions
    INSERT INTO dw.dim_author (author_name)
    SELECT DISTINCT author FROM _src WHERE author IS NOT NULL ORDER BY author
    ON CONFLICT (author_name) DO NOTHING;
    GET DIAGNOSTICS n_auth = ROW_COUNT;

    INSERT INTO dw.dim_domain (domain)
    SELECT DISTINCT domain FROM _src WHERE domain IS NOT NULL ORDER BY domain
    ON CONFLICT (domain) DO NOTHING;
    GET DIAGNOSTICS n_dom = ROW_COUNT;

    -- fact: insert new, update only if something changed
    WITH up AS (
        INSERT INTO dw.fact_story AS f
            (story_id, author_key, domain_key, created_at, hour_of_day,
                story_type, points, num_comments, has_discussion)
        SELECT s.object_id, a.author_key, d.domain_key, s.created_at,
            EXTRACT(HOUR FROM s.created_at AT TIME ZONE 'UTC')::smallint,
            s.story_type, s.points, s.num_comments, COALESCE(s.num_comments, 0) > 0
        FROM _src s
        LEFT JOIN dw.dim_author a ON a.author_name = s.author
        LEFT JOIN dw.dim_domain d ON d.domain = s.domain
        ON CONFLICT (story_id) DO UPDATE
            SET author_key = EXCLUDED.author_key, domain_key = EXCLUDED.domain_key,
                created_at = EXCLUDED.created_at, hour_of_day = EXCLUDED.hour_of_day,
                story_type = EXCLUDED.story_type, points = EXCLUDED.points,
                num_comments = EXCLUDED.num_comments, has_discussion = EXCLUDED.has_discussion
        WHERE (f.author_key, f.domain_key, f.created_at, f.hour_of_day, f.story_type,
                f.points, f.num_comments, f.has_discussion)
                IS DISTINCT FROM
            (EXCLUDED.author_key, EXCLUDED.domain_key, EXCLUDED.created_at, EXCLUDED.hour_of_day,
                EXCLUDED.story_type, EXCLUDED.points, EXCLUDED.num_comments, EXCLUDED.has_discussion)
        RETURNING story_id, (xmax = 0) AS inserted
    )
    INSERT INTO _written (story_id, inserted) SELECT story_id, inserted FROM up;

    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted)
        INTO n_ins, n_upd FROM _written;
    SELECT count(*) INTO n_clean FROM _src;

    -- metadata: only the first time this run is transformed
    IF v_status = 'running' THEN
        -- attribute each written story to the latest batch of this run that contained it
        UPDATE _written w
            SET batch_id = (SELECT max(s.batch_id) FROM stg.hn_stories_raw s
                            WHERE s.run_id = v_run AND s.object_id = w.story_id::text);

        UPDATE etl.batch_log b
            SET records_loaded = (SELECT count(*) FROM _written w WHERE w.batch_id = b.batch_id),
                status   = 'success',
                ended_at = COALESCE(b.ended_at, now())
        WHERE b.run_id = v_run;

        UPDATE etl.pipeline_runs
            SET records_loaded = n_ins + n_upd, status = 'success', ended_at = now()
        WHERE run_id = v_run;

        SELECT max(window_end) INTO v_win_end FROM etl.batch_log WHERE run_id = v_run;
        SELECT max(updated_at) INTO v_max_upd FROM clean.hn_stories;

        INSERT INTO etl.checkpoint AS k
            (pipeline_name, last_completed_window_end, max_updated_at, last_run_id)
        VALUES ('hn_stories', v_win_end, v_max_upd, v_run)
        ON CONFLICT (pipeline_name) DO UPDATE
            SET last_completed_window_end = GREATEST(k.last_completed_window_end, EXCLUDED.last_completed_window_end),
                max_updated_at            = GREATEST(k.max_updated_at, EXCLUDED.max_updated_at),
                last_run_id               = EXCLUDED.last_run_id,
                updated_at                = now();
        v_closed := TRUE;
    END IF;

    INSERT INTO _counts VALUES
        (1, 'run_id transformed',                         v_run::text),
        (2, 'run closed by this execution',               v_closed::text),
        (3, 'clean rows',                                 n_clean::text),
        (4, 'new authors',                                n_auth::text),
        (5, 'new domains',                                n_dom::text),
        (6, 'fact rows inserted',                         n_ins::text),
        (7, 'fact rows updated',                          n_upd::text),
        (8, 'fact rows unchanged',                        (n_clean - n_ins - n_upd)::text);
END
$$;

-- ---------------------------------------------------------------
-- Summary and checks (every ok should be true)
-- ---------------------------------------------------------------
SELECT metric, n FROM _counts ORDER BY ord;

SELECT run_id, run_type, status, records_extracted, records_loaded, records_rejected
FROM etl.pipeline_runs WHERE run_id = (SELECT run_id FROM _run);

SELECT pipeline_name, last_completed_window_end, max_updated_at, last_run_id FROM etl.checkpoint;

SELECT 'fact rows = clean rows' AS check_name,
    (SELECT count(*) FROM dw.fact_story) = (SELECT count(*) FROM clean.hn_stories) AS ok
UNION ALL
SELECT 'dim_author = distinct clean authors',
    (SELECT count(*) FROM dw.dim_author) = (SELECT count(DISTINCT author) FROM _src)
UNION ALL
SELECT 'dim_domain = distinct clean domains',
    (SELECT count(*) FROM dw.dim_domain) = (SELECT count(DISTINCT domain) FROM _src)
UNION ALL
SELECT 'domain_key NULL exactly when domain NULL',
    NOT EXISTS (SELECT 1 FROM dw.fact_story f JOIN _src s ON s.object_id = f.story_id
                WHERE (f.domain_key IS NULL) <> (s.domain IS NULL))
UNION ALL
SELECT 'run extracted = staged rows',
    (SELECT records_extracted FROM etl.pipeline_runs WHERE run_id = (SELECT run_id FROM _run))
    = (SELECT count(*) FROM stg.hn_stories_raw WHERE run_id = (SELECT run_id FROM _run))
UNION ALL
SELECT 'batch records_loaded sum = run records_loaded',
    COALESCE((SELECT sum(records_loaded) FROM etl.batch_log WHERE run_id = (SELECT run_id FROM _run)), 0)
    = (SELECT records_loaded FROM etl.pipeline_runs WHERE run_id = (SELECT run_id FROM _run))
UNION ALL
SELECT 'run status = success',
    (SELECT status FROM etl.pipeline_runs WHERE run_id = (SELECT run_id FROM _run)) = 'success';

COMMIT;