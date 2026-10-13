-- 05_validate.sql
-- Read-only validation of the whole pipeline: record counts, duplicates, missing values,
-- successful loads, referential integrity, plus analysis queries on the final model.
-- Run with:  psql -v ON_ERROR_STOP=1 -f /sql/05_validate.sql
--
-- Works on a database loaded by the SQL scripts (02-04) or by the Python ETL (scripts.etl).
-- Safe to run at any point (it only reads; it uses one session temp table and one temp function).
-- Failed runs/batches are history (kept for audit): they are listed in 2e, not counted as errors,
-- as long as the LATEST run succeeded.
-- Checks compare layers to each other instead of hardcoding counts, so they hold after the
-- full load, after the dirty fixture, and after a re-run of 04.
-- Section 1 = checks (ok must be true). Sections 2-4 = informational. Section 5 = analysis.
-- Analysis excludes fixture rows (story ids >= 99000000).

CREATE TEMP TABLE _checks (ord SERIAL, section TEXT, check_name TEXT, ok BOOLEAN);

-- Copy of etl.extract_domain (created by 04_transform.sql only), so this script also works on a
-- database that was loaded by the Python ETL.
CREATE FUNCTION pg_temp.extract_domain(url TEXT) RETURNS TEXT
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

-- ---------------------------------------------------------------
-- A. Record counts across layers
-- ---------------------------------------------------------------
INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every run: records_extracted = staged rows',
    COALESCE(bool_and(r.records_extracted = COALESCE(s.n, 0)), TRUE)
FROM etl.pipeline_runs r
LEFT JOIN (SELECT run_id, count(*) AS n FROM stg.hn_stories_raw GROUP BY run_id) s USING (run_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every successful batch: records_extracted = staged rows',
    COALESCE(bool_and(b.records_extracted = COALESCE(s.n, 0)) FILTER (WHERE b.status = 'success'), TRUE)
FROM etl.batch_log b
LEFT JOIN (SELECT batch_id, count(*) AS n FROM stg.hn_stories_raw GROUP BY batch_id) s USING (batch_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every run: rejected_records rows = records_rejected',
    COALESCE(bool_and(r.records_rejected = COALESCE(x.n, 0)), TRUE)
FROM etl.pipeline_runs r
LEFT JOIN (SELECT run_id, count(*) AS n FROM etl.rejected_records GROUP BY run_id) x USING (run_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every run: loaded + rejected <= extracted',
    COALESCE(bool_and(records_loaded + records_rejected <= records_extracted), TRUE)
FROM etl.pipeline_runs;

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every clean story comes from staging',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories c
                WHERE NOT EXISTS (SELECT 1 FROM stg.hn_stories_raw s WHERE s.object_id = c.object_id::text));

-- a staged id that is numeric but absent from clean must have been rejected
INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'every staged id is in clean or in rejected_records',
    NOT EXISTS (
        SELECT 1 FROM stg.hn_stories_raw s
        WHERE s.object_id ~ '^[0-9]{1,18}$'
        AND NOT EXISTS (SELECT 1 FROM clean.hn_stories c WHERE c.object_id = s.object_id::bigint)
        AND NOT EXISTS (SELECT 1 FROM etl.rejected_records j WHERE j.object_id = s.object_id));

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'fact_story rows = clean rows',
    (SELECT count(*) FROM dw.fact_story) = (SELECT count(*) FROM clean.hn_stories);

INSERT INTO _checks (section, check_name, ok)
SELECT 'A counts', 'clean row_hash = recomputed hash of cleaned values',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories c
        WHERE c.row_hash IS DISTINCT FROM
            md5(jsonb_build_array(c.title, c.url, c.author, c.points, c.num_comments, c.story_type)::text));

-- ---------------------------------------------------------------
-- B. Duplicates
-- ---------------------------------------------------------------
INSERT INTO _checks (section, check_name, ok)
SELECT 'B duplicates', 'clean: no duplicate object_id',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories GROUP BY object_id HAVING count(*) > 1);

INSERT INTO _checks (section, check_name, ok)
SELECT 'B duplicates', 'fact_story: no duplicate story_id',
    NOT EXISTS (SELECT 1 FROM dw.fact_story GROUP BY story_id HAVING count(*) > 1);

INSERT INTO _checks (section, check_name, ok)
SELECT 'B duplicates', 'dim_author: no duplicate author_name',
    NOT EXISTS (SELECT 1 FROM dw.dim_author GROUP BY author_name HAVING count(*) > 1);

INSERT INTO _checks (section, check_name, ok)
SELECT 'B duplicates', 'dim_domain: no duplicate domain',
    NOT EXISTS (SELECT 1 FROM dw.dim_domain GROUP BY domain HAVING count(*) > 1);

-- ---------------------------------------------------------------
-- C. Missing values
-- ---------------------------------------------------------------
INSERT INTO _checks (section, check_name, ok)
SELECT 'C missing', 'clean: object_id, created_at, story_type, row_hash never NULL',
    COALESCE(bool_and(object_id IS NOT NULL AND created_at IS NOT NULL
                      AND story_type IS NOT NULL AND row_hash IS NOT NULL), TRUE)
FROM clean.hn_stories;

INSERT INTO _checks (section, check_name, ok)
SELECT 'C missing', 'fact: created_at, hour_of_day, story_type, has_discussion never NULL',
    COALESCE(bool_and(created_at IS NOT NULL AND hour_of_day IS NOT NULL
                      AND story_type IS NOT NULL AND has_discussion IS NOT NULL), TRUE)
FROM dw.fact_story;

INSERT INTO _checks (section, check_name, ok)
SELECT 'C missing', 'domain_key is NULL exactly when the url has no domain',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories c
                JOIN dw.fact_story f ON f.story_id = c.object_id
                WHERE (f.domain_key IS NULL) <> (pg_temp.extract_domain(c.url) IS NULL));

INSERT INTO _checks (section, check_name, ok)
SELECT 'C missing', 'no empty-string title/url/author left in clean',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories
                WHERE btrim(title) = '' OR btrim(url) = '' OR btrim(author) = '');

INSERT INTO _checks (section, check_name, ok)
SELECT 'C missing', 'hour_of_day = UTC hour of created_at',
    NOT EXISTS (SELECT 1 FROM dw.fact_story
                WHERE hour_of_day <> EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC'));

-- ---------------------------------------------------------------
-- D. Successful loads and metadata
-- ---------------------------------------------------------------
INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'at least one run exists, none left running, the latest run succeeded',
    count(*) > 0 AND bool_and(status <> 'running')
    AND (SELECT status FROM etl.pipeline_runs ORDER BY run_id DESC LIMIT 1) = 'success'
FROM etl.pipeline_runs;

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'no batch left running; failed batches only inside failed runs',
    EXISTS (SELECT 1 FROM etl.batch_log)
    AND NOT EXISTS (SELECT 1 FROM etl.batch_log WHERE status = 'running')
    AND NOT EXISTS (SELECT 1 FROM etl.batch_log b JOIN etl.pipeline_runs r USING (run_id)
                    WHERE b.status = 'failed' AND r.status <> 'failed');

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'every run has ended_at >= started_at',
    COALESCE(bool_and(ended_at IS NOT NULL AND ended_at >= started_at), FALSE)
FROM etl.pipeline_runs;

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'every run: batch records_loaded sum = run records_loaded',
    COALESCE(bool_and(r.records_loaded = COALESCE(b.n, 0)), TRUE)
FROM etl.pipeline_runs r
LEFT JOIN (SELECT run_id, sum(records_loaded) AS n FROM etl.batch_log GROUP BY run_id) b USING (run_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'every run: batch records_rejected sum = run records_rejected',
    COALESCE(bool_and(r.records_rejected = COALESCE(b.n, 0)), TRUE)
FROM etl.pipeline_runs r
LEFT JOIN (SELECT run_id, sum(records_rejected) AS n FROM etl.batch_log GROUP BY run_id) b USING (run_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'checkpoint exists and points at the latest window and run',
    EXISTS (SELECT 1 FROM etl.checkpoint k
            WHERE k.pipeline_name = 'hn_stories'
            AND k.last_completed_window_end = (SELECT max(window_end) FROM etl.batch_log WHERE status = 'success')
            AND k.last_run_id = (SELECT max(run_id) FROM etl.pipeline_runs WHERE status = 'success'));

INSERT INTO _checks (section, check_name, ok)
SELECT 'D loads', 'checkpoint max_updated_at = max(clean.updated_at)',
    EXISTS (SELECT 1 FROM etl.checkpoint k
            WHERE k.pipeline_name = 'hn_stories'
            AND k.max_updated_at IS NOT DISTINCT FROM (SELECT max(updated_at) FROM clean.hn_stories));

-- ---------------------------------------------------------------
-- E. Referential integrity (orphans)
-- ---------------------------------------------------------------
INSERT INTO _checks (section, check_name, ok)
SELECT 'E integrity', 'fact -> dim_author / dim_domain: no orphan keys',
    NOT EXISTS (SELECT 1 FROM dw.fact_story f
                LEFT JOIN dw.dim_author a USING (author_key)
                LEFT JOIN dw.dim_domain d USING (domain_key)
                WHERE (f.author_key IS NOT NULL AND a.author_key IS NULL)
                OR (f.domain_key IS NOT NULL AND d.domain_key IS NULL));

INSERT INTO _checks (section, check_name, ok)
SELECT 'E integrity', 'clean first_run_id / last_run_id exist in pipeline_runs',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories c
                WHERE NOT EXISTS (SELECT 1 FROM etl.pipeline_runs r WHERE r.run_id = c.first_run_id)
                OR NOT EXISTS (SELECT 1 FROM etl.pipeline_runs r WHERE r.run_id = c.last_run_id));

INSERT INTO _checks (section, check_name, ok)
SELECT 'E integrity', 'clean: first_run_id <= last_run_id',
    NOT EXISTS (SELECT 1 FROM clean.hn_stories WHERE first_run_id > last_run_id);

INSERT INTO _checks (section, check_name, ok)
SELECT 'E integrity', 'batch_log, staging, rejects: no orphan run/batch ids',
    NOT EXISTS (SELECT 1 FROM etl.batch_log b WHERE NOT EXISTS (SELECT 1 FROM etl.pipeline_runs r WHERE r.run_id = b.run_id))
    AND NOT EXISTS (SELECT 1 FROM stg.hn_stories_raw s WHERE NOT EXISTS (SELECT 1 FROM etl.batch_log b WHERE b.batch_id = s.batch_id))
    AND NOT EXISTS (SELECT 1 FROM etl.rejected_records j WHERE j.batch_id IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM etl.batch_log b WHERE b.batch_id = j.batch_id));

-- ===============================================================
-- 1. Results
-- ===============================================================
\echo
\echo '== 1. Checks (ok must be true) =='
SELECT section, check_name, ok FROM _checks ORDER BY ord;

SELECT count(*) AS checks, count(*) FILTER (WHERE ok) AS passed,
    count(*) FILTER (WHERE NOT ok OR ok IS NULL) AS failed,
    CASE WHEN count(*) FILTER (WHERE NOT ok OR ok IS NULL) = 0
         THEN 'ALL CHECKS PASSED' ELSE 'SOME CHECKS FAILED' END AS verdict
FROM _checks;

-- ===============================================================
-- 2. Record counts by layer and by run (informational)
-- ===============================================================
\echo
\echo '== 2. Counts by layer =='
SELECT '1 stg.hn_stories_raw (all runs, append-only)' AS layer, count(*) AS n FROM stg.hn_stories_raw
UNION ALL SELECT '2 stg distinct object_id', count(DISTINCT object_id) FROM stg.hn_stories_raw
UNION ALL SELECT '3 etl.rejected_records', count(*) FROM etl.rejected_records
UNION ALL SELECT '4 clean.hn_stories', count(*) FROM clean.hn_stories
UNION ALL SELECT '5 dw.fact_story', count(*) FROM dw.fact_story
UNION ALL SELECT '6 dw.dim_author', count(*) FROM dw.dim_author
UNION ALL SELECT '7 dw.dim_domain', count(*) FROM dw.dim_domain
ORDER BY 1;

\echo
\echo '== 2b. Runs =='
SELECT run_id, run_type, status, range_start, range_end,
    started_at, ended_at, records_extracted, records_loaded, records_rejected, error_message
FROM etl.pipeline_runs ORDER BY run_id;

\echo
\echo '== 2c. Rejects by reason =='
SELECT run_id, reason, count(*) AS n FROM etl.rejected_records GROUP BY run_id, reason ORDER BY run_id, reason;

\echo
\echo '== 2d. Checkpoint =='
SELECT * FROM etl.checkpoint;

\echo
\echo '== 2e. Failed batches (history; none expected on a clean load) =='
SELECT b.run_id, b.window_start, b.window_end, b.records_extracted, b.error_message
FROM etl.batch_log b WHERE b.status = 'failed' ORDER BY b.batch_id;

-- ===============================================================
-- 3. Duplicates (informational)
-- ===============================================================
\echo
\echo '== 3. Duplicates in staging (expected after re-runs: staging is append-only) =='
SELECT count(*) AS object_ids_staged_more_than_once,
    COALESCE(sum(c - 1), 0) AS extra_staging_rows
FROM (SELECT object_id, count(*) AS c FROM stg.hn_stories_raw GROUP BY object_id HAVING count(*) > 1) d;

\echo
\echo '== 3b. Author names that differ only by case (dimension is case-sensitive) =='
SELECT count(*) AS case_collision_groups
FROM (SELECT lower(author_name) FROM dw.dim_author GROUP BY lower(author_name) HAVING count(*) > 1) x;

-- ===============================================================
-- 4. Missing values (informational)
-- ===============================================================
\echo
\echo '== 4. NULL counts in clean.hn_stories =='
SELECT count(*) AS rows,
    count(*) FILTER (WHERE title IS NULL)        AS null_title,
    count(*) FILTER (WHERE url IS NULL)          AS null_url,
    count(*) FILTER (WHERE author IS NULL)       AS null_author,
    count(*) FILTER (WHERE points IS NULL)       AS null_points,
    count(*) FILTER (WHERE num_comments IS NULL) AS null_num_comments,
    count(*) FILTER (WHERE updated_at IS NULL)   AS null_updated_at
FROM clean.hn_stories;

\echo
\echo '== 4b. Stories without url, by type (expected: Ask HN and text-only Show HN) =='
SELECT story_type, count(*) AS stories, count(*) FILTER (WHERE url IS NULL) AS without_url
FROM clean.hn_stories GROUP BY story_type ORDER BY story_type;

\echo
\echo '== 4c. Dimension rows no fact points to (expect 0) =='
SELECT (SELECT count(*) FROM dw.dim_author a WHERE NOT EXISTS (SELECT 1 FROM dw.fact_story f WHERE f.author_key = a.author_key)) AS unused_authors,
    (SELECT count(*) FROM dw.dim_domain d WHERE NOT EXISTS (SELECT 1 FROM dw.fact_story f WHERE f.domain_key = d.domain_key)) AS unused_domains;

-- ===============================================================
-- 5. Analysis (fixture rows excluded)
-- ===============================================================
\echo
\echo '== 5a. Top 10 domains by number of stories =='
SELECT d.domain, count(*) AS stories, round(avg(f.points), 1) AS avg_points,
    sum(f.num_comments) AS total_comments
FROM dw.fact_story f JOIN dw.dim_domain d USING (domain_key)
WHERE f.story_id < 99000000
GROUP BY d.domain ORDER BY stories DESC, d.domain LIMIT 10;

\echo
\echo '== 5b. Stories by UTC hour of day =='
SELECT hour_of_day, count(*) AS stories, round(avg(points), 1) AS avg_points,
    round(100.0 * count(*) FILTER (WHERE has_discussion) / count(*), 1) AS pct_with_discussion
FROM dw.fact_story WHERE story_id < 99000000
GROUP BY hour_of_day ORDER BY hour_of_day;

\echo
\echo '== 5c. Story type: volume, points, discussion rate =='
SELECT story_type, count(*) AS stories, round(avg(points), 1) AS avg_points,
    round(avg(num_comments), 1) AS avg_comments,
    round(100.0 * count(*) FILTER (WHERE has_discussion) / count(*), 1) AS pct_with_discussion
FROM dw.fact_story WHERE story_id < 99000000
GROUP BY story_type ORDER BY stories DESC;

\echo
\echo '== 5d. Top 10 authors by total points =='
SELECT a.author_name, count(*) AS stories, sum(f.points) AS total_points
FROM dw.fact_story f JOIN dw.dim_author a USING (author_key)
WHERE f.story_id < 99000000
GROUP BY a.author_name ORDER BY total_points DESC, a.author_name LIMIT 10;

DROP TABLE _checks;