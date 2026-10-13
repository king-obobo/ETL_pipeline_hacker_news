-- 02_load_staging.sql
-- Loads every /data/hn_*.json file into stg.hn_stories_raw: one batch per file.
-- Run with:  psql -f /sql/02_load_staging.sql   (autocommit mode; do NOT wrap in BEGIN,
--            the DO block commits once per file)
--
-- Method: pg_read_file() + jsonb_array_elements(). Needs superuser (the Docker image's
--         POSTGRES_USER is one) or the pg_read_server_files role.
-- Each file = one transaction: its batch_log row and staging rows commit together.
-- Staging is append-only: re-running creates a NEW run and appends again; dedup is
-- done in 03_clean. row_hash stays NULL here (computed in 03_clean).
-- The run stays 'running'; a later step closes it and fills records_loaded/rejected.
-- A failing file aborts the script: earlier files stay committed, the failed file's
-- batch rolls back, and the run is left 'running' (no handler: COMMIT and EXCEPTION
-- cannot share a block).

DO $$
DECLARE
    v_run     BIGINT;
    v_batch   BIGINT;
    v_files   TEXT[];
    f         TEXT;
    doc       JSONB;
    m         JSONB;
    n_meta    INTEGER;
    n_ret     INTEGER;
    n_arr     INTEGER;
    n_ins     INTEGER;
    v_total   INTEGER := 0;
    v_min     TIMESTAMPTZ;
    v_max     TIMESTAMPTZ;
BEGIN
    SELECT array_agg(d ORDER BY d) INTO v_files
    FROM pg_ls_dir('/data') AS d
    WHERE d ~ '^hn_.*\.json$';

    IF v_files IS NULL THEN
        RAISE EXCEPTION 'no hn_*.json files found in /data';
    END IF;

    INSERT INTO etl.pipeline_runs (run_type) VALUES ('full') RETURNING run_id INTO v_run;
    COMMIT;
    RAISE NOTICE 'run_id % : % file(s)', v_run, array_length(v_files, 1);

    FOREACH f IN ARRAY v_files LOOP
        doc := pg_read_file('/data/' || f)::jsonb;
        m   := doc -> 'meta';

        n_meta := (m ->> 'nbHits')::int;
        n_ret  := (m ->> 'hits_retrieved')::int;
        n_arr  := jsonb_array_length(doc -> 'hits');
        IF n_meta IS DISTINCT FROM n_ret OR n_ret IS DISTINCT FROM n_arr THEN
            RAISE EXCEPTION '%: count mismatch (nbHits=%, hits_retrieved=%, array=%)',
                            f, n_meta, n_ret, n_arr;
        END IF;

        INSERT INTO etl.batch_log (run_id, window_start, window_end, records_extracted)
        VALUES (v_run,
                (m ->> 'window_start_utc')::timestamptz,
                (m ->> 'window_end_utc')::timestamptz,
                n_meta)
        RETURNING batch_id INTO v_batch;

        INSERT INTO stg.hn_stories_raw
            (run_id, batch_id, object_id, title, url, author, points, num_comments,
            created_at, created_at_i, updated_at, tags, payload)
        SELECT v_run, v_batch,
            h ->> 'objectID', h ->> 'title', h ->> 'url', h ->> 'author',
            h ->> 'points', h ->> 'num_comments',
            h ->> 'created_at', h ->> 'created_at_i', h ->> 'updated_at',
            h ->> '_tags',
            h - '_highlightResult'
        FROM jsonb_array_elements(doc -> 'hits') AS h;
        GET DIAGNOSTICS n_ins = ROW_COUNT;

        IF n_ins <> n_meta THEN
            RAISE EXCEPTION '%: inserted % rows but nbHits=%', f, n_ins, n_meta;
        END IF;

        UPDATE etl.batch_log
            SET status = 'success', ended_at = now()
        WHERE batch_id = v_batch;
        COMMIT;

        v_total := v_total + n_ins;
        RAISE NOTICE 'staged % : % rows (batch %)', f, n_ins, v_batch;
    END LOOP;

    SELECT min(window_start), max(window_end) INTO v_min, v_max
    FROM etl.batch_log WHERE run_id = v_run;

    UPDATE etl.pipeline_runs
        SET range_start = v_min, range_end = v_max, records_extracted = v_total
    WHERE run_id = v_run;
    COMMIT;
END
$$;

-- ---------------------------------------------------------------
-- Checks on the latest run (every row should read ok = true)
-- ---------------------------------------------------------------
WITH r AS (SELECT max(run_id) AS run_id FROM etl.pipeline_runs WHERE run_type = 'full'),
per_batch AS (
    SELECT b.batch_id, b.records_extracted, count(s.*) AS staged
    FROM etl.batch_log b
    JOIN r USING (run_id)
    LEFT JOIN stg.hn_stories_raw s ON s.batch_id = b.batch_id
    GROUP BY b.batch_id, b.records_extracted
)
SELECT 'every batch: staged rows = nbHits' AS check_name, bool_and(staged = records_extracted) AS ok FROM per_batch
UNION ALL
SELECT 'all batches success', bool_and(status = 'success') FROM etl.batch_log JOIN r USING (run_id)
UNION ALL
SELECT 'run total = staged total',
    (SELECT records_extracted FROM etl.pipeline_runs JOIN r USING (run_id)) = (SELECT count(*) FROM stg.hn_stories_raw JOIN r USING (run_id))
UNION ALL
SELECT '_highlightResult absent from payload', NOT bool_or(payload ? '_highlightResult') FROM stg.hn_stories_raw JOIN r USING (run_id)
UNION ALL
SELECT 'object_id never null',
    bool_and(object_id IS NOT NULL) FROM stg.hn_stories_raw JOIN r USING (run_id)
UNION ALL
SELECT 'payload objectID = object_id',
    bool_and(payload ->> 'objectID' = object_id) FROM stg.hn_stories_raw JOIN r USING (run_id);