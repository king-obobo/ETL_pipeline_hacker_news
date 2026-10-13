-- 03_clean.sql
-- stg.hn_stories_raw (latest run that has staging rows)  ->  clean.hn_stories + etl.rejected_records
-- Run with:  psql -v ON_ERROR_STOP=1 -f /sql/03_clean.sql      (single transaction)
--
-- 1. validate: invalid rows -> etl.rejected_records (reason + original payload)
-- 2. clean: types, trim, HTML-unescape title, story_type from _tags, row_hash
-- 3. dedup: within the run (latest updated_at wins), then against clean via UPSERT
-- Idempotent: re-running on the same run changes nothing in clean; that run's rejects
-- are deleted and re-created, so they are never duplicated.
-- Sets records_rejected on batch_log / pipeline_runs. records_loaded and closing the
-- run are left to 04_transform.

BEGIN;

-- ---------------------------------------------------------------
-- Helper functions
-- ---------------------------------------------------------------
-- NULL instead of an error for bad dates. Only ISO-8601 timestamps are accepted, so
-- Postgres specials such as 'now' or 'yesterday' are not.
CREATE OR REPLACE FUNCTION etl.try_timestamptz(t TEXT) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF t IS NULL
        OR t !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})$'
    THEN
        RETURN NULL;
    END IF;
    RETURN t::timestamptz;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- Decodes numeric (&#39; &#x27;) and the common named entities (amp lt gt quot apos nbsp).
-- Single left-to-right pass, semicolon-terminated entities only; anything else is left as is.
CREATE OR REPLACE FUNCTION etl.html_unescape(t TEXT) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    rest TEXT := t;
    acc  TEXT := '';
    m    TEXT[];
    body TEXT;
    tok  TEXT;
    pos  INT;
    rep  TEXT;
    cp   INT;
BEGIN
    IF t IS NULL OR strpos(t, '&') = 0 THEN
        RETURN t;
    END IF;
    LOOP
        m := regexp_match(rest, '&(#[xX][0-9a-fA-F]{1,6}|#[0-9]{1,7}|[A-Za-z]+);');
        EXIT WHEN m IS NULL;
        body := m[1];
        tok  := '&' || body || ';';
        pos  := strpos(rest, tok);
        rep  := NULL;
        cp   := NULL;
        IF left(body, 2) IN ('#x', '#X') THEN
            cp := ('x' || lpad(substr(body, 3), 8, '0'))::bit(32)::int;
        ELSIF left(body, 1) = '#' THEN
            cp := substr(body, 2)::int;
        ELSE
            rep := CASE body
                WHEN 'amp'  THEN '&'
                WHEN 'lt'   THEN '<'
                WHEN 'gt'   THEN '>'
                WHEN 'quot' THEN '"'
                WHEN 'apos' THEN ''''
                WHEN 'nbsp' THEN chr(160)
            END;
        END IF;
        IF cp BETWEEN 1 AND 1114111 AND cp NOT BETWEEN 55296 AND 57343 THEN
            rep := chr(cp);
        END IF;
        acc  := acc || left(rest, pos - 1) || COALESCE(rep, tok);
        rest := substr(rest, pos + length(tok));
    END LOOP;
    RETURN acc || rest;
END
$$;

-- ---------------------------------------------------------------
-- 1. Validate: classify every staging row of the run
-- ---------------------------------------------------------------
CREATE TEMP TABLE _run ON COMMIT DROP AS
SELECT max(run_id) AS run_id FROM stg.hn_stories_raw;

CREATE TEMP TABLE _stg ON COMMIT DROP AS
SELECT s.stg_id, s.run_id, s.batch_id, s.object_id, s.title, s.url, s.author,
        s.points, s.num_comments, s.created_at, s.updated_at, s.payload,
        CASE
        WHEN s.object_id IS NULL OR btrim(s.object_id) = ''          THEN 'missing objectID'
        WHEN s.object_id !~ '^[0-9]{1,18}$'                          THEN 'non-integer objectID'
        WHEN etl.try_timestamptz(s.created_at) IS NULL               THEN 'unparseable created_at'
        WHEN s.points ~ '^-[0-9]+$'                                  THEN 'negative points'
        WHEN s.points IS NOT NULL AND s.points !~ '^[0-9]{1,9}$'     THEN 'non-integer points'
        WHEN s.num_comments ~ '^-[0-9]+$'                            THEN 'negative num_comments'
        WHEN s.num_comments IS NOT NULL AND s.num_comments !~ '^[0-9]{1,9}$'
                                                                    THEN 'non-integer num_comments'
        WHEN jsonb_typeof(s.payload -> '_tags') IS DISTINCT FROM 'array'
                                                                    THEN 'unknown story_type: _tags missing or not an array'
    END AS reason
FROM stg.hn_stories_raw s
JOIN _run r ON r.run_id = s.run_id;

-- ---------------------------------------------------------------
-- 2. Clean the valid rows
-- ---------------------------------------------------------------
CREATE TEMP TABLE _valid ON COMMIT DROP AS
SELECT c.*,
    md5(jsonb_build_array(c.title, c.url, c.author, c.points, c.num_comments, c.story_type)::text) AS row_hash
FROM (
    SELECT stg_id, run_id, batch_id,
        object_id::bigint                                   AS object_id,
        nullif(btrim(etl.html_unescape(title)), '')         AS title,
        nullif(btrim(url), '')                              AS url,
        nullif(btrim(author), '')                           AS author,
        points::int                                         AS points,
        num_comments::int                                   AS num_comments,
        etl.try_timestamptz(created_at)                     AS created_at,
        etl.try_timestamptz(updated_at)                     AS updated_at,   -- bad value -> NULL, not a reject
        CASE WHEN payload -> '_tags' ? 'show_hn' THEN 'show_hn'
                WHEN payload -> '_tags' ? 'ask_hn'  THEN 'ask_hn'
                ELSE 'regular' END                             AS story_type
    FROM _stg
    WHERE reason IS NULL
) c;

-- ---------------------------------------------------------------
-- 3a. Dedup inside the run: latest updated_at wins (then latest stg_id)
-- ---------------------------------------------------------------
CREATE TEMP TABLE _best ON COMMIT DROP AS
SELECT DISTINCT ON (object_id) *
FROM _valid
ORDER BY object_id, updated_at DESC NULLS LAST, stg_id DESC;

-- ---------------------------------------------------------------
-- 3b. Reject log, metadata, UPSERT into clean
-- ---------------------------------------------------------------
CREATE TEMP TABLE _counts (ord INT, metric TEXT, n BIGINT) ON COMMIT DROP;

DO $$
DECLARE
    v_run    BIGINT;
    n_staged INT;
    n_rej    INT;
    n_valid  INT;
    n_best   INT;
    n_ins    INT;
    n_upd    INT;
BEGIN
    SELECT run_id INTO v_run FROM _run;
    IF v_run IS NULL THEN
        RAISE EXCEPTION 'no staging rows to clean';
    END IF;

    SELECT count(*), count(*) FILTER (WHERE reason IS NOT NULL) INTO n_staged, n_rej FROM _stg;
    SELECT count(*) INTO n_valid FROM _valid;
    SELECT count(*) INTO n_best  FROM _best;

    -- rejects (delete + re-insert keeps re-runs from duplicating them)
    DELETE FROM etl.rejected_records WHERE run_id = v_run;
    INSERT INTO etl.rejected_records (run_id, batch_id, object_id, raw_payload, reason)
    SELECT run_id, batch_id, object_id, COALESCE(payload, '{}'::jsonb), reason
    FROM _stg WHERE reason IS NOT NULL;

    UPDATE etl.batch_log b
        SET records_rejected = (SELECT count(*) FROM _stg s
                                WHERE s.batch_id = b.batch_id AND s.reason IS NOT NULL)
        WHERE b.run_id = v_run;
    UPDATE etl.pipeline_runs SET records_rejected = n_rej WHERE run_id = v_run;

    -- upsert: insert new; update only if the content changed AND the incoming row is not older
    WITH up AS (
        INSERT INTO clean.hn_stories AS c
                (object_id, title, url, author, points, num_comments, created_at, updated_at,
                story_type, row_hash, first_run_id, last_run_id)
        SELECT object_id, title, url, author, points, num_comments, created_at, updated_at,
                story_type, row_hash, run_id, run_id
        FROM _best
        ON CONFLICT (object_id) DO UPDATE
            SET title = EXCLUDED.title, url = EXCLUDED.url, author = EXCLUDED.author,
                points = EXCLUDED.points, num_comments = EXCLUDED.num_comments,
                updated_at = EXCLUDED.updated_at, story_type = EXCLUDED.story_type,
                row_hash = EXCLUDED.row_hash, last_run_id = EXCLUDED.last_run_id,
                modified_at = now()
        WHERE c.row_hash IS DISTINCT FROM EXCLUDED.row_hash
        AND COALESCE(EXCLUDED.updated_at, '-infinity') >= COALESCE(c.updated_at, '-infinity')
        RETURNING (xmax = 0) AS inserted
    )
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted)
        INTO n_ins, n_upd FROM up;

    INSERT INTO _counts VALUES
        (1, 'run_id cleaned',                          v_run),
        (2, 'staged rows',                             n_staged),
        (3, 'rejected',                                n_rej),
        (4, 'valid rows',                              n_valid),
        (5, 'duplicates collapsed inside the run',     n_valid - n_best),
        (6, 'distinct stories',                        n_best),
        (7, 'inserted into clean',                     n_ins),
        (8, 'updated in clean (content changed)',      n_upd),
        (9, 'unchanged / skipped (same hash or older)', n_best - n_ins - n_upd);
END
$$;

-- ---------------------------------------------------------------
-- Summary and checks (every ok should be true)
-- ---------------------------------------------------------------
SELECT metric, n FROM _counts ORDER BY ord;

SELECT reason, count(*) AS n FROM etl.rejected_records
WHERE run_id = (SELECT run_id FROM _run) GROUP BY reason ORDER BY reason;

SELECT 'staged = valid + rejected' AS check_name,
    (SELECT n FROM _counts WHERE ord = 2)
    = (SELECT n FROM _counts WHERE ord = 4) + (SELECT n FROM _counts WHERE ord = 3) AS ok
UNION ALL
SELECT 'every distinct valid story exists in clean',
    NOT EXISTS (SELECT 1 FROM _best b LEFT JOIN clean.hn_stories c USING (object_id)
        WHERE c.object_id IS NULL)
UNION ALL
SELECT 'rejected_records rows = rejected count',
    (SELECT count(*) FROM etl.rejected_records WHERE run_id = (SELECT run_id FROM _run))
        = (SELECT n FROM _counts WHERE ord = 3)
UNION ALL
SELECT 'batch_log rejects sum = run rejects',
    COALESCE((SELECT sum(records_rejected) FROM etl.batch_log WHERE run_id = (SELECT run_id FROM _run)), 0)
        = (SELECT records_rejected FROM etl.pipeline_runs WHERE run_id = (SELECT run_id FROM _run));

COMMIT;