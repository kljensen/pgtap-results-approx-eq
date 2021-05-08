BEGIN;


SET search_path = pg_temp, pgtap;

SELECT pgtap.plan(1);

CREATE FUNCTION pg_temp.insert_thing (new_thing RECORD) RETURNS INTEGER AS $fun$
DECLARE
    inserted_id  INT;
BEGIN
    INSERT INTO things (name) VALUES (
        new_thing.name
        -- (plus 30 more columns)
    ) RETURNING id INTO inserted_id;
    RETURN inserted_id;
END;
$fun$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.identity (new_thing RECORD) RETURNS RECORD AS $fun$
BEGIN
    RETURN new_thing;
END;
$fun$ LANGUAGE plpgsql;

-- results_eq( cursor, cursor, description )
CREATE OR REPLACE FUNCTION pg_temp.results_close( refcursor, refcursor, text )
RETURNS TEXT AS $$
DECLARE
    have       ALIAS FOR $1;
    want       ALIAS FOR $2;
    have_rec   RECORD;
    want_rec   RECORD;
    have_found BOOLEAN;
    want_found BOOLEAN;
    rownum     INTEGER := 1;
BEGIN
    FETCH have INTO have_rec;
    have_found := FOUND;
    FETCH want INTO want_rec;
    want_found := FOUND;
    WHILE have_found OR want_found LOOP
        IF have_found <> want_found OR have_rec IS DISTINCT FROM want_rec OR pg_temp.identity(have_rec) IS DISTINCT FROM pg_temp.identity(want_rec)
        THEN
            RETURN ok( false, $3 ) || E'\n' || diag(
                '    Results differ beginning at row ' || rownum || E':\n' ||
                '        have: ' || CASE WHEN have_found THEN have_rec::text ELSE 'NULL' END || E'\n' ||
                '        want: ' || CASE WHEN want_found THEN want_rec::text ELSE 'NULL' END
            );
        END IF;
        rownum = rownum + 1;
        FETCH have INTO have_rec;
        have_found := FOUND;
        FETCH want INTO want_rec;
        want_found := FOUND;
    END LOOP;

    RETURN ok( true, $3 );
EXCEPTION
    WHEN datatype_mismatch THEN
        RETURN ok( false, $3 ) || E'\n' || diag(
            E'    Number of columns or their types differ between the queries' ||
            CASE WHEN have_rec::TEXT = want_rec::text THEN '' ELSE E':\n' ||
                '        have: ' || CASE WHEN have_found THEN have_rec::text ELSE 'NULL' END || E'\n' ||
                '        want: ' || CASE WHEN want_found THEN want_rec::text ELSE 'NULL' END
            END
        );
END;
$$ LANGUAGE plpgsql;

-- results_eq( sql, sql, description )
CREATE OR REPLACE FUNCTION pg_temp.results_close( TEXT, TEXT, TEXT )
RETURNS TEXT AS $$
DECLARE
    have REFCURSOR;
    want REFCURSOR;
    res  TEXT;
BEGIN
    OPEN have FOR EXECUTE _query($1);
    OPEN want FOR EXECUTE _query($2);
    res := results_eq(have, want, $3);
    CLOSE have;
    CLOSE want;
    RETURN res;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_temp.log_columns (r record)
    RETURNS void
    AS $$
DECLARE
    key text;
    val text;
BEGIN
    FOR key,
    val IN
    SELECT
        *
    FROM
        json_each_text(row_to_json(r))
        LOOP
            RAISE NOTICE '% % %', key, val, r.foo;
        END LOOP;
    RETURN;
END;
$$
LANGUAGE plpgsql;


SELECT pg_temp.results_close(
    $$VALUES ( 42, 0.12), (19, 10.3), (59, 1023.232)$$,
    $$VALUES ( 42, 0.12), (19, 10.3), (59, 1023.232)$$,
    'values should match approximately'
);

SELECT
    pg_temp.log_columns(vals)
FROM (
    VALUES (42, 0.12),
        (19, 10.3),
        (59, 1023.232)) AS vals (foo, bar);


SELECT
    *
FROM
    finish ();
ROLLBACK;


