BEGIN;


SET search_path = pg_temp, pgtap;

SELECT pgtap.plan(19);


-- results_eq( cursor, cursor, description )
CREATE OR REPLACE FUNCTION pg_temp.results_approx_eq( refcursor, refcursor, json, text )
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
        IF have_found <> want_found OR NOT pg_temp.records_approx_eq(have_rec, want_rec, $3)
        THEN
            RETURN ok( false, $4 ) || E'\n' || diag(
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

    RETURN ok( true, $4 );
EXCEPTION
    WHEN datatype_mismatch THEN
        RETURN ok( false, $4 ) || E'\n' || diag(
            E'    Number of columns or their types differ between the queries' ||
            CASE WHEN have_rec::TEXT = want_rec::text THEN '' ELSE E':\n' ||
                '        have: ' || CASE WHEN have_found THEN have_rec::text ELSE 'NULL' END || E'\n' ||
                '        want: ' || CASE WHEN want_found THEN want_rec::text ELSE 'NULL' END
            END
        );
END;
$$ LANGUAGE plpgsql;

-- results_eq( sql, sql, description )
CREATE OR REPLACE FUNCTION pg_temp.results_approx_eq( TEXT, TEXT, json, TEXT )
RETURNS TEXT AS $$
DECLARE
    have REFCURSOR;
    want REFCURSOR;
    res  TEXT;
BEGIN
    OPEN have FOR EXECUTE _query($1);
    OPEN want FOR EXECUTE _query($2);
    res :=pg_temp.results_approx_eq(have, want, $3, $4);
    CLOSE have;
    CLOSE want;
    RETURN res;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_temp.results_approx_eq( TEXT, TEXT, json )
RETURNS TEXT AS $$
    SELECT pg_temp.results_approx_eq( $1, $2, $3, ''::TEXT );
$$ LANGUAGE sql;

CREATE FUNCTION pg_temp.values_approx_eq(numeric, numeric, numeric)
    RETURNS boolean
    AS $$
    SELECT
        ABS($1 - $2) <= $3;
$$
LANGUAGE SQL
IMMUTABLE;

CREATE FUNCTION pg_temp.values_approx_eq(TEXT, TEXT, TEXT)
    RETURNS boolean
    AS $$
    SELECT
        pg_temp.values_approx_eq($1::numeric, $2::numeric, $3::numeric);
$$
LANGUAGE SQL
IMMUTABLE;

CREATE OR REPLACE FUNCTION pg_temp.records_approx_eq (r1 record, r2 record, tolerances json)
    RETURNS BOOLEAN
    AS $$
DECLARE
    j1key text;
    j2key text;
    val1 text;
    val2 text;
    tolerance text;
BEGIN
    -- Notice because I'm casting to TEXT that a record
    -- like ('0', 0) should equal (0, 0).
    IF r1::text = r2::text
    THEN
        return TRUE;
    END IF;
    FOR j1key, j2key, val1, val2, tolerance IN
    SELECT
        j1.key j1key,
        j2.key j2key,
        j1.value,
        j2.value,
        tolerances.value
    FROM
        json_each_text(row_to_json(r1)) j1
        full outer join json_each(row_to_json(r2)) j2 on j1.key = j2.key
        left join json_each(tolerances) tolerances on j1.key = tolerances.key
        LOOP
            IF j1key IS DISTINCT FROM j2key
            THEN
                RETURN FALSE;
            END IF;
            IF tolerance IS NULL
            THEN
                IF val1::text IS DISTINCT FROM val2::text
                THEN
                    RETURN FALSE;
                ELSE
                END IF;
            ELSE
                -- Did we indicate that we ought to ignore this column?
                IF tolerance = 'null'
                THEN
                    CONTINUE;
                END IF;
                IF NOT pg_temp.values_approx_eq(val1, val2, tolerance)
                THEN
                    RETURN FALSE;
                ELSE
                END IF;
            END IF;
        END LOOP;
    RETURN TRUE;
END;
$$
LANGUAGE plpgsql;



SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(5, 5, 0)$$,
    ARRAY[TRUE],
    'values_approx_eq should return true when values are exactly equal (zero tolerance)'
);

SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(5, 5, 1)$$,
    ARRAY[TRUE],
    'values_approx_eq should return true when values are exactly equal (non-zero tolerance)'
);

SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(6, 5, 1)$$,
    ARRAY[TRUE],
    'values_approx_eq should return true when values are within tolerance (lhs > rhs)'
);
SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(6, 7, 1)$$,
    ARRAY[TRUE],
    'values_approx_eq should return true when values are within tolerance (lhs < rhs)'
);

SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(9, 7, 1)$$,
    ARRAY[FALSE],
    'values_approx_eq should return false when values are outside of tolerance (lhs > rhs)'
);
SELECT results_eq(
    $$SELECT pg_temp.values_approx_eq(9, 11, 1)$$,
    ARRAY[FALSE],
    'values_approx_eq should return false when values are outside of tolerance (lhs < rhs)'
);

SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, 0.1), (43, 0.1), '{}'::json)$$,
    ARRAY[TRUE],
    'records_approx_eq should return true when records are exactly equal'
);
SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, 0.1, 0.001), (43, 0.1, 0.002), '{"f3": 0.001}'::json)$$,
    ARRAY[TRUE],
    'records_approx_eq should return true when records are within the tolerance'
);
SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, '0.1', 0.001), (43, 0.1, 0.002), '{"f3": 0.001}'::json)$$,
    ARRAY[TRUE],
    'records_approx_eq should return true when records have same text representation and are within the tolerance'
);
SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, '0.1', 0.001, 'foo'), (43, 0.1, 0.002, 'bar'), '{"f3": 0.001, "f4": null}'::json)$$,
    ARRAY[TRUE],
    'records_approx_eq should return true when records have a column that differs but is ignored'
);
SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, 0.1, 0.001), (43, 0.1, 0.003), '{"f3": 0.001}'::json)$$,
    ARRAY[FALSE],
    'records_approx_eq should return false when records are not within the tolerance'
);
SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, 0.1, 0.001, 5), (43, 0.1, 0.001), '{"f3": 0.001}'::json)$$,
    ARRAY[FALSE],
    'records_approx_eq should return false when records have different number columns'
);

SELECT results_eq(
    $$SELECT pg_temp.records_approx_eq((43, 'x', 0.001), (43, 0.1, 0.001), '{"f3": 0.001}'::json)$$,
    ARRAY[FALSE],
    'records_approx_eq should return false when records have different column types'
);

SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,1), (2, 2)) vals(a, b)$$,
        $$select * from (values (1,1), (2, 2)) vals(a, b)$$,
        '{}'::json
    ),
    true,
    'results_approx_eq, when results are equal'
);

SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,1), (2, 2)) vals(a, b)$$,
        $$select * from (values (1,1), (2, 3)) vals(a, b)$$,
        '{}'::json
    ),
    false,
    'results_approx_eq, when results are not equal'
);


SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,1), (2, 2)) vals(a, b)$$,
        $$select * from (values (2,1), (1, 2)) vals(a, b)$$,
        '{"a": 1}'::json
    ),
    true,
    'results_approx_eq, when results are within tolerance'
);

SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,1), (2, 2)) vals(a, b)$$,
        $$select * from (values (3,1), (1, 2)) vals(a, b)$$,
        '{"a": 1}'::json
    ),
    false,
    'results_approx_eq, when results are not within tolerance'
);

SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,50), (1, 10)) vals(a, b)$$,
        $$select * from (values (2,55), (0, 5)) vals(a, b)$$,
        json_build_object('a', 1, 'b', 5)
    ),
    true,
    'results_approx_eq, when results are within tolerance (two cols)'
);

SELECT * FROM check_test(
    pg_temp.results_approx_eq(
        $$select * from (values (1,50), (1, 10)) vals(a, b)$$,
        $$select * from (values (2,55), (0, 5)) vals(a, b)$$,
        json_build_object('a', 1, 'b', 4)
    ),
    false,
    'results_approx_eq, when results are not within tolerance (two cols)'
);

SELECT
    *
FROM
    finish ();
ROLLBACK;


