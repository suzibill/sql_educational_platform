CREATE TABLE IF NOT EXISTS table_1
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS table_2
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS table_3
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS test_1
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS test_2
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS test_3
(
    id     bigint PRIMARY KEY,
    string varchar NOT NULL
);


CREATE OR REPLACE FUNCTION fnc_table_ins() RETURNS TRIGGER AS
$$
BEGIN
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_table_upd() RETURNS TRIGGER AS
$$
BEGIN
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_test_ins() RETURNS TRIGGER AS
$$
BEGIN
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_test_upd() RETURNS TRIGGER AS
$$
BEGIN
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER trg_test_1
    AFTER INSERT
    ON table_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_table_ins();

CREATE TRIGGER trg_test_2
    AFTER UPDATE
    ON table_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_table_upd();

CREATE TRIGGER trg_test_3
    BEFORE INSERT
    ON table_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_table_ins();

CREATE TRIGGER trg_test_4
    AFTER INSERT
    ON test_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_test_ins();

CREATE TRIGGER trg_test_5
    AFTER UPDATE
    ON test_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_test_upd();

CREATE TRIGGER trg_test_6
    BEFORE INSERT
    ON test_1
    FOR EACH ROW
EXECUTE FUNCTION fnc_test_ins();

CREATE OR REPLACE FUNCTION fnc_table_test_1(IN n integer, OUT res integer) AS
$$
BEGIN
    SELECT 6 INTO n;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_table_test_2(IN n integer, IN c integer, OUT res integer) AS
$$
BEGIN
    SELECT 6 INTO n;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_table_test_1(IN n integer) RETURNS integer AS
$$
BEGIN
    SELECT 6 INTO n;
    RETURN 10;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE prc_delete_table() AS
$$
DECLARE
    table_name_del varchar ='test';
    row            record;
BEGIN
    FOR row IN
        (SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name LIKE format('%s%%', table_name_del))
        LOOP
            EXECUTE 'delete table ' || row.table_name || ' cascade';
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL prc_delete_table();

CREATE OR REPLACE PROCEDURE prc_part4_task2(
    INOUT results integer,
    IN ref REFCURSOR
) AS
$$
BEGIN
    OPEN ref FOR
        SELECT pc.proname,
               pg_catalog.pg_get_function_identity_arguments(pc.oid) AS pg_arg
        FROM pg_catalog.pg_namespace pn
                 JOIN pg_catalog.pg_proc pc ON pc.pronamespace = pn.oid
        WHERE pc.prokind = 'f'
          AND pg_catalog.pg_get_function_identity_arguments(pc.oid) IS NOT NULL
          AND pn.nspname = 'public';
    results := (SELECT COUNT(pc.proname)
                FROM pg_catalog.pg_namespace pn
                         JOIN pg_catalog.pg_proc pc ON pc.pronamespace = pn.oid
                WHERE pc.prokind = 'f'
                  AND pg_catalog.pg_get_function_identity_arguments(pc.oid) IS NOT NULL
                  AND pn.nspname = 'public');
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part4_task2(0, 'ref');
FETCH ALL IN "ref";
END;

SELECT * FROM information_schema.triggers;
SELECT * FROM pg_proc WHERE prosrc LIKE '%BEGIN%';

CREATE OR REPLACE PROCEDURE prc_part4_task3(
    INOUT results integer
) AS
$$
DECLARE
    row record;
BEGIN
    results = (SELECT COUNT(*)
               FROM information_schema.triggers);
    FOR row IN (SELECT trigger_name,
                       event_object_table
                FROM information_schema.triggers)
        LOOP
            EXECUTE format('drop trigger %s ON %s cascade', row.trigger_name, row.event_object_table);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL prc_part4_task3(NULL);

CREATE OR REPLACE PROCEDURE prc_part4_task4(
    IN param varchar,
    IN ref REFCURSOR
) as
$$
BEGIN
    OPEN ref FOR
        SELECT proname, prosrc
        FROM pg_proc
                 JOIN pg_namespace pn ON pg_proc.pronamespace = pn.oid
            AND nspname = 'public'
        WHERE prosrc LIKE '%' || param || '%';
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part4_task4('s', 'ref');
FETCH ALL IN "ref";
END;

CALL prc_part4_task4('BEGIN', 'ref');
