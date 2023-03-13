/* PART 1 */
CREATE OR REPLACE PROCEDURE
    insert_p2p(checked_peer varchar, checking_peer__ varchar, task_ varchar, status state_type)
AS
$$
BEGIN
    IF status = 'Start' THEN
        INSERT INTO checks(id, peer, task, date_)
        VALUES ((SELECT MAX(id) + 1 FROM checks), checked_peer, task_, current_date);
    END IF;
    INSERT INTO p2p(id, check_, checking_peer, state_, time_)
    VALUES ((SELECT MAX(id) + 1 FROM p2p),
            (SELECT MAX(id) FROM checks WHERE peer = checked_peer AND task_ = task), checking_peer__, status,
            current_time);
END;
$$ LANGUAGE 'plpgsql';

CALL insert_p2p('Aboba', 'Amogus', 'C3_s21_string', 'Failure');

/* PART 2 */
CREATE OR REPLACE PROCEDURE
    insert_verter(nick varchar, task__ varchar, status state_type, time__ time)
AS
$$
BEGIN
    INSERT INTO verter(id, check_, state_, time_)
    VALUES ((SELECT MAX(id) + 1 FROM verter),
            (SELECT check_
             FROM p2p
                      JOIN checks c ON p2p.check_ = c.id
             WHERE peer = nick
               AND task = task__
               AND state_ = 'Success'
             ORDER BY check_ DESC
             LIMIT 1),
            status,
            time__);
END;
$$ LANGUAGE 'plpgsql';

CALL insert_verter('Aboba', 'C3_s21_string', 'Success', '22:00:01');

/* PART 3 */
CREATE OR REPLACE FUNCTION fnc_trg_transferred_points()
    RETURNS TRIGGER AS
$trg_trans$
BEGIN
    UPDATE transferred_points
    SET point_amount = point_amount + 1
    WHERE new.checking_peer = new.checking_peer
      AND checked_peer IN (SELECT peer FROM checks WHERE checks.id = new.check_);
    RETURN new;
END;
$trg_trans$ LANGUAGE 'plpgsql';

CREATE OR REPLACE TRIGGER trg_transferred_points
    AFTER INSERT
    ON p2p
    FOR EACH ROW
    WHEN (new.state_ = 'Start')
EXECUTE PROCEDURE fnc_trg_transferred_points();

CALL insert_p2p('Aboba', 'Sus', 'C3_s21_string', 'Start');

SELECT *
FROM transferred_points
WHERE checking_peer = 'Sus'
  AND checked_peer = 'Aboba';

/* PART 4 */
CREATE TRIGGER trg_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE fn_trg_xp();

CREATE OR REPLACE FUNCTION fn_trg_xp()
    RETURNS TRIGGER AS
$trg_xp$
BEGIN
    IF new.xp_amount > (SELECT max_xp
                        FROM tasks
                                 JOIN checks c ON c.id = new.check_
                        WHERE new.check_ = c.id
                        LIMIT 1)
    THEN
        RAISE NOTICE 'xp > max_xp';
        RETURN NULL;
    END IF;
    IF (SELECT state_ FROM p2p WHERE state_ = 'Success' AND check_ = new.check_) IS NULL
    THEN
        RAISE NOTICE 'p2p stage NOT finished';
        RETURN NULL;
    END IF;
    IF (SELECT state_ FROM verter WHERE check_ = new.check_ LIMIT 1) is NOT NULL
        AND (SELECT state_ FROM verter WHERE state_ = 'Success' AND check_ = new.check_) IS NULL
    THEN
        RAISE NOTICE 'verter stage NOT finished';
        RETURN NULL;
    END IF;
    RETURN new;
END;
$trg_xp$ LANGUAGE 'plpgsql';

INSERT INTO xp (id, check_, xp_amount) VALUES ((SELECT MAX(id) FROM xp) + 1, 23, 1500);

SELECT (SELECT state_ FROM p2p WHERE state_ = 'Success' AND check_ = 22) IS NOT NULL;
SELECT state_ FROM verter WHERE state_ = 'Success' AND check_ = 22;
