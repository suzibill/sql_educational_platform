/* ex01 */
CREATE OR REPLACE FUNCTION fnc_part3_ex01()
    RETURNS TABLE
            (
                Peer1        varchar,
                Peer2        varchar,
                PointsAmount integer
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT t_p1.checking_peer                                              AS Peer1,
               t_p1.checked_peer                                               AS Peer2,
               coalesce(t_p1.point_amount, 0) - coalesce(t_p2.point_amount, 0) AS PointsAmount
        FROM transferred_points AS t_p1
                 FULL JOIN transferred_points AS t_p2
                           ON t_p1.checking_peer = t_p2.checked_peer AND t_p2.checking_peer = t_p1.checked_peer
        WHERE t_p1.id > t_p2.id
           OR t_p2.id IS NULL;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_part3_ex01();

CREATE OR REPLACE VIEW check_res AS
SELECT checks.id,
       checks.peer,
       checks.task,
       verter.state_ AS verter_state,
       p2p.state_    AS p2p_state
FROM checks
         JOIN p2p ON p2p.check_ = checks.id
    AND (p2p.state_ = 'Success' OR p2p.state_ = 'Failure')
         LEFT JOIN verter ON verter.check_ = checks.id
    AND (verter.state_ = 'Success' OR verter.state_ = 'Failure' OR verter.state_ = NULL);

/* ex02 */
CREATE OR REPLACE FUNCTION fnc_part3_ex02()
    RETURNS TABLE
            (
                Peer varchar,
                Task varchar,
                xp   bigint
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT check_res.peer AS Peer,
               check_res.task AS Task,
               xp.xp_amount   AS XP
        FROM check_res
                 JOIN xp ON xp.check_ = check_res.id
        WHERE check_res.p2p_state = 'Success'
          AND (check_res.verter_state = 'Success' OR check_res.verter_state IS NULL);
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_part3_ex02();

/* ex03 */
CREATE OR REPLACE FUNCTION fnc_part3_ex03(pdate date)
    RETURNS TABLE
            (
                peer varchar
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT time_tracking.peer
        FROM time_tracking
        WHERE date_ = pdate
          AND state_ = 2
        GROUP BY time_tracking.peer
        HAVING COUNT(state_) = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_part3_ex03('2023-02-01');

/* ex04 */
CREATE OR REPLACE PROCEDURE prc_part3_ex04(
    IN res_checks REFCURSOR = 'r_cur_part3_ex4'
) AS
$$
DECLARE
    f_COUNT   integer := (SELECT COUNT(id)
                          FROM check_res
                          WHERE check_res.p2p_state = 'Failure'
                             OR check_res.verter_state = 'Failure');
    all_COUNT integer := (SELECT COUNT(id)
                          FROM check_res);
    s_COUNT   integer= ALL_COUNT - f_COUNT;

BEGIN
    OPEN res_checks FOR SELECT ROUND((f_COUNT * 100 / all_COUNT)::numeric, 0) AS UnsuccessfulChecks,
                               ROUND((s_COUNT * 100 / all_COUNT)::numeric, 0) AS SuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex04();
FETCH ALL FROM "r_cur_part3_ex4";
END;

/* ex05 */
CREATE OR REPLACE PROCEDURE prc_part3_ex05(
    IN res_checks REFCURSOR = 'r_cur_part3_ex5'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT total.nickname AS Peer, total.points_total - minus.points_total AS PointsChange
        FROM ((SELECT peers.nickname, SUM(coalesce(transferred_points.point_amount, 0)) AS points_total
               FROM peers
                        LEFT JOIN transferred_points ON transferred_points.checking_peer = peers.nickname
               GROUP BY peers.nickname) AS total
            JOIN (SELECT peers.nickname, SUM(coalesce(transferred_points.point_amount, 0)) AS points_total
                  FROM peers
                           LEFT JOIN transferred_points ON transferred_points.checked_peer = peers.nickname
                  GROUP BY peers.nickname) AS minus ON total.nickname = minus.nickname)
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex05();
FETCH ALL FROM "r_cur_part3_ex5";
END;

/* ex06 */
CREATE OR REPLACE PROCEDURE prc_part3_ex06(
    IN res_checks REFCURSOR = 'r_cur_part3_ex6'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT total.nickname AS Peer, total.points_total - minus.points_total AS PointsChange
        FROM ((SELECT peers.nickname, SUM(coalesce(fnc_ex_1_p.PointsAmount, 0)) AS points_total
               FROM peers
                        LEFT JOIN (SELECT * FROM fnc_part3_ex01()) AS fnc_ex_1_p ON peers.nickname = fnc_ex_1_p.Peer1
               GROUP BY peers.nickname) AS total
            JOIN (SELECT peers.nickname, SUM(coalesce(fnc_ex_1_p.PointsAmount, 0)) AS points_total
                  FROM peers
                           LEFT JOIN (SELECT * FROM fnc_part3_ex01()) AS fnc_ex_1_p
                                     ON peers.nickname = fnc_ex_1_p.Peer2
                  GROUP BY peers.nickname) AS minus
              ON total.nickname = minus.nickname)
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex06();
FETCH ALL FROM "r_cur_part3_ex6";
END;

CREATE OR REPLACE VIEW COUNT_c AS
SELECT checks.date_,
       checks.task,
       COUNT(id) AS COUNT_checks
FROM checks
GROUP BY date_, task
ORDER BY checks.date_;

CREATE OR REPLACE VIEW maxi AS
SELECT COUNT_c.date_,
       MAX(COUNT_c.COUNT_checks) AS max_COUNT
FROM COUNT_c
GROUP BY COUNT_c.date_;

/* ex07 */
CREATE OR REPLACE PROCEDURE prc_part3_ex07(
    IN res_checks REFCURSOR = 'r_cur_part3_ex7'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT COUNT_c.date_ AS day,
               COUNT_c.task  AS Task
        FROM COUNT_c
                 LEFT JOIN maxi ON maxi.date_ = COUNT_c.date_
        WHERE maxi.max_COUNT = COUNT_c.COUNT_checks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex07();
FETCH ALL FROM "r_cur_part3_ex7";
END;

/* ex08 */
CREATE OR REPLACE PROCEDURE prc_part3_ex08(
    IN res_checks REFCURSOR = 'r_cur_part3_ex8'
) AS
$$
DECLARE
    number_of_check bigint := (SELECT p2p_cpy.check_
                               FROM p2p p2p_cpy
                                        LEFT JOIN p2p ON p2p_cpy.check_ = p2p.check_
                               WHERE p2p_cpy.state_ = 'Start'
                                 AND (p2p.state_ = 'Success' OR p2p.state_ = 'Failure')
                               ORDER BY p2p_cpy.check_ DESC
                               LIMIT 1);
    start_check     time   := (SELECT time_
                               FROM p2p
                               WHERE check_ = number_of_check
                                 AND state_ = 'Start');
    END_check       time   := (SELECT time_
                               FROM p2p
                               WHERE check_ = number_of_check
                                 AND (state_ = 'Success' OR state_ = 'Failure'));
BEGIN
    OPEN res_checks FOR
        SELECT END_check - start_check AS time_to_check;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex08();
FETCH ALL FROM "r_cur_part3_ex8";
END;

/* ex09 */
CREATE OR REPLACE PROCEDURE prc_part3_ex09(
    IN branch varchar,
    IN res_checks REFCURSOR = 'r_cur_part3_ex9'
) AS
$$
DECLARE
    branch_task_COUNT int := (SELECT COUNT(title)
                              FROM tasks
                              WHERE title ~ ('^' || branch || '[0-9]'));
BEGIN
    OPEN res_checks FOR
        WITH complete_tasks AS (SELECT DISTINCT ON (checks.Peer,checks.task) checks.peer,
                                                                             checks.task,
                                                                             checks.date_
                                FROM checks
                                         INNER JOIN verter ON verter.check_ = checks.id
                                         INNER JOIN p2p ON p2p.check_ = checks.id
                                WHERE checks.task ~ ('^' || branch || '[0-9]')
                                  AND p2p.state_ = 'Success'
                                  AND (verter.state_ = 'Success' OR verter.state_ = NULL)
                                ORDER BY checks.peer, checks.task, checks.date_ DESC),
             COUNT_uniq AS (SELECT peer        AS Peer,
                                   MAX(date_)  AS Day,
                                   COUNT(peer) AS c_u
                            FROM complete_tasks
                            GROUP BY Peer)
        SELECT COUNT_uniq.Peer,
               COUNT_uniq.Day
        FROM COUNT_uniq
        WHERE c_u = branch_task_COUNT
        ORDER BY COUNT_uniq.Day DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex09('C');
FETCH ALL FROM "r_cur_part3_ex9";
END;

CREATE OR REPLACE VIEW a_f AS
(
SELECT DISTINCT ON (a_f.peer1, a_f.peer2) *
FROM ((SELECT peer1, peer2
       FROM friends)
      UNION ALL
      (SELECT peer2, peer1
       FROM friends)) AS a_f
    );

CREATE OR REPLACE VIEW total_recom AS
(
SELECT a_f.peer1,
       recommendations.recommended_peer,
       COUNT(recommendations.recommended_peer) AS c_r
FROM a_f
         LEFT JOIN recommendations ON a_f.peer2 = recommendations.peer
WHERE a_f.peer1 <> recommendations.recommended_peer
GROUP BY a_f.peer1, recommendations.recommended_peer
ORDER BY peer1
    );

/* ex10 */
CREATE OR REPLACE PROCEDURE prc_part3_ex10(
    IN res_checks REFCURSOR = 'r_cur_part3_ex10'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT DISTINCT ON (total_recom.peer1) total_recom.peer1            AS Peer,
                                               total_recom.recommended_peer AS RecommendedPeer
        FROM total_recom;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex10();
FETCH ALL FROM "r_cur_part3_ex10";
END;

/* ex11 */
CREATE OR REPLACE PROCEDURE prc_part3_task11(
    ref REFCURSOR,
    IN block_one varchar,
    IN block_two varchar
) as
$$
DECLARE
    COUNT_peers int := (SELECT COUNT(*)
                        FROM peers);
BEGIN
    OPEN ref FOR
        WITH t_b1 AS (SELECT checks.peer AS ch1
                      FROM checks
                      WHERE checks.task ~ ('^' || block_one || '[0-9]')
                      GROUP BY ch1),
             t_b2 AS (SELECT checks.peer AS ch2
                      FROM checks
                      WHERE checks.task ~ ('^' || block_two || '[0-9]')
                      GROUP BY ch2),
             t_b_all AS (SELECT *
                         FROM t_b1
                         INTERSECT
                         SELECT *
                         FROM t_b2),
             t_b_not_started AS (SELECT nickname
                                 FROM peers
                                 EXCEPT
                                 (SELECT *
                                  FROM t_b1
                                  UNION
                                  SELECT *
                                  FROM t_b2))
        SELECT ROUND(
                           ((SELECT COUNT(*) * 100
                             FROM t_b1) / COUNT_peers) - (SELECT COUNT(*) * 100
                                                          FROM t_b_all) / COUNT_peers, 0) AS StartedBlock1,
               ROUND(
                           ((SELECT COUNT(*) * 100
                             FROM t_b2) / COUNT_peers) - (SELECT COUNT(*) * 100
                                                          FROM t_b_all) / COUNT_peers, 0) AS StartedBlock2,
               ROUND(
                       ((SELECT COUNT(*) * 100
                         FROM t_b_all) / COUNT_peers), 0)                                 AS StartedBothBlocks,
               ROUND(
                       ((SELECT COUNT(*) * 100
                         FROM t_b_not_started) / COUNT_peers), 0)                         AS DidntStartAnyBlock;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_task11('r_cur_part3_ex11', 'C', 'DO');
FETCH ALL IN r_cur_part3_ex11;
END;

/* ex12 */
CREATE OR REPLACE PROCEDURE prc_part3_ex12(ref REFCURSOR, COUNTs int DEFAULT 1) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peers.nickname   AS Peer,
               COUNT(a_f.peer2) AS friendsCOUNT
        FROM peers
                 LEFT JOIN a_f ON a_f.peer1 = peers.nickname
        GROUP BY peers.nickname
        ORDER BY friendsCOUNT DESC
        LIMIT COUNTs;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex12('r_cur_part3_ex12', 4);
FETCH ALL IN r_cur_part3_ex12;
END;

/* ex13 */
CREATE OR REPLACE PROCEDURE prc_part3_ex13(
    IN res_checks REFCURSOR = 'r_cur_part3_ex13'
) AS
$$
DECLARE
    fail    int := (SELECT COUNT(checks.id)
                    FROM checks
                             LEFT JOIN p2p ON p2p.check_ = checks.id
                             LEFT JOIN peers ON peers.nickname = checks.peer
                             LEFT JOIN verter ON checks.id = verter.check_
                    WHERE to_char(checks.date_, 'MM.DD') = to_char(peers.birthday, 'MM.DD')
                      AND (
                        p2p.state_ = 'Failure' OR verter.state_ = 'Failure'
                        ));
    success int := (SELECT COUNT(checks.id)
                    FROM checks
                             LEFT JOIN p2p ON p2p.check_ = checks.id
                             LEFT JOIN peers ON peers.nickname = checks.peer
                             LEFT JOIN verter ON checks.id = verter.check_
                    WHERE to_char(checks.date_, 'MM.DD') = to_char(peers.birthday, 'MM.DD')
                      AND (
                                p2p.state_ = 'Success' AND (verter.state_ = 'Success' OR verter.state_ IS NULL)
                        ));

BEGIN
    OPEN res_checks FOR SELECT ROUND((success * 100 / (success + fail))::numeric, 0) AS SuccessfulChecks,
                               ROUND((fail * 100 / (fail + success))::numeric, 0)    AS UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex13();
FETCH ALL FROM "r_cur_part3_ex13";
END;

CREATE OR REPLACE VIEW max_task_for_peer AS
(
SELECT checks.peer AS Peer,
       task
FROM xp
         LEFT JOIN checks ON checks.id = xp.check_
GROUP BY checks.task, checks.peer
    );

CREATE OR REPLACE VIEW max_xp_for_task AS
(
SELECT checks.peer,
       MAX(xp_amount) AS xp_max
FROM xp
         LEFT JOIN checks ON checks.id = xp.check_
GROUP BY checks.task, checks.peer
    );

/* ex14 */
CREATE OR REPLACE PROCEDURE prc_part3_ex14(
    IN res_checks REFCURSOR = 'r_cur_part3_ex14'
) AS
$$
BEGIN
    OPEN res_checks FOR SELECT peer        AS Peer,
                               SUM(xp_max) AS XP
                        FROM max_xp_for_task
                        GROUP BY Peer
                        ORDER BY XP DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex14();
FETCH ALL FROM "r_cur_part3_ex14";
END;

/* ex15 */
CREATE OR REPLACE PROCEDURE prc_part3_ex15(
    IN task1 varchar,
    IN task2 varchar,
    IN task3 varchar,
    IN res_checks REFCURSOR = 'r_cur_part3_ex15'
) AS
$$
BEGIN
    OPEN res_checks FOR
        WITH t1 AS (SELECT checks.peer
                    FROM xp
                             LEFT JOIN checks ON xp.check_ = checks.id
                    WHERE checks.task = task1
                       OR checks.task = task2
                    GROUP BY peer
                    HAVING COUNT(checks.task) = 2),
             t2 AS (SELECT DISTINCT checks.peer
                    FROM xp
                             LEFT JOIN checks ON xp.check_ = checks.id
                    WHERE checks.task = task3)
        SELECT peer
        FROM t1
        EXCEPT
        SELECT peer
        FROM t2;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex15(
        'C2_SimpleBashUtils',
        'C3_s21_string',
        'CPP6_3DViewer_v2_2'
    );
FETCH ALL FROM "r_cur_part3_ex15";
END;

/* ex16 */
CREATE OR REPLACE PROCEDURE prc_part3_ex16(
    IN res_checks REFCURSOR = 'r_cur_part3_ex16'
) AS
$$
BEGIN
    OPEN res_checks FOR
        WITH RECURSIVE COUNT_task_prev AS
                           ((SELECT tasks.title, 0 AS prev_COUNT FROM tasks WHERE parent_task = 'None')
                            UNION ALL
                            (SELECT tasks.title, prev_COUNT + 1
                             FROM tasks
                                      INNER JOIN COUNT_task_prev ON COUNT_task_prev.title = tasks.parent_task))
        SELECT *
        FROM COUNT_task_prev;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex16();
FETCH ALL FROM "r_cur_part3_ex16";
END;

CREATE OR REPLACE VIEW checks_all AS
(
SELECT checks.id,
       checks.date_,
       p2p.time_,
       CASE
           WHEN
               (
                       xp.xp_amount IS NULL OR
                       xp.xp_amount < tasks.max_xp * 0.8 OR
                       verter.state_ = 'Failure' OR
                       p2p.state_ = 'Failure'
                   ) THEN 0
           ELSE 1
           END AS success_check
FROM checks
         JOIN p2p ON p2p.check_ = checks.id AND p2p.state_ <> 'Start'
         LEFT JOIN verter ON verter.check_ = checks.id AND verter.state_ <> 'Start'
         LEFT JOIN xp ON xp.check_ = checks.id
         JOIN tasks ON tasks.title = checks.task
    );

/* ex17 */
CREATE OR REPLACE PROCEDURE prc_part3_ex17(
    IN numb int,
    IN res_checks REFCURSOR = 'r_cur_part3_ex17'
) AS
$$
BEGIN
    OPEN res_checks FOR
        WITH good_day AS (SELECT *,
                                 SUM(success_check) over (
                                     partition by date_
                                     ORDER BY
                                         date_,
                                         time_,
                                         id ROWS BETWEEN numb - 1 PRECEDING
                                         AND CURRENT ROW
                                     ) AS top_day
                          FROM checks_all)
        SELECT date_ AS Happy_Days
        FROM good_day
        GROUP BY date_
        HAVING MAX(top_day) >= numb;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex17(1);
FETCH ALL FROM "r_cur_part3_ex17";
END;

/* ex18 */
CREATE OR REPLACE PROCEDURE prc_part3_ex18(
    IN res_checks REFCURSOR = 'r_cur_part3_ex18'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT Peer,
               COUNT(task) AS XP
        FROM max_task_for_peer
        GROUP BY Peer
        ORDER BY XP DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex18();
FETCH ALL FROM "r_cur_part3_ex18";
END;

/* ex19 */
CREATE OR REPLACE PROCEDURE prc_part3_ex19(
    IN res_checks REFCURSOR = 'r_cur_part3_ex19'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT peer        AS Peer,
               SUM(xp_max) AS XP
        FROM max_xp_for_task
        GROUP BY Peer
        ORDER BY XP DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex19();
FETCH ALL FROM "r_cur_part3_ex19";
END;

/* ex20 */
CREATE OR REPLACE PROCEDURE prc_part3_ex20(
    IN res_checks REFCURSOR = 'r_cur_part3_ex20'
) AS
$$
BEGIN
    OPEN res_checks FOR
        WITH f AS
                 (WITH tt AS (SELECT peer, SUM(time_) AS t2
                              FROM time_tracking
                              WHERE state_ = '2'
                              GROUP BY peer)
                  SELECT t.peer, (t2 - SUM(time_)):: time AS time
                  FROM time_tracking t
                           JOIN tt ON tt.peer = t.peer
                  WHERE state_ = '1'
                    AND date_ = '2023-02-01'
                  GROUP BY t.peer, t2
                  ORDER BY time DESC
                  LIMIT 1)
        SELECT peer
        FROM f;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex20();
FETCH ALL FROM "r_cur_part3_ex20";
END;

/* ex21 */
CREATE OR REPLACE PROCEDURE prc_part3_ex21(
    IN n_COUNT integer,
    IN timing_ time,
    IN res_checks REFCURSOR = 'r_cur_part3_ex21'
) AS
$$
BEGIN
    OPEN res_checks FOR
        WITH n_peers AS (SELECT peer,
                                date_
                         FROM time_tracking
                         WHERE state_ = 1
                           AND time_ < timing_
                         GROUP BY peer, date_)
        SELECT peer AS Peer
        FROM n_peers
        GROUP BY peer
        HAVING (COUNT(peer)) >= n_COUNT;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex21(2, '12:15:18');
FETCH ALL FROM "r_cur_part3_ex21";
END;

/* ex22 */
CREATE OR REPLACE PROCEDURE prc_part3_ex22(
    IN m_COUNT integer,
    IN n_COUNT integer,
    IN res_checks REFCURSOR = 'r_cur_part3_ex22'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT peer
        FROM time_tracking tt
        WHERE tt.state_ = 2
          AND (current_date - tt.date_) <= n_COUNT
          AND NOT tt.time_ = (SELECT MAX(tt2.time_)
                              FROM time_tracking tt2
                              WHERE tt2.date_ = tt.date_
                                AND tt2.peer = tt.peer)
        GROUP BY peer
        HAVING (COUNT(peer)) > m_COUNT;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex22(1, 50);
FETCH ALL FROM "r_cur_part3_ex22";
END;

/* ex23 */
CREATE OR REPLACE PROCEDURE prc_part3_ex23(
    IN res_check REFCURSOR = 'r_cur_part3_ex23'
) AS
$$
BEGIN
    OPEN res_check FOR
        WITH t1 AS (SELECT t_t.peer
                    FROM time_tracking t_t
                    WHERE state_ = 1
                      AND t_t.date_ = current_date
                    GROUP BY t_t.peer, t_t.date_, t_t.time_
                    ORDER BY time_ DESC
                    LIMIT 1)
        SELECT *
        FROM t1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex23();
FETCH ALL FROM "r_cur_part3_ex23";
END;

ABORT TRANSACTION;

/* ex24 */
CREATE OR REPLACE PROCEDURE prc_part3_ex24(
    IN time_out_of_campus time,
    IN res_check REFCURSOR = 'r_cur_part3_ex24'
) AS
$$
DECLARE
    yesterday_date_ date := '2023-04-20';
BEGIN
    OPEN res_check FOR
        WITH in_campus AS (SELECT t_t.peer,
                                  t_t.time_,
                                  t_t.date_
                           FROM time_tracking t_t
                           WHERE t_t.state_ = 1
                             AND t_t.date_ = yesterday_date_
                             AND NOT t_t.time_ = (SELECT min(t_t1.time_)
                                                  FROM time_tracking t_t1
                                                  WHERE t_t.date_ = t_t1.date_
                                                    AND t_t.peer = t_t1.peer)
                           ORDER BY t_t.peer, t_t.time_),
             out_of_campus AS (SELECT t_t.peer,
                                      t_t.time_,
                                      t_t.date_
                               FROM time_tracking t_t
                               WHERE t_t.state_ = 2
                                 AND t_t.date_ = yesterday_date_
                                 AND NOT t_t.time_ = (SELECT MAX(t_t1.time_)
                                                      FROM time_tracking t_t1
                                                      WHERE t_t.date_ = t_t1.date_
                                                        AND t_t.peer = t_t1.peer)
                               ORDER BY t_t.peer, t_t.time_),
             peers_outs AS (SELECT o_c.peer,
                                   o_c.time_             AS out_time,
                                   o_c.date_,
                                   i_c.time_             AS in_time,
                                   i_c.time_ - o_c.time_ AS max_time_out
                            FROM out_of_campus o_c
                                     INNER JOIN in_campus i_c ON i_c.peer = o_c.peer)
        SELECT DISTINCT peer
        FROM peers_outs
        WHERE max_time_out > time_out_of_campus;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex24('00:01:00');
FETCH ALL FROM "r_cur_part3_ex24";
END;

/* ex25 */
CREATE OR REPLACE PROCEDURE prc_part3_ex25(
    IN res_checks REFCURSOR = 'r_cur_part3_ex25'
) AS
$$
BEGIN
    OPEN res_checks FOR
        SELECT Month, ROUND(SUM(earlyentries) * 100 / SUM(totalentries)) Early_Entries
        FROM (SELECT peer,
                     to_char(a.date_, 'Month')                                  AS Month,
                     COUNT(*)                                                   AS TotalEntries,
                     COUNT(CASE WHEN a.time_ < '12:00:00' THEN 1 ELSE NULL END) AS EarlyEntries
              FROM (SELECT peer, date_, time_ FROM time_tracking WHERE state_ = '1') a
                       JOIN peers p ON p.nickname = a.peer
              GROUP BY peer, Month, p.birthday, a.date_
              HAVING extract(month FROM a.date_) = extract(month FROM p.birthday)) b
        GROUP BY b.Month;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_part3_ex25();
FETCH ALL FROM "r_cur_part3_ex25";
END;
