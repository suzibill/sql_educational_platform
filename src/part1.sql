CREATE TABLE
    peers
(
    nickname varchar PRIMARY KEY,
    birthday date NOT NULL
);

CREATE TABLE
    friends
(
    id    bigint PRIMARY KEY,
    peer1 varchar NOT NULL,
    peer2 varchar NOT NULL,
    CONSTRAINT fk_friends_peer1 FOREIGN KEY (peer1) REFERENCES peers (nickname),
    CONSTRAINT fk_friends_peer2 FOREIGN KEY (peer2) REFERENCES peers (nickname),
    CONSTRAINT check_peer_nick CHECK (peer1 <> peer2)
);

CREATE TABLE
    recommendations
(
    id               bigint PRIMARY KEY,
    peer             varchar NOT NULL,
    recommended_peer varchar NOT NULL,
    CONSTRAINT fk_recommendations_peer FOREIGN KEY (peer) REFERENCES peers (nickname),
    CONSTRAINT fk_recommendations_recommended_peer FOREIGN KEY (recommended_peer) REFERENCES peers (nickname),
    CONSTRAINT check_peer_recommended CHECK (peer <> recommended_peer)
);

CREATE TABLE
    transferred_points
(
    id            bigint PRIMARY KEY,
    checking_peer varchar NOT NULL,
    checked_peer  varchar NOT NULL,
    point_amount  int     NOT NULL,
    CONSTRAINT fk_friends_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers (nickname),
    CONSTRAINT fk_friends_checked_peer FOREIGN KEY (checked_peer) REFERENCES peers (nickname),
    CONSTRAINT check_peer_nick CHECK (checking_peer <> checked_peer)
);

CREATE TABLE
    time_tracking
(
    id     bigint PRIMARY KEY,
    peer   varchar NOT NULL,
    date_  date    NOT NULL,
    time_  time    NOT NULL,
    state_ integer NOT NULL,
    CONSTRAINT check_time_tracking_state CHECK (state_ IN ('1', '2')),
    CONSTRAINT fk_time_tracking_peer FOREIGN KEY (peer) REFERENCES peers (nickname)
);

CREATE TABLE
    tasks
(
    title       varchar PRIMARY KEY,
    parent_task varchar NOT NULL,
    max_xp      bigint  NOT NULL
);

CREATE TABLE
    checks
(
    id    bigint PRIMARY KEY,
    peer  varchar NOT NULL,
    task  varchar NOT NULL,
    date_ date    NOT NULL,
    CONSTRAINT fk_checks_peer FOREIGN KEY (peer) REFERENCES peers (nickname),
    CONSTRAINT fk_checks_task FOREIGN KEY (task) REFERENCES tasks (title)
);

CREATE type state_type AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE
    p2p
(
    id            bigint PRIMARY KEY,
    check_        bigint     NOT NULL,
    checking_peer varchar    NOT NULL,
    state_        state_type NOT NULL,
    time_         time       NOT NULL,
    CONSTRAINT fk_p2p_check_ FOREIGN KEY (check_) REFERENCES checks (id),
    CONSTRAINT fk_p2p_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers (nickname)
);

CREATE TABLE
    verter
(
    id     bigint PRIMARY KEY,
    check_ bigint     NOT NULL,
    state_ state_type NOT NULL,
    time_  time       NOT NULL,
    CONSTRAINT fk_verter_check_ FOREIGN KEY (check_) REFERENCES checks (id)
);

CREATE TABLE
    xp
(
    id        bigint PRIMARY KEY,
    check_    bigint NOT NULL,
    xp_amount bigint NOT NULL,
    CONSTRAINT fk_xp_check_ FOREIGN KEY (check_) REFERENCES checks (id)
);

CREATE OR REPLACE PROCEDURE import_FROM_csv() AS
$$
DECLARE
    import_path varchar   = '/Users/casimira/SQL2_Info21_v1.0-0/src/csv/';
    import_name varchar[] = ARRAY ['peers', 'friends', 'recommendations','transferred_points','time_tracking', 'tasks', 'checks','p2p', 'verter', 'xp'];
BEGIN
    FOR i IN 1..array_length(import_name, 1)
        LOOP
            EXECUTE FORmat('COPY %s FROM ''%s%s.csv'' DELIMITER '','' CSV', import_name[i], import_path,
                           import_name[i]);
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL import_FROM_csv();

CREATE OR REPLACE PROCEDURE export_to_csv() AS
$$
DECLARE
    export_path varchar   = '/Users/wilfredo/02/SQL2_Info21_v1.0-0/src/backup/';
    export_name varchar[] = ARRAY ['peers', 'friends', 'recommendations','transferred_points','time_tracking', 'tasks', 'checks','p2p', 'verter', 'xp'];
BEGIN
    FOR i IN 1..array_length(export_name, 1)
        LOOP
            EXECUTE FORmat('COPY %s TO ''%s%s.csv'' WITH DELIMITER '','' CSV', export_name[i], export_path,
                           export_name[i]);
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL export_to_csv();

