create table
    peers (
        nickname varchar primary key,
        birthday date not null
    );

COPY peers
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/peers.csv' DELIMITER ',' CSV;

-- \COPY peers FROM 'csv/peers.csv' DELIMITER ',' CSV;

create table
    friends (
        id bigint primary key,
        peer1 varchar not null,
        peer2 varchar not null,
        constraint fk_friends_peer1 foreign key (peer1) references peers(nickname),
        constraint fk_friends_peer2 foreign key (peer2) references peers(nickname),
        constraint check_peer_nick check(peer1 <> peer2)
    );

COPY friends
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/friends.csv' DELIMITER ',' CSV;

create table
    recommendations (
        id bigint primary key,
        peer varchar not null,
        recommended_peer varchar not null,
        constraint fk_recommendations_peer foreign key (peer) references peers(nickname),
        constraint fk_recommendations_recommended_peer foreign key (recommended_peer) references peers(nickname),
        constraint check_peer_recommended check(peer <> recommended_peer)
    );

COPY recommendations
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/recommendations.csv' DELIMITER ',' CSV;

create table
    transferred_points (
        id bigint primary key,
        checking_peer varchar not null,
        checked_peer varchar not null,
        point_amount int not null,
        constraint fk_friends_checking_peer foreign key (checking_peer) references peers(nickname),
        constraint fk_friends_checked_peer foreign key (checked_peer) references peers(nickname),
        constraint check_peer_nick check(checking_peer <> checked_peer)
    );

COPY transferred_points
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/transferred_points.csv' DELIMITER ',' CSV;

create table
    time_tracking (
        id bigint primary key,
        peer varchar not null,
        date_ date not null,
        time_ time not null,
        state_ integer not null,
        constraint check_time_tracking_state check (state_ in ('1', '2')),
        constraint fk_time_tracking_peer foreign key (peer) references peers(nickname)
    );

COPY time_tracking
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/time_tracking.csv' DELIMITER ',' CSV;

create table
    tasks (
        title varchar primary key,
        parent_task varchar not null,
        max_xp bigint not null
    );

COPY tasks
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/tasks.csv' DELIMITER ',' CSV;

create table
    checks (
        id bigint primary key,
        peer varchar not null,
        task varchar not null,
        date_ date not null,
        constraint fk_checks_peer foreign key (peer) references peers(nickname),
        constraint fk_checks_task foreign key (task) references tasks(title)
    );

COPY checks
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/checks.csv' DELIMITER ',' CSV;

-- truncate checks cascade;
-- drop table p2p cascade;
-- drop table verter cascade;
-- drop type state_type cascade;

create type state_type as enum ('Start', 'Success', 'Failure');
create table
    p2p (
        id bigint primary key,
        check_ bigint not null,
        checking_peer varchar not null,
        state_ state_type not null,
        time_ time not null,
        constraint fk_p2p_check_ foreign key (check_) references checks(id),
        constraint fk_p2p_checking_peer foreign key (checking_peer) references peers(nickname)
    );

COPY p2p
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/p2p.csv' DELIMITER ',' CSV;

create table
    verter (
        id bigint primary key,
        check_ bigint not null,
        state_ state_type not null,
        time_ time not null,
        constraint fk_verter_check_ foreign key (check_) references checks(id)
    );

COPY verter
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/verter.csv' DELIMITER ',' CSV;

create table
    xp (
        id bigint primary key,
        check_ bigint not null,
        xp_amount bigint not null,
        constraint fk_xp_check_ foreign key (check_) references checks(id)
    );

COPY xp
FROM
    '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/xp.csv' DELIMITER ',' CSV;
