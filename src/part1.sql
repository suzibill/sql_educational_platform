create table
    peers (
        nickname varchar primary key,
        birthday date not null
    );

create table
    friends (
        id bigint primary key,
        peer1 varchar not null,
        peer2 varchar not null,
        constraint fk_friends_peer1 foreign key (peer1) references peers(nickname),
        constraint fk_friends_peer2 foreign key (peer2) references peers(nickname),
        constraint check_peer_nick check(peer1 <> peer2)
    );

create table
    recommendations (
        id bigint primary key,
        peer varchar not null,
        recommended_peer varchar not null,
        constraint fk_recommendations_peer foreign key (peer) references peers(nickname),
        constraint fk_recommendations_recommended_peer foreign key (recommended_peer) references peers(nickname),
        constraint check_peer_recommended check(peer <> recommended_peer)
    );

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

create table
    tasks (
        title varchar primary key,
        parent_task varchar not null,
        max_xp bigint not null,
        constraint fk_title_parent_task foreign key(parent_task) references tasks(title)
    );

create table
    checks (
        id bigint primary key,
        peer varchar not null,
        task varchar not null,
        date_ date not null,
        constraint fk_checks_peer foreign key (peer) references peers(nickname),
        constraint fk_checks_task foreign key (task) references tasks(title)
    );

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

create table
    verter (
        id bigint primary key,
        check_ bigint not null,
        state_ state_type not null,
        time_ time not null,
        constraint fk_verter_check_ foreign key (check_) references checks(id)
    );

create table
    xp (
        id bigint primary key,
        check_ bigint not null,
        xp_amount bigint not null,
        constraint fk_xp_check_ foreign key (check_) references checks(id)
    );

create or replace procedure import_from_csv() as
$$
    declare
        import_path varchar = '/Users/wilfredo/02/SQL2_Info21_v1.0-0/src/csv/';
--         import_path varchar = '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/csv/';
        import_name varchar[] = array ['peers', 'friends', 'recommendations','transferred_points','time_tracking', 'tasks', 'checks','p2p', 'verter', 'xp'];
        begin
        for i in 1..array_length(import_name,1)
        loop
            execute format('COPY %s FROM ''%s%s.csv'' DELIMITER '','' CSV',import_name[i],import_path,import_name[i]);
            end loop;
    end;
    $$
    language plpgsql;

-- truncate peers, friends, recommendations,transferred_points,time_tracking, tasks, checks,p2p, verter, xp cascade;
call import_from_csv();

create or replace procedure export_to_csv() as
    $$
    declare
        export_path varchar = '/Users/wilfredo/02/SQL2_Info21_v1.0-0/src/backup/';
--         import_path varchar = '/Users/suzibill/projects/SQL2_Info21_v1.0-0/src/backup/';
        export_name varchar[] = array ['peers', 'friends', 'recommendations','transferred_points','time_tracking', 'tasks', 'checks','p2p', 'verter', 'xp'];
        begin
        for i in 1..array_length(export_name,1)
        loop
            execute format('COPY %s TO ''%s%s.csv'' WITH DELIMITER '','' CSV',export_name[i],export_path,export_name[i]);
            end loop;
    end;
        $$
language plpgsql;

call export_to_csv();

