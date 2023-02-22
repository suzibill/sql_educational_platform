create table peers (
    nickname varchar primary key,
    birthday date not null
);

create table friends (
    id bigint primary key,
    peer1 varchar not null,
    peer2 varchar not null,
    constraint fk_friends_peer1 foreign key (peer1) references peers(nickname),
    constraint fk_friends_peer2 foreign key (peer2) references peers(nickname)
);

create table recommendations (
    id bigint primary key,
    peer varchar not null,
    recommended_peer varchar not null,
    constraint fk_recommendations_peer foreign key (peer) references peers(nickname),
    constraint fk_recommendations_recommended_peer foreign key (recommended_peer) references peers(nickname)
);

create table transferred_points (
    id bigint primary key,
    checking_peer varchar not null,
    checked_peer varchar not null,
    point_amount int not null,
    constraint fk_friends_checking_peer foreign key (checking_peer) references peers(nickname),
    constraint fk_friends_checked_peer foreign key (checked_peer) references peers(nickname)
);

create table time_tracking (
    id bigint primary key,
    peer varchar not null,
    date_ date not null,
    time_ time not null,
    state varchar not null,
    constraint check_time_tracking_state check (state in ('In', 'Out')),
    constraint fk_time_tracking_peer foreign key (peer) references peers(nickname)
);

create table checks (
    id bigint primary key,
    peer varchar not null,
    task varchar not null,
    date_ date not null,
    constraint fk_checks_peer foreign key (peer) references peers(nickname),
    constraint fk_checks_task foreign key (task) references tasks(title)
);

create table p2p (
    id bigint primary key,
    check_ bigint not null,
    checking_peer varchar not null,
    state varchar not null,
    time_ timestamp default current_timestamp,
    constraint check_p2p_state check (state in ('Start', 'Success', 'Failure')),
    constraint fk_p2p_check_ foreign key (check_) references checks(id),
    constraint fk_p2p_checking_peer foreign key (checking_peer) references peers(nickname)
);

create table verter (
    id bigint primary key,
    check_ bigint not null,
    state varchar not null,
    time_ timestamp default current_timestamp,
    constraint check_p2p_state check (state in ('Start', 'Success', 'Failure')),
    constraint fk_verter_check_ foreign key (check_) references checks(id)
);

create table xp (
    id bigint primary key,
    check_ bigint not null,
    xp_amount bigint not null,
    constraint fk_xp_check_ foreign key (check_) references checks(id)
);

create table tasks (
    title varchar primary key,
    parent_task varchar not null,
    max_xp bigint not null
);
