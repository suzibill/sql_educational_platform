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

create table p2p (
    
)

create table tasks (
    title varchar primary key,
    parent_task varchar not null,
    max_xp bigint not null
);