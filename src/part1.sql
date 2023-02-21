create table peers (
    nickname varchar primary key,
    birthday date not null default 'not defined',
    constraint fk_peers_nickname foreign key (nickname) references friends(peer1),
    constraint fk_peers_nickname foreign key (nickname) references friends(peer2)
);

create table friends (
    id bigint primary key,
    peer1 varchar not null,
    peer2 varchar not null
);