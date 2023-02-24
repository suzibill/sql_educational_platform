create or replace function fnc_part3_ex01() returns table (
    Peer1 varchar,
    Peer2 varchar,
    PointsAmount integer
) as
$$
begin return query
    select
        t_p1.checking_peer as Peer1, 
        t_p1.checked_peer as Peer2,
        coalesce(t_p1.point_amount,0) - coalesce(t_p2.point_amount, 0) as PointsAmount
    from 
        transferred_points as t_p1
        full join transferred_points as t_p2 
            on t_p1.checking_peer = t_p2.checked_peer and t_p2.checking_peer =t_p1.checked_peer
    where 
        t_p1.id > t_p2.id
        or t_p2.id is null;
end;
$$ language plpgsql;

SELECT * FROM fnc_part3_ex01();

create or replace view check_res as
select 
    checks.id,
    checks.peer,
    checks.task,
    verter.state_ as verter_state,
    p2p.state_ as p2p_state
from checks
    join p2p on p2p.check_ = checks.id
        and (
            p2p.state_ = 'Success' or p2p.state_ = 'Failure'
        )
        left join verter on verter.check_ = checks.id
        and (
            verter.state_ = 'Success'  or verter.state_  = 'Failure'
    );



create or replace function fnc_part3_ex02() returns table (
    Peer varchar,
    Task varchar,
    xp bigint
) as
$$
begin return query
    select 
        check_res.peer as Peer,
        check_res.task as Task,
        xp.xp_amount as XP
    from check_res
    join xp on xp.check_ = check_res.id
where 
    check_res.p2p_state = 'Success' and 
        (check_res.verter_state = 'Success' or check_res.verter_state is null);
end;
$$ language plpgsql;

SELECT * FROM fnc_part3_ex02();

create or replace function fnc_part3_ex03(pdate date) returns table(
    peer varchar
) as
$$
begin return query 
    select time_tracking.peer
    from time_tracking
    where date_ = pdate
        and state_ = 2
    group by time_tracking.peer
    having count(state_) = 1;
end;
$$ language plpgsql;

select * from fnc_part3_ex03('2023-02-01');
    
create or replace procedure prc_part3_ex04(
    in res_checks refcursor = 'r_cur_part3_ex4'
) as
$$
declare f_count integer := (
    select 
        count(id)
    from check_res
    where check_res.p2p_state = 'Failure' or check_res.verter_state = 'Failure'
);

all_count integer := (
    select count(id)
    from check_res
);

s_count integer= all_count - f_count;

begin
    open res_checks for select
        round((f_count/all_count)::numeric * 100, 0) as UnsuccessfulChecks,
        round((s_count/all_count)::numeric * 100, 0) as SuccessfulChecks;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex04();
    fetch all from "r_cur_part3_ex4";
end;

