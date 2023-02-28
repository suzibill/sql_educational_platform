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
            verter.state_ = 'Success'  or verter.state_  = 'Failure' or verter.state_ = null
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

create or replace procedure prc_part3_ex05(
    in res_checks refcursor = 'r_cur_part3_ex5'
) as
$$
begin 
open res_checks for 
    select 
        total.nickname as Peer,
        total.points_total - minus.points_total as PointsChange
    from (
        (
            select 
                peers.nickname,
                sum(coalesce(transferred_points.point_amount, 0)) as points_total
            from peers
                left join transferred_points on transferred_points.checking_peer = peers.nickname
            group by
                peers.nickname
        ) as total join (
            select
                peers.nickname,
                sum(coalesce(transferred_points.point_amount, 0)) as points_total
            from peers
                left join transferred_points on transferred_points.checked_peer = peers.nickname
            group by
                peers.nickname
        ) as minus on total.nickname = minus.nickname
    )
    order by PointsChange desc;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex05();
    fetch all from "r_cur_part3_ex5";
end;

create or replace procedure prc_part3_ex06(
    in res_checks refcursor = 'r_cur_part3_ex6'
) as
$$
begin 
open res_checks for 
    select 
        total.nickname as Peer,
        total.points_total - minus.points_total as PointsChange
    from(
        (    
            select 
                peers.nickname,
                sum(coalesce(fnc_ex_1_p.PointsAmount, 0)) as points_total
            from peers
                left join (select * from fnc_part3_ex01()) as fnc_ex_1_p on peers.nickname = fnc_ex_1_p.Peer1
            group by peers.nickname
        ) as total join (
            select
                peers.nickname,
                sum(coalesce(fnc_ex_1_p.PointsAmount, 0)) as points_total
            from peers
                left join (select * from fnc_part3_ex01()) as fnc_ex_1_p on peers.nickname = fnc_ex_1_p.Peer2
             group by  peers.nickname
        ) as minus on total.nickname = minus.nickname
    )
    order by PointsChange desc;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex06();
    fetch all from "r_cur_part3_ex6";
end;  


create or replace view count_c as
select
    checks.date_,
    checks.task,
    count(id) as count_checks
from checks
group by date_, task
order by checks.date_;

create or replace view maxi as 
select 
    count_c.date_,
    max(count_c.count_checks) as max_count
from count_c
group by count_c.date_;

create or replace procedure prc_part3_ex07(
    in res_checks refcursor = 'r_cur_part3_ex7'
) as
$$
begin 
open res_checks for 
select
    count_c.date_ as day,
    count_c.task as Task
from count_c 
left join maxi on maxi.date_ = count_c.date_
where maxi.max_count = count_c.count_checks;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex07();
    fetch all from "r_cur_part3_ex7";
end;  


create or replace procedure prc_part3_ex08(
    in res_checks refcursor = 'r_cur_part3_ex8'
) as
$$
declare 
number_of_check bigint := (
    select p2p_cpy.check_
    from p2p p2p_cpy
    left join p2p on p2p_cpy.check_ = p2p.check_
    where p2p_cpy.state_ = 'Start' and (p2p.state_ = 'Success' or p2p.state_ = 'Failure')
    order by p2p_cpy.check_ desc
    limit 1
);
start_check time := (
    select time_
    from p2p
    where check_ = number_of_check and state_ = 'Start'
);
end_check time := (
    select time_
    from p2p
    where check_ = number_of_check and (state_ = 'Success' or state_ = 'Failure')
);
begin 
open res_checks for 
select
    end_check - start_check as time_to_check;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex08();
    fetch all from "r_cur_part3_ex8";
end;  


create or replace procedure prc_part3_ex09(
    in branch varchar,
    in res_checks refcursor = 'r_cur_part3_ex9'
) as 
$$
declare
branch_task_count int := (
    select count(title)
    from tasks
    where title ~ ('^' || branch || '[0-9]')
);
begin 
open res_checks for 
with complete_tasks as (
    select  
        distinct on (checks.Peer,checks.task) checks.peer,
        checks.task,
        checks.date_
    from checks 
    inner join verter on verter.check_ = checks.id
    inner join p2p on p2p.check_ = checks.id
    where checks.task ~ ('^' || branch || '[0-9]') and 
    p2p.state_ = 'Success' and (verter.state_ = 'Success' or verter.state_ = null)
    order by checks.peer, checks.task, checks.date_ desc
), count_uniq as (
    select peer as Peer,
           max(date_) as Day,
           count (peer) as c_u
    from complete_tasks
    group by Peer
)
select count_uniq.Peer,
       count_uniq.Day
from count_uniq
where c_u = branch_task_count
order by count_uniq.Day desc;
end;
$$language plpgsql;

begin;
    call prc_part3_ex09('C');
    fetch all from "r_cur_part3_ex9";
end;  

abort TRANSACTION



create or replace view a_f as (
    select distinct on (a_f.peer1,a_f.peer2) *
    from (
            (
                select peer1, peer2
                from friends
            )
            union all
            (
                select peer2, peer1
                from friends
            )
    ) as a_f
);

create or replace view total_recom as (
    select a_f.peer1,
           recommendations.recommended_peer,
           count(recommendations.recommended_peer) as c_r
    from a_f
    left join recommendations on a_f.peer2 = recommendations.peer
    where  a_f.peer1 <> recommendations.recommended_peer
    group by a_f.peer1, recommendations.recommended_peer
    order by peer1
);

create or replace procedure prc_part3_ex10 (
    in res_checks refcursor = 'r_cur_part3_ex10'
) as
$$
begin 
open res_checks for
select 
    distinct on(total_recom.peer1) total_recom.peer1 as Peer,
    total_recom.recommended_peer as RecommendedPeer
from total_recom;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex10();
    fetch all from "r_cur_part3_ex10";
end;  

-- 11 нужна норм таблица

create or replace procedure prc_part3_task12 (ref refcursor, counts int default 1) as
$$
begin 
open ref for
select 
    peers.nickname as Peer,
    count(a_f.peer2) as FriendsCount
from 
    peers
left join a_f on a_f.peer1 = peers.nickname
group by peers.nickname
order by FriendsCount desc
limit counts;
end;
$$ language plpgsql;

begin;
    call prc_part3_task12('r_cur_part3_ex12',4);
    fetch all in r_cur_part3_ex12;
end;  
-- поменять имя процедуры, у меня задвоило

create or replace procedure prc_part3_ex13(
    in res_checks refcursor = 'r_cur_part3_ex13'
) as
$$
declare
    fail int := (
        select count(checks.id)
        from checks
        left join p2p on p2p.check_ = checks.id
        left join peers on peers.nickname = checks.peer
        left join verter on checks.id = verter.check_
        where to_char(checks.date_, 'MM.DD') = to_char(peers.birthday,'MM.DD')
            and (
                p2p.state_ = 'Failure' or verter.state_ = 'Failure' 
            )
    );
    success int :=(
        select count(checks.id)
        from checks
        left join p2p on p2p.check_ = checks.id
        left join peers on peers.nickname = checks.peer
        left join verter on checks.id = verter.check_
        where to_char(checks.date_, 'MM.DD') = to_char(peers.birthday,'MM.DD')
            and (
                p2p.state_ = 'Success' and (verter.state_ = 'Success' or verter.state_ is null)
            )
    );

begin
    open res_checks for select
        success as success,
        fail as failure,
        round((success/(success+fail))::numeric * 100, 0) as SuccessfulChecks,
        round((fail/(fail + success))::numeric * 100, 0) as UnsuccessfulChecks;
end;
$$ language plpgsql;   

begin;
    call prc_part3_ex13();
    fetch all from "r_cur_part3_ex13";
end;

create or replace view max_task_for_peer as (
select 
    checks.peer as Peer,
    task, 
from xp
left join checks on checks.id = xp.check_
group by checks.task, checks.peer
);


create or replace view max_xp_for_task as (
select 
    checks.peer,
    max(xp_amount) as xp_max 
from xp
left join checks on checks.id = xp.check_
group by checks.task, checks.peer
);

create or replace procedure prc_part3_ex14(
    in res_checks refcursor = 'r_cur_part3_ex14'
) as
$$
begin 
open res_checks for select 
    peer as Peer,
    sum(xp_max) as XP 
from max_xp_for_task
group by Peer
order by XP desc;
end;
$$ language plpgsql;   

begin;
    call prc_part3_ex14();
    fetch all from "r_cur_part3_ex14";
end;


-- create or replace procedure prc_part3_ex15(
--     in task1 varchar, 
--     in task2 varchar,
--     in task3 varchar,
--     in res_checks refcursor = 'r_cur_part3_ex15'
-- ) as
-- $$
-- begin
-- open res_checks for 
-- with t1 as (
--     select 
--         checks.peer
--     from checks
--     left join p2p on p2p.check_ = checks.id
--     left join verter on verter.check_ = checks.id
--     where 
--         p2p.state_ = 'Success' and 
--         (verter.state_ = 'Success' or verter.state_ = null) and 
--         (checks.task = task1 or checks.task = task2)
--     group by peer 
--     having count(checks.task) =2
-- ), t2 as (
--     select distinct checks.peer
--     from checks
--      eft join p2p on p2p.check_ = checks.id
--     left join verter on verter.check_ = checks.id
--     where 
--         p2p.state_ = 'Failure' or 
--         (verter.state_ = 'Failure' or verter.state_ = null)
--         and checks.task = task3    
-- )
-- select peer
-- from t1 
-- except 
-- select peer
-- from t2;
-- end;
-- $$ language plpgsql;

-- begin;
--     call prc_part3_ex15(
--         'SimpleBashUtils',
--         's21_string',
--         's21_math.h'
--     );
--     fetch all from "r_cur_part3_ex15";
-- end;

create or replace procedure prc_part3_ex16 (
    in res_checks refcursor = 'r_cur_part3_ex16'
) as
$$
begin 
open res_checks for 
with recursive count_task_previous as (
   (select tasks.title,
           0 as prev_count
    from tasks
    where parent_task = 'None'
   ) union all (
    select tasks.title,
            prev_count+1
    from tasks 
    inner join count_task_previous on count_task_previous.title = tasks.parent_task
   )
)
select * 
from count_task_previous;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex16();
    fetch all from "r_cur_part3_ex16";
end;

create or replace view checks_all as (
    select  checks.id,
            checks.date_,
            p2p.time_,
            case when
                (
                    xp.xp_amount is null or
                    xp.xp_amount < tasks.max_xp * 0.8 or
                    verter.state_ = 'Failure' or
                    p2p.state_ = 'Failure'
                ) then 0
                else 1
            end as success_check
    from checks
    join p2p on p2p.check_ = checks.id and p2p.state_ <> 'Start'
    left join verter on verter.check_ = checks.id and verter.state_ <> 'Start'
    left join xp on xp.check_ = checks.id
    join tasks on tasks.title =checks.task    
);

create or replace procedure prc_part3_ex17 (
    in numb int,
    in res_checks refcursor = 'r_cur_part3_ex17'
) as $$
begin 
open res_checks for
with good_day as (
    select *,
          sum(success_check) over (
            partition by date_
            order by 
                date_,
                time_,
                id rows between numb-1 preceding
                and current row
          ) as top_day
    from checks_all
)
select date_ as Happy_Days
from good_day
group by date_
having max(top_day) >= numb;
end;
$$language plpgsql;

begin;
    call prc_part3_ex17(1);
    fetch all from "r_cur_part3_ex17";
end;


create or replace procedure prc_part3_ex18(
    in res_checks refcursor = 'r_cur_part3_ex18'
) as
$$
begin 
open res_checks for 
select Peer,
       count(task) as XP
from max_task_for_peer
group by Peer
order by XP desc
limit 1;
end;
$$ language plpgsql;

begin;
    call prc_part3_ex18();
    fetch all from "r_cur_part3_ex18";
end;

create or replace procedure prc_part3_ex19(
    in res_checks refcursor = 'r_cur_part3_ex19'
) as
$$
begin 
open res_checks for 
select peer as Peer,
       sum(xp_max) as XP
from max_xp_for_task
group by Peer
order by XP desc
limit 1;
end;
$$ language plpgsql;   

begin;
    call prc_part3_ex19();
    fetch all from "r_cur_part3_ex19";
end;



 abort TRANSACTION;

-- 9,11 не сделаны, 15- надо фиксить
