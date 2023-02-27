/* PART 1 */
create or replace procedure
    insert_p2p(checked_peer varchar, checking_peer__ varchar, task_ varchar, status state_type)
    -- checked_peer peer being checked
    -- checking_peer__ peer who is checking
as
$$
begin
    if status = 'Start' then
        insert into checks(id, peer, task, date_) values ((select max(id) + 1 from checks), checked_peer, task_, current_date);
    end if;
    insert into p2p(id, check_, checking_peer, state_, time_)
    values ((select max(id) + 1 from p2p),
            (select max(id) from checks where peer = checked_peer and task_ = task), checking_peer__, status,
            current_time);
end;
$$ language 'plpgsql';

call insert_p2p('Aboba', 'Amogus', 'C3_s21_string', 'Failure');
-- TODO переписать инсерт в p2p чтобы он брал check id не из checks, а из p2p (последняя проверка этого пира)
-- Хотя тут есть вопросы к реализации
-- insert into checks(id, peer, task, date_)
-- values ((select max(id) + 1 from checks), 'Aboba', 'C2_SimpleBashUtils', current_date);
-- insert into p2p(id, check_, checking_peer, state_, time_)
-- values ((select max(id) + 1 from p2p),
--         (select max(id) from checks where peer = 'Aboba' and 'C2_SimpleBashUtils' = task), 'Amogus', 'Failure',
--         current_time);
-- select *
-- from p2p;
-- select *
-- from checks;

/* PART 2 */
create or replace procedure
    insert_verter(nick varchar, task__ varchar, status state_type, time__ time)
as
$$
begin
    insert into verter(id, check_, state_, time_)
    values ((select max(id) + 1 from verter),
            (select check_
             from p2p
                      join checks c on p2p.check_ = c.id
             where peer = nick
               and task = task__
               and state_ = 'Success'
             order by check_ desc
             limit 1),
            status,
            time__);
end;
$$ language 'plpgsql';

 call insert_verter('Aboba', 'C3_s21_string', 'Success', '22:00:01');
--
-- select check_
-- from p2p
--          join checks c on p2p.check_ = c.id
-- where peer = 'Aboba'
--   and task = 'C3_s21_string'
--   and state_ = 'Success'
-- order by check_ desc
-- limit 1;

/* PART 3 */
create or replace function fnc_trg_transferred_points()
    returns trigger as
$trg_trans$
begin
    update transferred_points
    set point_amount = point_amount + 1
    where new.checking_peer = new.checking_peer
      and checked_peer in (select peer from checks where checks.id = new.check_);
    return new;
end;
$trg_trans$ language 'plpgsql';

create or replace trigger trg_transferred_points
after insert on p2p
    for each row
    when (new.state_ = 'Start')
execute procedure fnc_trg_transferred_points();
call insert_p2p('Aboba', 'Sus', 'C3_s21_string', 'Start');

select * from transferred_points
where checking_peer = 'Sus' and checked_peer = 'Aboba';

/* PART 4 */
create trigger trg_xp
before insert on xp
for each row
execute procedure fn_trg_xp();

create or replace function fn_trg_xp()
returns trigger as
    $trg_xp$
begin
    if new.xp_amount > (select max_xp from tasks
                                      join checks c on c.id = new.check_
                                      where new.check_ = c.id
                                      limit 1)
    then
        raise notice 'xp > max_xp';
        return null;
    end if;
    if (select state_ from p2p where state_ = 'Success' and check_ = new.check_) is null
    then
        raise notice 'p2p stage not finished';
        return null;
    end if;
    if (select state_ from verter where check_ = new.check_ limit 1) is not null
           and (select state_ from verter where state_ = 'Success' and check_ = new.check_) is null
    then
        raise notice 'verter stage not finished';
        return null;
    end if;
    return new;
end;
$trg_xp$ language 'plpgsql';

insert into xp (id, check_, xp_amount) values ((select max(id) from xp) + 1, 23, 1500);

select (select state_ from p2p where state_ = 'Success' and check_ = 22) is not null;
select state_ from verter where state_ = 'Success' and check_ = 22;