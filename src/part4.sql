create table if not exists table_1 (
    id bigint primary key,
    string varchar not null
);

create table if not exists table_2 (
    id bigint primary key,
    string varchar not null
);

create table if not exists table_3 (
    id bigint primary key,
    string varchar not null
);

create table if not exists test_1 (
    id bigint primary key,
    string varchar not null
);

create table if not exists test_2 (
    id bigint primary key,
    string varchar not null
);

create table if not exists test_3 (
    id bigint primary key,
    string varchar not null
);


create or replace function fnc_table_ins() returns trigger as
    $$
    begin
        return new;
    end;
    $$
language plpgsql;

create or replace function fnc_table_upd() returns trigger as
    $$
    begin
        return new;
    end;
    $$
language plpgsql;

create or replace function fnc_test_ins() returns trigger as
    $$
    begin
        return new;
    end;
    $$
language plpgsql;

create or replace function fnc_test_upd() returns trigger as
    $$
    begin
        return new;
    end;
    $$
language plpgsql;

create trigger trg_test_1
    after insert
    on table_1
    for each row
execute function fnc_table_ins();

create trigger trg_test_2
    after update
    on table_1
    for each row
execute function fnc_table_upd();

create trigger trg_test_3
    before insert
    on table_1
    for each row
execute function fnc_table_ins();

create trigger trg_test_4
    after insert
    on test_1
    for each row
execute function fnc_test_ins();

create trigger trg_test_5
    after update
    on test_1
    for each row
execute function fnc_test_upd();

create trigger trg_test_6
    before insert
    on test_1
    for each row
execute function fnc_test_ins();

create or replace function fnc_table_test_1(in n integer, out res integer) as
$$
    begin
        select 6 into n;
    end;
$$
language plpgsql;

create or replace function fnc_table_test_2(in n integer,in c integer, out res integer) as
$$
    begin
        select 6 into n;
    end;
$$
language plpgsql;

create or replace function fnc_table_test_1(in n integer) returns integer as
$$
    begin
        select 6 into n;
        return 10;
    end;
$$
language plpgsql;

create or replace procedure prc_delete_table() as
$$
declare
    table_name_del varchar ='test';
    row record;
begin
    for row in
        (select table_name
        from information_schema.tables
        where table_schema = 'public'
            and table_name like format('%s%%',table_name_del))
        loop
        execute  'delete table ' || row.table_name || ' cascade';
        end loop;
end;
$$
language plpgsql;

call prc_delete_table();

create or replace procedure  prc_part4_task2(
    inout results integer,
    in ref refcursor
) as
$$
begin
open ref for
select pc.proname,
    pg_catalog.pg_get_function_identity_arguments(pc.oid) as pg_arg
    from pg_catalog.pg_namespace pn
    join pg_catalog.pg_proc pc on pc.pronamespace = pn.oid
    where pc.prokind = 'f'
    and
    pg_catalog.pg_get_function_identity_arguments(pc.oid) is not null
        and pn.nspname = 'public';
results := (
    select
        count(pc.proname)
    from pg_catalog.pg_namespace pn
    join pg_catalog.pg_proc pc on pc.pronamespace = pn.oid
    where pc.prokind = 'f'
    and
    pg_catalog.pg_get_function_identity_arguments(pc.oid) is not null
        and pn.nspname = 'public');
end;
$$ language  plpgsql;

begin ;
call prc_part4_task2(0, 'ref');
fetch all in "ref";
end;

select * from information_schema.triggers;
select * from pg_proc where prosrc like '%BEGIN%';

create or replace procedure prc_part4_task3(
inout results integer
) as
$$
declare row record;
begin
    results = (select
                   count(*)
                from information_schema.triggers);
    for row in (select
                    trigger_name, event_object_table
                from information_schema.triggers)
    loop
        execute format('drop trigger %s on %s cascade', row.trigger_name, row.event_object_table);
    end loop;
end;
$$ language plpgsql;

call prc_part4_task3(null);

create or replace procedure  prc_part4_task4 (
    in param varchar,
    in ref refcursor
) as
$$
begin
    open ref for
        select proname, prosrc
        from pg_proc
        join pg_namespace pn on pg_proc.pronamespace = pn.oid
            and nspname = 'public'
        where prosrc like '%' || param || '%';
end;
$$ language plpgsql;

begin;
call prc_part4_task4('s', 'ref');
fetch all in "ref";
end;

call prc_part4_task4('begin', 'ref');
