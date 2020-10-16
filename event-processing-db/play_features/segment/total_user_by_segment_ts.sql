-- WHERE (tenant_id = :tenant_id)
--     AND (source_id = :source_id)
--     AND ((day >= :start_time)
--     AND (day <= :end_time))
show tables;


insert into segment_users values
(1, 's1', ['a1', 'a2', 'a3'], '2020-01-01 00:00:00'),
(1, 's1', ['a1', 'a2', 'a3','a4'], '2020-01-01 00:00:01')
;

-- repeat 5 phut
insert into segment_users values
(1, 's1', ['a1', 'a2', 'a3','a4'], '2020-01-01 00:00:01');

insert into segment_users values
(1, 's1', ['a1', 'a2', 'a3','a4','a5'], '2020-01-01 00:05:02')
;
-- update 5 phut
insert into segment_users values
(1, 's1', ['a1', 'a2', 'a3','a4','a5'], '2020-01-01 00:05:02');

insert into segment_users values
(1, 's1', ['a1', 'a2', 'a3','a4','a5','a10','a14'], '2020-01-01 00:11:02');

insert into segment_users values
(1, 's2', ['a1','a4','a6','a10'], '2020-01-01 00:20:05'),
(1, 's3', ['a12', 'a14','a16','a19'], '2020-01-01 10:05:06')
;

insert into segment_users values
(1, 's1', ['a1', 'a2'], '2020-01-01 00:11:03');


select *
from segment_users;



-- select  distinct *
-- from (
--       select tenant_id,
--              segment_id,
--              users,
--              toStartOfFiveMinute(at) as time_stamp
--       from segment_users
--       where tenant_id = 1
--         and segment_id = 's1'
-- )


SELECT
    tenant_id,
    segment_id,
    round(avg(length(users))) as total_user,
    toStartOfTenMinutes(at) as time_stamp
FROM
(
    SELECT DISTINCT *
    FROM segment_users
    WHERE (tenant_id = 1) AND (segment_id = 's1') AND ((at >= '') AND (at <= ''))
)
GROUP BY tenant_id, segment_id, time_stamp;


select  distinct *
from segment_users
where tenant_id = 1
and segment_id = 's1'
;





select toStartOfFiveMinute(now());
select toStartOfFiveMinute(toDateTime('2020-01-01 00:01:01'))
select toStartOfFiveMinute(toDateTime('2020-01-01 00:04:59'))





SELECT
    tenant_id,
    segment_id,
    round(avg(length(users))) as total_user,
    toStartOfMonth(at) as time_stamp
FROM
(
    SELECT DISTINCT *
    FROM segment_users
    WHERE (tenant_id = 1) AND (segment_id = 's1')
)
GROUP BY tenant_id, segment_id, time_stamp;


select toUnixTimestamp(toStartOfMonth(now()))
select toUnixTimestamp(now())
select toStartOfMonth()
;

select toStartOfDay(now());


select toUnixTimestamp(concat(toString(toYear(now())), '-', toString(toMonth(now()))) )
;

select toDateTime(toStartOfFiveMinute(now()))
;

select toDateTime(toStartOfDay(now()));







------------------------------------
insert into segment_users values
(1, 's300', ['a30', 'a2','a3','a4','a5'], toDateTime('2020-09-11 01:00:02', 'UTC+7'));


insert into segment_users values
(1, 's1', ['a1', 'a2','a3','a4'], now());


insert into segment_users values
(1, 's100', ['a1'], toDateTime('2020-09-16 03:03:03', 'Asia/Ho_Chi_Minh'));


select * from segment_users;
SELECT
    tenant_id,
    segment_id,
    round(avg(length(users))) as total_user,
    toStartOfDay(at) as time_stamp
FROM
(
    SELECT DISTINCT *
    FROM segment_users
    WHERE (tenant_id = 1) AND (segment_id = 's1')
)
WHERE at >= subtractDays(now(), 5)
GROUP BY tenant_id, segment_id, time_stamp
;



    SELECT *
    FROM segment_users
    WHERE (tenant_id = 1) AND (segment_id = 's1')
    ORDER BY at;

    select * from segment_users;


select now();
select subtractDays(now(), 5); -- last days.
select subtractMonths(now(),1); -- last month

select toQuarter(now());
select toMonth()


select subtractQuarters(now(), 1); -- last quater


select subtractMonths(now(), 6); -- last haft year
select subtractYears(now(), 2); -- last year, 2 year