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











SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfDay(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 0)
                AND (segment_id = 's1')
                AND (at >= '2020-10-22 00:00:00')
                AND (at <= '2020-10-24 00:00:00')
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime('2020-10-22 00:00:00') TO toDateTime('2020-10-24 00:00:00') STEP 24*60*60
    )
) ARRAY JOIN arrayEnumerate(time_stamps) as idx
;


select * from segment_users where tenant_id = 0 and segment_id = 's1';

select toUnixTimestamp('2020-10-22 00:00:00') as start,
       toUnixTimestamp('2020-10-24 00:00:00') as end;







select * from segment_users where tenant_id = 0 and segment_id = 's1';

insert into segment_users values
(1, '1jGmXrizzOPLnJ5sXATC4QaOrEN', ['a1'], '2020-10-22 00:00:00'),
(1, '1jGmXrizzOPLnJ5sXATC4QaOrEN', ['a1','a2'], '2020-10-22 00:00:10'),
(1, '1jGmXrizzOPLnJ5sXATC4QaOrEN', ['a1','a2', 'a3','a4'], '2020-10-22 00:01:05'),
(1, '1jGmXrizzOPLnJ5sXATC4QaOrEN', ['a1','a2','a3','a5','a6'], '2020-10-22 00:06:00');

insert into segment_users values
(1, '1jGmXrizzOPLnJ5sXATC4QaOrEN', ['a1','a2','a3','a5','a6','a7','a8'], '2020-10-22 00:20:01');






SELECT  toStartOfHour(time_stamp) as time_stamp,
        uniqMerge(value) AS value
FROM events_total_distinct_users_by_source_scope_ts
WHERE (tenant_id = %(tenant_id)s)
        AND (source_scope = %(source_scope)s)
        AND (time_stamp >= %(start_time)s)
        AND (time_stamp <= %(end_time)s)
GROUP BY tenant_id,
         source_scope,
         time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime(%(start_time)s) TO toDateTime(%(end_time)s) STEP 3600

2020-10-25 19:03:53,440 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'source_scope': 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u', 'start_time': datetime.datetime(2020, 10, 10, 0, 0), 'end_time': datetime.datetime(2020, 10, 20, 0, 0)}
2020-10-25 19:03:53,476 INFO sqlalchemy.engine.base.Engine COMMIT
INFO:     127.0.0.1:37200 - "GET /data-source/total-user-ts/1/JS-1iRNWBw2hNQTRQibnJeb8NTep7u?timeRange=last_week&startTime=1602288000&endTime=1603152000 HTTP/1.1" 200 OK
2020-10-25 19:04:24,266 INFO sqlalchemy.engine.base.Engine BEGIN (implicit)
2020-10-25 19:04:24,266 INFO sqlalchemy.engine.base.Engine
    SELECT uniqMerge(value) AS total_user
    FROM events_total_distinct_users_by_source_scope_ts
    WHERE (tenant_id = %(tenant_id)s) AND (source_scope = %(source_scope)s)
    GROUP BY
        tenant_id,
        source_scope

2020-10-25 19:04:24,266 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'source_scope': 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u'}
2020-10-25 19:04:24.294 | INFO     | prile.eventify.repository.data_sources:get_total_users_per_source:24 - Total user get from DB: 1302
2020-10-25 19:04:24,294 INFO sqlalchemy.engine.base.Engine COMMIT
2020-10-25 19:04:24.295 | INFO     | prile.eventify.transformation.data_source:get_total_user:21 - Total user at transformation 1302
INFO:     127.0.0.1:37466 - "GET /data-source/total-user/1/JS-1iRNWBw2hNQTRQibnJeb8NTep7u HTTP/1.1" 200 OK
2020-10-25 19:08:43,244 INFO sqlalchemy.engine.base.Engine BEGIN (implicit)
2020-10-25 19:08:43,244 INFO sqlalchemy.engine.base.Engine
SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfHour(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = %(tenant_id)s)
                AND (segment_id = %(segment_id)s)
                AND (at >= %(start_time)s)
                AND (at <= %(end_time)s)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime(%(start_time)s) TO toDateTime(%(end_time)s) STEP 3600
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx

2020-10-25 19:08:43,244 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'segment_id': '1jGmXrizzOPLnJ5sXATC4QaOrEN', 'start_time': datetime.datetime(2020, 10, 22, 0, 0), 'end_time': datetime.datetime(2020, 10, 24, 0, 0)}
2020-10-25 19:08:43,280 INFO sqlalchemy.engine.base.Engine COMMIT
INFO:     127.0.0.1:39528 - "GET /reporting/segment/total-user/1/1jGmXrizzOPLnJ5sXATC4QaOrEN/?timeRange=last_month&startTime=1603324800&endTime=1603497600 HTTP/1.1" 200 OK
2020-10-25 19:10:49,972 INFO sqlalchemy.engine.base.Engine BEGIN (implicit)
2020-10-25 19:10:49,973 INFO sqlalchemy.engine.base.Engine
SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfHour(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = %(tenant_id)s)
                AND (segment_id = %(segment_id)s)
                AND (at >= %(start_time)s)
                AND (at <= %(end_time)s)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime(%(start_time)s) TO toDateTime(%(end_time)s) STEP 3600
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx

2020-10-25 19:10:49,973 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'segment_id': '1jGmXrizzOPLnJ5sXATC4QaOrEN', 'start_time': datetime.datetime(2020, 10, 22, 0, 0), 'end_time': datetime.datetime(2020, 10, 24, 0, 0)}
2020-10-25 19:10:50,008 INFO sqlalchemy.engine.base.Engine COMMIT
INFO:     127.0.0.1:40590 - "GET /reporting/segment/total-user/1/1jGmXrizzOPLnJ5sXATC4QaOrEN/?timeRange=last_30_day&startTime=1603324800&endTime=1603497600 HTTP/1.1" 200 OK
2020-10-25 19:10:58,883 INFO sqlalchemy.engine.base.Engine BEGIN (implicit)
2020-10-25 19:10:58,883 INFO sqlalchemy.engine.base.Engine
SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfHour(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 0)
                AND (segment_id = 's1')
                AND (at >= '2020-10-21 00:00:00')
                AND (at <= '2020-10-23 00:00:00')
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime(%(start_time)s) TO toDateTime(%(end_time)s) STEP 3600
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
;


select * from segment_users where segment_id = '1jGmXrizzOPLnJ5sXATC4QaOrEN';




SELECT DISTINCT *
FROM segment_users
WHERE (tenant_id = 0)
    AND (segment_id = 's1')
    AND (at <= '2020-10-23 00:00:00')
ORDER BY at DESC
LIMIT 1;


SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfFifteenMinutes(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 0)
                AND (segment_id = 's1')
                AND (at <= '2020-10-22 07:55:00')
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime('2020-10-22 12:00:00') TO toDateTime('2020-10-23 00:00:00') STEP 15*60
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= '2020-10-22 12:00:00'
  AND time_stamp <= '2020-10-23 00:00:00'
;
