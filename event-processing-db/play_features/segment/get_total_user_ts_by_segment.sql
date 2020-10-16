select toDateTime(formatDateTime(t1.data_point, '%Y-%m-%d %H:%M:%S')) as data_point,
       t2.value as value
from
(
    select toStartOfDay(yesterday()) + number * 60 * 60 as data_point
    from system.numbers
    limit 24
) as t1
left join
(
SELECT time_stamp ,
       uniqMerge(value) AS value
FROM event_total_user_by_source_ts
WHERE tenant_id = 1
  AND source_id = 'Google Ads'
  AND time_stamp >= toStartOfDay(yesterday())
  AND time_stamp < toStartOfDay(now())
GROUP BY tenant_id, source_id, time_stamp
ORDER BY  tenant_id, source_id, time_stamp
) as t2
on t1.data_point = t2.time_stamp;


------------------------LAST 7 DAY ----------------------------
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(time_stamp)) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
select  toStartOfDay(subtractDays(now(), 6)) + number*60*60*24 as time_stamp
from system.numbers
limit 7
)  as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toStartOfDay(at)          as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
         AND at >= subtractDays(now(), 6)
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on  t1.time_stamp = t2.time_stamp
)
;

------------------------LAST 14 DAY ----------------------------
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(time_stamp)) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
select  toStartOfDay(subtractDays(now(), 13)) + number*60*60*24 as time_stamp
from system.numbers
limit 14
)  as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toStartOfDay(at)          as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
         AND at >= subtractDays(now(), 13)
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on  t1.time_stamp = t2.time_stamp
)
;


-- LAST 30 DAYS
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(time_stamp)) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
select  toStartOfDay(subtractDays(now(), 29)) + number*60*60*24 as time_stamp
from system.numbers
limit 30
)  as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toStartOfDay(at)          as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
         AND at >= subtractDays(now(), 29)
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on  t1.time_stamp = t2.time_stamp
)
;



-- LAST MONTH
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(toDateTime(time_stamp))) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
select time_stamp
from (
         select toStartOfMonth(subtractMonths(now(), 1)) + number as time_stamp
         from system.numbers
         limit 31
         )
where time_stamp < toStartOfMonth(now())
)  as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toDate(at)          as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
         AND at >= toStartOfMonth(subtractMonths(now(), 1))
         AND at < toStartOfMonth(now())
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on  t1.time_stamp = t2.time_stamp
)
;



--- LAST QUATER
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(toDateTime(time_stamp))) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
    select time_stamp
    from (
          select toStartOfQuarter(subtractQuarters(now(), 1)) + number as time_stamp
          from system.numbers
          limit 31*3
        )
    where time_stamp < toStartOfQuarter(now())
) as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toDate(at)                as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
            AND at >= toStartOfQuarter(subtractQuarters(now(), 1))
            AND at < toStartOfQuarter(now())
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on t1.time_stamp = t2. time_stamp
)
;


---------LAST HALF YEAR
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(time_stamp)) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
select  toStartOfDay(subtractDays(now(), 179)) + number*60*60*24 as time_stamp
from system.numbers
limit 180
)  as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toStartOfDay(at)          as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
         AND at >= toStartOfDay(subtractDays(now(), 179))
         AND at <= now()
    )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on  t1.time_stamp = t2.time_stamp
)
;




---- LAST YEAR


select time_stamp from (
select toStartOfYear(subtractYears(now(), 1)) + number as time_stamp
from system.numbers
limit 366)
where time_stamp < toStartOfYear(now());



select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(toDateTime(time_stamp))) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
    select time_stamp from (
    select toStartOfYear(subtractYears(now(), 1)) + number as time_stamp
    from system.numbers
    limit 366)
    where time_stamp < toStartOfYear(now())
) as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toDate(at)                as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
            AND at >= toStartOfYear(subtractYears(now(), 1))
            AND at < toStartOfYear(now())
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on t1.time_stamp = t2. time_stamp
)
;


---------------------LAST_TWO_YEAR
select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(toDateTime(time_stamp))) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
    select time_stamp from (
    select toStartOfYear(subtractYears(now(), 2)) + number as time_stamp
    from system.numbers
    limit 366*2)
    where time_stamp < toStartOfYear(now())
) as t1
left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toDate(at)                as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
            AND at >= toStartOfYear(subtractYears(now(), 1))
            AND at < toStartOfYear(now())
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on t1.time_stamp = t2. time_stamp
)
;



select arrayZip(time_stamps, total_users) as data_point
from
(
select groupArray( toUnixTimestamp(toDateTime(time_stamp))) as time_stamps,
       arrayFill(x -> x!=0  ,groupArray(total_user)) as total_users
from
(
    select * from
    (
    select toStartOfMonth(toStartOfYear(subtractYears(now(), 2)) + (number + 1) * 28) as time_stamp
        from system.numbers
        limit 12
        ) union all
    (
        select toStartOfMonth(toStartOfYear(subtractYears(now(), 1)) + (number + 1) * 28) as time_stamp
        from system.numbers
        limit 12
    )
) as t1 left join
(
    SELECT tenant_id,
           segment_id,
           round(avg(length(users))) as total_user,
           toStartOfMonth(at)        as time_stamp
    FROM (
          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
            AND at >= toStartOfYear(subtractYears(now(), 2))
            AND at < toStartOfYear(now())
             )
    GROUP BY tenant_id, segment_id, time_stamp
) as t2 on t1.time_stamp = t2.time_stamp
);
















select toQuarter(toDateTime('2020-10-18 00:00:00')) - 2;




select toStartOf
select toStartOfQuarter(now());
select subtractMonths(now(), 1);
select toStartOfMonth(now());
select subtractDays(now(), 29);


select time_stamp
from (
         select toStartOfMonth(subtractMonths(now(), 1)) + number as time_stamp
         from system.numbers
         limit 31
         )
where time_stamp < toStartOfMonth(now());







          SELECT DISTINCT *
          FROM segment_users
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
          ORDER BY  at;



insert into segment_users values (1,'s2',['a1','a2','a3'], '2020-09-08 00:00:03');

insert into segment_users values (1,'s2',['a1','a2','a3'], '2020-09-09 00:00:03');

insert into segment_users values (1,'s1',['a1','a2'], '2020-04-02 00:00:03');

insert into segment_users values (1,'s1',['a1','a2','a3'], '2020-05-05 00:00:03');

insert into segment_users values (1,'s1',[], '2020-03-23 00:00:03');

insert into segment_users values (1,'s1',['a1','a2','a3','a4','a5'], '2019-03-24 00:00:03');

insert into segment_users values (1,'s1',['a1','a2','a3','a4','a5'], '2018-04-24 00:00:03');

insert into segment_users values (1,'s1',['a1'], '2019-05-24 00:00:03');




;





-- LAST 7 DAYS
SELECT arrayZip(time_stamps, total_users) AS data_point
FROM
(
    SELECT
        groupArray(toUnixTimestamp(time_stamp)) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        WITH
            60*60 AS delta,
            1600707600 AS start_unix,
            1600793999 AS end_unix,
            FROM_UNIXTIME(start_unix) AS start,
            FROM_UNIXTIME(end_unix) AS end,
            ceil((end_unix - start_unix) / delta) AS n
        SELECT toStartOfHour(start) + number * delta AS time_stamp
        FROM system.numbers
        LIMIT n
    ) AS t1
    LEFT JOIN
    (
        SELECT
            tenant_id,
            segment_id,
            round(avg(length(users))) AS total_user,
            toStartOfDay(at) AS time_stamp
        FROM
        (
            WITH
                FROM_UNIXTIME(1600707600) AS start,
                FROM_UNIXTIME(1600793999) AS end
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 1)
                AND (segment_id = 's1')
                AND (at >= start)
                AND (at <= end)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
    ) AS t2 ON t1.time_stamp = t2.time_stamp
);



with
    60 as delta,
    1600707600 as start_unix,
    1600793999 as end_unix,
    FROM_UNIXTIME(start_unix) as start,
    FROM_UNIXTIME(end_unix) as end,
    ceil((end_unix - start_unix) / (60 * delta)) as n
select addMinutes(toStartOfFifteenMinutes(start), number * delta) as time_stamp
from system.numbers
limit n
;


select * from segment_users where segment_id = 's1';

select toUnixTimestamp(subtractDays(now(), 0))






WITH
    60*60*24 AS delta,
    1598321205 AS start_unix,
    1600913105 AS end_unix,
    FROM_UNIXTIME(start_unix) AS start,
    FROM_UNIXTIME(end_unix) AS end,
    ceil((end_unix - start_unix) / delta) AS n
SELECT toStartOfDay(start) + number * delta AS time_stamp
FROM system.numbers
LIMIT n
;


SELECT
    tenant_id,
    segment_id,
    round(avg(length(users))) AS total_user,
    toStartOfDay(at) AS time_stamp
FROM
(
    WITH
        FROM_UNIXTIME({start_time}) AS start,
        FROM_UNIXTIME({end_time}) AS end
    SELECT DISTINCT *
    FROM segment_users
    WHERE (tenant_id = :tenant_id)
        AND (segment_id = :segment_id)
        AND (at >= start)
        AND (at <= end)
)
GROUP BY
    tenant_id,
    segment_id,
    time_stamp
;





SELECT arrayZip(time_stamps, total_users) AS data_point
FROM
(
    SELECT
        groupArray(toUnixTimestamp(time_stamp)) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        WITH
            60*60*24 AS delta,
            1598321205 AS start_unix,
            1600913105 AS end_unix,
            FROM_UNIXTIME(start_unix) AS start,
            FROM_UNIXTIME(end_unix) AS end,
            ceil((end_unix - start_unix) / delta) AS n
        SELECT toStartOfDay(start) + number * delta AS time_stamp
        FROM system.numbers
        LIMIT n
    ) AS t1
    LEFT JOIN
    (
        SELECT
            tenant_id,
            segment_id,
            round(avg(length(users))) AS total_user,
            toStartOfDay(at) AS time_stamp
        FROM
        (
            WITH
                FROM_UNIXTIME(1598321205) AS start,
                FROM_UNIXTIME(1600913105) AS end
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 1)
                AND (segment_id = 's1')
                AND (at >= start)
                AND (at <= end)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
    ) AS t2 ON t1.time_stamp = t2.time_stamp
)





SELECT arrayZip(time_stamps, total_users) AS data_point
FROM
(
    SELECT
        groupArray(toUnixTimestamp(time_stamp)) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        WITH
            60*60 AS delta,
            1600794000 AS start_unix,
            1600834509 AS end_unix,
            FROM_UNIXTIME(start_unix) AS start,
            FROM_UNIXTIME(end_unix) AS end,
            ceil((end_unix - start_unix) / delta) AS n
        SELECT toStartOfHour(start) + number * delta AS time_stamp
        FROM system.numbers
        LIMIT n
    ) AS t1
    LEFT JOIN
    (
        SELECT
            tenant_id,
            segment_id,
            round(avg(length(users))) AS total_user,
            toStartOfHour(at) AS time_stamp
        FROM
        (
            WITH
                FROM_UNIXTIME(1600794000) AS start,
                FROM_UNIXTIME(1600834509) AS end
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = %(tenant_id)s)
                AND (segment_id = %(segment_id)s)
                AND (at >= start)
                AND (at <= end)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
    ) AS t2 ON t1.time_stamp = t2.time_stamp
)
