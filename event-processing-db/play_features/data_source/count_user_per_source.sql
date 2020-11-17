select * from events;


CREATE TABLE IF NOT EXISTS events
(
    `tenant_id` UInt16,
    `user_id` String,
    `anonymous_id` String,
    `event_name` String,
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float32),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at)
;

-- show create table events;
-- select * from events;
-- insert into events
--     with
--         ['Added to Card','App Launched','App Uninstalled','Category Viewed','Order Completed','Payment Offer Applied', 'Product Viewed', 'Searched'] as event_names,
--     ['Google Ads','Google Search','Facebook Content', 'Others'] as sources
--     select
--         'event-123' as event_id,
--         rand(1)%1+1 as tenant_id,
--         'u' as user_id,
--         concat('a', toString(number)) as anonymous_id,
--         event_names[rand(2)%length(event_names)+1] as event_name,
--         ['source.scope'] as `str_properties.keys`,
--         [sources[rand(3)%length(sources)+1]] as `str_properties.vals`,
--         [] as `num_properties.keys`,
--         [] as `num_properties.vals`,
--         [''] as `arr_properties.keys`,
--         [[]] as `arr_properties.vals`,
--         toDateTime('2017-02-28 00:00:10')+number*3600 as at
--     from system.numbers
--     limit 100
-- ;

select * from events;
insert into events values
(1,'user', 'a1', 'order', ['source.scope'],['fb'],[],[],[],[],yesterday()),
(1,'user', 'a1', 'checkout', ['source.scope'],['fb'],[],[],[],[],yesterday()),
 (1,'user', 'a1', 'view', ['source.scope'],['web'],[],[],[],[],yesterday()),
(1,'user', 'a2', 'checkout', ['source.scope'],['web'],[],[],[],[],yesterday())
;

insert into events values
(1,'user', 'a4', 'checkout', ['source.scope'],['web'],[],[],[],[],now());


insert into events values
(1,'user', 'a4', 'checkout', ['source.scope'],['fb'],[],[],[],[],now());

insert into events values
(1,'user', 'a5', 'checkout', ['source.scope'],['web'],[],[],[],[],now());



----------------------------------------------------- UNIQ
CREATE TABLE IF NOT EXISTS event_total_user_by_source_daily
(
    `day` Date,
    `tenant_id` UInt16,
    `source_id` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, day)

;

CREATE MATERIALIZED VIEW IF NOT EXISTS event_total_user_by_source_daily_mv TO event_total_user_by_source_daily AS
SELECT
    toDate(at) AS day,
    tenant_id,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    day,
    tenant_id,
    source_id
;

----------------------aggregate by day
select * from events;




select * from users_source_agg_daily_v;

-----------------------agregate by week
select * from events;
select toStartOfMonth(day) as m,
       tenant_id,
       source_id,
       uniqMerge(value)
from users_source_agg_daily
group by m, tenant_id, source_id;

-----------------------aggregate by year
select toStartOfYear(day) as y,
       tenant_id,
       source_id,
       uniqMerge(value)
from users_source_agg_daily
group by y, tenant_id, source_id;



-------------------------------------------------------count events name per source
CREATE TABLE IF NOT EXISTS event_total_event_name_by_source_daily
(
    `day` Date,
    `tenant_id` UInt16,
    `event_name` String,
    `source_id` String,
    `total` UInt16
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, event_name, day)
;


CREATE MATERIALIZED VIEW event_total_event_name_by_source_daily_mv TO event_total_event_name_by_source_daily AS
SELECT
    toDate(at) AS day,
    tenant_id,
    event_name,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    count() AS total
FROM events
GROUP BY (day, tenant_id, event_name, source_id)
;

---------------- aggregate by day
SELECT
    day,
    tenant_id,
    source_id,
    event_name,
    sum(total)
FROM event_total_event_name_by_source_daily
WHERE (tenant_id = 1) AND (source_id = 'fb') AND ((day >= '2020-08-24') AND (day <= '2020-08-26'))
GROUP BY
    day,
    tenant_id,
    event_name,
    source_id
ORDER BY
    day ASC,
    source_id ASC,
    event_name ASC
;

-----------------aggregate by month
select toStartOfMonth(day) as month,
       tenant_id,
       source_id,
       event_name,
       sum(count)
from event_name_source_agg_daily
group by day, tenant_id, event_name, source_id
order by day, tenant_id, event_name, source_id;



SELECT
     day as time_stamp,
    tenant_id,
    source_id,
    uniqMerge(value) AS user_count
FROM event_total_user_by_source_daily
WHERE (tenant_id = 1) AND (source_id = 'fb') AND ((day >= '2020-08-24') AND (day <= '2020-08-26'))
GROUP BY
    time_stamp,
    tenant_id,
    source_id
ORDER BY
    time_stamp ASC,
    tenant_id ASC,
    source_id ASC
;



SELECT
    uniqMerge(value) AS total_user
FROM event_total_user_by_source_daily
WHERE (tenant_id = 1) AND (source_id = 'Google Ads')
GROUP BY
    tenant_id,
    source_id
ORDER BY
    tenant_id ASC,
    source_id ASC
;

SELECT
       tenant_id,
       source_id,
    uniqMerge(value) AS total_user
FROM event_total_user_by_source_daily
group by tenant_id, source_id;

show create table event_total_user_by_source_daily_mv;
truncate table events;
truncate table event_total_user_by_source_daily;
select * from event_total_user_by_source_daily limit 10;

-------------------------------------------------------------toStartOfHour supprot -------------------------------------
drop table event_total_user_by_source_daily_mv;
drop table event_total_user_by_source_daily;
drop table event_total_user_by_source_daily;
truncate table events;
truncate table event_total_user_by_source_ts;


-- TO DAY, YESTERDAY
-- CREATE MATERIALIZED VIEW event_total_user_by_source_daily_mv TO event_total_user_by_source_daily AS
-- SELECT
--     toDate(at) AS day,
--     tenant_id,
--     str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
--     uniqState(anonymous_id) AS value
-- FROM events
-- GROUP BY
--     day,
--     tenant_id,
--     source_id

-- LAST WEEK
CREATE TABLE event_total_user_by_source_ts
(
    `time_stamp` DateTime,
    `tenant_id` UInt16,
    `source_id` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_id, time_stamp);

insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 10:28:00');

insert into events values
('event-1',1,'a10', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 10:28:00');

-- yesterday
insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 10:28:00');

insert into events values
('event-1',1,'a6', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 11:28:01');

insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 13:28:00');

insert into events values
('event-1',1,'a6', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 15:28:01');

insert into events values
('event-1',1,'a9', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 16:28:00');

insert into events values
('event-1',1,'a10', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-16 17:28:01');
--today
insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 04:27:00');

insert into events values
('event-1',1,'a2', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 04:27:01');

insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 04:27:02');

insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 05:27:02');

insert into events values
('event-1',1,'a3', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 05:28:02');

insert into events values
('event-1',1,'a2', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 05:28:02');

insert into events values
('event-1',1,'a4', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-17 05:28:02');

insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-18 05:28:02');

insert into events values
('event-1',1,'a2', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-18 05:28:02');

insert into events values
('event-1',1,'a5', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-18 05:28:02');


create materialized view event_total_user_by_source_ts_v to event_total_user_by_source_ts as
SELECT
    toStartOfHour(at) AS time_stamp,
    tenant_id,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    time_stamp,
    tenant_id,
    source_id
;
-----------------------------------------RESULT FINALL----------------------------------------
--today
SELECT time_stamp as data_point,
       tenant_id,
       source_id,
       uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE tenant_id = 1 AND source_id = 's1' AND time_stamp >= toStartOfDay(now())
GROUP BY tenant_id, source_id, data_point
ORDER BY  tenant_id, source_id, data_point;

-- yesterday
SELECT time_stamp as data_point,
       tenant_id,
       source_id,
       uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE tenant_id = 1 AND source_id = 's1'
  AND time_stamp < toStartOfDay(now())
  AND time_stamp >= toStartOfDay(yesterday())
GROUP BY tenant_id, source_id, data_point
ORDER BY  tenant_id, source_id, data_point;

-- last week
SELECT toStartOfDay(time_stamp) AS data_point,
       tenant_id,
       source_id,
       uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE tenant_id = 1 AND source_id = 's1' AND time_stamp >= subtractDays(now(), 7)
GROUP BY tenant_id, source_id, data_point
ORDER BY  tenant_id, source_id, data_point;


-----------------------------------------RESULT FINALL----------------------------------------
show create table event_total_user_by_source_ts;
show create table event_total_user_by_source_ts_mv;

select * from event_total_user_by_source_ts;
select * from events;

select now();
select timezone();
select toStartOfHour(now())
show create table event_total_user_by_source_daily;

select now();
select subtractDays(now(),7);


SELECT time_stamp as data_point,
       tenant_id,
       source_id,
       uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE tenant_id = 1 AND source_id = 'Google Ads' AND time_stamp >= toStartOfDay(now())
GROUP BY tenant_id, source_id, data_point
ORDER BY  tenant_id, source_id, data_point
;

select toStartOfDay(now())
;


SELECT toStartOfDay(time_stamp)) AS data_point,
       uniqMerge(value) AS value
FROM event_total_user_by_source_ts
WHERE tenant_id = %(tenant_id)s
      AND source_id = %(source_id)s
      AND time_stamp >=  subtractDays(now(),7)
GROUP BY tenant_id, source_id, data_point
ORDER BY  tenant_id, source_id, data_point


WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toDateTime(toStartOfSecond(dt64, 'Europe/Moscow'));




WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toDateTime(formatDateTime(toStartOfDay(dt64, 'UTC'), '%Y-%m-%d %H:%M:%S'))
;
select now();
select subtractDays(now(),6);



----------------------------------------------FILL MISS DAY
-- LAST WEEK
select data_point, value
from
(
select toStartOfDay(subtractDays(now(), 6)) + number*60*60*24 as data_point
from system.numbers
limit 7
) as t1
 left   join
(
    SELECT toStartOfDay(time_stamp) AS time_stamp,
           uniqMerge(value)         AS value
    FROM event_total_user_by_source_ts
    WHERE tenant_id = 1
      AND source_id = 'Google Ads'
      AND time_stamp >= subtractDays(now(), 6)
      AND time_stamp < now()
    GROUP BY tenant_id, source_id, time_stamp
    ORDER BY tenant_id, source_id, time_stamp
) as t2
on t1.data_point = t2.time_stamp
;


--- TODAY
select t1.data_point, t2.value from
(
    select data_point
    from (
          select toStartOfDay(now()) + number * 60 * 60 as data_point
          from system.numbers
          limit 24
             )
    where data_point <= now()
) as t1
left join
(
    SELECT time_stamp,
    uniqMerge(value) AS value FROM event_total_user_by_source_ts
WHERE tenant_id = 1
    AND source_id = 'Google Ads'
    AND time_stamp >= toStartOfDay(now())
    AND time_stamp < now()
GROUP BY tenant_id, source_id, time_stamp
ORDER BY  tenant_id, source_id, time_stamp
) as t2 on t1.data_point = t2.time_stamp;

-- YESTERDAY
select toUnixTimestamp(t1.data_point), t2.value from
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
on t1.data_point = t2.time_stamp
;






SELECT
    uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE (tenant_id = 1) AND (source_id = 'IOS-1gazgMAxIp60M6k9YE3HdghgRda')
GROUP BY
    tenant_id,
    source_id
;



show create event_total_user_by_source_ts;

CREATE TABLE event_total_user_by_source_ts
(
    `time_stamp` DateTime,
    `tenant_id` UInt16,
    `source_id` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_id, time_stamp)
;



SELECT
    toUnixTimestamp(t1.data_point) AS data_point,
    t2.value AS value
FROM
(
    SELECT data_point
    FROM
    (
        SELECT toStartOfDay(now()) + ((number * 60) * 60) AS data_point
        FROM system.numbers
        LIMIT 24
    )
    WHERE data_point <= now()
) AS t1
LEFT JOIN
(
    SELECT
        time_stamp,
        uniqMerge(value) AS value
    FROM event_total_user_by_source_ts
    WHERE (tenant_id = 1)
        AND (source_id = 'IOS-1gazgMAxIp60M6k9YE3HdghgRda')
        AND (time_stamp >= toStartOfDay(now()))
        AND (time_stamp < now())
    GROUP BY
        tenant_id,
        source_id,
        time_stamp
    ORDER BY
        tenant_id ASC,
        source_id ASC,
        time_stamp ASC
) AS t2 ON t1.data_point = t2.time_stamp
;


select * from event_total_user_by_source_scope_ts;

show create table event_total_user_by_source_scope_ts;
show create table event_total_user_by_source_scope_ts_mv;



--------- RENAME MV's name.

-- Rename table.
CREATE TABLE events_total_distinct_users_by_source_scope_ts
(
    `tenant_id` UInt16,
    `source_scope` String,
    `time_stamp` DateTime,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_scope, time_stamp)
;

-- Drop MV.
drop table event_total_user_by_source_scope_ts_mv;

-- Create new MV
drop table events_total_distinct_users_by_source_scope_ts_mv;
CREATE MATERIALIZED VIEW events_total_distinct_users_by_source_scope_ts_mv TO events_total_distinct_users_by_source_scope_ts
AS
SELECT
    tenant_id,
    scope as source_scope,
    toStartOfFiveMinute(at) AS time_stamp,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    tenant_id,
    source_scope,
    time_stamp
;


-----------TEST
select * from events_total_distinct_users_by_source_scope_ts;

SELECT uniqMerge(value) AS total_user
FROM events_total_distinct_users_by_source_scope_ts
WHERE (tenant_id = 0) AND (source_scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u')
GROUP BY
    tenant_id,
    source_scope;

truncate table events_total_distinct_users_by_source_scope_ts;
select count() from events_total_distinct_users_by_source_scope_ts;


truncate table events_total_distinct_users_by_source_scope_ts;
INSERT INTO events_total_distinct_users_by_source_scope_ts
SELECT
    tenant_id,
    scope as source_scope,
    toStartOfFiveMinute(at) AS time_stamp,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    tenant_id,
    source_scope,
    time_stamp;

select count(*) from events_total_distinct_users_by_source_scope_ts;


select count(source_scope) from events_total_distinct_users_by_source_scope_ts;
select * from events_total_distinct_users_by_source_scope_ts where source_scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u';

select count(distinct anonymous_id)
from events
where scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u';



show tables ;