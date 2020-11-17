truncate table event_total_user_by_source_ts;
truncate table events;
drop table event_total_user_by_source_ts_mv;

show create table event_total_user_by_source_ts;
CREATE TABLE eventify.event_total_user_by_source_ts
(
    `time_stamp` DateTime,
    `tenant_id` UInt16,
    `source_id` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
;
show create table event_total_user_by_source_ts_mv;

CREATE MATERIALIZED VIEW eventify.event_total_user_by_source_ts_mv TO eventify.event_total_user_by_source_ts
AS
SELECT
    toStartOfFifteenMinutes(at) AS time_stamp,
    tenant_id,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    uniqState(anonymous_id) AS value
FROM eventify.events
GROUP BY
    time_stamp,
    tenant_id,
    source_id
;

-- TODAY
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
    WHERE (tenant_id = :tenant_id)
        AND (source_id = :source_id)
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


SELECT FROM_UNIXTIME(423543535);


--- get start - end time
-- select toUnixTimestamp(toStartOfDay(toDateTime('2020-09-23 00:00:00', 'Asia/Ho_Chi_Minh')));
-- 1600794000
-- select toUnixTimestamp(toDateTime(now(), 'Asia/Ho_Chi_Minh'));
-- 1600826725



select now();
select toStartOfFifteenMinutes(now());

select subtractHours(toStartOfDay(now()), 7);
select toUnixTimestamp(toDateTime('2020-09-22 17:00:00')); 1600794000
select toUnixTimestamp(toDateTime('2020-09-23 04:15:09')); 1600834509, 1600834449


with
    15 as fiffteen,
    1600794000 as start_unix,
     1600834449 as end_unix,
    FROM_UNIXTIME(start_unix) as start,
   FROM_UNIXTIME(end_unix) as end,
  ceil((end_unix - start_unix)/(60*fiffteen)) as n
select addMinutes(toStartOfFifteenMinutes(start), number * fiffteen) as time_stamp
from system.numbers
limit n
;




select
       toUnixTimestamp(t1.time_stamp),
       t1.time_stamp,
       t2.value
from
(
    with
    15 as fiffteen,
    1600794000 as start_unix,
     1600834509 as end_unix,
    FROM_UNIXTIME(start_unix) as start,
   FROM_UNIXTIME(end_unix) as end,
  ceil((end_unix - start_unix)/(60*fiffteen)) as n
    select addMinutes(toStartOfFifteenMinutes(start), number * fiffteen) as time_stamp
from system.numbers
limit n
) as t1
left join
    (
        with
            FROM_UNIXTIME(1600794000) as start,
            FROM_UNIXTIME(1600834509) as end
        SELECT time_stamp,
            uniqMerge(value) AS value
     FROM event_total_user_by_source_ts
     WHERE (tenant_id = 1)
       AND (source_id = 'Google Ads')
       AND (time_stamp >= start)
       AND (time_stamp <= end)
     GROUP BY tenant_id,
              source_id,
              time_stamp
    ) as t2
on t1.time_stamp = t2.time_stamp
order by t1.time_stamp
;


insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:00:00');
insert into events values
('event-1',1,'a2', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:01:00');
insert into events values
('event-1',1,'a3', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:02:00');
insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:04:00');


insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:15:00');
insert into events values
('event-1',1,'a4', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:16:00');
insert into events values
('event-1',1,'a5', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 17:29:00');


insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 21:15:00');
insert into events values
('event-1',1,'a5', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 21:16:00');
insert into events values
('event-1',1,'a6', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-22 21:17:00');

insert into events values
('event-1',1,'a6', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-23 04:15:09');




------------------------ YESTERDAY
select toUnixTimestamp(toDateTime('2020-09-21 17:00:00')); 1600707600
select toUnixTimestamp(toDateTime('2020-09-22 16:59:59')); 1600793999


select toUnixTimestamp(t1.time_stamp),
       t1.time_stamp,
       t2.value
from
(
    with
        15 as delta,
        1600707600 as start_unix,
        1600793999 as end_unix,
        FROM_UNIXTIME(start_unix) as start,
        FROM_UNIXTIME(end_unix) as end,
        ceil((end_unix - start_unix) / (60 * delta)) as n
    select addMinutes(toStartOfFifteenMinutes(start), number * delta) as time_stamp
    from system.numbers
    limit n
) as t1
left join
    (
        with
            FROM_UNIXTIME(1600707600) as start,
            FROM_UNIXTIME(1600793999) as end
        SELECT time_stamp,
            uniqMerge(value) AS value
     FROM event_total_user_by_source_ts
     WHERE (tenant_id = 1)
       AND (source_id = 'Google Ads')
       AND (time_stamp >= start)
       AND (time_stamp <= end)
     GROUP BY tenant_id,
              source_id,
              time_stamp
    ) as t2
on t1.time_stamp = t2.time_stamp
;

insert into events values
('event-1',1,'a10', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-21 17:00:00');
insert into events values
('event-1',1,'a11', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-21 17:01:00');
insert into events values
('event-1',1,'a12', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-21 17:02:00');
insert into events values
('event-1',1,'a1', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-21 17:02:00');



-- LAST WEEK
select toUnixTimestamp(toDateTime('2020-09-10 17:00:00')); 1599757200
select toUnixTimestamp(toDateTime('2020-09-30 16:59:59')); 1601485199

select toUnixTimestamp(t1.time_stamp),
       t1.time_stamp,
       t2.value
from
(
    with
        60 as delta,
        1599757200 as start_unix,
        1601485199 as end_unix,
        FROM_UNIXTIME(start_unix) as start,
        FROM_UNIXTIME(end_unix) as end,
        ceil((end_unix - start_unix) / (60 * delta)) as n
    select addMinutes(toStartOfFifteenMinutes(start), number * delta) as time_stamp
    from system.numbers
    limit n
) as t1
left join
    (
        with
            FROM_UNIXTIME(1599757200) as start,
            FROM_UNIXTIME(1601485199) as end
        SELECT toStartOfHour(time_stamp) as time_stamp,
            uniqMerge(value) AS value
     FROM event_total_user_by_source_ts
     WHERE (tenant_id = 1)
           AND (source_id = 'ADR-1htXpdVcfiprEmhWgs3GSEd13qF')
           AND (time_stamp >= start)
           AND (time_stamp <= end)
     GROUP BY tenant_id,
              source_id,
              time_stamp
    ) as t2
on t1.time_stamp = t2.time_stamp
;

select timezone();

insert into events values
('event-1',1,'a20', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 17:00:00');
insert into events values
('event-1',1,'a20', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 17:46:00');
insert into events values
('event-1',1,'a23', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 17:16:00');
insert into events values
('event-1',1,'a22', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-12 18:00:00');
insert into events values
('event-1',1,'a21', 'checkout', [],[],['source.scope'],['Google Ads'],[],[],[],[],'2020-09-13 17:02:00');




INSERT INTO events WITH
    ['Added to Card', 'App Launched', 'App Uninstalled', 'Category Viewed', 'Order Completed', 'Payment Offer Applied', 'Product Viewed', 'Searched'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['session-123', 'session-345'] as sessions,
    ['ADR-1htXpdVcfiprEmhWgs3GSEd13qF'] as sources
SELECT
    rand(6)%10000 as event_id,
    (rand(1) % 1) + 1 AS tenant_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['product','session_id', 'device','source.scope'] AS `str_properties.keys`,
    [products[(rand(3) % length(products)) + 1], sessions[rand(4)%length(sessions) + 1], devices[rand(5) % length(devices) + 1], sources[rand(6)%length(sources) + 1]] AS `str_properties.vals`,
    [] AS `num_properties.keys`,
    [] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-09-21 00:00:00') + rand(10)%1000*2  AS at
FROM system.numbers
LIMIT 1000;


select * from events limit 10;
select * from event_total_user_by_source_ts limit 10;





---------------REFACTOR

SELECT
       toUnixTimestamp(t1.time_stamp) AS time_stamp,
       t1.time_stamp as date_time,
       t2.value as value
FROM
(
    WITH
        15 AS delta,
        {start_time} AS start_unix,
        {end_time} AS end_unix,
        FROM_UNIXTIME(start_unix) AS start,
        FROM_UNIXTIME(end_unix) AS end,
        ceil((end_unix - start_unix)/(60*delta)) AS n
    SELECT addMinutes(toStartOfFifteenMinutes(start), number * delta) AS time_stamp
    FROM system.numbers
    LIMIT n
) AS t1
LEFT JOIN
(
    WITH
        FROM_UNIXTIME({start_time}) AS start,
        FROM_UNIXTIME({end_time}) AS end
    SELECT  time_stamp,
            uniqMerge(value) AS value
    FROM event_total_user_by_source_scope_ts
    WHERE (tenant_id = :tenant_id)
            AND (source_scope = :source_scope)
            AND (time_stamp >= start)
            AND (time_stamp <= end)
    GROUP BY tenant_id,
             source_scope,
             time_stamp
) AS t2
ON t1.time_stamp = t2.time_stamp
ORDER BY t1.time_stamp
;



---------EDIT
select * from events_total_distinct_users_by_source_scope_ts;



SELECT  (time_stamp) as time_stamp,
        uniqMerge(value) AS value
FROM events_total_distinct_users_by_source_scope_ts
WHERE (tenant_id = 0)
        AND (source_scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u')
        AND (time_stamp >= '2020-10-15 00:00:00')
        AND (time_stamp <= '2020-10-15 10:00:00')
GROUP BY tenant_id,
         source_scope,
         time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-10-15 00:00:00') TO toDateTime('2020-10-15 10:00:00') STEP 5*60
;

select toUnixTimestamp('2020-10-20 00:00:00') as start,
       toUnixTimestamp('2020-10-30 00:00:00') as end;



select * from events limit 10;
select * from events where scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u' limit 10;


select toUnixTimestamp('2020-10-21 00:00:00') as start,
       toUnixTimestamp('2020-10-23 00:00:00') as end;

------------------Fix bug data source TS
SELECT  toStartOfFifteenMinutes(time_stamp) as time_stamp,
        uniqMerge(value) AS value
FROM events_total_distinct_users_by_source_scope_ts
WHERE (tenant_id = 1)
        AND (source_scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u')
        AND (time_stamp >= '2020-10-22 10:00:00')
        AND (time_stamp <= '2020-10-22 16:00:00')
GROUP BY tenant_id,
         source_scope,
         time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-10-22 10:00:00') TO toDateTime('2020-10-22 16:00:00') STEP 15*60;




select * from events_total_distinct_users_by_source_scope_ts where source_scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u';

select count(distinct anonymous_id)
from events
where tenant_id =1 and  scope = 'JS-1iRNWBw2hNQTRQibnJeb8NTep7u';



show create table events_total_distinct_users_by_source_scope_ts;
CREATE TABLE eventify.events_total_distinct_users_by_source_scope_ts
(
    `tenant_id` UInt16,
    `source_scope` String,
    `time_stamp` DateTime,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_scope, time_stamp);

show create table events_total_distinct_users_by_source_scope_ts_mv;
CREATE MATERIALIZED VIEW eventify.events_total_distinct_users_by_source_scope_ts_mv TO eventify.events_total_distinct_users_by_source_scope_ts
AS
SELECT
    tenant_id,
    scope AS source_scope,
    toStartOfFiveMinute(at) AS time_stamp,
    uniqState(anonymous_id) AS value
FROM eventify.events
GROUP BY
    tenant_id,
    source_scope,
    time_stamp


