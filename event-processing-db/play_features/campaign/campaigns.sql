select * from events;

-- Conversion Event - Campaign
-- campaign_id, total_value

-- tenant, campaign_id, segment, start, end



SELECT
       campaign_id,
       sumOrDefault(total_value) AS revenue,
       countOrDefault(DISTINCT anonymous_id) AS distinct_users,
       countOrDefault(anonymous_id) AS users
FROM
(
    SELECT
        anonymous_id,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value,
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_id')] AS campaign_id
    FROM events
    WHERE tenant_id = 1
        AND at >= '2020-01-01 00:00:00'
        AND at <= '2020-10-10 00:00:00'
)
WHERE campaign_id = 'c1'
GROUP BY campaign_id
;



SELECT
       campaign_id,
       sumOrDefault(total_value) AS revenue,
       countOrDefault(DISTINCT anonymous_id) AS distinct_users,
       countOrDefault(anonymous_id) AS users
FROM
(
    SELECT
        anonymous_id,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value,
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_id')] AS campaign_id
    FROM events
    WHERE tenant_id = 1
        AND at >= '2020-01-01 00:00:00'
        AND at <= '2020-10-10 00:00:00'
)
WHERE campaign_id = 'c1'
GROUP BY campaign_id
;

-----------------Campaign Engagement Table
drop table event_campaign_engagement;



select
from events_campaign_engagement
where tenant_id = 1 and event_name = 'click' and



;


----------------------------------------------Campaign Uplift
select * from events;
SELECT
       tenant_id,
       campaign_id,
       sumOrDefault(total_value) AS revenue,
       countOrDefault(DISTINCT anonymous_id) AS distinct_users,
       countOrDefault(anonymous_id) AS users
FROM
(
    SELECT
        `tenant_id`,
        anonymous_id,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value,
       `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_campaign')] AS campaign_id
    FROM events
    WHERE tenant_id = 1
        AND at >='2020-01-01 00:00:00'
        AND at <= '2020-10-10 00:00:00'
)
WHERE campaign_id ='c1'
GROUP BY tenant_id, campaign_id;



insert into segment_users values
(1, 's1', ['u1', 'u2', 'u3'], toDateTime('2020-02-05 00:00:00'));


------------------------------------ Campaign Trending Metric TS
select * from events_campaign_engagement;


------------------------------------Total click by tenant
SELECT t1.time_stamp,
       t2.avg_click_per_campaign
FROM
(
    WITH
        60*60*24 AS delta,
        toDateTime('2020-10-01 00:00:00') AS start,
        toDateTime('2020-10-10 00:00:00') AS end,
        ceil((end - start) / delta) + 1 AS n
    SELECT toStartOfDay(start) + number * delta AS time_stamp
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT avg(clicks) AS avg_click_per_campaign,
       time_stamp
    FROM
    (
        SELECT campaign_id,
               count(campaign_id) AS clicks,
               toStartOfDay(at) AS time_stamp
        FROM (
            SELECT
                   `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                    at
            FROM events_campaign_engagement
            WHERE tenant_id = 1
              AND event_name = 'click'
              AND at >= '2020-10-01 00:00:00'
              AND at <= '2020-10-10 00:00:00'
        )
        GROUP BY campaign_id, time_stamp
    ) GROUP BY time_stamp
) AS t2 ON t1.time_stamp = t2.time_stamp
;


--------------------------------------Num of clicks by a campaign
SELECT t1.time_stamp,
       t2.clicks
FROM
(
    WITH
        60*60*24 AS delta,
        toDateTime('2020-10-01 00:00:00') AS start,
        toDateTime('2020-10-10 00:00:00') AS end,
        ceil((end - start) / delta) + 1 AS n
    SELECT toStartOfDay(start) + number * delta AS time_stamp
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT
           count(campaign_id) AS clicks,
           toStartOfDay(at) AS time_stamp
    FROM (
        SELECT
               `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                at
        FROM events_campaign_engagement
        WHERE tenant_id = 1
          AND event_name = 'click'
          AND campaign_id = 'c1'
          AND at >= '2020-10-01 00:00:00'
          AND at <= '2020-10-10 00:00:00'
    )
    GROUP BY time_stamp
) AS t2 ON t1.time_stamp = t2.time_stamp
;


----fix bug
SELECT t1.date_time AS date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.avg_click_per_campaign AS value
FROM
(
    WITH
        60*60*24 AS delta,
        toDateTime('2020-01-01 00:00:00') AS start,
        toDateTime('2020-01-01 00:00:00') AS end,
        ceil((end - start) / delta) + 1 AS n
    SELECT toStartOfDay(start) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT avg(clicks) AS avg_click_per_campaign,
       time_stamp
    FROM
    (
        SELECT campaign_id,
               count(campaign_id) AS clicks,
               toStartOfDay(at) AS time_stamp
        FROM (
            SELECT
                   `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                    at
            FROM events_campaign_engagement
            WHERE tenant_id =1
              AND event_name = 'click'
              AND at >= toDateTime('2020-01-01 00:00:00')
              AND at <= toDateTime('2020-01-01 00:00:00')
        )
        GROUP BY campaign_id, time_stamp
    ) GROUP BY time_stamp
) AS t2 ON t1.date_time = t2.time_stamp


SELECT t1.date_time AS date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.clicks AS value
FROM
(
    WITH
        60*60*24 AS delta,
        1601510400 AS start_unix,
        1602288000 AS end_unix,
        ceil((end_unix - start_unix) / delta) + 1 AS n
    SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT
           count(campaign_id) AS clicks,
           toStartOfDay(FROM_UNIXTIME(at)) AS date_time
    FROM
    (
        SELECT
               `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                toUnixTimestamp(at) as at
        FROM events_campaign_engagement
        WHERE tenant_id = 1
          AND event_name = 'click'
          AND campaign_id = 'c1'
          AND at >= 1601510400
          AND at <= 1602288000
    )
    GROUP BY date_time
) AS t2 ON t1.date_time = t2.date_time
;



SELECT date_time AS date_time,
       toUnixTimestamp(date_time) as time_stamp,
       2 AS value
FROM
(
    WITH
        60*60*24 AS delta,
        1601510400 AS start_unix,
        1602288000 AS end_unix,
        ceil((end_unix - start_unix) / delta) + 1 AS n
    SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
)
;

select * from events;
select toUnixTimestamp(toDateTime('2020-10-01 00:00:00')); -- 1601510400
select toUnixTimestamp(toDateTime('2020-10-10 00:00:00')); -- 1602288000

select toUnixTimestamp(toDateTime('2020-01-01 00:00:00')); -- 1577836800
select toUnixTimestamp(toDateTime('2020-01-10 00:00:00')); -- 1578614400




SELECT t1.date_time as date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.avg_click_per_campaign
FROM
(
    WITH
        60*60*24 AS delta,
        1601510400 AS start_unix,
        1602288000 AS end_unix,
        ceil((end_unix - start_unix) / delta) + 1 AS n
    SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT avg(clicks) AS avg_click_per_campaign,
            date_time
    FROM
    (
        SELECT campaign_id,
               count(campaign_id) AS clicks,
               toStartOfDay(FROM_UNIXTIME(at)) AS date_time
        FROM
        (
            SELECT
                   `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                    toUnixTimestamp(at) as at
            FROM events_campaign_engagement
            WHERE tenant_id = 1
              AND event_name = 'click'
              AND at >= 1601510400
              AND at <= 1602288000
        )
        GROUP BY campaign_id, date_time
    ) GROUP BY date_time
) AS t2 ON t1.date_time = t2.date_time;




SELECT t1.date_time as date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.avg_click_per_campaign as value
FROM
(
    WITH
        60*60*24 AS delta,
        %(start_time)s AS start_unix,
        %(end_time)s AS end_unix,
        ceil((end_unix - start_unix) / delta) + 1 AS n
    SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT avg(clicks) AS avg_click_per_campaign,
            date_time
    FROM
    (
        SELECT campaign_id,
               count(campaign_id) AS clicks,
               toStartOfDay(FROM_UNIXTIME(at)) AS date_time
        FROM
        (
            SELECT
                   `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                    toUnixTimestamp(at) as at
            FROM events_campaign_engagement
            WHERE tenant_id = 1
              AND event_name = %(event_name)s
              AND at >= %(start_time)s
              AND at <= %(end_time)s
        )
        GROUP BY campaign_id, date_time
    ) GROUP BY date_time
) AS t2 ON t1.date_time = t2.date_time




SELECT t1.date_time AS date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.clicks AS value
FROM
(
    WITH
        60*60*24 AS delta,
        1601510400 AS start_unix,
        1602288000 AS end_unix,
        ceil((end_unix - start_unix) / delta) + 1 AS n
    SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
    FROM system.numbers
    LIMIT n
) AS t1 LEFT JOIN
(
    SELECT
           count(campaign_id) AS clicks,
           toStartOfDay(FROM_UNIXTIME(at)) AS date_time
    FROM
    (
        SELECT
               `str_properties.vals`[indexOf(str_properties.keys, 'utm_campaign')] AS campaign_id,
                toUnixTimestamp(at) as at
        FROM events_campaign_engagement
        WHERE tenant_id = 1
          AND event_name ='click'
          AND campaign_id = 'c1'
          AND at >= 1601510400
          AND at <= 1602288000
    )
    GROUP BY date_time
) AS t2 ON t1.date_time = t2.date_time;

show  tables ;

