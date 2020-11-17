show tables;

select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';

show create table events;
truncate table events;


INSERT INTO events WITH
    ['Added to Card', 'App Launched', 'App Uninstalled', 'Category Viewed', 'Order Completed', 'Payment Offer Applied', 'Product Viewed', 'Searched'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['ses3'] as sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda',
    'IOS-1gazdzPC63nBGx0ufPvAnTEYQI6',
    'ADR-1hALTPDa3657iJLX75Epw0ZaHcv',
    'ADR-1gazha8AIVek6HoMVPiD8Edhpfb',
    'ADR-1gazfw2EpFzbWtDrW3UtQEWWuZt',
    'ADR-1gazi1MawjcpcSJX57cqBY1hPWc',
    'IOS-1gazh1GiSdLxvAaxpIfxTBhXKjD',
    'ADR-1gazepem8rDut2q43JJveB7IjOD',
    'ADR-1gazjekxriGoTSiOsrAiy1VswLi',
    'ADR-1gazgddT6Km80IriMr9Mwn0dPh3',
    'ADR-1hARTg5ZfLb9sauXFer0WR3V8W5',
    'IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources
SELECT
    rand(6)%10000 as id,
    (rand(1) % 1) + 1 AS tenant_id,
    concat('a', toString(1)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['product','sessionId', 'device','source.scope'] AS `str_properties.keys`,
    [products[(rand(3) % length(products)) + 1], sessions[rand(4)%length(sessions) + 1], devices[rand(5) % length(devices) + 1], sources[rand(6)%length(sources) + 1]] AS `str_properties.vals`,
    [] AS `num_properties.keys`,
    [] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-09-15 01:10:00') AS at
FROM system.numbers
LIMIT 1
;

select * from events;
alter table events delete where at = toDateTime('2020-09-15 01:10:00');



select avg(duration) as avg_visit_duration
from
(
select
        max(at) - min(at) as duration
from
(
    select
        tenant_id,
        anonymous_id,
       `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] as session_id,
       at
    from events
    where tenant_id = 1 and anonymous_id = 'a1'
)
group by tenant_id, anonymous_id, session_id
);


--- SOLUTION FINAL
SELECT avgOrDefault(sums) AS avg_session_duration
FROM
(
    SELECT
            avgOrDefault(arraySum(delta_arr)) as sums
    FROM
    (
          SELECT session_id,
                 arrayDifference(time_arr) AS delta_arr
          FROM
          (
                SELECT session_id,
                       groupArray(toUnixTimestamp(at)) AS time_arr
                FROM
                (
                    SELECT `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] AS session_id,
                         at
                    FROM events
                    WHERE tenant_id = 1 AND anonymous_id = 'a2'
                    ORDER BY at
                )
                GROUP BY session_id
          )
    )
)
;

SELECT
        avgOrDefault(arrdelta_arr) AS avg_session_duration
FROM
(
      SELECT session_id,
             arrayDifference(time_arr) AS delta_arr
      FROM
      (
            SELECT session_id,
                   groupArray(toUnixTimestamp(at)) AS time_arr
            FROM
            (
                SELECT `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] AS session_id,
                     at
                FROM events
                WHERE tenant_id = 1 AND anonymous_id = 'a1'
                ORDER BY at
            )
            GROUP BY session_id
      )
)


-----------------------------------------SOLUTION FINAL
SELECT avg(delta) AS avg_between_visit
FROM (
       SELECT
              runningDifference(cityHash64(session_id))  AS  sess_diff,
              runningDifference(toUnixTimestamp(at)) AS delta
       FROM
        (
             SELECT
                    `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] AS session_id,
                    at
             FROM events
             WHERE tenant_id = 1 AND anonymous_id = 'a1'
             ORDER BY at
        )
)
WHERE sess_diff != 0
;

-- max

select
       session_id,
       at,
       runningDifference(at) as run_diff
from
(
    select
        tenant_id,
        anonymous_id,
       `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] as session_id,
       at
    from events
    where tenant_id = 1 and anonymous_id = 'a1'
    order by at asc
)

;



-- C1 : array filter
select
    arrayFilter(session_id_arr, arrayPopFront() session_id_arr)
from
(
    select groupArray(session_id) as session_id_arr,
           arrayDifference(groupArray(toUnixTimestamp(at))) as delta_arr
    from
    (
            select
                tenant_id,
                anonymous_id,
                `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] as session_id,
                at
            from events
            where tenant_id = 1 and anonymous_id = 'a1'
            order by at asc
    )
)
;

select
        arrayEnumerate(sess_delta) as idx_arr,
       arrayFilter((i,x) -> x[i].1 != x[i-1].1, idx_arr, sess_delta)
from (
      select arrayZip(session_id_arr, delta_arr) as sess_delta
      from (
            select groupArray(session_id)                           as session_id_arr,
                   arrayDifference(groupArray(toUnixTimestamp(at))) as delta_arr
            from (
                  select tenant_id,
                         anonymous_id,
                         `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] as session_id,
                         at
                  from events
                  where tenant_id = 1
                    and anonymous_id = 'a1'
                  order by at asc
                     )
               )
         )

;
-- array join
select
       arrayJoin(s)
from (
      select session_id                         as session_id_arr,
             arrayDifference(groupArray(toUnixTimestamp(at))) as delta_arr
      from (
            select tenant_id,
                   anonymous_id,
                   `str_properties.vals`[indexOf(`str_properties.keys`, 'sessionId')] as session_id,
                   at
            from events
            where tenant_id = 1
              and anonymous_id = 'a1'
            order by at asc
      )
)
;
--

select * from user_profile_final_v;

SELECT
       arrayFilter(x -> ((x.1) IN ['revenue']), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros
FROM user_profile_final_v
WHERE tenant_id = 1 AND anonymous_id = 'a1';

SELECT num_pros_vals[indexOf(num_pros_keys, 'revenue')]
FROM user_profile_final_v
WHERE tenant_id = 1 and anonymous_id = 'a1';



SELECT num_pros_vals[indexOf(num_pros_keys, 'total_order')]
FROM user_profile_final_v
WHERE tenant_id = 1 and anonymous_id = 'a1';



SELECT avgOrDefault(delta) AS avg_between_session
FROM (
       SELECT
              runningDifference(cityHash64(session_id))  AS  sess_diff,
              runningDifference(toUnixTimestamp(at)) AS delta
       FROM
        (
             SELECT
                    session_id,
                    at
             FROM events
             WHERE tenant_id =1 AND anonymous_id = 'DBzEQzzUKZVe19AhAyKrHG0SpOe'
             ORDER BY at
        )
)
WHERE sess_diff != 0;

select * from events;


SELECT
        avgOrDefault(arraySum(delta_arr)) AS avg_session_duration
FROM
(
      SELECT session_id,
             arrayDifference(time_arr) AS delta_arr
      FROM
      (
            SELECT session_id,
                   groupArray(toUnixTimestamp(at)) AS time_arr
            FROM
            (
                SELECT session_id, at
                FROM events
                WHERE tenant_id = 1 AND anonymous_id = 'DBzEQzzUKZVe19AhAyKrHG0SpOe'
                ORDER BY at
            )
            GROUP BY session_id
      )
)
