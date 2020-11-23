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
            WHERE (tenant_id = 1)
                AND (segment_id = '1jsIYxjbtyLb37ZPWfVH9gPNzW7')
                AND (at <= '2020-11-18 10:07:58.936')
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime('2020-11-05 10:07:58') TO toDateTime('2020-11-18 10:07:58') STEP 1*60*60
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= 0
  AND time_stamp <= 9999999999
;
----------------------------

SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
            select toStartOfFiveMinute(at) as time_stamp,
            count(distinct user)    as total_user
            from segment_user
            where tenant_id = 1
              and segment_id = 's1'
              and at <= now64()
            group by tenant_id, segment_id, time_stamp
               order by time_stamp WITH FILL FROM toDateTime('2020-11-17 00:00:00') TO toDateTime('2020-11-18 10:00:00') STEP 1*60*60
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= '2020-11-17 00:00:00'
  AND time_stamp <= '2020-11-18 10:00:00'
;

-----------------------------------------------------
select time_stamp,
       total_users,
       runningAccumulate(sum_total_users)
from (
      select time_stamp,
             total_users,
             sumState(total_users) as sum_total_users
      from (

            select toStartOfFiveMinute(at) as time_stamp,
                   count(distinct user)    as total_users
            from segment_user
            where tenant_id = 1
              and segment_id = 's1'
              and at <= now64()
            group by tenant_id, segment_id, time_stamp
               order by time_stamp WITH FILL FROM toDateTime('2020-11-17 00:00:00') TO toDateTime('2020-11-18 10:00:00') STEP 1*60*60

           )
      group by time_stamp, total_users
        order by time_stamp
         )




-----------fake data

insert into segment_user values
('s1', 1, 'a1', 1, '2020-01-01 00:00:01'),
('s1', 1, 'a2', 1, '2020-01-01 00:00:10'),
('s1', 1, 'a3', 1, '2020-01-01 00:01:02'),
('s1', 1, 'a4', 1, '2020-01-01 00:03:08'),
('s1', 1, 'a5', 1, '2020-01-01 00:08:12'),
('s1', 1, 'a6', 1, '2020-01-01 00:10:00'),
('s1', 1, 'a7', 1, '2020-01-01 01:12:08'),
('s1', 1, 'a8', 1, '2020-01-01 02:12:01'),
('s1', 1, 'a9', 1, '2020-01-01 02:06:09');

insert into segment_user values
('s1', 1, 'a1', 1, '2020-01-01 00:01:01');

insert into segment_user values
('s1', 1, 'a1', 0, '2020-01-01 00:00:01'),















--------------------- Tinh Truoc

SELECT tenant_id,
       segment_id,
       toStartOfFiveMinute(at) as time_stamp,
       uniqState(user) as value
FROM segment_user
WHERE status == 1
GROUP BY tenant_id, segment_id, time_stamp
;



-------- Temp

s1 a1 in 1
s1 a2 in 2
s1 a1  out 3
      4
s1 a3 5
s2 a1 7
s2 a2 8

segment size from 3 to 5 :
3 - 0
4 - 0
5 - 1


show create table events_total_distinct_users_by_source_scope_ts;
-- CREATE TABLE eventify_stag.events_total_distinct_users_by_source_scope_ts
-- (
--     `tenant_id` UInt16,
--     `source_scope` String,
--     `time_stamp` DateTime,
--     `value` AggregateFunction(uniq, String)
-- )
-- ENGINE = MergeTree()
-- PARTITION BY toYYYYMM(time_stamp)
-- ORDER BY (tenant_id, source_scope, time_stamp)
-- ;

show create table events_total_distinct_users_by_source_scope_ts_mv;
CREATE MATERIALIZED VIEW events_total_distinct_users_by_source_scope_ts_mv TO events_total_distinct_users_by_source_scope_ts
AS
SELECT
    tenant_id,
    scope AS source_scope,
    toStartOfFiveMinute(at) AS time_stamp,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    tenant_id,
    source_scope,
    time_stamp;

---------------------------------GET SAMPLE USER BY SEGMENT
select * from segment_users_final_v;

    SELECT
        tenant_id,
        segment_id,
        users
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            users
        FROM segment_users_final_v
        WHERE (tenant_id = 1) AND (segment_id = '1js3CPNZzSelNVm78GG2E8Aksev')
    )
    ARRAY JOIN users
;

-->
drop table segment_user_final_v;
show create table segment_user_final_v;
select * from segment_user;

SELECT tenant_id,
       segment_id,
       user
FROM segment_user_final_v
WHERE tenant_id = 0 AND segment_id = 's4' AND status = 1
;


SELECT
    tenant_id,
    anonymous_id,
    [] AS identities,
    str_pros,
    num_pros,
    [] AS arr_pros,
    at
FROM
(
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = 0 AND segment_id = 's4' AND status = 1
    LIMIT :limit
) AS s
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           str_pros,
           num_pros,
           ps.at > pn.at ? ps.at : pn.at AS at
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((str_key, str_val)) AS str_pros,
               max(at) AS at
        FROM profile_str_final_v
        WHERE tenant_id = :tenant_id AND str_key IN :str_pros_filter
        GROUP BY tenant_id, anonymous_id
    ) AS ps
    JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((num_key, num_val)) AS num_pros,
               max(at)                        AS at
        FROM profile_num_final_v
        WHERE tenant_id = :tenant_id AND num_key IN :num_pros_filter
        GROUP BY tenant_id, anonymous_id
    ) AS pn
    ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
) AS p
ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id);

----------------------TOTAL USER PER SEGMENT


SELECT
    length(users) AS total_user
FROM
(
    SELECT
        argMaxMerge(users) as users
    FROM segment_users_final
    WHERE (tenant_id = :tenant_id) AND (segment_id = :segment_id)
    GROUP BY
        tenant_id,
        segment_id
);

select * from segment_user_final_v where segment_id = 's3';

SELECT count(user) AS total_user
from segment_user_final_v
where tenant_id = :tenant_id and segment_id = segment_id and status = 1
group by tenant_id, segment_id;


---------------------GET TOTAL USER CONVERSION

 SELECT tenant_id, users
 FROM
 (
    SELECT
        tenant_id,
        argMaxMerge(users) as users
    FROM segment_users_final
    WHERE (tenant_id = :tenant_id) AND (segment_id = :segment_id)
    GROUP BY
        tenant_id,
        segment_id
 ) ARRAY JOIN users
;





SELECT
    count() as total_user_have_conversion
FROM
(
    SELECT tenant_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = :tenant_id AND segment_id = :segment_id AND status = 1
)  as sf
JOIN
(
    SELECT tenant_id,
           anonymous_id
    FROM profile_num_final_v
    WHERE tenant_id = :tenant_id AND num_key = 'revenue'
) as pf
ON sf.tenant_id = pf.tenant_id and sf.user =pf.anonymous_id
;







--
select tenant_id,
       anonymous_id
from profile_num_final_v
where tenant_id = 1 and num_key = 'revenue';

insert into profile_num values ('a1', 1, 'revenue', 1, now64());
insert into profile_num values ('a2', 1, 'revenuee', 1, now64());


SELECT
    tenant_id,
    anonymous_id,
    arrayExists(key -> key == 'revenue', num_keys) as have_conversion
FROM
    (
        select tenant_id,
               anonymous_id,
               groupArray(num_key) as num_keys
        from profile_num
        where tenant_id = 1
        group by tenant_id, anonymous_id
    )
    WHERE have_conversion != 0;

-----------------------------------------GET REPORT AGG STR
SELECT
    pf.cate as key,
    count() AS value
FROM
(
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = :tenant_id AND segment_id = :segment_id AND status = 1
) AS sf
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           str_val AS cate
    FROM profile_str_final_v
    WHERE tenant_id = :tenant_id AND str_key = :pros_key
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.user = pf.anonymous_id)
GROUP BY
    tenant_id,
    segment_id,
    cate
ORDER BY
    value DESC,
    key ASC

------------------------------------GET REPORT NUM PROS

SELECT
    keys[n] AS key,
    values[n] AS value
FROM
(
    SELECT
        ['{MAX}', '{AVG}', '{MED}', '{MIN}'] AS keys,
        [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS values
    FROM
    (
        SELECT tenant_id,
               segment_id,
               user
        FROM segment_user_final_v
        WHERE tenant_id = :tenant_id AND segment_id = :segment_id AND status = 1
    ) AS sf
    INNER JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               num_val AS v
        FROM profile_num_final_v
        WHERE tenant_id = :tenant_id AND num_key = :pros_key AND v >= 0
    ) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.user = pf.anonymous_id)
) ARRAY JOIN arrayEnumerate(keys) AS n

----------------------------REPORT DAY SINCE LAST ORDER

SELECT
    ['{MAX}', '{AVG}', '{MED}', '{MIN}'] AS {METRIC_KEY},
    [toFloat64(maxOrDefault(days)), avgOrDefault(days), medianOrDefault(days), toFloat64(minOrDefault(days))] AS {METRIC_VALUE}
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
    FROM
    (
        SELECT tenant_id,
               segment_id,
               user
        FROM segment_user_final_v
        WHERE tenant_id = :tenant_id AND segment_id = :segment_id AND status = 1
    ) AS sf
    INNER JOIN
    (
        SELECT  tenant_id,
                anonymous_id,
                num_val AS t
        FROM profile_num_final_v
        WHERE tenant_id = :tenant_id AND num_key = :pros_key AND t > 0
    ) AS pf ON (sf.user = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
)
;


--------------------------GET TOTAL REVENUE


SELECT
    sum(v) as total_revenue
FROM
(
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = :tenant_id AND segment_id = :segment_id AND status = 1
) AS sf
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           num_val AS v
    FROM profile_num_final_v
    WHERE tenant_id = :tenant_id AND num_key = :pros_key AND v >= 0
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.user = pf.anonymous_id)

---------------------------------- GET SEGMENT TOTAL TS

SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
            SELECT toStartOfFiveMinute(at) AS time_stamp,
            COUNT(DISTINCT user)    AS total_user
            FROM segment_user
            WHERE tenant_id = :tenant_id
              AND segment_id = :segment_id
              AND at <= :end_time
            GROUP BY tenant_id, segment_id, time_stamp
               ORDER BY time_stamp WITH FILL FROM toDateTime(:start_time) TO toDateTime(:end_time) STEP {fill_steps}
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) AS idx
WHERE time_stamp >= :start_time
  AND time_stamp <= :end_time;


select
       segment_id,
       tenant_id,
       toStartOfFiveMinute(time_stamp) as time_stamp,
       total_user
from segment_size
where tenant_id = 0 and segment_id = 's3' and time_stamp <= '2020-11-14 02:55:31'
order by time_stamp WITH FILL FROM toDateTime('2020-11-14 02:45:31') TO toDateTime('2020-11-14 02:55:31') STEP  60

;

select toDateTime('2020-11-14 02:45:31'), toDateTime('2020-11-14 02:45:31');


select segment_id,
       tenant_id,
       time_stamp,
       total_user
from segment_size
where tenant_id = 0 and segment_id = 's3';

------------test
select * from segment_size_final_v;

-----------------------------------------------

SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
            SELECT toStartOfFiveMinute(at) AS time_stamp,
            COUNT(DISTINCT user)    AS total_user
            FROM segment_user
            WHERE tenant_id = :tenant_id
              AND segment_id = :segment_id
              AND at <= :end_time
            GROUP BY tenant_id, segment_id, time_stamp
               ORDER BY time_stamp WITH FILL FROM toDateTime(:start_time) TO toDateTime(:end_time) STEP {fill_steps}
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) AS idx
WHERE time_stamp >= :start_time
  AND time_stamp <= :end_time;



SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT groupArray(time_stamp) as time_stamps,
            arrayFill(x -> x != 0, groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
               time_stamp,
               total_user
        FROM segment_size
        WHERE tenant_id = 0 AND segment_id = 's16' AND time_stamp <= '2020-11-14 05:00:00'
        ORDER BY time_stamp WITH fill FROM toDateTime('2020-11-14 00:00:00') TO toDateTime('2020-11-14 05:00:00') STEP 10*60
    )
)ARRAY JOIN arrayEnumerate(time_stamps) AS idx
WHERE time_stamp >= '2020-11-14 00:00:00'
  AND time_stamp <= '2020-11-14 05:00:00'
;

insert into segment_size values
('s16', 0, '2020-11-14 04:10:00', 10);

select * from segment_size;



SELECT
       tenant_id,
       segment_id,
       length(users) AS num_of_users
FROM segment_users
WHERE tenant_id = 1
    AND segment_id = '1js1xIzEc03e9XIdm34Iw9u1liz'
    AND toUnixTimestamp64Milli(at) <= now64()
ORDER BY at DESC
LIMIT 1;

SELECT tenant_id,
       segment_id,
       total_user AS num_of_users
FROM segment_size
WHERE tenant_id = 0 AND segment_id = 's16' AND toUnixTimestamp(time_stamp) <= 1605327000
ORDER BY time_stamp DESC
LIMIT 1

;
select * from eventify.segment_users;
select * from segment_size;
select toUnixTimestamp('2020-11-14 04:10:00');