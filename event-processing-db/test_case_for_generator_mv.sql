----------------------------------------------------PROFILE

CREATE TABLE IF NOT EXISTS user_profile
(
    `tenant_id` UInt16,
    `user_id` String,
    `anonymous_id` String,
    `identifies` Array(String),
    `str_properties` Nested(    keys String,     vals String),
    `num_properties` Nested(    keys String,     vals Float32),
    `arr_properties` Nested(    keys String,     vals Array(String)),
    `created_at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, anonymous_id, created_at);



insert into user_profile values
(1, 'u1', 'a0', [], ['gender', 'location'], ['f','DN'], ['total_value', 'last_order_at'], [0, toUnixTimestamp('2020-08-06 00:01:00')], [], [], now());

insert into user_profile values
(2, 'u1', 'a1', [], ['gender','location'], ['f','DH'], ['total_value','last_order_at'], [1,toUnixTimestamp('2020-08-05 00:05:00')], [], [], now());

insert into user_profile values
(1, 'u1', 'a2', [], ['gender','location'], ['o','HP'], ['total_value','last_order_at'], [2,toUnixTimestamp('2020-08-04 12:08:12')], [], [], now());

insert into user_profile values
(2, 'u1', 'a3', [], ['gender','location'], ['m','QN'], ['total_value','last_order_at'], [3,toUnixTimestamp('2020-08-03 11:02:00')], [], [], now());

insert into user_profile values
(1, 'u1', 'a4', [], ['gender'], ['m'], ['total_value','last_order_at'], [400, toUnixTimestamp('2020-08-02 09:45:08')], [], [], now());

insert into user_profile values
(2, 'u1', 'a5', [], ['gender'], ['m'], ['total_value','last_order_at'], [600.05,toUnixTimestamp('2020-08-01 08:30:00')], [], [], now());

-- test multi user

insert into user_profile
    with [['male','female'] as genders, ['HoChiMinh','HaNoi','DaNang', 'VungTau','CanTho'] as locations, ['facebook','google','tiktok'] as sources]
    select
           rand(1)%10+1 as tenant_id,
           'u' as user_id,
           concat('a', toString(number)) as anonymous_id,
           [] as identifies,
           ['gender','location_city', 'source'] as `str_properties.keys`,
           [genders[rand(2)%length(genders)+1], locations[rand(3)%length(locations)+1], sources[rand(4)%length(sources)+1] ] as `str_properties.vals`,
           ['total_values', 'last_order_at'] as `num_properties.keys`,
           [rand(5)%300, toUnixTimestamp('2016-01-01 00:00:00')+number*60] as `num_properties.vals`,
           [''],[[]],
           now()
    from system.numbers
    limit 900000,100000
    limit 1000000
;


CREATE TABLE IF NOT EXISTS user_profile_final
(
    `tenant_id` UInt16,
    `user_id` AggregateFunction(argMax, String, DateTime),
    `anonymous_id` String,
    `identifies` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.vals` AggregateFunction(argMax, Array(Float32), DateTime),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime),
    `created_at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id)
;


CREATE MATERIALIZED VIEW IF NOT EXISTS user_profile_final_mv TO user_profile_final AS
SELECT
    tenant_id,
    argMaxState(user_id, created_at) AS user_id,
    anonymous_id,
    argMaxState(identifies, created_at) AS identifies,
    argMaxState(str_properties.keys, created_at) AS `str_properties.keys`,
    argMaxState(str_properties.vals, created_at) AS `str_properties.vals`,
    argMaxState(num_properties.keys, created_at) AS `num_properties.keys`,
    argMaxState(num_properties.vals, created_at) AS `num_properties.vals`,
    argMaxState(arr_properties.keys, created_at) AS `arr_properties.keys`,
    argMaxState(arr_properties.vals, created_at) AS `arr_properties.vals`,
    max(created_at) AS created_at_final
FROM user_profile
GROUP BY
    tenant_id,
    anonymous_id
;




--------------------------------------------------SEGMENT

CREATE TABLE IF NOT EXISTS segments(
    tenant_id UInt16,
    segment_id String,
    users Array(String),
    at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id)
;

CREATE TABLE IF NOT EXISTS segments_final(
    tenant_id UInt16,
    segment_id String,
    users AggregateFunction(argMax, Array(String), DateTime),
    at_final SimpleAggregateFunction(max, DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, segment_id)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS segments_final_mv TO segments_final AS
SELECT
       tenant_id,
       segment_id,
       argMaxState(users, at) AS users,
       max(at) as at_final
FROM segments
GROUP BY tenant_id, segment_id
;



insert into segments values (1, 's0', ['a0','a2','a4'],now());
insert into segments values (1,'s1', ['a0'],now());
insert into segments values (2,'s2', ['a1', 'a3'], now());
insert into segments values (2, 's3', ['a1', 'a3','a5'], now());


insert into segments values (2, 's3', ['a3'], now());
--test
insert into segments values (1, 's4', ['a6', 'a7'], now());
insert into segments values (1, 's5', ['a8', 'a9'], now());

SELECT tenant_id,
       segment_id
FROM
(
    SELECT tenant_id,
           segment_id,
           argMaxMerge(users) AS users
    FROM segments_final
    GROUP BY tenant_id, segment_id
) ARRAY JOIN users
;




truncate table segments;
truncate table segments_final;
---------------------------------------------------SEGMENT_AGG

CREATE TABLE IF NOT EXISTS segment_agg
(   `tenant_id` UInt16,
    `segment_id` String,
    `time_stamp` DateTime,
    `metric_name` String,
    `metrics_agg` Nested(    keys String,     vals Float32)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, segment_id, metric_name)
;


CREATE TABLE IF NOT EXISTS segment_agg_final
(
    `tenant_id` UInt16,
    `segment_id` String,
    `time_stamp_final` SimpleAggregateFunction(max, DateTime),
    `metric_name` String,
    `metrics_agg.keys` AggregateFunction(argMax, Array(String), DateTime),
    `metrics_agg.vals` AggregateFunction(argMax, Array(Float32), DateTime)
)
ENGINE = AggregatingMergeTree()
PARTITION BY tuple()
ORDER BY (tenant_id, segment_id, metric_name)
;


CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_final_mv TO segment_agg_final AS
SELECT
    tenant_id,
    segment_id,
    max(time_stamp) AS time_stamp_final,
    metric_name,
    argMaxState(metrics_agg.keys, time_stamp) AS `metrics_agg.keys`,
    argMaxState(metrics_agg.vals, time_stamp) AS `metrics_agg.vals`
FROM segment_agg
GROUP BY
    tenant_id,
    segment_id,
    metric_name
;

--------------------------------------------GENDER
drop table segment_agg_gender_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_gender_mv TO segment_agg AS
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'gender' AS metric_name,
    groupArray(cate) AS `metrics_agg.keys`,
    groupArray(count) AS `metrics_agg.vals`
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        pf.cate,
        count() AS count
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            users
        FROM
        (
            SELECT
                tenant_id,
                segment_id,
                argMaxMerge(users) AS users
            FROM segments_final
            GROUP BY
                tenant_id,
                segment_id
        )
        ARRAY JOIN users
    ) AS sf
    INNER JOIN
    (
        SELECT
            tenant_id,
            anonymous_id,
            str_vals[indexOf(str_keys, 'gender')] AS cate
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(str_properties.keys) AS str_keys,
                argMaxMerge(str_properties.vals) AS str_vals
            FROM user_profile_final
            GROUP BY
                tenant_id,
                anonymous_id
        )
    ) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE cate != ''
    GROUP BY
        tenant_id,
        segment_id,
        cate
    ORDER BY
        count DESC,
        cate ASC
)
GROUP BY
    tenant_id,
    segment_id
;

----------------------------------------------REVENUE
drop table segment_agg_revenue_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_revenue_mv TO segment_agg AS
SELECT
    sf.tenant_id,
    sf.segment_id,
    now() AS time_stamp,
    'revenue' AS metric_name,
    ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
    [max(v), avg(v), median(v), min(v)] AS `metrics_agg.vals`
FROM
(
    SELECT
        tenant_id,
        segment_id,
        users
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            argMaxMerge(users) AS users
        FROM segments_final
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        num_vals[indexOf(num_keys, 'total_value')] AS v
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(num_properties.keys) AS num_keys,
            argMaxMerge(num_properties.vals) AS num_vals
        FROM user_profile_final
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
WHERE v > 0
GROUP BY
    tenant_id,
    segment_id
;



--------------------Days since last order
drop table segment_agg_days_since_last_order_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_days_since_last_order_mv TO segment_agg AS
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'days_since_last_order' AS metric_name,
    ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
    [toFloat64(max(days)), avg(days), median(days), toFloat64(min(days))] AS `metrics_agg.vals`
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            users
        FROM
        (
            SELECT
                tenant_id,
                segment_id,
                argMaxMerge(users) AS users
            FROM segments_final
            GROUP BY
                tenant_id,
                segment_id
        )
        ARRAY JOIN users
    ) AS sf
    INNER JOIN
    (
        SELECT
            tenant_id,
            anonymous_id,
            num_vals[indexOf(num_keys, 'last_order_at')] AS t
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(num_properties.keys) AS num_keys,
                argMaxMerge(num_properties.vals) AS num_vals
            FROM user_profile_final
            GROUP BY
                     tenant_id,
                     anonymous_id
        )
    ) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE t >= 0
)
GROUP BY
    tenant_id,
    segment_id
;


----------------------------------------------VIEW
CREATE VIEW IF NOT EXISTS segment_agg_final_v
AS SELECT
        tenant_id,
        segment_id,
        max(time_stamp_final) as time_stamp,
        metric_name,
        argMaxMerge(metrics_agg.keys) as metrics_agg_keys,
        argMaxMerge(metrics_agg.vals) as metrics_agg_vals
FROM segment_agg_final
GROUP BY tenant_id, segment_id, metric_name
ORDER BY tenant_id, segment_id, metric_name
;

SELECT * FROM segment_agg_final_v;




















--- select
select * from segments;
select * from segment_agg;
select * from segments_final;





where tenant_id = 1 and segment_id = 's0' and metric_name = 'gender'


-- drop
drop table user_profile_final_mv;
drop table user_profile_final;
drop table user_profile;

drop table segments;
drop table segment_agg;
drop table segment_agg_final;
drop table segment_agg_final_mv;
drop table segment_agg_gender_mv;
drop table segment_agg_revenue_mv;
drop table segment_agg_day_since_last_order_mv;

-- truncate
truncate table user_profile_final_mv;
truncate table user_profile_final;
truncate table user_profile;

truncate table segments;
truncate table segments_final;
truncate table segment_agg;
truncate table segment_agg_final;
truncate table segment_agg_final_mv;
truncate table segment_agg_gender_mv;
truncate table segment_agg_revenue_mv;
truncate table segment_agg_day_since_last_order_mv;








--------------------------------------------------------------TEST USER PROFILE
select * from user_profile where tenant_id = 1 and anonymous_id = 'a0';

select tenant_id,
       argMaxMerge(user_id),
       anonymous_id,
       argMaxMerge(str_properties.keys),
       argMaxMerge(str_properties.vals),
       argMaxMerge(num_properties.keys),
       argMaxMerge(num_properties.vals),
       max(created_at_final)
from user_profile_final
group by tenant_id, anonymous_id
limit 1;
--  1.314 sec

select count() from user_profile;
select count() from user_profile_final final;

optimize table user_profile_final;


select * from user_profile
limit 10;









