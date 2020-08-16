----------------------------------------------------PROFILE

CREATE TABLE IF NOT EXISTS user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity`       Nested(    keys String,     vals String),
    `str_properties` Nested(    keys String,     vals String),
    `num_properties` Nested(    keys String,     vals Float32),
    `arr_properties` Nested(    keys String,     vals Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);



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
    with [['male','female'] as genders, ['HoChiMinh','HaNoi','DaNang', 'VungTau','CanTho'] as locations, ['facebook','google','youtube'] as sources]
    select
           concat('a', toString(number)) as anonymous_id,
           rand(1)%1+1 as tenant_id,
           ['user_id'] as `identity.keys`,
           [concat('user-',randomPrintableASCII(5))] as `identity.vals`,
           ['gender','location_city', 'context_campaign_source'] as `str_properties.keys`,
           [genders[rand(2)%length(genders)+1], locations[rand(3)%length(locations)+1], sources[rand(4)%length(sources)+1] ] as `str_properties.vals`,
           ['total_value', 'last_order_at'] as `num_properties.keys`,
           [rand(5)%300, toUnixTimestamp('2016-01-01 00:00:00')+number*60] as `num_properties.vals`,
           [''],[[]],
           now()
    from system.numbers
    limit 100
    limit 1000000
    limit 900000,100000

;


CREATE TABLE IF NOT EXISTS user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` AggregateFunction(argMax, Array(String), DateTime),
    `identity.vals` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.vals` AggregateFunction(argMax, Array(Float32), DateTime),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime),
    `at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id)
;


CREATE MATERIALIZED VIEW IF NOT EXISTS user_profile_final_mv TO user_profile_final AS
SELECT
    anonymous_id,
    tenant_id,
    argMaxState(identity.keys, at) AS `identity.keys`,
    argMaxState(identity.vals, at) AS `identity.vals`,
    argMaxState(str_properties.keys, at) AS `str_properties.keys`,
    argMaxState(str_properties.vals, at) AS `str_properties.vals`,
    argMaxState(num_properties.keys, at) AS `num_properties.keys`,
    argMaxState(num_properties.vals, at) AS `num_properties.vals`,
    argMaxState(arr_properties.keys, at) AS `arr_properties.keys`,
    argMaxState(arr_properties.vals, at) AS `arr_properties.vals`,
    max(at) AS at_final
FROM user_profile
GROUP BY
    tenant_id,
    anonymous_id
;


CREATE VIEW IF NOT EXISTS user_profile_final_v AS
SELECT
     anonymous_id,
     tenant_id ,
    argMaxMerge(identity.keys) AS identity_keys,
    argMaxMerge(identity.vals) AS identity_vals,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM user_profile_final
GROUP BY tenant_id, anonymous_id;

select * from user_profile_final_v;
select * from user_profile_final;
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


insert into segments values (1,'s1', ['a0','a8','a12','a36','a42','a105','a208','a402','a508','a612','886','a924'],now());
insert into segments values (1,'s2', ['a2','a6','a18','a38','a40','a106','a205','a401','a503','a619','882','a929'],now());
insert into segments values (1,'s3', ['a3','a8','a17','a35','a49','a105','a207','a401','a507','a614','885','a926'],now());
insert into segments values (1,'s4', ['a4','a9','a17','a39','a48','a100','a207','a409','a506','a610','886','a925'],now());
insert into segments values (1,'s5', ['a5','a8','a12','a36','a42','a103','a212','a416','a582','a674','845','a936'],now());
insert into segments values (1,'s6', ['a6','a8','a12','a74','a42','a172','a206','a473','a508','a612','886','a924'],now());

--test
insert into segments values (1, 's4', ['a6', 'a7'], now());
insert into segments values (1, 's5', ['a8', 'a9'], now());

-- if 1.000.000 insert into segments get 5s ??
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

--------------------------------------------LOCATION
drop table segment_agg_gender_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_locatoin_city_mv TO segment_agg AS
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'location_city' AS metric_name,
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
            str_vals[indexOf(str_keys, 'location_city')] AS cate
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


--------------------------------------------SOURCE

CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_source_mv TO segment_agg AS
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'source' AS metric_name,
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
            str_vals[indexOf(str_keys, 'context_campaign_source')] AS cate
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

INSERT INTO segment_agg values
(1,'s1','2020-08-11 04:14:14','days_since_last_order',['max','avg','med','min'],[1684,1684,1684,1684]),
(1,'s1','2020-08-11 04:14:14','gender',['female','male'],[7,4]),
(1,'s1','2020-08-11 04:14:14','location_city',['VungTau','DaNang','HoChiMinh','CanTho','HaNoi'],[4,3,2,1,1]),
(1,'s1','2020-08-11 04:14:14','revenue',['max','avg','med','min'],[278,178.18182,205,58]),
(1,'s1','2020-08-11 04:14:14','source',['google','facebook','youtube'],[5,4,2]),
(1,'s2','2020-08-11 04:14:17','days_since_last_order',['max','avg','med','min'],[1684,1684,1684,1684]),
(1,'s2','2020-08-11 04:14:17','gender',['female','male'],[6,5]),
(1,'s2','2020-08-11 04:14:17','location_city',['VungTau','CanTho','DaNang','HaNoi','HoChiMinh'],[4,2,2,2,1]),
(1,'s2','2020-08-11 04:14:17','revenue',['max','avg','med','min'],[282,153,143,6]),
(1,'s2','2020-08-11 04:14:17','source',['facebook','google','youtube'],[7,2,2]),
(1,'s3','2020-08-11 04:14:20','days_since_last_order',['max','avg','med','min'],[1684,1684,1684,1684]),
(1,'s3','2020-08-11 04:14:20','gender',['male','female'],[6,5]),
(1,'s3','2020-08-11 04:14:20','location_city',['HoChiMinh','VungTau','HaNoi','DaNang'],[4,4,2,1]),
(1,'s3','2020-08-11 04:14:20','revenue',['max','avg','med','min'],[283,184.27272,205,59]),
(1,'s3','2020-08-11 04:14:20','source',['facebook','youtube','google'],[6,3,2]);


insert into segment_agg values
1,s1,2020-08-11 04:14:14,days_since_last_order,"['max','avg','med','min']","[1684,1684,1684,1684]"
1,s1,2020-08-11 04:14:14,gender,"['female','male']","[7,4]"
1,s1,2020-08-11 04:14:14,location_city,"['VungTau','DaNang','HoChiMinh','CanTho','HaNoi']","[4,3,2,1,1]"
1,s1,2020-08-11 04:14:14,revenue,"['max','avg','med','min']","[278,178.18182,205,58]"




truncate table segment_agg;
select * from ;

jdbc:clickhouse://20.10.23.165:9000/primedata
























--- select
select * from segments;
select * from segment_agg;
select * from segments_final;

select * from user_profile



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
drop table segment_agg_days_since_last_order_mv;

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
truncate table segment_agg_days_since_last_order_mv;


-------------------------------------------------EVENTS----------------------------------------------
show tables;
select * from eventss;
CREATE TABLE IF NOT EXISTS eventss
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

insert into eventss values
(1, 'u', 'a0', 'Payment Offer Applied', [],[],[],[],[],[], '2015-01-01 00:00:01'),
(1, 'u1', 'a0', 'Order Completed', [],[],[],[],[],[], now());

insert into eventss
    with
        ['Added to Card','App Launched','App Uninstalled','Category Viewed','Order Completed','Payment Offer Applied', 'Product Viewed', 'Searched'] as event_names,
        ['fb','gg'] as sources
    select
        rand(1)%1+1 as tenant_id,
        'u' as user_id,
        concat('a', toString(number)) as anonymous_id,
        event_names[rand(2)%length(event_names)+1] as event_name,
        ['source'] as `str_properties.keys`,
        [sources[rand(3)%length(sources)+1]] as `str_properties.vals`,
        [] as `num_properties.keys`,
        [] as `num_properties.vals`,
        [''] as `arr_properties.keys`,
        [[]] as `arr_properties.vals`,
        toDateTime('2017-01-01 00:00:10')+number*60 as at
    from system.numbers
    limit 50
;

CREATE TABLE IF NOT EXISTS events_summary
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `event_name` String,
    `count` UInt16,
    `first_time` AggregateFunction(min, DateTime),
    `last_time` AggregateFunction(max, DateTime)
)
ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (tenant_id, anonymous_id, event_name)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS events_summary_mv TO events_summary AS
SELECT
    tenant_id,
    anonymous_id,
    event_name,
    count(*) as count,
    minState(at) as first_time,
    maxState(at) as last_time
FROM eventss
GROUP BY tenant_id, anonymous_id, event_name
;

------------------------------------------ API summary events by anonymous_id and event_name
-- time:
create view if not exists events_summary_v as
select
    tenant_id,
    anonymous_id,
    event_name,
    sum(count) as count,
    minMerge(first_time) as first_time,
    maxMerge(last_time) as last_time
from events_summary
group by tenant_id, anonymous_id, event_name
order by tenant_id, anonymous_id, event_name
;
select * from events_summary_v where tenant_id = 1 and anonymous_id = 'a10'; -- 142ms
select * from (
                  select tenant_id,
                         anonymous_id,
                         event_name,
                         sum(count)           as count,
                         minMerge(first_time) as first_time,
                         maxMerge(last_time)  as last_time
                  from events_summary
                  group by tenant_id, anonymous_id, event_name
                  )
where tenant_id = 1 and anonymous_id = 'a30'
order by tenant_id, anonymous_id, event_name; -- 148

-- Manual - time:
select
    tenant_id,
    anonymous_id,
    event_name,
    count(),
    min(at),
    max(at)
from eventss
group by tenant_id, anonymous_id, event_name
order by tenant_id, anonymous_id, event_name
;
------------------------------------------ API get events by anonymous_id, list of event name

select
    *
from eventss
where tenant_id = 1
    and anonymous_id = 'a20'
    and event_name in ['App Launched', 'App Uninstalled', 'Category Viewed', 'Searched']
order by at desc
limit 100
;

select * from eventss where anonymous_id = 'a0';

truncate table eventss;
truncate table events_summary;

"App Launched", "App Uninstalled", "Category Viewed", "Searched"