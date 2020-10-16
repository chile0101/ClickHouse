show tables;

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

SELECT * FROM segment_agg_final_v where tenant_id = 1 and segment_id = 's1' and metric_name = 'gender';

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

show create table segment_agg_final_v;


show tables;
------------------------------------------------------PROFILE
drop table user_profile;
drop table user_profile_final;
drop table user_profile_final_mv;
drop table user_profile_final_v;



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

select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';
select * from user_profile_final;


show tables from primedata;
CREATE TABLE IF NOT EXISTS users(
    tenant_id UInt16,
    anonymous_id String,
    segments Array(String),
    at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at)
;

CREATE TABLE IF NOT EXISTS users_final(
    tenant_id UInt16,
    anonymous_id String,
    segments AggregateFunction(argMax, Array(String), DateTime),
    at_final SimpleAggregateFunction(max, DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id)
;

CREATE TABLE IF NOT EXISTS segments(
    tenant_id UInt16,
    segment_id String,
    users Array(String),
    at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, at)
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