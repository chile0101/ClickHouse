drop table user_profile;
CREATE TABLE IF NOT EXISTS user_profile
(
    tenant_id    UInt16,
    user_id      String,
    anonymous_id String,
    identifies   Array(String),

    str_properties Nested(
        keys String,
        vals String
        ),
    num_properties Nested(
        keys String,
        vals Float32
        ),
    arr_properties Nested(
        keys String,
        vals Array(String)
        ),
    created_at   DateTime

) Engine MergeTree()
Order By (tenant_id, anonymous_id, created_at);

drop table user_profile_final;
CREATE TABLE IF NOT EXISTS user_profile_final
(
    tenant_id             UInt16,
    user_id               AggregateFunction(argMax, String, DateTime),
    anonymous_id          String,
    identifies            AggregateFunction(argMax, Array(String), DateTime),
    "str_properties.keys" AggregateFunction(argMax, Array(String), DateTime),
    "str_properties.vals" AggregateFunction(argMax, Array(String), DateTime),
    "num_properties.keys" AggregateFunction(argMax, Array(String), DateTime),
    "num_properties.vals" AggregateFunction(argMax, Array(Float32), DateTime),
    "arr_properties.keys" AggregateFunction(argMax, Array(String), DateTime),
    "arr_properties.vals" AggregateFunction(argMax, Array(Array(String)), DateTime),
    created_at_final            SimpleAggregateFunction(max, DateTime)
)
Engine = AggregatingMergeTree()
Order by (tenant_id, anonymous_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS user_profile_final_mv
TO user_profile_final
AS
SELECT tenant_id,
       argMaxState(user_id, created_at)             AS user_id,
       anonymous_id,
       argMaxState(identifies, created_at)          AS identifies,
       argMaxState(str_properties.keys, created_at) AS "str_properties.keys",
       argMaxState(str_properties.vals, created_at) AS "str_properties.vals",
       argMaxState(num_properties.keys, created_at) AS "num_properties.keys",
       argMaxState(num_properties.vals, created_at) AS "num_properties.vals",
       argMaxState(arr_properties.keys, created_at) AS "arr_properties.keys",
       argMaxState(arr_properties.vals, created_at) AS "arr_properties.vals",
       max(created_at)                              AS created_at_final
FROM user_profile
GROUP BY tenant_id, anonymous_id;


CREATE TABLE segments
(
    segment_id String,
    users      Array(String)
) ENGINE = MergeTree()
ORDER BY (segment_id);

CREATE TABLE segment_agg
(
    segment_id  String,
    time_stamp  DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (segment_id, metric_name);


--- gender_mv
CREATE MATERIALIZED VIEW segment_agg_gender_mv
TO segment_agg
AS
SELECT segment_id,
       now() AS time_stamp,
       'gender'                   AS metric_name,
       groupArray(gender)         AS "metrics_agg.keys",
       groupArray(count)          AS "metrics_agg.vals"
FROM (
      SELECT segment_id,
             p.gender,
             count() AS count
      FROM segments ARRAY JOIN users
      JOIN (
          SELECT anonymous_id,
                 pf.str_vals[indexOf(pf.str_keys, 'gender')] AS gender
          FROM (SELECT anonymous_id,
                       argMaxMerge(str_properties.keys) AS str_keys,
                       argMaxMerge(str_properties.vals) AS str_vals
                FROM user_profile_final
                GROUP BY tenant_id, anonymous_id) AS pf ) AS p
      ON (p.anonymous_id = segments.users)
      WHERE gender != '' AND gender IS NOT NULL
      GROUP BY segment_id, gender
      ORDER BY count DESC , gender
)
GROUP BY segment_id;



insert into segments values ('s1', ['a1','a2']);
insert into segments values ('s2', ['a2', 'a3']);
insert into segments values ('s3', ['a1', 'a3']);
insert into segments values ('s1', ['a2','a4']);

select * from segments;
select * from segment_agg order by  segment_id, time_stamp, metric_name;




----------- location_mv
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_location_mv
TO segment_agg
AS
SELECT
    segment_id,
     now() AS time_stamp,
    'location_city' AS metric_name,
    groupArray(location_city) AS "metrics_agg.keys",
    groupArray(count) AS "metrics_agg.vals"
FROM (
    SELECT
        segment_id,
        location_city,
        count() AS count
    FROM segments ARRAY JOIN users
    JOIN (
        SELECT anonymous_id,
                 pf.str_vals[indexOf(pf.str_keys, 'location_city')] AS location_city
          FROM (SELECT anonymous_id,
                       argMaxMerge(str_properties.keys) AS str_keys,
                       argMaxMerge(str_properties.vals) AS str_vals
                FROM user_profile_final
                GROUP BY tenant_id, anonymous_id) AS pf ) AS p
    ON segments.users = p.anonymous_id
    WHERE location_city != '' AND location_city IS NOT NULL
    GROUP BY segment_id, location_city
    ORDER BY count DESC, location_city
)
GROUP BY segment_id;


--- source_mv
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_source_mv
TO segment_agg
AS
SELECT
    segment_id,
     now() AS time_stamp,
    'context_campaign_source' AS metric_name,
    groupArray(source) AS "metrics_agg.keys",
    groupArray(count) AS "metrics_agg.vals"
FROM (
    SELECT
        segment_id,
        source,
        count() AS count
    FROM segments ARRAY JOIN users
    JOIN (
       SELECT anonymous_id,
                  pf.str_vals[indexOf(pf.str_keys, 'context_campaign_source')] AS source
          FROM (SELECT anonymous_id,
                       argMaxMerge(str_properties.keys) AS str_keys,
                       argMaxMerge(str_properties.vals) AS str_vals
                FROM user_profile_final
                GROUP BY tenant_id, anonymous_id) AS pf ) AS p
    ON segments.users = p.anonymous_id
    WHERE source != '' AND source IS NOT NULL
    GROUP BY segment_id, source
    ORDER BY count DESC, source
)
GROUP BY segment_id;




--------------- segment last point convert replacing -> aggregate merge tree.
CREATE TABLE IF NOT EXISTS segment_agg_final(
    segment_id String,
    time_stamp_final SimpleAggregateFunction(max, DateTime),
    metric_name String,
    "metrics_agg.keys" AggregateFunction(argMax, Array(String), DateTime),
    "metrics_agg.vals" AggregateFunction(argMax, Array(Float32), DateTime)
) ENGINE = AggregatingMergeTree()
ORDER BY(segment_id, metric_name);


CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_final_mv
TO segment_agg_final
AS
SELECT segment_id,
       max(time_stamp)             AS time_stamp_final,
       metric_name,
       argMaxState(metrics_agg.keys, time_stamp) AS "metrics_agg.keys",
       argMaxState(metrics_agg.vals, time_stamp) AS "metrics_agg.vals"
FROM segment_agg
GROUP BY segment_id, metric_name;


insert into segments values ('s1', ['a2','a3']);
insert into segments values ('s2', ['a2', 'a3']);
insert into segments values ('s3', ['a1', 'a3']);
insert into segments values ('s1', ['a2','a4']);

select * from segments;
select * from segment_agg;
truncate table segments; truncate table segment_agg;

--- select
select
       segment_id,
       metric_name,
       max(time_stamp_final),
       argMaxMerge(metrics_agg.keys) as metrics_keys,
       argMaxMerge(metrics_agg.vals) as metrics_vals
from segment_agg_final
group by segment_id, metric_name
order by segment_id, metric_name;



