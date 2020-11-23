show tables;
drop table profile_str;
drop table profile_num;


--show create profile_str;
CREATE TABLE profile_str
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_key` LowCardinality(String),
    `str_val` String,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, str_key, at)
;

-- show create profile_str_final;
CREATE TABLE profile_str_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_key` LowCardinality(String),
    `str_val` AggregateFunction(argMax, String, DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, str_key)
;

show create table eventify_stag.profile_str_final_mv;
CREATE MATERIALIZED VIEW profile_str_final_mv TO profile_str_final
AS
SELECT
    anonymous_id,
    tenant_id,
    str_key AS str_key,
    argMaxState(str_val, profile_str.at) AS str_val,
    max(profile_str.at) AS at
FROM profile_str
GROUP BY
    tenant_id,
    anonymous_id,
    str_key
ORDER BY
    tenant_id ASC,
    anonymous_id ASC,
    str_key ASC
;


-- show create table profile_str_final_v;
CREATE VIEW profile_str_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    str_key,
    argMaxMerge(str_val) AS str_val,
    max(at) AS at
FROM profile_str_final
GROUP BY
    tenant_id,
    anonymous_id,
    str_key
ORDER BY
    tenant_id ASC,
    anonymous_id ASC,
    str_key ASC
;


-- show create table profile_num;
CREATE TABLE profile_num
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `num_key` LowCardinality(String),
    `num_val` Float64,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, num_key, at);


-- show create table profile_num_final;
CREATE TABLE profile_num_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `num_key` LowCardinality(String),
    `num_val` AggregateFunction(argMax, Float64, DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, num_key)
;

-- show create table profile_num_final_mv;
CREATE MATERIALIZED VIEW profile_num_final_mv TO profile_num_final
AS
SELECT
    anonymous_id,
    tenant_id,
    num_key AS num_key,
    argMaxState(num_val, profile_num.at) AS num_val,
    max(profile_num.at) AS at
FROM profile_num
GROUP BY
    tenant_id,
    anonymous_id,
    num_key
ORDER BY
    tenant_id ASC,
    anonymous_id ASC,
    num_key ASC
;


-- show create table profile_num_final_v;
CREATE VIEW profile_num_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    num_key,
    argMaxMerge(num_val) AS num_val,
    max(at) AS at
FROM profile_num_final
GROUP BY
    tenant_id,
    anonymous_id,
    num_key
ORDER BY
    tenant_id ASC,
    anonymous_id ASC,
    num_key ASC
;

-- show create table profile_arr;
CREATE TABLE profile_arr
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `arr_key` LowCardinality(String),
    `arr_val` Array(String),
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, arr_key, at)
;

-- show create table profile_arr_final;
CREATE TABLE profile_arr_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `arr_key` LowCardinality(String),
    `arr_val` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, arr_key)
;


-- show create table profile_arr_final_mv;
CREATE MATERIALIZED VIEW profile_arr_final_mv TO profile_arr_final
AS
SELECT
    anonymous_id,
    tenant_id,
    arr_key AS arr_key,
    argMaxState(arr_val, profile_arr.at) AS arr_val,
    max(profile_arr.at) AS at
FROM profile_arr
GROUP BY
    tenant_id,
    anonymous_id,
    arr_key
ORDER BY
    tenant_id ASC,
    anonymous_id ASC,
    arr_key ASC;


show tables ;
-- show create segment_size;
CREATE TABLE segment_size
(
    `segment_id` String,
    `tenant_id` UInt16,
    `time_stamp` DateTime,
    `total_user` UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, segment_id, time_stamp)
;
--------------------------------segment user

-- show create segment_user;
CREATE TABLE segment_user
(
    `segment_id` String,
    `tenant_id` UInt16,
    `user` String,
    `status` UInt8,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, user, at);

-- show create table segment_user_final;
CREATE TABLE segment_user_final
(
    `segment_id` String,
    `tenant_id` UInt16,
    `user` String,
    `status` AggregateFunction(argMax, UInt8, DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, user, at);


-- show create table segment_user_final_v;
CREATE VIEW segment_user_final_v
AS
SELECT
    segment_id,
    tenant_id,
    user,
    argMaxMerge(status) AS status,
    max(at) AS at
FROM eventify_stag.segment_user_final
GROUP BY
    tenant_id,
    segment_id,
    user
;