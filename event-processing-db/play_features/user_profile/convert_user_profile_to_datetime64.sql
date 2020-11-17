show create table user_profile;

CREATE TABLE eventify.user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

-- copy



show create table user_profile_final;
CREATE TABLE eventify.user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` AggregateFunction(argMax, Array(String), DateTime),
    `identity.vals` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.vals` AggregateFunction(argMax, Array(Float64), DateTime),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime),
    `at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);


show create table user_profile_final_mv;
CREATE MATERIALIZED VIEW eventify.user_profile_final_mv TO eventify.user_profile_final
AS
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
FROM eventify.user_profile
GROUP BY
    tenant_id,
    anonymous_id;


show create table user_profile_final_v;
CREATE VIEW eventify.user_profile_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    argMaxMerge(identity.keys) AS identity_keys,
    argMaxMerge(identity.vals) AS identity_vals,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM eventify.user_profile_final
GROUP BY
    tenant_id,
    anonymous_id;


------------test alter table
show tables;

show create table test;
select * from test;

alter table test modify column date_time DateTime64;


drop table test_copy;
CREATE TABLE test_copy
(
    `anonymous_id` String,
    `value` Int32,
    `date` Date,
    `date_time` DateTime64 default now64()
)
ENGINE = MergeTree()
ORDER BY (anonymous_id, date, date_time);

insert into test_copy
select anonymous_id,
       value,
       date,
       date_time
from test;

select * from test_copy;

drop table test;

rename table test_copy to test;

select * from test;

------------------------ Choi luon tren server

CREATE TABLE eventify.user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

drop table eventify.user_profile_final;

CREATE TABLE eventify.user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `identity.vals` AggregateFunction(argMax, Array(String), DateTime64),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime64),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `num_properties.vals` AggregateFunction(argMax, Array(Float64), DateTime64),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime64),
    `at_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);

CREATE MATERIALIZED VIEW eventify.user_profile_final_mv TO eventify.user_profile_final
AS
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
FROM eventify.user_profile
GROUP BY
    tenant_id,
    anonymous_id;

drop table user_profile_final_mv;


CREATE TABLE eventify.user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

CREATE TABLE eventify.user_profile_c
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);


insert into user_profile
select anonymous_id,
       tenant_id,
       identity.keys,
       identity.vals,
       str_properties.keys,
       str_properties.vals,
       num_properties.keys,
       num_properties.vals,
       arr_properties.keys,
       arr_properties.vals,
       at
from user_profile_c;

select * from user_profile_c;

drop table eventify.user_profile_final_v;

select count() from eventify.user_profile;
select count() from eventify.user_profile_final;
select * from eventify.user_profile;
select * from eventify.user_profile_final;

drop table eventify.user_profile_c;

show tables ;

CREATE VIEW eventify.user_profile_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    argMaxMerge(identity.keys) AS identity_keys,
    argMaxMerge(identity.vals) AS identity_vals,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM eventify.user_profile_final
GROUP BY
    tenant_id,
    anonymous_id;


select * from eventify.user_profile_final_v order by at desc ;




