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
    `at` DateTime,
    `at_unix` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at)
;

show create user_profile_final;
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
ORDER BY (tenant_id, anonymous_id)
SETTINGS index_granularity = 8192;



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




insert into user_profile values
('a111', 0, [],[], ['gender'],['Male'],[],[],[],[],'2020-01-01 00:00:00', '2020-01-01 00:00:00.001'),
('a111', 0, [],[], ['gender'],['Male'],['revenue'],[12],[],[],'2020-01-01 00:00:00', '2020-01-01 00:00:00.001');

select * from user_profile_final_v where tenant_id = 0 and anonymous_id = 'a111';




select now64();

----------------------UPDATE user profile material view
drop table user_profile_final_mv;
drop table user_profile_final_v;
drop table user_profile_final;
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
    `at_final` AggregateFunction(argMax,DateTime, DateTime64),
    `at_unix_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);


CREATE MATERIALIZED VIEW eventify.user_profile_final_mv TO eventify.user_profile_final AS
SELECT
    anonymous_id,
    tenant_id,
    argMaxState(identity.keys, at_unix) AS `identity.keys`,
    argMaxState(identity.vals, at_unix) AS `identity.vals`,
    argMaxState(str_properties.keys, at_unix) AS `str_properties.keys`,
    argMaxState(str_properties.vals, at_unix) AS `str_properties.vals`,
    argMaxState(num_properties.keys, at_unix) AS `num_properties.keys`,
    argMaxState(num_properties.vals, at_unix) AS `num_properties.vals`,
    argMaxState(arr_properties.keys, at_unix) AS `arr_properties.keys`,
    argMaxState(arr_properties.vals, at_unix) AS `arr_properties.vals`,
    argMaxState(at, at_unix) AS at_final,
    max(at_unix) AS at_unix_final
FROM eventify.user_profile
GROUP BY
    tenant_id,
    anonymous_id;
;

select * from user_profile;
select * from user_profile_final;

show create user_profile_final_v;
CREATE VIEW user_profile_final_v
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
FROM user_profile_final
GROUP BY
    tenant_id,
    anonymous_id;


insert into user_profile values
('a112', 0, [],[], ['gender'],['Male'],[],[],[],[],'2020-01-01 00:00:00', '2020-01-01 00:00:00.001');


alter table user_profile modify at_unix DateTime64 default now64();
------------------------------------BACKED

select * from user_profile;
select * from user_profile_final;
select * from user_profile_final_v;
select count(*) from eventify_demo.user_profile;
select count() from user_profile;
select count() from eventify.user_profile_final;
select * from user_profile_final_v;

insert into eventify_demo.user_profile
select
    anonymous_id,
    tenant_id,
    `identity.keys`,
    `identity.vals`,
    `str_properties.keys`,
    `str_properties.vals`,
    `num_properties.keys`,
    `num_properties.vals`,
    `arr_properties.keys`,
    `arr_properties.vals`,
    `at`
from user_profile;

INSERT INTO eventify.user_profile_final
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




    SELECT
        tenant_id,
        anonymous_id,
        arrayZip(identity_keys, identity_vals) as identities,
        arrayZip(str_pros_keys, str_pros_vals) AS str_pros,
        arrayZip(num_pros_keys, num_pros_vals) AS num_pros,
        arrayZip(arr_pros_keys, arr_pros_vals) AS arr_pros,
        at
    FROM
    (
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
    FROM user_profile_final
    WHERE tenant_id = 1 AND anonymous_id = 'DCg5lymvV1UVegrVGyN1GQ5L9AK'
    GROUP BY
        tenant_id,
        anonymous_id
    )


select * from user_profile where tenant_id = 1 and anonymous_id = 'DCg5lymvV1UVegrVGyN1GQ5L9AK';
alter table user_profile drop column at_unix;


show create table user_profile_final_v;