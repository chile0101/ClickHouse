select * from segment_users;

show create table segment_users;

drop table segment_users;
CREATE TABLE eventify.segment_users
(
    `tenant_id` UInt16,
    `segment_id` String,
    `users` Array(String),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, at);

insert into segment_users
select tenant_id,
       segment_id,
       users,
       at
from segment_users_c;

select count() from segment_users;
select count() from segment_users_c;

select * from segment_users_c;

show create segment_users_final;

drop table segment_users_final;
CREATE TABLE eventify.segment_users_final
(
    `tenant_id` UInt16,
    `segment_id` String,
    `users` AggregateFunction(argMax, Array(String), DateTime64),
    `at_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, segment_id)
;


show create table segment_users_final_mv;
drop table segment_users_final_mv;
CREATE MATERIALIZED VIEW eventify.segment_users_final_mv TO eventify.segment_users_final
AS
SELECT
    tenant_id,
    segment_id,
    argMaxState(users, at) AS users,
    max(at) AS at_final
FROM eventify.segment_users
GROUP BY
    tenant_id,
    segment_id;

show create table segment_users_final_v;
drop table segment_users_final_v;
CREATE VIEW eventify.segment_users_final_v
AS
SELECT
    tenant_id,
    segment_id,
    argMaxMerge(users) AS users,
    max(at_final) AS at_final
FROM eventify.segment_users_final
GROUP BY
    tenant_id,
    segment_id;


show tables;

select count(*) from segment_users;
select count() from segment_users_c;
select * from segment_users;
drop table segment_users_c;

----------------------------------------- change user_segments to DateTime64

show create table user_segments;

drop table user_segments;
CREATE TABLE eventify.user_segments
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `segments` Array(String),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

insert into user_segments
select tenant_id,
       anonymous_id,
       segments,
       at
from user_segments_c;


show create table user_segments_final;
drop table user_segments_final;
CREATE TABLE eventify.user_segments_final
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `segments` AggregateFunction(argMax, Array(String), DateTime64),
    `at_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);

show create table user_segments_final_mv;
drop table user_segments_final_mv;
CREATE MATERIALIZED VIEW eventify.user_segments_final_mv TO eventify.user_segments_final
AS
SELECT
    tenant_id,
    anonymous_id,
    argMaxState(segments, at) AS segments,
    max(at) AS at_final
FROM eventify.user_segments
GROUP BY
    tenant_id,
    anonymous_id;


select count() from user_segments;
drop table user_segments_c;
select count() from user_segments_c;

select * from segment_users_final_v;



