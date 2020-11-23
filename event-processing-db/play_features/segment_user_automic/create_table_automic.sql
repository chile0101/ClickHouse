show tables;

show create segment_users;
-- CREATE TABLE eventify_stag.segment_users
-- (
--     `tenant_id` UInt16,
--     `segment_id` String,
--     `users` Array(String),
--     `at` DateTime64(3)
-- )
-- ENGINE = MergeTree()
-- PARTITION BY toYYYYMM(at)
-- ORDER BY (tenant_id, segment_id, at)

show create table segment_users_final;
-- CREATE TABLE eventify_stag.segment_users_final
-- (
--     `tenant_id` UInt16,
--     `segment_id` String,
--     `users` AggregateFunction(argMax, Array(String), DateTime64(3)),
--     `at_final` SimpleAggregateFunction(max, DateTime64(3))
-- )
-- ENGINE = AggregatingMergeTree()
-- ORDER BY (tenant_id, segment_id)

show create table segment_users_final_mv;
-- CREATE MATERIALIZED VIEW eventify_stag.segment_users_final_mv TO eventify_stag.segment_users_final
-- AS
-- SELECT
--     tenant_id,
--     segment_id,
--     argMaxState(users, at) AS users,
--     max(at) AS at_final
-- FROM eventify_stag.segment_users
-- GROUP BY
--     tenant_id,
--     segment_id

show create table segment_users_final_v;
-- CREATE VIEW eventify_stag.segment_users_final_v
-- (
--     `tenant_id` UInt16,
--     `segment_id` String,
--     `users` Array(String),
--     `at_final` SimpleAggregateFunction(max, DateTime64(3))
-- ) AS
-- SELECT
--     tenant_id,
--     segment_id,
--     argMaxMerge(users) AS users,
--     max(at_final) AS at_final
-- FROM eventify_stag.segment_users_final
-- GROUP BY
--     tenant_id,
--     segment_id


CREATE TABLE segment_user
(
    `segment_id` String,
    `tenant_id` UInt16,
    `user` String,
    `status` UInt8, -- 1 user still inside segment
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id,user, at)
;

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
ORDER BY (tenant_id, segment_id,user, at)
;
select * from segment_user;



drop table segment_user_final_mv;
show create table segment_user_final_mv;
show create profile_str_final_mv;
CREATE MATERIALIZED VIEW segment_user_final_mv TO segment_user_final
AS
SELECT
    segment_id,
    tenant_id,
    user,
    argMaxState(status, segment_user.at) as status,
    max(segment_user.at) AS at
FROM segment_user
GROUP BY
    tenant_id,
    segment_id,
    user
ORDER BY tenant_id, segment_id, user
;
drop table segment_user_final_v;


CREATE VIEW eventify_stag.segment_user_final_v
(
    `segment_id` String,
    `tenant_id` UInt16,
    `user` String,
    `status` UInt8,
    `at` SimpleAggregateFunction(max, DateTime64(3))
) AS
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
    user;

show create segment_user_final_v;
select * from segment_user_final;
select * from segment_user_final_v;


-----------------------------INSERT DATA
insert into segment_user
select segment_id,
       tenant_id,
        user,
       1 as status,
       at_final
from segment_users_final_v array join users as user;

select * from segment_users_final_v array join users;

select count() from segment_users; --15681
select count() from segment_users_final_v; -- 16
select count() from segment_user ; --26652
select count() from segment_user_final_v; --26652


select tenant_id, segment_id, count(user)
from segment_user
group by tenant_id, segment_id;