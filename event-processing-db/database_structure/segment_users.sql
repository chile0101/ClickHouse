CREATE TABLE segment_users
(
    `tenant_id` UInt16,
    `segment_id` String,
    `users` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, at)
;


CREATE TABLE segment_users_final
(
    `tenant_id` UInt16,
    `segment_id` String,
    `users` AggregateFunction(argMax, Array(String), DateTime),
    `at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, segment_id)
;


CREATE MATERIALIZED VIEW segment_users_final_mv TO segment_users_final AS
SELECT
    tenant_id,
    segment_id,
    argMaxState(users, at) AS users,
    max(at) AS at_final
FROM segment_users
GROUP BY
    tenant_id,
    segment_id
;


CREATE VIEW segment_users_final_v AS
SELECT
     tenant_id ,
     segment_id,
     argMaxMerge(users) as users,
     max(at_final) as at_final
FROM segment_users_final
GROUP BY tenant_id, segment_id;


-- Can custom.
insert into segment_users values
(1, 's1', ['a1','a2'], '2020-10-01 00:05:00'),
(1, 's2', ['a1','a2','a3'], '2020-10-02 12:00:01')
;


insert into segment_users values
(1, 's1', ['a1','a2'], '2020-10-02 00:05:00'),
(1, 's1', ['a1','a2','a3'], '2020-10-04 12:00:01')
;

