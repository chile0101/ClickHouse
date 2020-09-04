-- TODO: edit name users-> user_segments
CREATE TABLE user_segments
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `segments` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at)


CREATE TABLE user_segments_final
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `segments` AggregateFunction(argMax, Array(String), DateTime),
    `at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_segments_final_mv TO user_segments_final AS
SELECT
       tenant_id,
       anonymous_id,
       argMaxState(segments, at) AS segments,
       max(at) as at_final
FROM user_segments
GROUP BY tenant_id, anonymous_id
;

CREATE VIEW IF NOT EXISTS user_segments_final_v AS
SELECT tenant_id,
        anonymous_id,
        argMaxMerge(segments) AS segments,
        max(at_final) as at
FROM user_segments_final
GROUP BY tenant_id, anonymous_id
;

