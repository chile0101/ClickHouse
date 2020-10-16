CREATE TABLE segment_agg
(
    `tenant_id` UInt16,
    `segment_id` String,
    `time_stamp` DateTime,
    `metric_name` String,
    `metrics_agg.keys` Array(String),
    `metrics_agg.vals` Array(Float32)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, segment_id, metric_name)
;

CREATE TABLE segment_agg_final
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