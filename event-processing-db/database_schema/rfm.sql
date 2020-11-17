CREATE TABLE IF NOT EXISTS rfm_metrics
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `metrics` Nested(    keys String,     vals Float64),
--     `recency` UInt16 DEFAULT  10000,
--     `frequency1` UInt16 DEFAULT 0,
--     `frequency30` UInt16 DEFAULT 0,
--     `frequency60` UInt16 DEFAULT 0,
--     `frequency90` UInt16 DEFAULT 0,
--     `monetary1` Float64 DEFAULT 0,
--     `monetary30` Float64 DEFAULT 0,
--     `monetary60` Float64 DEFAULT 0,
--     `monetary90` Float64 DEFAULT 0,
--     `quantity1` UInt32 DEFAULT 0,
--     `quantity30` UInt32 DEFAULT 0,
--     `quantity60` UInt32 DEFAULT 0,
--     `quantity90` UInt32 DEFAULT 0,
--     `intensity1` Int DEFAULT 0,
--     `intensity30` Int DEFAULT 0,
--     `intensity60` Int DEFAULT 0,
--     `intensity90` Int DEFAULT 0,
--     `momentum1` Float64 DEFAULT 0,
--     `momentum30` Float64 DEFAULT 0,
--     `momentum60` Float64 DEFAULT 0,
--     `momentum90` Float64 DEFAULT 0,
--     `propensity1` Float64 DEFAULT 0,
--     `propensity30` Float64 DEFAULT 0,
--     `propensity60` Float64 DEFAULT 0,
--     `propensity90` Float64 DEFAULT 0,
    `date` Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date)
ORDER BY (date, tenant_id, anonymous_id);

DROP TABLE IF EXISTS rfm_metrics;