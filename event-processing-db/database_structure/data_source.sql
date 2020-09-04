CREATE TABLE IF NOT EXISTS event_total_user_by_source_daily
(
    `day` Date,
    `tenant_id` UInt16,
    `source_id` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, day)

;

CREATE MATERIALIZED VIEW IF NOT EXISTS event_total_user_by_source_daily_mv TO event_total_user_by_source_daily AS
SELECT
    toDate(at) AS day,
    tenant_id,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    day,
    tenant_id,
    source_id
;

SELECT
    day,
    tenant_id,
    source_id,
    event_name,
    sum(total)
FROM event_total_event_name_by_source_daily
WHERE (tenant_id = 1) AND (source_id = 'fb') AND ((day >= '2020-08-24') AND (day <= '2020-08-26'))
GROUP BY
    day,
    tenant_id,
    event_name,
    source_id
ORDER BY
    day ASC,
    source_id ASC,
    event_name ASC
;

---------------------

CREATE TABLE IF NOT EXISTS event_total_event_name_by_source_daily
(
    `day` Date,
    `tenant_id` UInt16,
    `event_name` String,
    `source_id` String,
    `total` UInt16
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, event_name, day)
;


CREATE MATERIALIZED VIEW event_total_event_name_by_source_daily_mv TO event_total_event_name_by_source_daily AS
SELECT
    toDate(at) AS day,
    tenant_id,
    event_name,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    count() AS total
FROM events
GROUP BY (day, tenant_id, event_name, source_id)
;

------------------

