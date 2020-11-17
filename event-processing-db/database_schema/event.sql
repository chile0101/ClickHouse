drop table events;
CREATE TABLE events
(   `id` String,
    `tenant_id` UInt16,
    `anonymous_id` String,
    `event_name` String,
    `session_id` String,
    `scope` String,
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
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, id, anonymous_id, event_name, at)


CREATE TABLE events_total_distinct_users_by_source_scope_ts
(
    `tenant_id` UInt16,
    `source_scope` String,
    `time_stamp` DateTime,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_scope, time_stamp)
;

CREATE MATERIALIZED VIEW events_total_distinct_users_by_source_scope_ts_mv TO events_total_distinct_users_by_source_scope_ts
AS
SELECT
    tenant_id,
    scope as source_scope,
    toStartOfFiveMinute(at) AS time_stamp,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    tenant_id,
    source_scope,
    time_stamp
;