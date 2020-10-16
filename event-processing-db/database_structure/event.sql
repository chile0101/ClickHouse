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
ORDER BY (tenant_id, id, anonymous_id, event_name, at);


CREATE TABLE event_total_user_by_source_scope_ts
(
    `time_stamp` DateTime,
    `tenant_id` UInt16,
    `source_scope` String,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_scope, time_stamp)
;

CREATE MATERIALIZED VIEW event_total_user_by_source_scope_ts_mv TO event_total_user_by_source_scope_ts
AS
SELECT
    toStartOfFifteenMinutes(at) AS time_stamp,
    tenant_id,
    scope as source_scope,
    uniqState(anonymous_id) AS value
FROM events
GROUP BY
    time_stamp,
    tenant_id,
    source_scope;
;



CREATE TABLE events_campaign_engagement (
    `id` String,
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
)ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at)
;

select * from events_campaign_engagement;