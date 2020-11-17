CREATE TABLE user_profile
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
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

CREATE TABLE user_profile_final
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
    `at_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);

CREATE MATERIALIZED VIEW user_profile_final_mv TO user_profile_final
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
FROM user_profile
GROUP BY
    tenant_id,
    anonymous_id;


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