CREATE TABLE events
(
    `tenant_id` UInt16,
    `user_id` String,
    `anonymous_id` String,
    `event_name` String,
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float32),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at)
;

INSERT INTO events WITH
    ['Added to Card', 'App Launched', 'App Uninstalled', 'Category Viewed', 'Order Completed', 'Payment Offer Applied', 'Product Viewed', 'Searched'] AS event_names,
    ['fb', 'gg'] AS sources
SELECT
    (rand(1) % 1) + 1 AS tenant_id,
    'u' AS user_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    ['source'] AS `str_properties.keys`,
    [sources[(rand(3) % length(sources)) + 1]] AS `str_properties.vals`,
    [] AS `num_properties.keys`,
    [] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2017-02-28 00:00:10') + (number * 60) AS at
FROM system.numbers
LIMIT 100




