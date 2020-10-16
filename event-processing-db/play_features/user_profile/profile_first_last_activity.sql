
truncate table events;

select *
from events
where tenant_id = 1 and anonymous_id = 'a1'
;


INSERT INTO events WITH
    ['Added to Card', 'App Launched', 'App Uninstalled', 'Category Viewed', 'Order Completed', 'Payment Offer Applied', 'Product Viewed', 'Searched'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['session-123'] as sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda',
    'IOS-1gazdzPC63nBGx0ufPvAnTEYQI6',
    'IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources
SELECT
    rand(6)%10000 as id,
    (rand(1) % 1) + 1 AS tenant_id,
    concat('a', toString(1)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['product','sessionId', 'device','source.scope'] AS `str_properties.keys`,
    [products[(rand(3) % length(products)) + 1], sessions[rand(4)%length(sessions) + 1], devices[rand(5) % length(devices) + 1], sources[rand(6)%length(sources) + 1]] AS `str_properties.vals`,
    [] AS `num_properties.keys`,
    [] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-09-15 00:00:00') + number*60 AS at
FROM system.numbers
LIMIT 10
;


select groupArray(toUnixTimestamp(at))
from  (
select arraySort(groupArray(toUnixTimestamp(at)))
from events
where tenant_id = 1 and anonymous_id = 'a1'
order by at
);

--- SOLUTION FINAL
SELECT (time_stamps[1],time_stamps[-1])
FROM
(
    SELECT arraySort(groupArray(at)) AS time_stamps
    FROM events
    WHERE tenant_id = 1 AND anonymous_id = 'a113'
)


    SELECT arraySort(groupArray(at)) AS time_stamps
    FROM events
    WHERE tenant_id = 1 AND anonymous_id = 'a1'
;



SELECT  time_stamps[1] AS first_activity,
        time_stamps[-1] AS last_activity
FROM
(
    SELECT arraySort(groupArray(toUnixTimestamp(at))) AS time_stamps
    FROM events
    WHERE tenant_id = 1 AND anonymous_id = 'a2'
)
;

select *
from user_profile_final_v where anonymous_id = 'a2';
