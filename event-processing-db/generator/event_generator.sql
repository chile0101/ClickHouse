INSERT INTO events
WITH
    ['Added to Card', 'App Launched', 'App Uninstalled', 'Category Viewed', 'Order Completed', 'Payment Offer Applied', 'Product Viewed', 'Searched'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['ss1','ss2','ss3'] AS sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda', 'ADR-1hARTg5ZfLb9sauXFer0WR3V8W5','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,
    ['c1','c2','c3','c4','c5'] AS utm_campaigns
SELECT
    rand(6)%10000 AS id,
    1 AS tenant_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    sessions[rand(4)%length(sessions) + 1] AS session_id,
    sources[rand(6)%length(sources) + 1] AS scope,
    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['product', 'device', 'utm_campaign','utm_source','utm_medium','utm_content'] AS `str_properties.keys`,
    [products[(rand(3) % length(products)) + 1], devices[rand(5) % length(devices) + 1], utm_campaigns[rand(11)%length(utm_campaigns) + 1], 'Newsletter','email','image link'] AS `str_properties.vals`,
    ['total_value'] AS `num_properties.keys`,
    [number] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-10-01 00:00:00') + number * rand(12)%60 AS at
FROM numbers(10)
;

truncate table events;
select * from events;
--------------------------------------------------------
truncate table events_campaign_engagement;


INSERT INTO events_campaign_engagement
WITH
    ['click'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['ss1','ss2','ss3'] AS sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda', 'ADR-1hARTg5ZfLb9sauXFer0WR3V8W5','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,
    ['c1','c2','c3','c4','c5'] AS utm_campaigns
SELECT
    rand(6)%10000 AS id,
    1 AS tenant_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    sessions[rand(4)%length(sessions) + 1] AS session_id,
    sources[rand(6)%length(sources) + 1] AS scope,
    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['product', 'device', 'utm_campaign','utm_source','utm_medium','utm_content'] AS `str_properties.keys`,
    [products[(rand(3) % length(products)) + 1], devices[rand(5) % length(devices) + 1], utm_campaigns[rand(11)%length(utm_campaigns) + 1], 'Newsletter','email','image link'] AS `str_properties.vals`,
    ['total_value'] AS `num_properties.keys`,
    [number] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-10-08 00:00:00') + number * rand(12)%60 AS at
FROM numbers(3)
;


with ['1','2','3'] as arr
select arr[5]