# utm_medum : cach no trả tiền cho delivery



show tables;
show create table events_campaign_engagement;

drop table events_campaign;
CREATE TABLE events_campaign
(
    `id`                  String,
    `tenant_id`           UInt16,
    `anonymous_id`        String,
    `event_name`          String,
    `session_id`          String,
    `scope`               String,
    `identity.keys`       Array(String),
    `identity.vals`       Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at`                  DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, id, anonymous_id, event_name,at)
;

INSERT INTO events_campaign
WITH
    ['click'] AS event_names,
    ['Iphone X', 'Xiaomi'] AS products,
    ['Laptop','Mobile'] AS devices,
    ['ss1','ss2','ss3'] AS sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda', 'ADR-1hARTg5ZfLb9sauXFer0WR3V8W5','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,
    ['campaign1','campaign2','campaign3','campaign4','campaign5'] AS utm_campaigns,
    ['facebook', 'google', 'mailchimp'] as utm_sources,
    ['cpc', 'cpm'] as utm_mediums,
    ['content1','content2','content3','content4','content5'] as utm_contents,
    ['Male', 'Female'] as genders,
    ['HaNoi', 'HoChiMinh','DaNang'] as locations
SELECT
    rand(1)%10000 AS id,
    1 AS tenant_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    sessions[rand(3)%length(sessions) + 1] AS session_id,
    sources[rand(4)%length(sources) + 1] AS scope,

    ['user_id'] as `identity.keys`,
    [rand(5)%10+1] as `identity.vals`,
    ['currency','product', 'device','gender', 'location','utm_campaign','utm_content','utm_source'] AS `str_properties.keys`,
    [   'VND',
        products[(rand(6) % length(products)) + 1],
        devices[rand(7) % length(devices) + 1],
        genders[rand(8) % length(genders) + 1],
        locations[rand(9) % length(locations) + 1],
        utm_campaigns[rand(10) % length(utm_campaigns) + 1],
        utm_sources[rand(11) % length(utm_sources) + 1],
        utm_contents[rand(12) % length(utm_contents) + 1]] AS `str_properties.vals`,
    ['total_value'] AS `num_properties.keys`,
    [number] AS `num_properties.vals`,
    [''] AS `arr_properties.keys`,
    [[]] AS `arr_properties.vals`,
    toDateTime('2020-01-01 00:00:38') + number * rand(13)%60 AS at
FROM numbers(10)
;

select * from events_campaign;
truncate table events_campaign;

