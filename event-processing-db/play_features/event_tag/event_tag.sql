show tables;
show create table events;


insert into events


with
    `str_properties.vals`[indexOf(`str_properties.keys` ,'campaign_name')] as campaign1,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'properties.campaign_detail.utm_campaign')] as campaign2,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'source.properties.campaign_detail.utm_campaign')] as campaign3,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'source.properties.utm_campaign')] as campaign4,

    `str_properties.vals`[indexOf(`str_properties.keys` ,'campaign_source')] as source1,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'properties.campaign_detail.utm_source')] as source2,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'source.properties.campaign_detail.utm_source')] as source3,

    `str_properties.vals`[indexOf(`str_properties.keys` ,'properties.campaign_detail.utm_medium')] as medium1,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'source.properties.campaign_detail.utm_medium')] as medium2,

    `str_properties.vals`[indexOf(`str_properties.keys` ,'campaign_content')] as content1,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'properties.campaign_detail.utm_content')] as content2,
    `str_properties.vals`[indexOf(`str_properties.keys` ,'source.properties.campaign_detail.utm_content')] as content3
select

     multiIf(campaign1 != '',campaign1, campaign2 != '',campaign2, campaign3 != '', campaign3, campaign4 != '', campaign4,'') as campaign,
     multiIf(source1 != '', source1, source2 != '', source2, source3 != '', source3, '') as source,
     multiIf(medium1 != '', medium1, medium2 != '', medium2,'') as medium,
     multiIf(content1 != '', content1, content2 != '', content2, content3 != '', content3,'') as content
from events

    ;


select id,
       tenant_id,
       anonymous_id,
       event_name,
       session_id,
       scope,
        `identity.keys`,
        `identity.vals`,
        `str_properties.keys`,
        `str_properties.vals`,
        `num_properties.keys`,
        `num_properties.vals`,
        `arr_properties.keys`,
        `arr_properties.vals`,
        ['touch_type','campaign','event_tag'] as `tags.keys`,
        ['direct','null','view'] as `tags.vals`,
        at,
       campaign, source, tactic, content
from eventify_stag.events
where event_name LIKE 'checkout_completed%'
;

select distinct arrayJoin(`str_properties.keys`) as key from events where key LIKE '%campaign%'



CREATE TABLE eventify_dev.events
(
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
    `tags.keys` Array(String),
    `tags.vals` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, id, anonymous_id, event_name, at);


select * from events order by at desc ;
select distinct event_name from events;
select distinct (`tags.keys`,`tags.vals`) from events;

view
sessionUpdated
sessionCreated
checkout_completed_(r)
identify
searched_(r)
goal
category_viewed_(r)
cart_viewed_(r)
product_viewed_(r)
view_(r)
checkout_completed
mergedProfile
reach_goal
order_checked_(r)
coupon_applied_(r)
searched_test


;


show create table events;

CREATE TABLE eventify_stag.events
(
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
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, id, anonymous_id, event_name, at)
;


show create table eventify_dev.events;
CREATE TABLE eventify_dev.events
(
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
    `tags.keys` Array(String),
    `tags.vals` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, id, anonymous_id, event_name, at);


alter table events add column `tags.keys` Array(String);
alter table events add column `tags.vals` Array(String);



  SELECT
            id,
            tenant_id,
            anonymous_id,
            toUnixTimestamp(toStartOfDay(at)) AS date_unix,
            toUnixTimestamp(at) AS time_unix,
            session_id,
            event_name,
            arrayZip(`identity.keys`, `identity.vals`) AS identities,
            arrayZip(`str_properties.keys`, `str_properties.vals`) AS str_pros,
            arrayZip(`num_properties.keys`, `num_properties.vals`) AS num_pros,
            arrayZip(`arr_properties.keys`, `arr_properties.vals`) AS arr_pros
        FROM events
        WHERE (tenant_id = 1) AND (anonymous_id = 'DF37wf8BWT7auM2Wu6el4070uR8') AND event_name NOT IN ['sessionCreated', 'sessionUpdated', 'profileUpdated', 'segmentOptIn', 'segmentOptOut', 'reach_goal', 'goal']
        ORDER BY at DESC
        LIMIT 0 * 10, 10
;

SELECT countDistinct(event_name) AS total_rows
FROM events
WHERE (tenant_id = 1)
AND (anonymous_id = 'DF4CS5vxccaVvYdd8pFTIP0h0TI');

select * from events;




CREATE TABLE events
(
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
    `tags.keys` Array(String),
    `tags.vals` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id,anonymous_id, event_name, at);


insert into events_c
select id,
       tenant_id,
       anonymous_id,
       event_name,
       session_id,
       scope,
       identity.keys,
       identity.vals,
       str_properties.keys,
       str_properties.vals,
       num_properties.keys,
       num_properties.vals,
       arr_properties.keys,
       arr_properties.vals,
       [] as `tags.keys`,
       [] as `tags.vals`,
       at
from events;

select count() from events_c;
drop table events;

insert into events
select * from events_c;


select * from events;




     SELECT
            tenant_id,
            anonymous_id,
            event_name,
            count(event_name) AS count,
            min(at) AS first_time,
            max(at) AS last_time
        FROM events
        WHERE (tenant_id = 1)
            AND (anonymous_id = 'DF4CS5vxccaVvYdd8pFTIP0h0TI')
            AND (event_name NOT IN  ['sessionCreated', 'sessionUpdated', 'profileUpdated', 'segmentOptIn', 'segmentOptOut', 'reach_goal', 'goal'])
        GROUP BY
            tenant_id,
            anonymous_id,
            event_name
        ORDER BY event_name ASC
        LIMIT 10;

select * from events where anonymous_id = 'DF4CS5vxccaVvYdd8pFTIP0h0TI';

select count() from events;


select max(at), min(at) from events order by at desc ;
select * from events where anonymous_id = 'DF9CBvWUdOmgJb5PjYVFaxQotvR';







select * from events order by at desc ;
select * from events_conversion order by at desc ;
select * from events_campaign order by at desc ;

show tables ;

drop table events_c;
select * from events_c;
select * from event;