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
     multiIf(campaign1 != '',campaign1, campaign2 != '',campaign2, campaign3 != '', campaign3, campaign4 != '', campaign4,'') as campaign
    mu
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

