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


select * from attribution_views;


select dimension_key,
       values[indexOf(value_keys,'conversions')] as "conversions",
       values[indexOf(value_keys,'revenue')] as "revenue"
FROM (
      select dimension_key, groupArray(value_key) as value_keys, groupArray(value_value) as values
      from (
            select dimension_name, dimension_key, value_key, sum(value_value) as value_value
            from attribution_views
            where tenant_id = 1
              and model_name = 'first_interaction'
              and dimension_name = 'campaign'
              and whole_journey = 1
              and direct_filter_out = 0
              and window = 14
              and execution_time > 0
              and execution_time < 9999999999
            group by dimension_name, dimension_key, value_key
               )
      group by dimension_key
);


---------------------

show create table attribution_views;
drop table attribution_views;
CREATE TABLE eventify_stag.attribution_views
(
    `tenant_id` UInt16,
    `config_pipeline_id` String,
    `config_data_id` String,
    `execution_time` DateTime,
    `created_at` DateTime,
    `window` UInt8,        -- 7/14/28
    `whole_journey` UInt8, -- true/false
    `include_direct` UInt8, -- true/false
    `model_name` String,   -- linear/time_decay...
    `dimension.keys` Array(String), -- ['channel_grouping','touch_type','source','campaign']
    `dimension.names` Array(String), -- ['cdp','organic','facebook','campaign 1']
    `value_key` String,
    `value_value` Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(execution_time)
ORDER BY (tenant_id,window, whole_journey, model_name,include_direct, execution_time);



insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','organic'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','organic'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','organic'],'revenue',120);

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','google','organic'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','google','organic'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','google','organic'],'revenue',120);

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','google','c1'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','google','c2'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','google','c3'],'revenue',120);

select * from attribution_views;



select tenant_id, window, channel_grouping, traffic, source, direct_filter,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'conversions')] as conversions,
       values[indexOf(keys,'visits')] as visits,
       conversions/visits as conversion_rate,
       revenue/conversions as revenue_per_conversion
from (
      select tenant_id,
             window,
             channel_grouping,
             traffic,
             direct_filter,
             source,
             groupArray(value_key) as keys,
             groupArray(value)     as values
      from (
            select tenant_id,
                   window,
                   channel_grouping,
                   traffic,
                   direct_filter,
                   source,
                   value_key,
                   sum(value_value) as value
            from (
                  with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'touch_type')] as touch_type,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         direct_filter,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = 1
                    and window = 7
                    and execution_time <= now()
                    and execution_time >= toStartOfDay(now())
                    and model_name = 'linear'
                    and whole_journey = 1
                    and direct_filter = 1
                    and channel_grouping = 'cdp'
                    and traffic = 'all_traffic'
                     )
            group by tenant_id, window, model_name, channel_grouping, traffic,direct_filter, source, value_key
               )
      group by tenant_id, window, channel_grouping, traffic, source, direct_filter
)
order by revenue
limit 2,1
;



select tenant_id, window, channel_grouping, traffic, source,campaign,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'conversions')] as conversions,
       values[indexOf(keys,'visits')] as visits,
       conversions/visits as conversion_rate,
       revenue/conversions as revenue_per_conversion
from (
      select tenant_id,
             window,
             channel_grouping,
             traffic,
             source,
             campaign,
             groupArray(value_key) as keys,
             groupArray(value)     as values
      from (
            select tenant_id,
                   window,
                   channel_grouping,
                   traffic,
                   source,
                   campaign,
                   value_key,
                   sum(value_value) as value
            from (
                  with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'traffic')] as traffic,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = 1
                    and window = 7
                    and execution_time <= now()
                    and execution_time >= toStartOfDay(now())
                    and model_name = 'linear'
                    and whole_journey = 1
                    and channel_grouping = 'cdp_managed'
                    and traffic = 'all_traffic'
                    and source = 'fb'
                     )
            group by tenant_id, window, model_name, channel_grouping, traffic, source,campaign, value_key
               )
      group by tenant_id, window, channel_grouping, traffic, source, campaign
         )
;






select tenant_id, window, channel_grouping, traffic, source, direct_filter,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'conversions')] as conversion,
       values[indexOf(keys,'visits')] as visit,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from (
      select tenant_id,
             window,
             channel_grouping,
             traffic,
             direct_filter,
             source,
             groupArray(value_key) as keys,
             groupArray(value)     as values
      from (
            select tenant_id,
                   window,
                   channel_grouping,
                   traffic,
                   direct_filter,
                   source,
                   value_key,
                   sum(value_value) as value
            from (
                  with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'traffic')] as traffic,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         direct_filter,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = 1
                    and window = 7
                    and execution_time <= now()
                    and execution_time >= toStartOfDay(now())
                    and model_name = 'linear'
                    and whole_journey = 1
                    and direct_filter = 1
                    and channel_grouping = 'cdp_managed'
                    and traffic = 'all_traffic'
                     )
            group by tenant_id, window, model_name, channel_grouping, traffic,direct_filter, source, value_key
               )
      group by tenant_id, window, channel_grouping, traffic, source, direct_filter
)
order by revenue
limit 1,1

;





select tenant_id, window, channel_grouping, traffic, source, direct_filter,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'conversions')] as conversion,
       values[indexOf(keys,'visits')] as visit,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from (
      select tenant_id,
             window,
             channel_grouping,
             traffic,
             direct_filter,
             source,
             groupArray(value_key) as keys,
             groupArray(value)     as values
      from (
            select tenant_id,
                   window,
                   channel_grouping,
                   traffic,
                   direct_filter,
                   source,
                   value_key,
                   sum(value_value) as value
            from (
                  with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'traffic')] as traffic,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         direct_filter,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = :tenant_id
                    and window = :window_time
                    and execution_time <= :end_time
                    and execution_time >= :start_time
                    and model_name = :attribution_model
                    and whole_journey = 1
                    and direct_filter = :direct_filter
                    and channel_grouping = :channel_grouping
                    and traffic = :traffic
                     )
            group by tenant_id, window, model_name, channel_grouping, traffic,direct_filter, source, value_key
               )
      group by tenant_id, window, channel_grouping, traffic, source, direct_filter
)
order by revenue
limit :offset, :limit

;











 with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'traffic')] as traffic,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         direct_filter,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = 1
                    and window = 7
                    and execution_time <= 9999999999
                    and execution_time >= 0
                    and model_name = 'linear'
                    and whole_journey = 1
                    and direct_filter = 1
                    and channel_grouping = 'cdp_managed'
                    and traffic = 'all_traffic'
;

select toStartOfDay(now());

select toUnixTimestamp(toDateTime('2020-12-15 09:13:35'));




select tenant_id, window, channel_grouping, traffic, campaign, direct_filter,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'conversions')] as conversion,
       values[indexOf(keys,'visits')] as visit,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from (
      select tenant_id,
             window,
             channel_grouping,
             traffic,
             direct_filter,
             campaign,
             groupArray(value_key) as keys,
             groupArray(value)     as values
      from (
            select tenant_id,
                   window,
                   channel_grouping,
                   traffic,
                   direct_filter,
                   source,
                   campaign,
                   value_key,
                   sum(value_value) as value
            from (
                  with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping,
                      dimension.names[indexOf(dimension.keys, 'traffic')] as traffic,
                      dimension.names[indexOf(dimension.keys, 'source')] as source,
                      dimension.names[indexOf(dimension.keys, 'campaign')] as campaign
                  select tenant_id,
                         window,
                         model_name,
                         channel_grouping,
                         traffic,
                         source,
                         direct_filter,
                         campaign,
                         value_key,
                         value_value
                  from attribution_views
                  where tenant_id = :tenant_id
                    and window = :window_time
                    and execution_time <= :end_time
                    and execution_time >= :start_time
                    and model_name = :attribution_model
                    and whole_journey = 1
                    and source = :channel
                    and direct_filter = :direct_filter
                    and channel_grouping = :channel_grouping
                    and traffic = :traffic
                     )
            group by tenant_id, window, model_name, channel_grouping, traffic,direct_filter, source, value_key, campaign
               )
      group by tenant_id, window, channel_grouping, traffic, source, direct_filter,campaign
);


select toUnixTimestamp('2021-01-01 00:00:00');
-------------------------------

truncate table attribution_views;





insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['non_cdp','facebook','organic'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['non_cdp','facebook','organic'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['non_cdp','facebook','organic'],'revenue',120);


insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['non_cdp','facebook','c3'],'conversion',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['non_cdp','facebook','c3'],'conversion',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['non_cdp','facebook','c3'],'conversion',120);



insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','facebook','c3'],'visit',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','facebook','c3'],'visit',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','campaign'], ['cdp','facebook','c3'],'visit',120);


insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','paid'],'visit',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','paid'],'visit',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['channel_grouping','channel','touch_type'], ['cdp','facebook','paid'],'visit',120);

-- channel grouping

with `dimension.names`[indexOf(dimension.keys, 'channel_grouping')] as channel_grouping
select *
from attribution_views
where dimension.keys[1] = 'channel_grouping' and dimension.keys[2] = 'channel' and dimension.keys[3] = 'campaign'
    and channel_grouping = 'cdp'


-- touch type

select * from attribution_views;

select channel,
       campaign,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'visit')] as visit,
       values[indexOf(keys,'conversion')] as conversion,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from(
    select channel, campaign, groupArray(value_key) as keys, groupArray(value_value) as values
    from
    (
        select dimension.names[2] as channel,
               dimension.names[3] as campaign,
               value_key,
               sum(value_value) as value_value
        from attribution_views
        where tenant_id = 1
            and window = 7
            and toUnixTimestamp(execution_time) <= 9999999999
            and toUnixTimestamp(execution_time) >= 0
            and model_name = 'linear'
            and whole_journey = 1
            and direct_filter = 1
            and dimension.keys[1] = 'channel_grouping' and dimension.keys[2] = 'channel' and dimension.keys[3] = 'campaign'
            and dimension.names[1] = 'cdp'
        group by channel, campaign, value_key
    ) group by channel, campaign
)
;





truncate table attribution_views;


insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c1'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c2'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c3'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c3'],'revenue',120);

insert into attribution_views values
 (1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['touch_type','organic','google','c3'],'revenue',120);


insert into attribution_views values
 (1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c3'],'conversion',12),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear_model',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c3'],'visit',200);


select * from attribution_views;







select campaign,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'visit')] as visit,
       values[indexOf(keys,'conversion')] as conversion,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from(
    select campaign, groupArray(value_key) as keys, groupArray(value_value) as values
    from (
        with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
            `dimension.names`[indexOf(`dimension.keys`,'traffic_type')] as traffic_type,
            `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
            `dimension.names`[indexOf(`dimension.keys`,'campaign')] as campaign
        select
            campaign,
            value_key,
            sum(value_value) as value_value
        from attribution_views
        where tenant_id = 1
            and window = 7
            and toUnixTimestamp(execution_time) >= 1577836800
            and toUnixTimestamp(execution_time) <= 1609459200
            and model_name ='linear_model'
            and whole_journey = 1
            and include_direct = 1
            and dimension_filter = 'campaign_grouping'
            and channel = 'facebook'
            and traffic_type = 'cdp'
        group by campaign, value_key
    ) group by campaign
)
;


select count(distinct channel)
from(
    with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
                `dimension.names`[indexOf(`dimension.keys`,'traffic_type')] as traffic_type,
                `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
                `dimension.names`[indexOf(`dimension.keys`,'campaign')] as campaign
            select
                channel,
                value_key,
                sum(value_value) as value_value
            from attribution_views
            where tenant_id = 1
                and window = 7
                and toUnixTimestamp(execution_time) >= 1577836800
                and toUnixTimestamp(execution_time) <= 1609459200
                and model_name ='linear'
                and whole_journey = 1
                and include_direct = 1
                and dimension_filter = 'campaign_grouping'
                and traffic_type = 'cdp'
            group by channel, value_key
    );




select * from attribution_views;


select count(distinct channel)
from
(
    with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
        `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
        `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel
    select
--         channel,
--         value_key,
--         sum(value_value) as value_value
    *
    from attribution_views
    where tenant_id = 1
        and window = 7
        and toUnixTimestamp(execution_time) >= 0
        and toUnixTimestamp(execution_time) <= 9999999999
        and model_name = 'linear'
        and whole_journey = 1
        and include_direct = 1
        and dimension_filter = 'campaign_grouping'
        and dimension_filter_value = 'cdp'
    group by channel, value_key
);





select count(distinct campaign)
from
(
    with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
        `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
        `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
        `dimension.names`[indexOf(`dimension.keys`,'campaign')] as campaign
    select
        campaign,
        value_key,
        sum(value_value) as value_value
    from attribution_views
    where tenant_id = :tenant_id
                and window = :window
                and toUnixTimestamp(execution_time) >= :start_time
                and toUnixTimestamp(execution_time) <= :end_time
                and model_name = :model_name
                and whole_journey = 1
                and include_direct = :include_direct
                and dimension_filter = :dimension_filter
                and channel = :channel
                and dimension_filter_value = 'cdp'
        group by campaign, value_key
)
;



select channel,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'visit')] as visit,
       values[indexOf(keys,'conversion')] as conversion,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from(
    select channel, groupArray(value_key) as keys, groupArray(value_value) as values
    from (
            with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
                `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
                `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
            select
                channel,
                value_key,
                sum(value_value) as value_value
            from attribution_views
            where tenant_id = %(tenant_id)s
                and window = %(window)s
                and toUnixTimestamp(execution_time) >= %(start_time)s
                and toUnixTimestamp(execution_time) <= %(end_time)s
                and model_name = %(model_name)s
                and whole_journey = 1
                and include_direct = %(include_direct)s
                and dimension_filter = %(dimension_filter)s
                and dimension_filter_value = 'cdp'
            group by channel, value_key
     )       group by channel
) order by %(order_by)s
 limit %(offset)s, %(limit)s;










select channel,
       values[indexOf(keys,'revenue')] as revenue,
       values[indexOf(keys,'visit')] as visit,
       values[indexOf(keys,'conversion')] as conversion,
       conversion/visit as conversion_rate,
       revenue/conversion as revenue_per_conversion
from(
    select channel, groupArray(value_key) as keys, groupArray(value_value) as values
    from (
            with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
                `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
                `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
            select
                channel,
                value_key,
                sum(value_value) as value_value
            from attribution_views
            where tenant_id = :tenant_id
                and window = :window
                and toUnixTimestamp(execution_time) >= :start_time
                and toUnixTimestamp(execution_time) <= :end_time
                and model_name = :model_name
                and whole_journey = 1
                and include_direct = :include_direct
                and dimension_filter = :dimension_filter
                {addtional_condition}
            group by channel, value_key
     )       group by channel
)
;






select count() from events_attribution_test;
select * from events_attribution_test;

show create table events_attribution_test;


select tenant_id,
       anonymous_id                                                                    as "universal_id",
       id                                                                              as "touch_id",
       tag.vals[indexOf(tag.keys, 'source')]                                           as "touch_type",
       toUnixTimestamp(at)                                                             as "touch_timestamp",
       if(`tag.vals`[indexOf(`tag.keys`, 'event_tag')] == 'conversion', 1, 0)          as "is_conversion",
       event_name                                                                      as "touch_name",
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] as "conversion_value",
       tag.vals[indexOf(`tag.keys`, 'event_tag')]                                      as "event_tag",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_source')]        as "channel",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_name')]          as "campaign",
       `tag.vals`[indexOf(`tag.keys`, 'campaign_grouping')]                            as "campaign_grouping"
from events_attribution_test
where tenant_id = 1
  and anonymous_id in (
    select anonymous_id as "univeral_id"
    from events_attribution_test
    where tenant_id = 1
      and at > '2020-01-01 00:00:00'
      and at < '2021-01-01 00:00:00'
      and tag.vals[indexOf(tag.keys, 'event_tag')] = 'conversion'

)
  and at > toDateTime('2020-12-10 08:59:48')
  and at < toDateTime('2020-12-17 08:59:48')
  and hasAny([campaign_grouping], ['cdp']) == 1
  and hasAny([touch_type],[]) == 0
  and hasAny([touch_type],  ['direct', 'paid', 'in-app', 'organic', 'conversion']) == 1
  and event_tag != 'system_event'
;

select * from events_attribution_test_c;
select count() from events_attribution_test;
select count() from events;
select * from events_attribution_test;

show create table events_attribution_test;
CREATE TABLE eventify_stag.events_attribution_test_c
(
    `id` String,
    `tenant_id` UInt16,
    `anonymous_id` String,
    `session_id` String,
    `scope` String,
    `identity.keys` Array(LowCardinality(String)),
    `identity.vals` Array(String),
    `str_properties.keys` Array(LowCardinality(String)),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(LowCardinality(String)),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(LowCardinality(String)),
    `arr_properties.vals` Array(Array(String)),
    `tag.keys` Array(LowCardinality(String)),
    `tag.vals` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, at)
;


show create table events;
show tables;

select * from attribution_views;

select toUnixTimestamp('2020-12-20 00:00:00');




    with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
        `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
        `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel,
        `dimension.names`[indexOf(`dimension.keys`,'campaign')] as dimension
    select
        dimension,
        value_key,
        sum(value_value) as value_value
    from attribution_views
    where tenant_id =1
            and window = 7
            and toUnixTimestamp(execution_time) >= 1577836800
            and toUnixTimestamp(execution_time) <= 1609459200
            and model_name = 'linear'
            and whole_journey = 1
            and include_direct = 1
            and dimension_filter = 'campaign_grouping'
            and channel ='facebook'
            and dimension_filter_value = 'cdp'
    group by dimension, value_key;


select * from attribution_views;



select
       maxIf(value_value, value_key = 'revenue') as max_revenue,
       maxIf(value_value, value_key = 'conversions') as max_conversions,
       maxIf(value_value, value_key = 'visits') as max_visits
from
(
    with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
        `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
        `dimension.names`[indexOf(`dimension.keys`,'channel')] as dimension
    select
        dimension,
        value_key,
        sum(value_value) as value_value
    from attribution_views
    where tenant_id = 1
        and window = 7
        and toUnixTimestamp(execution_time) >= 0
        and toUnixTimestamp(execution_time) <= 9999999999
        and model_name = 'linear'
        and whole_journey = 1
        and include_direct = 1
        and dimension_filter = 'campaign_grouping'
        and dimension_filter_value = 'cdp'
    group by dimension, value_key
);



select * from events order by at desc ;