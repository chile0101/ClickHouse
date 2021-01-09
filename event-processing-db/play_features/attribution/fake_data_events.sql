select *
from events_attribution_test;
select distinct event_name
from events_attribution_test;
['view','sessionCreated','sessionUpdated','viewFromReferrer','identify','goal','product_viewed','product_wishlisted','category_viewed','checkout_completed','order_checked','cart_viewed','reach_goal','search','search_by_cscid','search_by_jobs','search_by_distance','checkout_started','product_cart_added','sign_up','checkout_baby','searched','newsletter_signed_up']


select tenant_id,
       anonymous_id                                                                                  as "universal_id",
       id                                                                                            as "touch_id",
       tag.vals[indexOf(tag.keys, 'source')]                                                         as "touch_type",
       toUnixTimestamp(at)                                                                           as "touch_timestamp",
       if(`num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] > 0, 1, 0) as "is_conversion",
       event_name                                                                                    as "touch_name",
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')]               as "conversion_value",
       tag.vals[indexOf(tag.keys, 'event_tag')]                                                      as "event_tag",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_source')]                      as "source",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_name')]                        as "campaign",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_tactic')]                      as "tactic"
from events_attribution_test
where tenant_id = :tenant_id
  and anonymous_id in (
    select anonymous_id as "univeral_id"
    from events_attribution_test
    where tenant_id = :tenant_id
      and tag.vals[indexOf(tag.keys, 'event_tag')] = 'conversion'
      and at > :timerange_start
      and at < :timerange_end
)
  and event_tag != 'system_event'
  and at > :window_start_time
  and at < :window_end_time
  and hasAny([campaign], :campaign_filter) == 1
  and hasAny([touch_type], :direct_filter_out) == 0
  and hasAny([touch_type], :touch_type_filter) == 1
;


truncate table events_attribution_test;

insert into events_attribution_test
with ['view','sessionCreated','sessionUpdated','viewFromReferrer','identify','goal','product_viewed','product_wishlisted','category_viewed','checkout_completed','order_checked','cart_viewed','reach_goal','search','search_by_cscid','search_by_jobs','search_by_distance','checkout_started','product_cart_added','sign_up','checkout_baby','searched','newsletter_signed_up'] as event_names,
    ['source','event_tag','campaign_grouping'] as tag_keys,
    [['direct', 'visit', 'cdp'],
        ['direct','visit','non_cdp'],
        ['paid','click','cdp'],
        ['paid','click','non_cdp'],
        ['paid','impression','cdp'],
        ['paid','impression','non_cdp'],
        ['in-app','intension','cdp'],
        ['in-app','intension','non_cdp'],
        ['conversion','conversion','cdp'],
        ['conversion','conversion','non_cdp'],
        ['organic','impression','cdp'],
        ['organic','impression','non_cdp'],
        ['organic','visit','cdp'],
        ['organic','visit','non_cdp']
        ] as tag_vals,
    ['facebook','google','mailchimp','youtube','onesignal','instagram','prime-data','gmail','google-driver','tiki','tiktok','lazada','shoppee','sendo','chotot','bkhcm','khtn'] as sources,
    ['','Summer Sale','Winter Sale','10-10','11-11','12-12','Black Friday','Happy New Year','Lunar New Year','1-1','2-2','3-3','4-4','5-5','6-6','7-7','8-8','9-9'] as campaigns
select randomPrintableASCII(10)                                                                 as id,
       1                                                                                        as tenant_id,
       concat('a', toString(rand(1) % 300))                                                     as anonymous_id,
       event_names[rand(2) % length(event_names) + 1]                                           as event_name,
       randomPrintableASCII(3)                                                                  as session_id,
       randomPrintableASCII(4)                                                                  as scope,
       []                                                                                       as `identify.keys`,
       []                                                                                       as `identify.vals`,
       ['campaign_source', 'campaign_name', 'campaign_tactic']                                  as `str_properties.keys`,
       [sources[rand(5) % length(sources) + 1], campaigns[rand(6) % length(campaigns) + 1], ''] as `str_properties.vals`,
       ['properties.total_value']                                                               as `num_properties.keys`,
       [rand(7) % 100000]                                                                       as `num_properties.vals`,
       ['']                                                                                     as `arr_properties.keys`,
       [[]]                                                                                     as `arr_properties.vals`,
       tag_keys                                                                                 as `tag.keys`,
       tag_vals[rand(8) % length(tag_vals) + 1]                                                 as `tag.vals`,
       toDateTime('2020-12-01 00:00:00') + rand(9) % (2 * 30 * 24 * 3600)                       as at
from numbers(100000)
;


select *
from events_attribution_test;


select *
from attribution_views
where `dimension.names`[indexOf(`dimension.keys`, 'dimension_filter_value')] = 'all_traffic';

truncate table attribution_views;

show create table attribution_views;

drop table attribution_views;
CREATE TABLE eventify_stag.attribution_views
(
    `tenant_id`          UInt16,
    `config_pipeline_id` String,
    `config_data_id`     String,
    `execution_time`     DateTime,
    `created_at`         DateTime,
    `window`             Nullable(UInt8),
    `whole_journey`      Nullable(UInt8),
    `include_direct`     Nullable(UInt8),
    `model_name`         Nullable(String),
    `dimension.keys`     Array(String),
    `dimension.names`    Array(String),
    `value_key`          String,
    `value_value`        Float64
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(execution_time)
        ORDER BY (config_data_id, tenant_id, execution_time)
;


truncate table attribution_views;

select count(*) from attribution_views;









